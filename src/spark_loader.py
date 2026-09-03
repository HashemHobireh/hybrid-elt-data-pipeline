"""تحميل CSV كبير إلى orders_raw باستخدام PySpark وMongoDB Connector."""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from config import settings  # noqa: E402


def get_spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(settings.SPARK_APP_NAME)
        .master(settings.SPARK_MASTER_URL)
        .config("spark.jars.packages", settings.SPARK_MONGO_CONNECTOR_PACKAGE)
        .config("spark.mongodb.write.connection.uri", settings.MONGO_URI)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_header_columns(file_path: Path) -> list[str]:
    """قراءة أسماء الأعمدة فقط لبناء Schema ثابتة."""
    import csv

    with Path(file_path).open("r", encoding="utf-8-sig", errors="replace", newline="") as file:
        return next(csv.reader(file), [])


def build_fixed_schema(columns: list[str]):
    from pyspark.sql.types import StructField, StructType, StringType

    return StructType([StructField(column.strip(), StringType(), True) for column in columns])


def load_with_spark(
    input_path: Path,
    run_id: str,
    mongo_uri: str | None = None,
    db_name: str | None = None,
    collection_name: str | None = None,
) -> dict:
    """قراءة الملف الكبير بـDataFrame API وكتابته بالتوازي إلى MongoDB."""
    from pyspark.sql import functions as F

    mongo_uri = mongo_uri or settings.MONGO_URI
    db_name = db_name or settings.MONGO_DB_NAME
    collection_name = collection_name or settings.COLLECTION_RAW
    input_path = Path(input_path)
    columns = read_header_columns(input_path)
    schema = build_fixed_schema(columns)
    print(f"عدد الأعمدة: {len(columns)} | Schema ثابتة: StringType لكل الحقول الخام")

    spark = get_spark_session()
    started_at = time.time()
    try:
        df = (
            spark.read.option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .schema(schema)
            .csv(str(input_path))
        )
        input_partitions = df.rdd.getNumPartitions()
        print(f"Input Partitions: {input_partitions} (بدون repartition غير مبرر)")

        df_raw = df.select(
            F.lit(run_id).alias("run_id"),
            F.lit(str(input_path)).alias("source_file"),
            (F.monotonically_increasing_id() + F.lit(1)).alias("source_row_number"),
            F.lit(datetime.now(timezone.utc).isoformat()).alias("ingested_at"),
            F.lit("pyspark").alias("engine_used"),
            F.struct(*[F.col(c) for c in columns]).alias("raw_record"),
        )
        rows_read = df_raw.count()
        print(f"rows_read: {rows_read:,}")
        print("بدء الكتابة المتوازية إلى MongoDB عبر MongoDB Spark Connector...")
        (
            df_raw.write.format("mongodb")
            .mode("append")
            .option("spark.mongodb.write.connection.uri", mongo_uri)
            .option("spark.mongodb.write.database", db_name)
            .option("spark.mongodb.write.collection", collection_name)
            .save()
        )
        elapsed = time.time() - started_at
        return {
            "run_id": run_id,
            "input_path": str(input_path),
            "collection": collection_name,
            "input_partitions": input_partitions,
            "rows_read": rows_read,
            "raw_loaded": rows_read,
            "elapsed_seconds": round(elapsed, 3),
            "throughput": round(rows_read / elapsed, 2) if elapsed else 0.0,
        }
    finally:
        spark.stop()
        print("تم إغلاق SparkSession بأمان.")


def main() -> None:
    parser = argparse.ArgumentParser(description="PySpark Loader إلى orders_raw")
    parser.add_argument("--input", required=True)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()
    from file_router import generate_run_id

    summary = load_with_spark(Path(args.input), args.run_id or generate_run_id())
    print("\n== ملخص التحميل الخام عبر PySpark ==")
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
