"""تحميل CSV صغير إلى orders_raw باستخدام Streaming وinsert_many."""

import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import (
    AutoReconnect,
    BulkWriteError,
    NetworkTimeout,
    PyMongoError,
    ServerSelectionTimeoutError,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from config import settings  # noqa: E402


def build_raw_document(
    row: dict, run_id: str, source_file: str, source_row_number: int, engine_used: str
) -> dict:
    """إنشاء سجل Raw دون تنظيف أو تحويل للقيم الأصلية."""
    return {
        "run_id": run_id,
        "source_file": source_file,
        "source_row_number": source_row_number,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "engine_used": engine_used,
        "raw_record": dict(row),
    }


def _insert_batch(collection, buffer, batch_number, batch_errors, start_time, max_retries=3):
    """إدخال دفعة مع إعادة محاولة أخطاء الاتصال ثم تسجيل السبب بوضوح."""
    batch_start = time.time()
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            result = collection.insert_many(buffer, ordered=False)
            inserted_count = len(result.inserted_ids)
            batch_elapsed = time.time() - batch_start
            rate = inserted_count / batch_elapsed if batch_elapsed else 0.0
            total_elapsed = time.time() - start_time
            print(
                f"دفعة {batch_number:>4} | سجلات: {inserted_count:>6}/{len(buffer):<6} | "
                f"زمن الدفعة: {batch_elapsed:.3f}s | معدل: {rate:,.2f} سجل/ثانية | "
                f"الزمن الكلي: {total_elapsed:.2f}s"
            )
            return inserted_count
        except BulkWriteError as exc:
            inserted_count = exc.details.get("nInserted", 0)
            errors = exc.details.get("writeErrors", [])
            batch_errors.append({
                "batch_number": batch_number,
                "attempted": len(buffer),
                "inserted": inserted_count,
                "failed": len(errors),
                "error_type": "BulkWriteError",
                "error": errors[0].get("errmsg", str(exc)) if errors else str(exc),
            })
            print(f"[خطأ دفعة] {batch_number}: {batch_errors[-1]['error']}")
            return inserted_count
        except (AutoReconnect, NetworkTimeout, ServerSelectionTimeoutError) as exc:
            last_error = exc
            batch_errors.append({
                "batch_number": batch_number,
                "attempt": attempt,
                "attempted": len(buffer),
                "error_type": type(exc).__name__,
                "error": str(exc),
            })
            if attempt < max_retries:
                delay = 2 ** (attempt - 1)
                print(f"[إعادة محاولة] الدفعة {batch_number} بعد {delay} ثانية: {exc}")
                time.sleep(delay)
            else:
                print(f"[فشل نهائي] الدفعة {batch_number}: {exc}")
        except PyMongoError as exc:
            batch_errors.append({
                "batch_number": batch_number,
                "attempted": len(buffer),
                "failed": len(buffer),
                "error_type": type(exc).__name__,
                "error": str(exc),
            })
            print(f"[فشل دفعة] {batch_number}: {exc}")
            return 0

    raise RuntimeError(f"تعذر إدخال الدفعة {batch_number} بعد {max_retries} محاولات") from last_error


def load_batch(
    input_path: Path,
    run_id: str,
    batch_size: int | None = None,
    mongo_uri: str | None = None,
    db_name: str | None = None,
    collection_name: str | None = None,
) -> dict:
    """قراءة CSV سطرًا بسطر وكتابة Raw على دفعات قابلة للضبط."""
    batch_size = batch_size or settings.BATCH_SIZE
    mongo_uri = mongo_uri or settings.MONGO_URI
    db_name = db_name or settings.MONGO_DB_NAME
    collection_name = collection_name or settings.COLLECTION_RAW
    input_path = Path(input_path)

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=settings.MONGO_TIMEOUT_MS)
    collection = client[db_name][collection_name]
    total_read = total_inserted = batch_number = 0
    batch_errors = []
    start_time = time.time()

    try:
        with input_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as file:
            reader = csv.DictReader(file)
            buffer = []
            for row_number, row in enumerate(reader, start=1):
                total_read += 1
                buffer.append(build_raw_document(
                    row, run_id, str(input_path), row_number, "python_batch"
                ))
                if len(buffer) >= batch_size:
                    batch_number += 1
                    total_inserted += _insert_batch(
                        collection, buffer, batch_number, batch_errors, start_time
                    )
                    buffer = []
            if buffer:
                batch_number += 1
                total_inserted += _insert_batch(
                    collection, buffer, batch_number, batch_errors, start_time
                )
    finally:
        client.close()

    elapsed = time.time() - start_time
    return {
        "run_id": run_id,
        "input_path": str(input_path),
        "collection": collection_name,
        "batch_size": batch_size,
        "batches_count": batch_number,
        "rows_read": total_read,
        "raw_loaded": total_inserted,
        "failed_rows": total_read - total_inserted,
        "elapsed_seconds": round(elapsed, 3),
        "throughput": round(total_inserted / elapsed, 2) if elapsed else 0.0,
        "batch_errors": batch_errors,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Python Batch Streaming إلى orders_raw")
    parser.add_argument("--input", required=True)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--id-run", dest="run_id_legacy", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--batch-size", type=int, default=None)
    args = parser.parse_args()

    from file_router import generate_run_id
    run_id = args.run_id or args.run_id_legacy or generate_run_id()
    summary = load_batch(Path(args.input), run_id, args.batch_size)
    print("\n== ملخص التحميل الخام ==")
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
