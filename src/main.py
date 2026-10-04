"""نقطة التشغيل الرئيسية للمشروع كله."""

import argparse
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings  # noqa: E402
from file_router import choose_engine  # noqa: E402
from metrics import save_results  # noqa: E402


def run_pipeline(input_path: Path | None, batch_size: int | None = None,
                 threshold_mb: float | None = None, raw_only: bool = False,
                 classify_run_id: str | None = None,
                 progress_every_batches: int = 20,
                 start_row: int | None = None,
                 end_row: int | None = None,
                 seen_order_ids: set | None = None) -> dict:
    """تشغيل Pipeline كامل أو تصنيف تشغيل Raw موجود مسبقًا."""
    from elt_pipeline import run_full_classification

    started_at = time.time()
    if classify_run_id:
        print(f"== تصنيف تشغيل Raw موجود: {classify_run_id} ==")
        base_filter = {"$or": [{"run_id": classify_run_id}, {"id_run": classify_run_id}]}
        filter_query = base_filter
        if start_row is not None or end_row is not None:
            row_filter = {}
            if start_row is not None:
                row_filter["$gte"] = start_row
            if end_row is not None:
                row_filter["$lt"] = end_row
            filter_query = {"$and": [
                base_filter,
                {"$or": [
                    {"source_row_number": row_filter},
                    {"number_row_source": row_filter},
                ]},
            ]}
            print(f"نطاق الصفوف: {start_row or '-∞'} إلى {end_row or '∞'}")
        classification = run_full_classification(
            batch_size=batch_size or settings.BATCH_SIZE,
            progress_every_batches=progress_every_batches,
            filter_query=filter_query,
            seen_order_ids=seen_order_ids,
        )
        result = {
            "mode": "classify_existing_raw",
            "run_id": classify_run_id,
            "classification": classification,
        }
    else:
        if input_path is None:
            raise ValueError("يجب تحديد --input أو --classify-run-id")
        decision = choose_engine(input_path, threshold_mb)
        print(f"حجم الملف: {decision['file_size_mb']} MB")
        print(f"المحرك المختار: {decision['engine_used']}")
        print(f"سبب الاختيار: {decision['reason']}")

        if decision["engine_used"] == "python_batch":
            from batch_loader import load_batch
            raw_summary = load_batch(
                input_path, decision["run_id"], batch_size=batch_size
            )
        else:
            from spark_loader import load_with_spark
            raw_summary = load_with_spark(input_path, decision["run_id"])

        result = {
            "mode": "raw_only" if raw_only else "full_pipeline",
            "run_id": decision["run_id"],
            "file_name": decision["file_name"],
            "file_size_mb": decision["file_size_mb"],
            "threshold_mb": decision["threshold_mb"],
            "engine_used": decision["engine_used"],
            "router_reason": decision["reason"],
            "raw_load": raw_summary,
        }
        if not raw_only:
            print("\n== بدء Transform & Quality & Final Load ==")
            result["classification"] = run_full_classification(
                batch_size=batch_size or settings.BATCH_SIZE,
                progress_every_batches=progress_every_batches,
                filter_query={"run_id": decision["run_id"]},
                seen_order_ids=seen_order_ids,
            )

    result["total_elapsed_seconds"] = round(time.time() - started_at, 3)
    save_results(result, settings.RESULTS_JSON_PATH)
    print(f"\nتم حفظ النتائج في: {settings.RESULTS_JSON_PATH}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Midterm Hybrid Data Pipeline")
    parser.add_argument("--input", type=Path, help="مسار CSV المصدر")
    parser.add_argument("--classify-run-id", help="تصنيف Raw موجود مسبقًا حسب run_id")
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--threshold-mb", type=float, default=None)
    parser.add_argument("--raw-only", action="store_true", help="تحميل Raw فقط دون التصنيف")
    parser.add_argument("--progress-every-batches", type=int, default=20)
    parser.add_argument("--start-row", type=int, default=None, help="بداية رقم الصف شاملة")
    parser.add_argument("--end-row", type=int, default=None, help="نهاية رقم الصف غير شاملة")
    args = parser.parse_args()
    settings.ensure_directories()
    run_pipeline(
        args.input, args.batch_size, args.threshold_mb, args.raw_only,
        args.classify_run_id, args.progress_every_batches,
        args.start_row, args.end_row,
    )


if __name__ == "__main__":
    main()
