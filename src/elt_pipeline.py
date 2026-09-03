"""مرحلة Transform, Quality, Classification وFinal Load بعد orders_raw."""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pymongo import InsertOne, MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from config import settings  # noqa: E402
from quality_rules import classify_and_clean  # noqa: E402


def create_indexes(db) -> None:
    raw = db[settings.COLLECTION_RAW]
    validated = db[settings.COLLECTION_VALIDATED]
    quarantine = db[settings.COLLECTION_QUARANTINE]

    validated.create_index("order_id", unique=True, name="uniq_order_id")
    quarantine.create_index(
        [("run_id", 1), ("source_row_number", 1)],
        unique=True,
        name="uniq_quarantine_source",
        sparse=True,
    )
    # فهارس التوافق للسجلات التي حُمّلت بالنسخة القديمة من المشروع.
    quarantine.create_index(
        [("id_run", 1), ("number_row_source", 1)],
        name="idx_quarantine_legacy_source",
        sparse=True,
    )
    raw.create_index(
        [("run_id", 1), ("source_row_number", 1)],
        name="idx_raw_run_row",
        sparse=True,
    )
    raw.create_index(
        [("id_run", 1), ("number_row_source", 1)],
        name="idx_raw_legacy_run_row",
        sparse=True,
    )
    print(f"[OK] فهارس Raw وUnique Index على order_id جاهزة.")


def _raw_fields(doc: dict) -> tuple[dict, str | None, int | None, bool]:
    """دعم الحقول الرسمية الجديدة والحقول القديمة الموجودة في قاعدة البيانات."""
    legacy = "run_id" not in doc and "raw_record" not in doc
    raw_record = doc.get("raw_record", doc.get("record_raw", {}))
    run_id = doc.get("run_id", doc.get("id_run"))
    source_row = doc.get("source_row_number", doc.get("number_row_source"))
    return raw_record, run_id, source_row, legacy


def _build_validated_doc(cleaned_fields, quality_status, corrections, run_id, order_id):
    return {
        **dict(cleaned_fields),
        "order_id": order_id,
        "quality_status": quality_status,
        "corrections": corrections,
        "source_run_id": run_id,
    }


def _build_quarantine_doc(
    record_raw, quarantine_codes, quarantine_reasons, run_id, order_id, source_row_number
):
    return {
        "order_id": order_id,
        "error_codes": quarantine_codes,
        "error_details": quarantine_reasons,
        "raw_record": record_raw,
        "run_id": run_id,
        "source_row_number": source_row_number,
        "quarantined_at": datetime.now(timezone.utc).isoformat(),
    }


def run_full_classification(batch_size=5000, progress_every_batches=20, filter_query=None):
    """تصنيف Raw كاملًا أو حسب filter_query مع كتابة نهائية قابلة لإعادة التشغيل."""
    client = MongoClient(settings.MONGO_URI, serverSelectionTimeoutMS=settings.MONGO_TIMEOUT_MS)
    db = client[settings.MONGO_DB_NAME]
    raw_collection = db[settings.COLLECTION_RAW]
    validated_collection = db[settings.COLLECTION_VALIDATED]
    quarantine_collection = db[settings.COLLECTION_QUARANTINE]
    filter_query = filter_query or {}
    started_at = time.time()

    try:
        create_indexes(db)
        total_raw = raw_collection.count_documents(filter_query)
        print(f"إجمالي السجلات المستهدفة للتصنيف: {total_raw:,}")
        seen_order_ids = set()
        counters = {"valid": 0, "corrected": 0, "quarantined": 0}
        quarantine_code_counts = {}
        correction_rule_counts = {}
        validated_ops = []
        quarantine_ops = []
        read_count = 0
        inserted_count = updated_count = unchanged_count = 0
        batch_errors = []

        def flush_batches():
            nonlocal validated_ops, quarantine_ops
            nonlocal inserted_count, updated_count, unchanged_count
            if validated_ops:
                try:
                    result = validated_collection.bulk_write(validated_ops, ordered=False)
                    inserted_count += result.upserted_count
                    updated_count += result.modified_count
                    unchanged_count += result.matched_count - result.modified_count
                except BulkWriteError as exc:
                    batch_errors.append({
                        "collection": settings.COLLECTION_VALIDATED,
                        "error_type": "BulkWriteError",
                        "error": str(exc),
                    })
                    print(f"[خطأ] فشل Bulk Upsert: {exc.details.get('writeErrors', [])[:1]}")
                    raise
                finally:
                    validated_ops = []
            if quarantine_ops:
                try:
                    quarantine_collection.bulk_write(quarantine_ops, ordered=False)
                except BulkWriteError as exc:
                    batch_errors.append({
                        "collection": settings.COLLECTION_QUARANTINE,
                        "error_type": "BulkWriteError",
                        "error": str(exc),
                    })
                    print(f"[خطأ] فشل Bulk Quarantine: {exc.details.get('writeErrors', [])[:1]}")
                    raise
                finally:
                    quarantine_ops = []

        cursor = raw_collection.find(
            filter_query,
            {"raw_record": 1, "record_raw": 1, "run_id": 1, "id_run": 1,
             "source_row_number": 1, "number_row_source": 1},
            batch_size=batch_size,
            no_cursor_timeout=True,
        )
        try:
            for doc in cursor:
                read_count += 1
                raw_record, run_id, source_row, legacy = _raw_fields(doc)
                result = classify_and_clean(raw_record, seen_order_ids)
                status = result["quality_status"]
                counters[status] += 1

                if status == "quarantined":
                    for code in result["quarantine_codes"]:
                        quarantine_code_counts[code] = quarantine_code_counts.get(code, 0) + 1
                    quarantine_doc = _build_quarantine_doc(
                        raw_record, result["quarantine_codes"], result["quarantine_reasons"],
                        run_id, result["order_id"], source_row,
                    )
                    quarantine_key = (
                        {"id_run": run_id, "number_row_source": source_row}
                        if legacy else {"run_id": run_id, "source_row_number": source_row}
                    )
                    quarantine_ops.append(UpdateOne(
                        quarantine_key,
                        {"$set": quarantine_doc},
                        upsert=True,
                    ))
                else:
                    for correction in result["corrections"]:
                        code = correction["rule_code"]
                        correction_rule_counts[code] = correction_rule_counts.get(code, 0) + 1
                    validated_doc = _build_validated_doc(
                        result["cleaned_fields"], status, result["corrections"],
                        run_id, result["order_id"],
                    )
                    validated_ops.append(UpdateOne(
                        {"order_id": result["order_id"]},
                        {
                            "$set": validated_doc,
                            "$setOnInsert": {
                                "first_validated_at": datetime.now(timezone.utc).isoformat()
                            },
                        },
                        upsert=True,
                    ))

                if len(validated_ops) >= batch_size or len(quarantine_ops) >= batch_size:
                    flush_batches()

                if read_count % (batch_size * progress_every_batches) == 0:
                    elapsed = time.time() - started_at
                    rate = read_count / elapsed if elapsed else 0.0
                    remaining = (total_raw - read_count) / rate if rate else 0.0
                    print(
                        f"تقدم: {read_count:,}/{total_raw:,} "
                        f"({read_count / total_raw * 100:.1f}% إذا كان الإجمالي) | "
                        f"معدل: {rate:,.0f} سجل/ثانية | "
                        f"المتبقي التقريبي: {remaining / 60:.1f} دقيقة"
                    )
        finally:
            cursor.close()
        flush_batches()

        elapsed = time.time() - started_at
        consistency = {
            "raw_count": read_count,
            "classified_count": counters["valid"] + counters["corrected"] + counters["quarantined"],
            "equation_holds": read_count == sum(counters.values()),
        }
        def find_run_id(query):
            if not isinstance(query, dict):
                return None
            direct = query.get("run_id") or query.get("id_run")
            if direct:
                return direct
            for operator in ("$or", "$and"):
                for clause in query.get(operator, []):
                    found = find_run_id(clause)
                    if found:
                        return found
            return None

        run_id_filter = find_run_id(filter_query)
        return {
            "run_id_filter": run_id_filter,
            "rows_read": read_count,
            "raw_loaded": read_count,
            "valid_count": counters["valid"],
            "corrected_count": counters["corrected"],
            "quarantine_count": counters["quarantined"],
            "elapsed_seconds": round(elapsed, 3),
            "throughput": round(read_count / elapsed, 2) if elapsed else 0.0,
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "unchanged_count": unchanged_count,
            "error_case_counts": quarantine_code_counts,
            "correction_rule_counts": correction_rule_counts,
            "consistency": consistency,
            "batch_errors": batch_errors,
        }
    finally:
        client.close()
