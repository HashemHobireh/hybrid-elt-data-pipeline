"""
src/scheduler.py
----------------
متطلبات المشروع النهائي - القسم 4: المهام المجدولة (Scheduled Jobs).
- مهمتان مجدولتان على الأقل تؤديان وظائف فعلية (تحديث العروض المادية + إنشاء تقرير دوري).
- تعمل وفق جدول زمني محدد وتدعم التشغيل اليدوي المباشر أثناء الاختبار والمناقشة.
- تسجيل دقيق لنتيجة التنفيذ ووقت البداية والنهاية وحالة النجاح والفشل في قاعدة البيانات وملفات التقارير.
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apscheduler.schedulers.background import BackgroundScheduler  # noqa: E402
from apscheduler.triggers.interval import IntervalTrigger  # noqa: E402
from config import settings  # noqa: E402
from materialized_views import refresh_all_materialized_views  # noqa: E402
from mongo_setup import get_client  # noqa: E402

logger = logging.getLogger("scheduler")

# الذاكرة المحلية لسجلات تشغيل المهام
IN_MEMORY_JOB_LOGS = []

_SCHEDULER_INSTANCE = None


# ---------------------------------------------------------------------------
# وظائف المهام الفعلية
# ---------------------------------------------------------------------------

def task_refresh_materialized_views(db) -> dict:
    """المهمة الأولى: التحديث الدوري التزايدي لكافة العروض المادية."""
    return refresh_all_materialized_views(db, incremental=True)


def task_generate_periodic_report(db) -> dict:
    """
    المهمة الثانية: تقرير دوري شامل يفحص اتساق البيانات،
    عدد السجلات في كل مرحلة، ونسب النجاح ونشاط العزل.
    """
    raw_col = db[settings.COLLECTION_RAW]
    valid_col = db[settings.COLLECTION_VALIDATED]
    quar_col = db[settings.COLLECTION_QUARANTINE]

    raw_count = raw_col.count_documents({})
    valid_count = valid_col.count_documents({})
    quar_count = quar_col.count_documents({})

    # عينة من أحدث أخطاء العزل
    top_errors_pipeline = [
        {"$unwind": "$error_codes"},
        {"$group": {"_id": "$error_codes", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5},
    ]
    top_errors = list(quar_col.aggregate(top_errors_pipeline))

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database": db.name,
        "collections_summary": {
            "orders_raw": raw_count,
            "orders_validated": valid_count,
            "orders_quarantine": quar_count,
        },
        "top_quarantine_reasons": top_errors,
        "health_status": "HEALTHY" if raw_count >= 0 else "WARNING",
    }

    # حفظ في reports/periodic_report.json
    out_path = settings.REPORTS_DIR / "periodic_report.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    return report


# ---------------------------------------------------------------------------
# تعريف المهام المجدولة
# ---------------------------------------------------------------------------

JOBS_METADATA = {
    "refresh_materialized_views": {
        "title": "تحديث العروض المادية",
        "description": "تشغيل التحديث التزايدي للعروض المادية (Materialized Views) لحفظ المجاميع الجاهزة.",
        "interval_minutes": 15,
        "action": task_refresh_materialized_views,
    },
    "generate_periodic_report": {
        "title": "إنشاء تقرير خط البيانات الدوري",
        "description": "فحص سلامة المجموعات، نسب البيانات المعزولة، وتحديث تقرير النظام الدوري.",
        "interval_minutes": 60,
        "action": task_generate_periodic_report,
    },
}


def log_job_execution(db, log_entry: dict) -> None:
    """تسجيل نتيجة تنفيذ المهمة في الذاكرة وفي مجموعة MongoDB job_logs."""
    IN_MEMORY_JOB_LOGS.insert(0, log_entry)
    if len(IN_MEMORY_JOB_LOGS) > 100:
        IN_MEMORY_JOB_LOGS.pop()

    if db is not None:
        try:
            col = db[settings.COLLECTION_JOB_LOGS]
            col.insert_one(dict(log_entry))
        except Exception as exc:
            logger.warning(f"تعذر كتابة سجل المهمة إلى MongoDB: {exc}")


def run_job_now(job_name: str, trigger_mode: str = "manual", db=None) -> dict:
    """
    تنفيذ مهمة محددة فوراً (يدوياً أو عبر المجدول)
    مع تسجيل أوقات البداية والنهاية والنتيجة بدقة.
    """
    if job_name not in JOBS_METADATA:
        raise ValueError(f"المهمة '{job_name}' غير موجودة. المتاح: {list(JOBS_METADATA.keys())}")

    job_info = JOBS_METADATA[job_name]
    started_at = datetime.now(timezone.utc)
    t0 = time.time()
    status = "SUCCESS"
    details = None
    error_msg = None

    client = None
    if db is None:
        try:
            client = get_client(timeout_ms=500)
            client.admin.command("ping")
            db = client[settings.MONGO_DB_NAME]
        except Exception as exc:
            error_msg = f"خطأ الاتصال بـ MongoDB: {exc}"
            status = "FAILED"

    if status != "FAILED":
        try:
            details = job_info["action"](db)
        except Exception as exc:
            status = "FAILED"
            error_msg = str(exc)
            logger.exception(f"خطأ أثناء تنفيذ المهمة {job_name}: {exc}")

    ended_at = datetime.now(timezone.utc)
    duration_sec = round(time.time() - t0, 3)

    log_entry = {
        "job_name": job_name,
        "title": job_info["title"],
        "trigger": trigger_mode,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_seconds": duration_sec,
        "status": status,
        "error": error_msg,
        "result_summary": details,
    }

    log_job_execution(db, log_entry)

    if client:
        client.close()

    return log_entry


def list_jobs_status() -> list[dict]:
    """استرجاع قائمة المهام المجدولة، فترات تكرارها، وآخر تشغيل لكل مهمة."""
    result = []
    for name, info in JOBS_METADATA.items():
        # البحث عن آخر سجل تشغيل للمهمة
        last_run = next((item for item in IN_MEMORY_JOB_LOGS if item["job_name"] == name), None)
        next_run_time = None
        if _SCHEDULER_INSTANCE and _SCHEDULER_INSTANCE.running:
            job_obj = _SCHEDULER_INSTANCE.get_job(name)
            if job_obj and job_obj.next_run_time:
                next_run_time = job_obj.next_run_time.isoformat()

        result.append({
            "job_name": name,
            "title": info["title"],
            "description": info["description"],
            "interval_minutes": info["interval_minutes"],
            "next_run_time": next_run_time,
            "last_execution": last_run,
        })
    return result


def get_job_history(limit: int = 20) -> list[dict]:
    """استرجاع سجلات التشغيل الأخيرة للمهام."""
    return IN_MEMORY_JOB_LOGS[:limit]


# ---------------------------------------------------------------------------
# إدارة المجدول التلقائي (Scheduler Lifecycle)
# ---------------------------------------------------------------------------

def init_scheduler() -> BackgroundScheduler:
    global _SCHEDULER_INSTANCE
    if _SCHEDULER_INSTANCE is not None:
        return _SCHEDULER_INSTANCE

    scheduler = BackgroundScheduler(daemon=True)

    for name, info in JOBS_METADATA.items():
        scheduler.add_job(
            func=run_job_now,
            trigger=IntervalTrigger(minutes=info["interval_minutes"]),
            args=[name, "scheduled"],
            id=name,
            name=info["title"],
            replace_existing=True,
        )

    _SCHEDULER_INSTANCE = scheduler
    return scheduler


def start_scheduler():
    scheduler = init_scheduler()
    if not scheduler.running:
        scheduler.start()
        logger.info("تم بدء تشغيل المجدول التلقائي للمهام (APScheduler).")
    return scheduler


def shutdown_scheduler():
    global _SCHEDULER_INSTANCE
    if _SCHEDULER_INSTANCE and _SCHEDULER_INSTANCE.running:
        _SCHEDULER_INSTANCE.shutdown(wait=False)
        logger.info("تم إيقاف المجدول التلقائي.")
        _SCHEDULER_INSTANCE = None


if __name__ == "__main__":
    print("== تجربة تشغيل المهام المجدولة يدوياً ==")
    for job_id in JOBS_METADATA.keys():
        print(f"تشغيل {job_id}...")
        res = run_job_now(job_id, trigger_mode="manual")
        print(f"الحالة: {res['status']} | استغرق: {res['duration_seconds']}s")
