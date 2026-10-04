"""
src/api.py
----------
متطلبات المشروع النهائي - القسم 5: واجهة API موحدة للتشغيل والاختبار.
واجهة FastAPI موحدة لتمكين نظام التقييم من تشغيل وظائف المشروع واختبارها
بصورة موحدة، مع توفر صفحة Swagger الافتراضية عبر /docs.

المسارات المنفذة:
GET  /health
POST /ingest
POST /indexes
GET  /queries
GET  /queries/{name}
GET  /aggregations
GET  /aggregations/{name}
POST /refresh-mv
GET  /jobs
POST /jobs/{name}/run
"""

import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import Body, FastAPI, HTTPException, Query  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402

from aggregations import AGGREGATIONS_REGISTRY, execute_aggregation, list_aggregations  # noqa: E402
from config import settings  # noqa: E402
from main import run_pipeline  # noqa: E402
from materialized_views import (  # noqa: E402
    MATERIALIZED_VIEWS_REGISTRY,
    get_materialized_view_data,
    list_materialized_views,
    refresh_all_materialized_views,
    refresh_materialized_view,
)
from mongo_setup import get_client  # noqa: E402
from queries_and_indexes import (  # noqa: E402
    QUERIES_REGISTRY,
    create_phase2_indexes,
    explain_query,
    execute_query,
    run_explain_comparison,
)
from scheduler import get_job_history, list_jobs_status, run_job_now, shutdown_scheduler, start_scheduler  # noqa: E402


@asynccontextmanager
async def lifespan(app: FastAPI):
    # بدء المجدول عند الإقلاع (تجاوزه أثناء اختبارات pytest لتفادي الخيوط الخلفية)
    scheduler = None
    if "pytest" not in sys.modules:
        try:
            scheduler = start_scheduler()
        except Exception as exc:
            print(f"تحذير أثناء بدء المجدول: {exc}")
    yield
    # إيقاف المجدول بأمان عند الخروج
    if scheduler:
        try:
            shutdown_scheduler()
        except Exception:
            pass


app = FastAPI(
    title="Hybrid ELT Data Pipeline API",
    description="واجهة موحدة لتشغيل واختبار خط معالجة البيانات، الاستعلامات، الفهارس، التجميعات، والعروض المادية.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# تفعيل CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    client = get_client()
    return client[settings.MONGO_DB_NAME]


# ---------------------------------------------------------------------------
# نماذج Pydantic للطلبات
# ---------------------------------------------------------------------------

class IngestRequest(BaseModel):
    file_path: Optional[str] = Field(
        default=None,
        description="مسار ملف CSV المصدر. افتراضياً: data/orders_small_sample.csv",
    )
    batch_size: Optional[int] = Field(default=None, description="حجم الدفعة في التحميل والتصنيف")
    threshold_mb: Optional[float] = Field(default=None, description="حد الفصل للمحرك (Threshold)")
    raw_only: bool = Field(default=False, description="تحميل إلى orders_raw فقط دون تصنيف")


class RefreshMvRequest(BaseModel):
    view_name: Optional[str] = Field(
        default=None,
        description="اسم العرض المادي لتحديثه منفرداً (أو اتركه فارغاً لتحديث جميع العروض)",
    )
    incremental: bool = Field(default=True, description="تحديث تزايدي دون إعادة بناء الكل")


# ---------------------------------------------------------------------------
# 1. GET /health
# ---------------------------------------------------------------------------
@app.get("/health", tags=["System"])
def health_check():
    """فحص سلامة النظام، الاتصال بـ MongoDB، وأعداد السجلات في المجموعات."""
    db_status = "connected"
    counts = {}
    try:
        client = get_client(timeout_ms=500)
        client.admin.command("ping")
        db = client[settings.MONGO_DB_NAME]
        counts = {
            settings.COLLECTION_RAW: db[settings.COLLECTION_RAW].count_documents({}),
            settings.COLLECTION_VALIDATED: db[settings.COLLECTION_VALIDATED].count_documents({}),
            settings.COLLECTION_QUARANTINE: db[settings.COLLECTION_QUARANTINE].count_documents({}),
        }
        client.close()
    except Exception as exc:
        db_status = f"disconnected: {exc}"

    return {
        "status": "healthy" if db_status == "connected" else "degraded",
        "mongodb": db_status,
        "database_name": settings.MONGO_DB_NAME,
        "collection_counts": counts,
        "engine_threshold_mb": settings.SMALL_FILE_THRESHOLD_MB,
        "standard_currency": settings.STANDARD_CURRENCY,
    }


# ---------------------------------------------------------------------------
# 2. POST /ingest
# ---------------------------------------------------------------------------
@app.post("/ingest", tags=["Pipeline"])
def ingest_data(request: Optional[IngestRequest] = Body(default=None)):
    """
    تشغيل Pipeline الإدخال والتنظيف والتصنيف الكامل (يستخدم نفس بوابة الإدخال للمشروع).
    يدعم تمرير مسار ملف جديد أو استخدام ملف العينة الافتراضي.
    """
    req = request or IngestRequest()
    target_path = Path(req.file_path) if req.file_path else settings.SAMPLE_FILE_PATH

    if not target_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"ملف الإدخال غير موجود: {target_path}",
        )

    try:
        result = run_pipeline(
            input_path=target_path,
            batch_size=req.batch_size,
            threshold_mb=req.threshold_mb,
            raw_only=req.raw_only,
        )
        return {
            "status": "success",
            "message": "اكتمل تشغيل خط البيانات بنجاح.",
            "pipeline_summary": result,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"خطأ أثناء معالجة البيانات: {exc}")


# ---------------------------------------------------------------------------
# 3. POST /indexes
# ---------------------------------------------------------------------------
@app.post("/indexes", tags=["Indexes"])
def create_indexes():
    """إنشاء جميع فهارس الأداء (Compound و Single Field) على مجموعات البيانات."""
    try:
        client = get_client()
        db = client[settings.MONGO_DB_NAME]
        res = create_phase2_indexes(db)
        client.close()
        return res
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"خطأ أثناء إنشاء الفهارس: {exc}")


# ---------------------------------------------------------------------------
# 4. GET /queries
# ---------------------------------------------------------------------------
@app.get("/queries", tags=["Queries"])
def list_queries():
    """استعراض قائمة الاستعلامات العملية الخمسة المتاحة مع معاييرها وفهارسها."""
    return [
        {
            "name": name,
            "title": info["title"],
            "description": info["description"],
            "target_collection": info["collection"],
            "index_used": info["index_used"],
            "default_params": info["default_params"],
        }
        for name, info in QUERIES_REGISTRY.items()
    ]


# ---------------------------------------------------------------------------
# 5. GET /queries/{name}
# ---------------------------------------------------------------------------
@app.get("/queries/{name}", tags=["Queries"])
def run_query(
    name: str,
    customer_id: Optional[str] = Query(default=None, description="معرف العميل"),
    city: Optional[str] = Query(default=None, description="اسم المدينة"),
    status: Optional[str] = Query(default=None, description="حالة الطلب"),
    start_date: Optional[str] = Query(default=None, description="تاريخ البداية YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="تاريخ النهاية YYYY-MM-DD"),
    payment_status: Optional[str] = Query(default=None, description="حالة الدفع"),
    error_code: Optional[str] = Query(default=None, description="كود خطأ العزل"),
    limit: int = Query(default=50, description="الحد الأقصى لعدد النتائج"),
    explain: bool = Query(default=False, description="تشغيل explain('executionStats') لمراقبة الأداء"),
):
    """
    تنفيذ استعلام عملي بالاسم.
    إذا تم تفعيل خيار explain=True يعيد تفاصيل executionStats (COLLSCAN مقابل IXSCAN والزمن والوثائق المفحوصة).
    """
    if name not in QUERIES_REGISTRY:
        raise HTTPException(
            status_code=404,
            detail=f"الاستعلام '{name}' غير موجود. المتاح: {list(QUERIES_REGISTRY.keys())}",
        )

    params = {
        "customer_id": customer_id,
        "city": city,
        "status": status,
        "start_date": start_date,
        "end_date": end_date,
        "payment_status": payment_status,
        "error_code": error_code,
        "limit": limit,
    }

    try:
        client = get_client()
        db = client[settings.MONGO_DB_NAME]
        if explain:
            result = explain_query(db, name, params)
        else:
            result = execute_query(db, name, params)
        client.close()
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"خطأ أثناء تنفيذ الاستعلام: {exc}")


# مسار مساعد لإجراء مقارنة explain الشاملة
@app.get("/explain-comparison", tags=["Queries"])
def explain_comparison():
    """تشغيل مقارنة Explain الشاملة لثلاثة استعلامات قبل وبعد الفهارس وتوليد التقارير."""
    try:
        client = get_client()
        db = client[settings.MONGO_DB_NAME]
        report = run_explain_comparison(db)
        client.close()
        return report
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"خطأ أثناء إجراء مقارنة Explain: {exc}")


# ---------------------------------------------------------------------------
# 6. GET /aggregations
# ---------------------------------------------------------------------------
@app.get("/aggregations", tags=["Aggregations"])
def get_aggregations_list():
    """استعراض قائمة التقارير التجميعية الخمسة المتاحة وأوصافها."""
    return list_aggregations()


# ---------------------------------------------------------------------------
# 7. GET /aggregations/{name}
# ---------------------------------------------------------------------------
@app.get("/aggregations/{name}", tags=["Aggregations"])
def run_aggregation(
    name: str,
    limit: int = Query(default=20, description="الحد الأقصى لعدد النتائج"),
):
    """تشغيل تقرير تجميعي محدد بالاسم واسترجاع البيانات المجمعة المحسوبة."""
    if name not in AGGREGATIONS_REGISTRY:
        raise HTTPException(
            status_code=404,
            detail=f"التقرير التجميعي '{name}' غير مسجل. المتاح: {list(AGGREGATIONS_REGISTRY.keys())}",
        )

    try:
        client = get_client()
        db = client[settings.MONGO_DB_NAME]
        res = execute_aggregation(db, name, {"limit": limit})
        client.close()
        return res
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"خطأ أثناء تشغيل التجميع: {exc}")


# ---------------------------------------------------------------------------
# 8. POST /refresh-mv
# ---------------------------------------------------------------------------
@app.post("/refresh-mv", tags=["Materialized Views"])
def refresh_views(request: Optional[RefreshMvRequest] = Body(default=None)):
    """
    تحديث العروض المادية (Materialized Views) بآلية تزايدية (Incremental) عبر $merge.
    يمكن تحديد اسم عرض معين أو ترك الحقل فارغاً لتحديث جميع العروض.
    """
    req = request or RefreshMvRequest()
    try:
        client = get_client()
        db = client[settings.MONGO_DB_NAME]
        if req.view_name:
            if req.view_name not in MATERIALIZED_VIEWS_REGISTRY:
                client.close()
                raise HTTPException(
                    status_code=404,
                    detail=f"العرض '{req.view_name}' غير موجود. المتاح: {list(MATERIALIZED_VIEWS_REGISTRY.keys())}",
                )
            result = refresh_materialized_view(db, req.view_name, req.incremental)
        else:
            result = refresh_all_materialized_views(db, req.incremental)
        client.close()
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"خطأ أثناء تحديث العروض المادية: {exc}")


@app.get("/materialized-views", tags=["Materialized Views"])
def list_views():
    """استعراض قائمة العروض المادية المتاحة ومجموعاتها."""
    return list_materialized_views()


@app.get("/materialized-views/{name}/data", tags=["Materialized Views"])
def get_view_data(name: str, limit: int = Query(default=50)):
    """قراءة البيانات الجاهزة من العرض المادي مباشرة."""
    if name not in MATERIALIZED_VIEWS_REGISTRY:
        raise HTTPException(status_code=404, detail="العرض غير موجود")
    try:
        client = get_client()
        db = client[settings.MONGO_DB_NAME]
        data = get_materialized_view_data(db, name, limit)
        client.close()
        return data
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# 9. GET /jobs
# ---------------------------------------------------------------------------
@app.get("/jobs", tags=["Scheduled Jobs"])
def get_jobs():
    """استعراض قائمة المهام المجدولة، مواعيد تكرارها، ونتائج آخر تشغيل لكل مهمة."""
    return {
        "jobs": list_jobs_status(),
        "recent_history": get_job_history(limit=15),
    }


# ---------------------------------------------------------------------------
# 10. POST /jobs/{name}/run
# ---------------------------------------------------------------------------
@app.post("/jobs/{name}/run", tags=["Scheduled Jobs"])
def trigger_job(name: str):
    """
    تشغيل يدوي فوري لمهمة مجدولة محددة بالاسم.
    يسجل وقت البداية والنهاية وحالة النجاح أو الفشل في سجلات التشغيل.
    """
    try:
        result = run_job_now(name, trigger_mode="api_manual")
        return result
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"خطأ أثناء تشغيل المهمة: {exc}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host=settings.API_HOST, port=settings.API_PORT, reload=True)
