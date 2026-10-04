"""
src/queries_and_indexes.py
--------------------------
متطلبات المشروع النهائي - القسم 1: الاستعلامات والفهارس وExplain.
- إنشاء 3 فهارس على الأقل (منها Compound Index واحد على الأقل).
- تنفيذ 5 استعلامات عملية تلائم بيانات المشروع.
- تنفيذ وتحليل explain("executionStats") لـ 3 استعلامات قبل وبعد الفهارس.
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings  # noqa: E402

# ---------------------------------------------------------------------------
# تعريف الفهارس المطلوبة
# ---------------------------------------------------------------------------
INDEX_DEFINITIONS = [
    {
        "name": "idx_customer_date",
        "collection": settings.COLLECTION_VALIDATED,
        "keys": [("customer_id", 1), ("order_date", -1)],
        "type": "Compound Index",
        "reason": (
            "تسريع استعلام سجل طلبات العميل وتاريخها مرتبة من الأحدث إلى الأقدم، "
            "حيث يتم تجنب فحص كامل المجموعة (COLLSCAN) والترتيب في الذاكرة (Sort in memory)."
        ),
    },
    {
        "name": "idx_city_status_date",
        "collection": settings.COLLECTION_VALIDATED,
        "keys": [("city", 1), ("status", 1), ("order_date", -1)],
        "type": "Compound Index",
        "reason": (
            "تسريع استعلامات الطلبات المؤكدة حسب المدينة مرتبة زمنياً، "
            "مما يدعم شاشات عمليات التوزيع اللوجستي لكل منطقة."
        ),
    },
    {
        "name": "idx_order_date",
        "collection": settings.COLLECTION_VALIDATED,
        "keys": [("order_date", 1)],
        "type": "Single Field Index",
        "reason": "تسريع استعلامات وفلاتر النطاقات الزمنية (Range Queries) بين تاريخين.",
    },
    {
        "name": "idx_payment_status",
        "collection": settings.COLLECTION_VALIDATED,
        "keys": [("payment_status", 1)],
        "type": "Single Field Index",
        "reason": "تسريع استخراج الطلبات غير المدفوعة أو المعلقة لمتابعة التحصيل.",
    },
    {
        "name": "idx_quarantine_error_codes",
        "collection": settings.COLLECTION_QUARANTINE,
        "keys": [("error_codes", 1)],
        "type": "Multikey Index",
        "reason": "تسريع فلترة السجلات المعزولة حسب كود الخطأ البرمجي في مصفوفة error_codes.",
    },
]


def create_phase2_indexes(db) -> dict:
    """إنشاء جميع الفهارس المحددة في المشروع النهائي."""
    created = []
    started_at = time.time()
    for item in INDEX_DEFINITIONS:
        col = db[item["collection"]]
        idx_name = col.create_index(item["keys"], name=item["name"])
        created.append({
            "name": idx_name,
            "collection": item["collection"],
            "keys": item["keys"],
            "type": item["type"],
        })
    return {
        "status": "success",
        "created_indexes": created,
        "total_indexes": len(created),
        "elapsed_seconds": round(time.time() - started_at, 4),
    }


def drop_phase2_indexes(db) -> dict:
    """حذف الفهارس المخصصة للمقارنة والـ Explain."""
    dropped = []
    for item in INDEX_DEFINITIONS:
        col = db[item["collection"]]
        try:
            col.drop_index(item["name"])
            dropped.append(item["name"])
        except Exception:
            pass
    return {"status": "dropped", "dropped_indexes": dropped}


# ---------------------------------------------------------------------------
# سجل الاستعلامات الخمسة العملية
# ---------------------------------------------------------------------------
QUERIES_REGISTRY = {
    "customer_order_history": {
        "title": "سجل طلبات العميل مرتبة زمنياً",
        "collection": settings.COLLECTION_VALIDATED,
        "description": "استرجاع جميع طلبات عميل محدد مرتبة من الأحدث إلى الأقدم.",
        "index_used": "idx_customer_date",
        "default_params": {"customer_id": "عميل-0", "limit": 20},
        "build_query": lambda p: (
            {"customer_id": p.get("customer_id", "عميل-0")},
            [("order_date", -1)],
            {"_id": 0, "order_id": 1, "customer_id": 1, "customer_name": 1,
             "order_date": 1, "total_amount": 1, "status": 1, "city": 1}
        ),
    },
    "city_confirmed_orders": {
        "title": "طلبات المدينة المؤكدة",
        "collection": settings.COLLECTION_VALIDATED,
        "description": "استرجاع الطلبات المؤكدة في مدينة معينة مرتبة حسب التاريخ.",
        "index_used": "idx_city_status_date",
        "default_params": {"city": "تعز", "status": "مؤكد", "limit": 50},
        "build_query": lambda p: (
            {"city": p.get("city", "تعز"), "status": p.get("status", "مؤكد")},
            [("order_date", -1)],
            {"_id": 0, "order_id": 1, "city": 1, "status": 1, "order_date": 1,
             "total_amount": 1, "customer_name": 1}
        ),
    },
    "date_range_orders": {
        "title": "الطلبات ضمن نطاق زمني",
        "collection": settings.COLLECTION_VALIDATED,
        "description": "استرجاع الطلبات المنفذة بين تاريخين محددين.",
        "index_used": "idx_order_date",
        "default_params": {"start_date": "2025-01-01", "end_date": "2025-03-31", "limit": 50},
        "build_query": lambda p: (
            {"order_date": {"$gte": p.get("start_date", "2025-01-01"),
                            "$lte": p.get("end_date", "2025-03-31")}},
            [("order_date", 1)],
            {"_id": 0, "order_id": 1, "order_date": 1, "total_amount": 1, "status": 1}
        ),
    },
    "unpaid_orders_lookup": {
        "title": "الطلبات غير المدفوعة",
        "collection": settings.COLLECTION_VALIDATED,
        "description": "استخراج الطلبات المعلقة أو غير المدفوعة لمتابعة السداد.",
        "index_used": "idx_payment_status",
        "default_params": {"payment_status": "غير مدفوع", "limit": 50},
        "build_query": lambda p: (
            {"payment_status": p.get("payment_status", "غير مدفوع")},
            [("order_date", -1)],
            {"_id": 0, "order_id": 1, "customer_id": 1, "customer_phone": 1,
             "payment_status": 1, "total_amount": 1, "order_date": 1}
        ),
    },
    "quarantine_by_error": {
        "title": "سجلات العزل حسب كود الخطأ",
        "collection": settings.COLLECTION_QUARANTINE,
        "description": "البحث في السجلات المعزولة التي تحتوي على خطأ جوهري محدد.",
        "index_used": "idx_quarantine_error_codes",
        "default_params": {"error_code": "INVALID_IMPOSSIBLE_DATE", "limit": 50},
        "build_query": lambda p: (
            {"error_codes": p.get("error_code", "INVALID_IMPOSSIBLE_DATE")},
            [("quarantined_at", -1)],
            {"_id": 0, "order_id": 1, "error_codes": 1, "error_details": 1,
             "quarantined_at": 1, "source_row_number": 1}
        ),
    },
}


def execute_query(db, query_name: str, params: dict | None = None) -> dict:
    """تنفيذ استعلام محدد بالاسم وإرجاع البيانات وزمن التنفيذ."""
    if query_name not in QUERIES_REGISTRY:
        raise ValueError(f"الاستعلام '{query_name}' غير مسجل. الخيارات: {list(QUERIES_REGISTRY.keys())}")

    q_info = QUERIES_REGISTRY[query_name]
    merged_params = dict(q_info["default_params"])
    if params:
        merged_params.update({k: v for k, v in params.items() if v is not None})

    filter_dict, sort_spec, projection = q_info["build_query"](merged_params)
    limit = int(merged_params.get("limit", 50))

    col = db[q_info["collection"]]
    started_at = time.time()
    cursor = col.find(filter_dict, projection)
    if sort_spec:
        cursor = cursor.sort(sort_spec)
    if limit > 0:
        cursor = cursor.limit(limit)

    results = list(cursor)
    elapsed_ms = round((time.time() - started_at) * 1000, 2)

    return {
        "query_name": query_name,
        "title": q_info["title"],
        "collection": q_info["collection"],
        "params": merged_params,
        "index_used": q_info["index_used"],
        "execution_time_ms": elapsed_ms,
        "count": len(results),
        "data": results,
    }


def explain_query(db, query_name: str, params: dict | None = None) -> dict:
    """تشغيل explain('executionStats') على استعلام واستخراج المؤشرات الجوهرية."""
    if query_name not in QUERIES_REGISTRY:
        raise ValueError(f"الاستعلام '{query_name}' غير معروف.")

    q_info = QUERIES_REGISTRY[query_name]
    merged_params = dict(q_info["default_params"])
    if params:
        merged_params.update({k: v for k, v in params.items() if v is not None})

    filter_dict, sort_spec, projection = q_info["build_query"](merged_params)
    limit = int(merged_params.get("limit", 100))

    col = db[q_info["collection"]]
    cursor = col.find(filter_dict, projection)
    if sort_spec:
        cursor = cursor.sort(sort_spec)
    if limit > 0:
        cursor = cursor.limit(limit)

    raw_explain = cursor.explain("executionStats")
    exec_stats = raw_explain.get("executionStats", {})
    query_planner = raw_explain.get("queryPlanner", {})
    winning_plan = query_planner.get("winningPlan", {})

    def extract_stage_info(plan):
        stages = []
        cur = plan
        while cur:
            st = cur.get("stage", "UNKNOWN")
            stages.append(st)
            if "inputStage" in cur:
                cur = cur["inputStage"]
            elif "inputStages" in cur and cur["inputStages"]:
                cur = cur["inputStages"][0]
            else:
                break
        return stages

    stages_chain = extract_stage_info(winning_plan)
    uses_index = "IXSCAN" in stages_chain

    # استخراج اسم الفهرس إن وجد
    index_name = None
    cur = winning_plan
    while cur:
        if cur.get("stage") == "IXSCAN":
            index_name = cur.get("indexName")
            break
        cur = cur.get("inputStage")

    return {
        "query_name": query_name,
        "title": q_info["title"],
        "collection": q_info["collection"],
        "winning_stage": stages_chain[0] if stages_chain else "UNKNOWN",
        "stages_chain": stages_chain,
        "uses_index": uses_index,
        "index_name": index_name,
        "execution_time_millis": exec_stats.get("executionTimeMillis", 0),
        "total_docs_examined": exec_stats.get("totalDocsExamined", 0),
        "total_keys_examined": exec_stats.get("totalKeysExamined", 0),
        "n_returned": exec_stats.get("nReturned", 0),
        "raw_explain": raw_explain,
    }


def run_explain_comparison(db) -> dict:
    """
    تنفيذ متطلب القسم 1:
    تشغيل explain('executionStats') لـ 3 استعلامات قبل وبعد الفهارس،
    وحفظ التقرير والتحليل في reports/.
    """
    target_queries = ["customer_order_history", "city_confirmed_orders", "date_range_orders"]

    # 1. حذف الفهارس لقياس الأداء قبل الفهرسة (COLLSCAN)
    drop_phase2_indexes(db)
    before_stats = {}
    for q_name in target_queries:
        before_stats[q_name] = explain_query(db, q_name)

    # 2. إنشاء الفهارس
    create_phase2_indexes(db)

    # 3. قياس الأداء بعد الفهرسة (IXSCAN)
    after_stats = {}
    for q_name in target_queries:
        after_stats[q_name] = explain_query(db, q_name)

    # 4. بناء تقرير المقارنة
    comparisons = []
    for q_name in target_queries:
        b = before_stats[q_name]
        a = after_stats[q_name]
        comparisons.append({
            "query_name": q_name,
            "title": QUERIES_REGISTRY[q_name]["title"],
            "collection": QUERIES_REGISTRY[q_name]["collection"],
            "index_used": QUERIES_REGISTRY[q_name]["index_used"],
            "before_index": {
                "stage": b["winning_stage"],
                "execution_time_ms": b["execution_time_millis"],
                "docs_examined": b["total_docs_examined"],
                "keys_examined": b["total_keys_examined"],
                "returned": b["n_returned"],
            },
            "after_index": {
                "stage": a["winning_stage"],
                "index_name": a["index_name"],
                "execution_time_ms": a["execution_time_millis"],
                "docs_examined": a["total_docs_examined"],
                "keys_examined": a["total_keys_examined"],
                "returned": a["n_returned"],
            },
            "speedup_ratio": (
                round(b["execution_time_millis"] / max(a["execution_time_millis"], 1), 2)
                if a["execution_time_millis"] > 0 else "N/A"
            ),
            "docs_reduction_ratio": (
                round(b["total_docs_examined"] / max(a["total_docs_examined"], 1), 2)
                if a["total_docs_examined"] > 0 else "N/A"
            ),
        })

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database": db.name,
        "comparisons": comparisons,
    }

    # حفظ JSON
    reports_dir = settings.REPORTS_DIR
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / "explain_results.json"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    # توليد ملف Markdown تحليلي شامل
    md_path = reports_dir / "explain_analysis.md"
    md_content = _build_explain_markdown(comparisons)
    md_path.write_text(md_content, encoding="utf-8")

    return report


def _build_explain_markdown(comparisons: list[dict]) -> str:
    lines = [
        "# تقرير تحليل أداء الفهارس والاستعلامات (ExecutionStats Report)",
        "",
        "تم فحص الاستعلامات الثلاثة المختارة باستخدام `explain('executionStats')` لمقارنة الأداء **قبل وبعد إنشاء الفهارس**، وتوضيح سبب اختيار كل فهرس وأثره العملي على استهلاك الموارد والزمن.",
        "",
        "## 1. جدول المقارنة الشامل",
        "",
        "| الاستعلام | الفهرس المطبق | المرحلة قبل | الوثائق المفحوصة قبل | الزمن قبل (ms) | المرحلة بعد | الفهرس المستخدم | الوثائق المفحوصة بعد | الزمن بعد (ms) |",
        "|---|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|",
    ]
    for c in comparisons:
        b = c["before_index"]
        a = c["after_index"]
        lines.append(
            f"| **{c['title']}** (`{c['query_name']}`) | `{c['index_used']}` | "
            f"`{b['stage']}` | {b['docs_examined']:,} | {b['execution_time_ms']} | "
            f"`{a['stage']}` | `{a['index_name']}` | {a['docs_examined']:,} | {a['execution_time_ms']} |"
        )

    lines.extend([
        "",
        "---",
        "",
        "## 2. التحليل التفصيلي وسبب اختيار كل فهرس",
        "",
    ])

    for c in comparisons:
        q_name = c["query_name"]
        lines.append(f"### استعلام: {c['title']} (`{q_name}`)")
        if q_name == "customer_order_history":
            lines.extend([
                "- **نوع الفهرس**: `Compound Index` على `(customer_id: 1, order_date: -1)`.",
                "- **السبب والهدف**: الاستعلام يبحث عن سجل طلبات عميل معين ويفرزها من الأحدث إلى الأقدم. "
                "بدون الفهرس يقوم MongoDB بمسح كامل المجموعة `COLLSCAN` ثم ترتيب النتائج في الذاكرة `SORT`. "
                "بوجود الفهرس المركب، يقفز المحرك مباشرة إلى وثائق العميل المعني (`IXSCAN`) ويحصل عليها مرتبة مسبقاً من بنية B-Tree دون تكلفة فرز.",
            ])
        elif q_name == "city_confirmed_orders":
            lines.extend([
                "- **نوع الفهرس**: `Compound Index` على `(city: 1, status: 1, order_date: -1)`.",
                "- **السبب والهدف**: يخدم هذا الاستعلام لوجستيات التوزيع والفرز الجغرافي. "
                "الفهرس المركب يطابق الحقول الأكثر انتقائية أولاً (`city`, `status`) ثم يوفر الترتيب الزمني مباشرة.",
            ])
        elif q_name == "date_range_orders":
            lines.extend([
                "- **نوع الفهرس**: `Single Field Index` على `(order_date: 1)`.",
                "- **السبب والهدف**: استعلامات النطاق الزمني (`$gte`, `$lte`) تستهلك مسحاً كاملاً إذا لم تُفهرس. "
                "الفهرس يتيح للمحرك حصر النطاق الزمني المطلوب فقط وتجنب قراءة باقي التواريخ.",
            ])

        lines.extend([
            f"- **الأثر المقاس**: انخفض عدد الوثائق المفحوصة من `{c['before_index']['docs_examined']}` وثيقة إلى `{c['after_index']['docs_examined']}` وثيقة فقط.",
            "",
        ])

    return "\n".join(lines)


if __name__ == "__main__":
    from mongo_setup import get_client
    try:
        client = get_client()
        db = client[settings.MONGO_DB_NAME]
        print("جاري إنشاء الفهارس...")
        res = create_phase2_indexes(db)
        print(f"تم إنشاء {res['total_indexes']} فهارس بنجاح في {res['elapsed_seconds']} ثانية.")
    except Exception as exc:
        print(f"تنبيه: تعذر الاتصال بـ MongoDB: {exc}")
