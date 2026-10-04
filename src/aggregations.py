"""
src/aggregations.py
-------------------
متطلبات المشروع النهائي - القسم 2: التجميعات (خمسة تقارير تجميعية على الأقل).
كل تقرير يمتلك اسمًا واضحًا وطريقة تشغيل مستقلة، ويعيد نتائج فعلية من البيانات.
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings  # noqa: E402

# ---------------------------------------------------------------------------
# سجل تقارير التجميع
# ---------------------------------------------------------------------------
AGGREGATIONS_REGISTRY = {
    "sales_by_city": {
        "title": "تقرير إجمالي المبيعات حسب المدينة",
        "description": "حساب إجمالي الإيرادات، عدد الطلبات، ومتوسط قيمة الطلب لكل مدينة.",
        "collection": settings.COLLECTION_VALIDATED,
        "build_pipeline": lambda params: [
            {
                "$match": {
                    "city": {"$exists": True, "$nin": [None, "", "null", "NULL"]},
                    "status": {"$nin": ["ملغي", "ملغى"]},
                }
            },
            {
                "$addFields": {
                    "numeric_total": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0,
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$city",
                    "city": {"$first": "$city"},
                    "total_revenue": {"$sum": "$numeric_total"},
                    "orders_count": {"$sum": 1},
                    "avg_order_value": {"$avg": "$numeric_total"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "city": "$_id",
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "orders_count": 1,
                    "avg_order_value": {"$round": ["$avg_order_value", 2]},
                }
            },
            {"$sort": {"total_revenue": -1}},
            {"$limit": int(params.get("limit", 50))},
        ],
    },
    "top_products": {
        "title": "تقرير أفضل المنتجات مبيعاً من حيث الكمية والإيرادات",
        "description": "تفكيك عناصر الطلبات وحساب إجمالي الكميات المباعة والعوائد لكل منتج.",
        "collection": settings.COLLECTION_VALIDATED,
        "build_pipeline": lambda params: [
            {
                "$match": {
                    "items": {"$exists": True, "$type": "array", "$ne": []},
                    "status": {"$nin": ["ملغي", "ملغى"]},
                }
            },
            {"$unwind": "$items"},
            {
                "$addFields": {
                    "item_qty": {
                        "$convert": {
                            "input": "$items.qty",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0,
                        }
                    },
                    "item_price": {
                        "$convert": {
                            "input": "$items.unit_price",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0,
                        }
                    },
                    "item_name": {
                        "$ifNull": [
                            "$items.name",
                            {"$ifNull": ["$items.sku", "منتج غير محدد"]}
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": "$item_name",
                    "product_name": {"$first": "$item_name"},
                    "total_quantity_sold": {"$sum": "$item_qty"},
                    "total_revenue": {
                        "$sum": {"$multiply": ["$item_qty", "$item_price"]}
                    },
                    "orders_count": {"$sum": 1},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "product_name": "$_id",
                    "total_quantity_sold": "$total_quantity_sold",
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "orders_count": 1,
                }
            },
            {"$sort": {"total_quantity_sold": -1}},
            {"$limit": int(params.get("limit", 15))},
        ],
    },
    "top_customers": {
        "title": "تقرير أفضل العملاء الأكثر إنفاقاً",
        "description": "ترتيب العملاء تنازلياً حسب إجمالي المبالغ المدفوعة وعدد الطلبات المكتملة.",
        "collection": settings.COLLECTION_VALIDATED,
        "build_pipeline": lambda params: [
            {
                "$match": {
                    "customer_id": {"$exists": True, "$nin": [None, "", "null", "NULL"]},
                    "status": {"$nin": ["ملغي", "ملغى"]},
                }
            },
            {
                "$addFields": {
                    "numeric_total": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0,
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$customer_id",
                    "customer_id": {"$first": "$customer_id"},
                    "customer_name": {"$first": "$customer_name"},
                    "total_spent": {"$sum": "$numeric_total"},
                    "orders_count": {"$sum": 1},
                    "avg_order_value": {"$avg": "$numeric_total"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "customer_id": "$_id",
                    "customer_name": {"$ifNull": ["$customer_name", "عميل"]},
                    "total_spent": {"$round": ["$total_spent", 2]},
                    "orders_count": 1,
                    "avg_order_value": {"$round": ["$avg_order_value", 2]},
                }
            },
            {"$sort": {"total_spent": -1}},
            {"$limit": int(params.get("limit", 10))},
        ],
    },
    "monthly_sales_trend": {
        "title": "تقرير المبيعات حسب الفترة (شهرياً)",
        "description": "تجميع مسار المبيعات وتطور الإيرادات عبر الأشهر والسنوات.",
        "collection": settings.COLLECTION_VALIDATED,
        "build_pipeline": lambda params: [
            {
                "$match": {
                    "order_date": {"$regex": r"^\d{4}-\d{2}"},
                    "status": {"$nin": ["ملغي", "ملغى"]},
                }
            },
            {
                "$addFields": {
                    "month": {"$substrCP": ["$order_date", 0, 7]},
                    "numeric_total": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0,
                        }
                    },
                }
            },
            {
                "$group": {
                    "_id": "$month",
                    "period": {"$first": "$month"},
                    "total_revenue": {"$sum": "$numeric_total"},
                    "orders_count": {"$sum": 1},
                    "avg_order_value": {"$avg": "$numeric_total"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "period": "$_id",
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "orders_count": 1,
                    "avg_order_value": {"$round": ["$avg_order_value", 2]},
                }
            },
            {"$sort": {"period": 1}},
            {"$limit": int(params.get("limit", 24))},
        ],
    },
    "orders_distribution_by_status": {
        "title": "تقرير توزيع الطلبات حسب الحالة وحالة الدفع",
        "description": "تحليل أعداد وقيم الطلبات موزعة حسب حالة الإنجاز وحالة السداد.",
        "collection": settings.COLLECTION_VALIDATED,
        "build_pipeline": lambda params: [
            {
                "$addFields": {
                    "numeric_total": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0,
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "status": {"$ifNull": ["$status", "غير محدد"]},
                        "payment_status": {"$ifNull": ["$payment_status", "غير محدد"]},
                    },
                    "orders_count": {"$sum": 1},
                    "total_amount": {"$sum": "$numeric_total"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "status": "$_id.status",
                    "payment_status": "$_id.payment_status",
                    "orders_count": 1,
                    "total_amount": {"$round": ["$total_amount", 2]},
                }
            },
            {"$sort": {"orders_count": -1}},
            {"$limit": int(params.get("limit", 50))},
        ],
    },
    "quarantine_error_summary": {
        "title": "تقرير إحصائيات أسباب عزل السجلات في Quarantine",
        "description": "تحليل توزيع الأخطاء الجوهرية التي تسببت في إرسال السجلات إلى العزل.",
        "collection": settings.COLLECTION_QUARANTINE,
        "build_pipeline": lambda params: [
            {"$unwind": "$error_codes"},
            {
                "$group": {
                    "_id": "$error_codes",
                    "error_code": {"$first": "$error_codes"},
                    "quarantined_records_count": {"$sum": 1},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "error_code": "$_id",
                    "quarantined_records_count": 1,
                }
            },
            {"$sort": {"quarantined_records_count": -1}},
            {"$limit": int(params.get("limit", 20))},
        ],
    },
}


def list_aggregations() -> list[dict]:
    """قائمة بكافة التقارير التجميعية المتاحة وأوصافها."""
    return [
        {
            "name": name,
            "title": info["title"],
            "description": info["description"],
            "target_collection": info["collection"],
        }
        for name, info in AGGREGATIONS_REGISTRY.items()
    ]


def execute_aggregation(db, report_name: str, params: dict | None = None) -> dict:
    """تشغيل تقرير تجميعي محدد بالاسم وإرجاع النتيجة ومقاييس الأداء."""
    if report_name not in AGGREGATIONS_REGISTRY:
        raise ValueError(
            f"التقرير التجميعي '{report_name}' غير موجود. المتاح: {list(AGGREGATIONS_REGISTRY.keys())}"
        )

    info = AGGREGATIONS_REGISTRY[report_name]
    params = params or {}
    pipeline = info["build_pipeline"](params)

    col = db[info["collection"]]
    started_at = time.time()
    results = list(col.aggregate(pipeline, allowDiskUse=True))
    elapsed_ms = round((time.time() - started_at) * 1000, 2)

    return {
        "report_name": report_name,
        "title": info["title"],
        "description": info["description"],
        "target_collection": info["collection"],
        "params": params,
        "count": len(results),
        "execution_time_ms": elapsed_ms,
        "data": results,
    }


if __name__ == "__main__":
    from mongo_setup import get_client
    try:
        client = get_client()
        db = client[settings.MONGO_DB_NAME]
        print("== تجربة التجميعات ==")
        for name in ["sales_by_city", "orders_distribution_by_status"]:
            res = execute_aggregation(db, name, {"limit": 5})
            print(f"[{name}] عدد النتائج: {res['count']} في {res['execution_time_ms']}ms")
    except Exception as exc:
        print(f"تنبيه: {exc}")
