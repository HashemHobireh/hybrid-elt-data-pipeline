"""
src/materialized_views.py
-------------------------
متطلبات المشروع النهائي - القسم 3: العروض المادية (Materialized Views).
إنشاء 2 Materialized Views على الأقل مبنية على نتائج Aggregations مع آلية
تحديث تزايدي (Incremental Refresh) تستفيد من حقول التاريخ دون إعادة بناء
كامل البيانات من الصفر في كل مرة، باستخدام مرحلة $merge في MongoDB.
"""

import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings  # noqa: E402

MATERIALIZED_VIEWS_REGISTRY = {
    "daily_sales_summary": {
        "title": "عرض ملخص المبيعات اليومية المادي",
        "collection": settings.COLLECTION_MV_DAILY_SALES,
        "source_collection": settings.COLLECTION_VALIDATED,
        "description": "تخزين مسبق لإجمالي الإيرادات وعدد الطلبات لكل يوم، مع دعم التحديث التزايدي.",
    },
    "top_products_summary": {
        "title": "عرض أفضل المنتجات المادي",
        "collection": settings.COLLECTION_MV_TOP_PRODUCTS,
        "source_collection": settings.COLLECTION_VALIDATED,
        "description": "تخزين مسبق لإجمالي المبيعات والكميات حسب المنتج لدعم لوحات التحكم السريعة.",
    },
    "city_sales_summary": {
        "title": "عرض مبيعات المدن المادي",
        "collection": settings.COLLECTION_MV_CITY_SALES,
        "source_collection": settings.COLLECTION_VALIDATED,
        "description": "تخزين مسبق لتوزيع المبيعات والأداء التجاري حسب المدينة.",
    },
}


def _get_last_refreshed_date(db, target_collection: str) -> str | None:
    """استرجاع أحدث تاريخ مسجل في العرض المادي لتحديد نقطة انطلاق التحديث التزايدي."""
    col = db[target_collection]
    latest_doc = col.find_one(sort=[("_id", -1)])
    if latest_doc and "_id" in latest_doc and isinstance(latest_doc["_id"], str):
        # نعود يوماً واحداً إلى الوراء لضمان شمل أي طلبات متأخرة لنفس اليوم
        try:
            dt = datetime.strptime(latest_doc["_id"][:10], "%Y-%m-%d") - timedelta(days=1)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None


def refresh_daily_sales_summary(db, incremental: bool = True) -> dict:
    """
    تحديث عرض المبيعات اليومية المادي.
    في الوضع التزايدي، يستعلم فقط عن السجلات من أحدث تاريخ مسجل مسبقاً.
    """
    started_at = time.time()
    source_col = db[settings.COLLECTION_VALIDATED]
    target_col_name = settings.COLLECTION_MV_DAILY_SALES

    match_filter = {
        "order_date": {"$regex": r"^\d{4}-\d{2}-\d{2}"},
        "status": {"$nin": ["ملغي", "ملغى"]},
    }

    last_date = _get_last_refreshed_date(db, target_col_name) if incremental else None
    mode = "incremental" if last_date else "full"
    if last_date:
        match_filter["order_date"]["$gte"] = last_date

    pipeline = [
        {"$match": match_filter},
        {
            "$addFields": {
                "day_str": {"$substrCP": ["$order_date", 0, 10]},
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
                "_id": "$day_str",
                "date": {"$first": "$day_str"},
                "total_revenue": {"$sum": "$numeric_total"},
                "orders_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$numeric_total"},
            }
        },
        {
            "$project": {
                "_id": 1,
                "date": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "orders_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "last_refreshed_at": {"$literal": datetime.now(timezone.utc).isoformat()},
                "refresh_mode": {"$literal": mode},
            }
        },
        {
            "$merge": {
                "into": target_col_name,
                "id": "_id",
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]

    source_col.aggregate(pipeline, allowDiskUse=True)
    count = db[target_col_name].count_documents({})
    elapsed_ms = round((time.time() - started_at) * 1000, 2)

    return {
        "view_name": "daily_sales_summary",
        "target_collection": target_col_name,
        "mode": mode,
        "incremental_start_date": last_date,
        "current_total_documents": count,
        "execution_time_ms": elapsed_ms,
        "status": "success",
    }


def refresh_top_products_summary(db, incremental: bool = True) -> dict:
    """تحديث عرض المنتجات المادي مع دمج النتائج باستخدام $merge."""
    started_at = time.time()
    source_col = db[settings.COLLECTION_VALIDATED]
    target_col_name = settings.COLLECTION_MV_TOP_PRODUCTS

    pipeline = [
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
                "item_id_key": {
                    "$ifNull": [
                        "$items.sku",
                        {"$ifNull": ["$items.name", "SKU_UNKNOWN"]}
                    ]
                },
                "item_title": {
                    "$ifNull": [
                        "$items.name",
                        {"$ifNull": ["$items.sku", "منتج غير محدد"]}
                    ]
                },
            }
        },
        {
            "$group": {
                "_id": "$item_id_key",
                "product_name": {"$first": "$item_title"},
                "total_quantity_sold": {"$sum": "$item_qty"},
                "total_revenue": {
                    "$sum": {"$multiply": ["$item_qty", "$item_price"]}
                },
                "orders_count": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 1,
                "product_name": 1,
                "total_quantity_sold": "$total_quantity_sold",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "orders_count": 1,
                "last_refreshed_at": {"$literal": datetime.now(timezone.utc).isoformat()},
            }
        },
        {
            "$merge": {
                "into": target_col_name,
                "id": "_id",
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]

    source_col.aggregate(pipeline, allowDiskUse=True)
    count = db[target_col_name].count_documents({})
    elapsed_ms = round((time.time() - started_at) * 1000, 2)

    return {
        "view_name": "top_products_summary",
        "target_collection": target_col_name,
        "mode": "merge",
        "current_total_documents": count,
        "execution_time_ms": elapsed_ms,
        "status": "success",
    }


def refresh_city_sales_summary(db, incremental: bool = True) -> dict:
    """تحديث عرض مبيعات المدن المادي عبر $merge."""
    started_at = time.time()
    source_col = db[settings.COLLECTION_VALIDATED]
    target_col_name = settings.COLLECTION_MV_CITY_SALES

    pipeline = [
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
                "_id": 1,
                "city": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "orders_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "last_refreshed_at": {"$literal": datetime.now(timezone.utc).isoformat()},
            }
        },
        {
            "$merge": {
                "into": target_col_name,
                "id": "_id",
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]

    source_col.aggregate(pipeline, allowDiskUse=True)
    count = db[target_col_name].count_documents({})
    elapsed_ms = round((time.time() - started_at) * 1000, 2)

    return {
        "view_name": "city_sales_summary",
        "target_collection": target_col_name,
        "mode": "merge",
        "current_total_documents": count,
        "execution_time_ms": elapsed_ms,
        "status": "success",
    }


def refresh_materialized_view(db, view_name: str, incremental: bool = True) -> dict:
    """تحديث عرض مادي معين بالاسم."""
    if view_name == "daily_sales_summary":
        return refresh_daily_sales_summary(db, incremental)
    elif view_name == "top_products_summary":
        return refresh_top_products_summary(db, incremental)
    elif view_name == "city_sales_summary":
        return refresh_city_sales_summary(db, incremental)
    else:
        raise ValueError(
            f"العرض المادي '{view_name}' غير معروف. المتاح: {list(MATERIALIZED_VIEWS_REGISTRY.keys())}"
        )


def refresh_all_materialized_views(db, incremental: bool = True) -> dict:
    """تحديث كافة العروض المادية المتاحة وإرجاع النتائج الإجمالية."""
    results = {}
    started_at = time.time()
    for name in MATERIALIZED_VIEWS_REGISTRY.keys():
        results[name] = refresh_materialized_view(db, name, incremental)

    return {
        "status": "success",
        "total_views_refreshed": len(results),
        "total_elapsed_ms": round((time.time() - started_at) * 1000, 2),
        "views": results,
    }


def get_materialized_view_data(db, view_name: str, limit: int = 50) -> dict:
    """قراءة البيانات المخزنة مسبقاً في العرض المادي مباشرة بأعلى سرعة."""
    if view_name not in MATERIALIZED_VIEWS_REGISTRY:
        raise ValueError(f"العرض '{view_name}' غير مسجل.")

    info = MATERIALIZED_VIEWS_REGISTRY[view_name]
    col = db[info["collection"]]
    started_at = time.time()
    cursor = col.find({}, {"_id": 0}).limit(limit)
    data = list(cursor)
    elapsed_ms = round((time.time() - started_at) * 1000, 2)

    return {
        "view_name": view_name,
        "title": info["title"],
        "collection": info["collection"],
        "count": len(data),
        "execution_time_ms": elapsed_ms,
        "data": data,
    }


def list_materialized_views() -> list[dict]:
    """قائمة بكافة العروض المادية المتاحة ومجموعاتها المصدرية والهدف."""
    return [
        {
            "name": name,
            "title": info["title"],
            "description": info["description"],
            "target_collection": info["collection"],
            "source_collection": info["source_collection"],
        }
        for name, info in MATERIALIZED_VIEWS_REGISTRY.items()
    ]


if __name__ == "__main__":
    from mongo_setup import get_client
    try:
        client = get_client()
        db = client[settings.MONGO_DB_NAME]
        print("== تحديث العروض المادية ==")
        res = refresh_all_materialized_views(db, incremental=True)
        print(f"تم تحديث {res['total_views_refreshed']} عروض في {res['total_elapsed_ms']}ms.")
    except Exception as exc:
        print(f"تنبيه: {exc}")
