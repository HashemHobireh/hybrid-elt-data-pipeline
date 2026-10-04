"""
tests/test_phase2_queries.py
----------------------------
اختبارات استعلامات وفهارس المشروع النهائي (القسم 1).
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from queries_and_indexes import INDEX_DEFINITIONS, QUERIES_REGISTRY  # noqa: E402


def test_minimum_three_indexes_defined():
    assert len(INDEX_DEFINITIONS) >= 3


def test_at_least_one_compound_index():
    compound_indexes = [idx for idx in INDEX_DEFINITIONS if idx.get("type") == "Compound Index"]
    assert len(compound_indexes) >= 1
    # التأكد من أن الفهرس المركب يحتوي على حقلين على الأقل
    for idx in compound_indexes:
        assert len(idx["keys"]) >= 2


def test_at_least_five_queries_registered():
    assert len(QUERIES_REGISTRY) >= 5


def test_customer_order_history_query_builder():
    info = QUERIES_REGISTRY["customer_order_history"]
    filter_dict, sort_spec, projection = info["build_query"]({"customer_id": "CUS-100"})
    assert filter_dict == {"customer_id": "CUS-100"}
    assert sort_spec == [("order_date", -1)]
    assert "total_amount" in projection


def test_city_confirmed_orders_query_builder():
    info = QUERIES_REGISTRY["city_confirmed_orders"]
    filter_dict, sort_spec, projection = info["build_query"]({"city": "صنعاء", "status": "مؤكد"})
    assert filter_dict == {"city": "صنعاء", "status": "مؤكد"}
    assert sort_spec == [("order_date", -1)]


def test_date_range_orders_query_builder():
    info = QUERIES_REGISTRY["date_range_orders"]
    filter_dict, sort_spec, _ = info["build_query"]({"start_date": "2025-01-01", "end_date": "2025-01-31"})
    assert "order_date" in filter_dict
    assert filter_dict["order_date"]["$gte"] == "2025-01-01"
    assert filter_dict["order_date"]["$lte"] == "2025-01-31"


def test_quarantine_by_error_query_builder():
    info = QUERIES_REGISTRY["quarantine_by_error"]
    filter_dict, _, _ = info["build_query"]({"error_code": "MISSING_ORDER_ID"})
    assert filter_dict == {"error_codes": "MISSING_ORDER_ID"}
