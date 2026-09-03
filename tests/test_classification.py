import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from quality_rules import classify_and_clean  # noqa: E402


def record(**overrides):
    value = {
        "order_id": "ORD-1",
        "customer_id": "CUS-1",
        "payment_amount": "1000",
        "delivery_cost": "0",
        "total_amount": "1000",
        "currency": "YER",
        "customer_phone": "+967777777777",
        "customer_email": "user@example.com",
        "order_date": "2025-01-31",
        "status": "مؤكد",
        "payment_status": "مدفوع",
        "items_json": json.dumps([{"unit_price": "1000", "qty": 1}], ensure_ascii=False),
    }
    value.update(overrides)
    return value


def test_valid_record_is_not_changed():
    result = classify_and_clean(record(), set())
    assert result["quality_status"] == "valid"
    assert result["corrections"] == []
    assert result["quarantine_codes"] == []


def test_corrected_record_has_audit_trail():
    result = classify_and_clean(
        record(
            payment_amount="٥٠٠٠",
            total_amount="5001",
            currency="ريال يمني",
            customer_phone="777 777 777",
            customer_email="user@@example..com",
            order_date="31/01/2025",
            status=" مؤكد ",
        ),
        set(),
    )
    assert result["quality_status"] == "corrected"
    assert len(result["corrections"]) >= 5
    assert {item["field"] for item in result["corrections"]} >= {
        "payment_amount", "currency", "customer_phone", "customer_email", "order_date"
    }
    assert all({"field", "original_value", "corrected_value", "rule_code"} <= item.keys()
               for item in result["corrections"])


def test_unrecoverable_record_is_quarantined_with_reason():
    result = classify_and_clean(
        record(order_id="", customer_id="", order_date="2025-02-30", items_json="{bad json}"),
        set(),
    )
    assert result["quality_status"] == "quarantined"
    assert "MISSING_ORDER_ID" in result["quarantine_codes"]
    assert "MISSING_CUSTOMER_ID" in result["quarantine_codes"]
    assert "INVALID_IMPOSSIBLE_DATE" in result["quarantine_codes"]
    assert "CORRUPTED_ITEMS_JSON" in result["quarantine_codes"]
    assert "MULTIPLE_CONFLICTING_ERRORS" in result["quarantine_codes"]
    assert len(result["quarantine_reasons"]) == len(result["quarantine_codes"])


def test_duplicate_business_key_is_quarantined():
    seen = set()
    first = classify_and_clean(record(order_id="ORD-DUP"), seen)
    second = classify_and_clean(record(order_id="ORD-DUP"), seen)
    assert first["quality_status"] == "valid"
    assert second["quality_status"] == "quarantined"
    assert "DUPLICATE_ORDER_ID" in second["quarantine_codes"]
