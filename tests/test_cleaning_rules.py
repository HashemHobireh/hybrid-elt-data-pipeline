"""
tests/test_cleaning_rules.py
------------------------------
اختبارات pytest لقواعد التنظيف الأساسية (القسم 9: "يجب إضافة اختبارات
لقواعد التنظيف والتصنيف الأساسية").

تشغيل: pytest tests/test_cleaning_rules.py -v
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from quality_rules import (  # noqa: E402
    rule_arabic_numerals,
    rule_currency,
    rule_thousands_separator,
    rule_price_words,
    rule_phone,
    rule_email,
    rule_date,
    rule_trim_and_synonyms,
    rule_recompute_total,
    normalize_record_keys,
)


class TestArabicNumerals:
    def test_converts_arabic_digits(self):
        result, corrected = rule_arabic_numerals("١٢٣٤٥")
        assert result == "12345"
        assert corrected is True

    def test_no_change_for_latin_digits(self):
        result, corrected = rule_arabic_numerals("12345")
        assert result == "12345"
        assert corrected is False

    def test_none_value(self):
        result, corrected = rule_arabic_numerals(None)
        assert result is None
        assert corrected is False


class TestCurrency:
    def test_converts_yemeni_rial_text(self):
        result, corrected = rule_currency("لاير يمني")
        assert result == "YER"
        assert corrected is True

    def test_already_standard(self):
        result, corrected = rule_currency("YER")
        assert result == "YER"
        assert corrected is False

    def test_unknown_currency_unchanged(self):
        result, corrected = rule_currency("USD_UNKNOWN_XYZ")
        assert corrected is False


class TestThousandsSeparator:
    def test_removes_comma(self):
        result, corrected = rule_thousands_separator("125,000.00")
        assert result == "125000.00"
        assert corrected is True

    def test_no_comma_unchanged(self):
        result, corrected = rule_thousands_separator("125000")
        assert corrected is False


class TestPriceWords:
    def test_known_word_converted(self):
        result, corrected = rule_price_words("ألفان")
        assert result == "2000"
        assert corrected is True

    def test_unknown_word_unchanged(self):
        result, corrected = rule_price_words("مبلغ غريب")
        assert corrected is False


class TestPhone:
    def test_removes_spaces_and_adds_plus(self):
        result, corrected = rule_phone("967 77 123 4567")
        assert result == "+967771234567"
        assert corrected is True

    def test_local_format_converted(self):
        result, corrected = rule_phone("771234567")
        assert result == "+967771234567"
        assert corrected is True

    def test_unparseable_unchanged(self):
        result, corrected = rule_phone("abc-not-a-phone")
        assert corrected is False


class TestEmail:
    def test_fixes_repeated_at_symbol(self):
        result, corrected, valid = rule_email("user@@mail.com")
        assert result == "user@mail.com"
        assert corrected is True
        assert valid is True

    def test_fixes_repeated_dots(self):
        result, corrected, valid = rule_email("user@mail..com")
        assert result == "user@mail.com"
        assert valid is True

    def test_unrecoverable_email_marked_invalid(self):
        result, corrected, valid = rule_email("not-an-email")
        assert valid is False


class TestDate:
    def test_converts_slash_format(self):
        result, corrected, valid = rule_date("2025/01/31")
        assert result == "2025-01-31"
        assert valid is True

    def test_already_standard_format(self):
        result, corrected, valid = rule_date("2025-01-31")
        assert corrected is False
        assert valid is True

    def test_impossible_date_marked_invalid(self):
        result, corrected, valid = rule_date("2025-13-99")
        assert valid is False


class TestTrimAndSynonyms:
    def test_trims_whitespace(self):
        result, corrected = rule_trim_and_synonyms("  مؤكد  ")
        assert result == "مؤكد"
        assert corrected is True

    def test_synonym_mapping(self):
        result, corrected = rule_trim_and_synonyms(
            "تم الدفع", {"تم الدفع": "مدفوع"}
        )
        assert result == "مدفوع"
        assert corrected is True


class TestRecomputeTotal:
    def test_recomputes_mismatched_total(self):
        items = [{"unit_price": 100, "qty": 2}]
        result, corrected = rule_recompute_total(items, "10", "999")
        assert result == "210.0"
        assert corrected is True

    def test_matching_total_unchanged(self):
        items = [{"unit_price": 100, "qty": 2}]
        result, corrected = rule_recompute_total(items, "0", "200")
        assert corrected is False


class TestNormalizeKeys:
    def test_removes_bom_from_key(self):
        bom_key = "\ufeff" + "order_id"
        record = {bom_key: "1", "customer_id": "2"}
        normalized = normalize_record_keys(record)
        assert "order_id" in normalized
