"""
quality_rules.py
------------------
قواعد التنظيف الآلي (القسم 6.6) + بناء Audit Trail (القسم 6.7)
+ منطق التصنيف والعزل (القسم 6.6/6.8).

كل دالة تصحيح تُرجع (القيمة_الجديدة, تم_التصحيح: bool). لا تُصحَّح القيمة
إلا عندما تكون قاعدة التحويل واضحة ولا تعتمد على التخمين (حسب نص المشروع).
"""

import json
import re
from datetime import datetime

# ---------------------------------------------------------------------------
# أدوات عامة
# ---------------------------------------------------------------------------

ARABIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"
LATIN_DIGITS = "0123456789"
ARABIC_TO_LATIN = str.maketrans(ARABIC_DIGITS, LATIN_DIGITS)

NULL_STRINGS = {"", "null", "none", "nan", "n/a", "na", "-"}


def normalize_record_keys(record: dict) -> dict:
    """
    يزيل علامة BOM (\\ufeff) التي قد تلتصق باسم أول عمود عند قراءة ملفات
    CSV بترميز utf-8 عادي (بدل utf-8-sig)، ويقص أي مسافات زائدة من الأسماء.
    هذا يضمن أن المفاتيح مثل order_id تبقى قابلة للوصول دائمًا.
    """
    cleaned = {}
    for k, v in record.items():
        clean_key = k.replace("\ufeff", "").strip()
        cleaned[clean_key] = v
    return cleaned


def _is_empty(value) -> bool:
    """التحقق مما إذا كانت القيمة فارغة أو تعبر عن قيمة منعدمة (NULL/None/nan)."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in NULL_STRINGS
    return False


def _parse_float(value) -> float | None:
    """تحويل القيمة إلى float بأمان بعد معالجة الأرقام العربية وفواصل الآلاف."""
    if _is_empty(value):
        return None
    val_str = str(value).strip()
    val_latin, _ = rule_arabic_numerals(val_str)
    val_clean, _ = rule_thousands_separator(val_latin)
    try:
        return float(val_clean)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# القاعدة 1: الأرقام العربية -> أرقام لاتينية
# ---------------------------------------------------------------------------

def rule_arabic_numerals(value):
    if value is None:
        return value, False
    val_str = str(value)
    if not any(ch in ARABIC_DIGITS for ch in val_str):
        return val_str if not isinstance(value, str) else value, False
    new_value = val_str.translate(ARABIC_TO_LATIN)
    return new_value, True


# ---------------------------------------------------------------------------
# القاعدة 2: رمز/اسم العملة -> توحيد إلى YER
# ---------------------------------------------------------------------------

CURRENCY_SYNONYMS = {
    "لاير": "YER", "لاير يمني": "YER", "ريال يمني": "YER", "ريال": "YER",
    "yer": "YER", "yr": "YER", "yem": "YER", "ر.ي": "YER", "ر.ي.": "YER",
    "ر. ي": "YER", "ريال.يمني": "YER",
}


def rule_currency(value):
    if _is_empty(value):
        return value, False
    normalized = value.strip().lower()
    for synonym, code in CURRENCY_SYNONYMS.items():
        if synonym.lower() == normalized:
            if value.strip() != code:
                return code, True
            return value, False
    return value, False


# ---------------------------------------------------------------------------
# القاعدة 3: فواصل الآلاف -> إزالة وتحويل لرقم
# ---------------------------------------------------------------------------

def rule_thousands_separator(value):
    if _is_empty(value):
        return value, False
    val_str = str(value)
    if "," not in val_str:
        return val_str if not isinstance(value, str) else value, False
    if re.fullmatch(r"[\d,]+(\.\d+)?", val_str.strip()):
        new_value = val_str.replace(",", "")
        return new_value, True
    return value, False


# ---------------------------------------------------------------------------
# القاعدة 4: السعر بالكلمات -> قيم معروفة محددة فقط
# ---------------------------------------------------------------------------

PRICE_WORDS = {
    "ألف": "1000", "الف": "1000",
    "ألفان": "2000", "الفان": "2000", "ألفين": "2000", "الفين": "2000",
    "ثلاثة آلاف": "3000", "ثلاثة الاف": "3000",
    "أربعة آلاف": "4000", "اربعة الاف": "4000",
    "خمسة آلاف": "5000", "خمسة الاف": "5000",
    "ستة آلاف": "6000", "ستة الاف": "6000",
    "سبعة آلاف": "7000", "سبعة الاف": "7000",
    "ثمانية آلاف": "8000", "ثمانية الاف": "8000",
    "تسعة آلاف": "9000", "تسعة الاف": "9000",
    "عشرة آلاف": "10000", "عشرة الاف": "10000",
    "عشرين ألف": "20000", "عشرين الف": "20000",
    "مئة": "100", "مائة": "100", "مئتان": "200",
    "خمسمائة": "500", "خمسمئة": "500",
}


def rule_price_words(value):
    if _is_empty(value):
        return value, False
    normalized = str(value).strip()
    if normalized in PRICE_WORDS:
        return PRICE_WORDS[normalized], True
    return value, False


# ---------------------------------------------------------------------------
# القاعدة 5: رقم الهاتف -> إزالة المسافات وتوحيد الصيغة ودعم الأرقام العربية
# ---------------------------------------------------------------------------

def rule_phone(value):
    if _is_empty(value):
        return value, False
    original = str(value)
    val_latin, _ = rule_arabic_numerals(original)
    no_spaces = re.sub(r"\s+", "", val_latin)
    digits_only = re.sub(r"[^\d+]", "", no_spaces)
    if digits_only.startswith("+967") and len(digits_only) == 13:
        result = digits_only
    elif digits_only.startswith("967") and len(digits_only) == 12:
        result = "+" + digits_only
    elif digits_only.startswith("0") and len(digits_only) == 10:
        result = "+967" + digits_only[1:]
    elif re.fullmatch(r"7\d{8}", digits_only):
        result = "+967" + digits_only
    else:
        return original, False

    if result != original:
        return result, True
    return original, False


# ---------------------------------------------------------------------------
# القاعدة 6: البريد الإلكتروني -> إصلاح التكرار الواضح فقط
# ---------------------------------------------------------------------------

_EMAIL_BASIC_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def rule_email(value):
    """
    يعالج فقط أخطاء التكرار الواضحة (@@ مكررة، أو نقاط متتالية ..).
    إذا بقي البريد غير صالح بعد الإصلاح، يُرجع is_valid=False ليُعزل السجل.
    """
    if _is_empty(value):
        return value, False, False

    val_str = str(value).strip()
    fixed = re.sub(r"@{2,}", "@", val_str)
    fixed = re.sub(r"\.{2,}", ".", fixed)
    corrected = fixed != value

    is_valid = bool(_EMAIL_BASIC_PATTERN.match(fixed))
    return fixed, corrected, is_valid


# ---------------------------------------------------------------------------
# القاعدة 7: التاريخ -> صيغة قياسية YYYY-MM-DD ودعم الأرقام العربية
# ---------------------------------------------------------------------------

_DATE_FORMATS = ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%dT%H:%M:%S"]


def rule_date(value):
    """يرجع (القيمة_الجديدة, تم_التصحيح, صالح). تاريخ مستحيل => صالح=False."""
    if _is_empty(value):
        return value, False, False

    raw = str(value).strip()
    raw_latin, _ = rule_arabic_numerals(raw)
    for fmt in _DATE_FORMATS:
        try:
            parsed = datetime.strptime(raw_latin, fmt)
            standardized = parsed.strftime("%Y-%m-%d")
            corrected = standardized != raw
            return standardized, corrected, True
        except ValueError:
            continue
    return raw, False, False


# ---------------------------------------------------------------------------
# القاعدة 8: المسافات والمرادفات -> Trim وتوحيد القاموس القياسي
# ---------------------------------------------------------------------------

STATUS_SYNONYMS = {
    "مؤكد": "مؤكد", "تم التأكيد": "مؤكد", "مؤكّد": "مؤكد",
    "ملغي": "ملغي", "الغاء": "ملغي", "ملغى": "ملغي",
}

PAYMENT_STATUS_SYNONYMS = {
    "مدفوع": "مدفوع", "تم الدفع": "مدفوع", "تم السداد": "مدفوع",
    "غير مدفوع": "غير مدفوع", "لم يدفع": "غير مدفوع",
}

DELIVERY_COST_SYNONYMS = {
    "مجاني": "0", "مجانا": "0", "مجاناً": "0", "free": "0", "بدون توصيل": "0",
}


def rule_trim_and_synonyms(value, synonym_dict=None):
    if _is_empty(value):
        return value, False
    trimmed = str(value).strip()
    changed = trimmed != value
    if synonym_dict and trimmed in synonym_dict:
        mapped = synonym_dict[trimmed]
        if mapped != trimmed:
            changed = True
        trimmed = mapped
    return trimmed, changed


# ---------------------------------------------------------------------------
# القاعدة 9 (إضافية): إعادة حساب إجمالي الطلب من العناصر + التوصيل
# ---------------------------------------------------------------------------

def rule_recompute_total(items, delivery_cost, total_amount):
    try:
        def clean_val(v):
            if v is None:
                return 0.0
            parsed = _parse_float(v)
            if parsed is None:
                raise ValueError(f"قيمة غير قابلة للتحويل إلى رقم: {v}")
            return parsed

        items_sum = sum(clean_val(i.get("unit_price")) * clean_val(i.get("qty")) for i in items)
        delivery = clean_val(delivery_cost) if not _is_empty(delivery_cost) else 0.0
        recomputed = round(items_sum + delivery, 2)
        current = clean_val(total_amount) if not _is_empty(total_amount) else None
        if current is None or abs(current - recomputed) > 0.01:
            return str(recomputed), True
        return total_amount, False
    except (ValueError, TypeError, KeyError):
        return total_amount, False


# ---------------------------------------------------------------------------
# التصنيف الكامل لسجل واحد: تنظيف + Audit Trail + قرار Quarantine
# ---------------------------------------------------------------------------

QUARANTINE_REASONS = {
    "MISSING_ORDER_ID": "معرف الطلب مفقود ولا يمكن استنتاجه.",
    "MISSING_CUSTOMER_ID": "معرف العميل مفقود.",
    "INVALID_IMPOSSIBLE_DATE": "تاريخ غير منطقي أو مستحيل.",
    "CORRUPTED_ITEMS_JSON": "JSON ناقص أو غير قابل للتحليل.",
    "EMPTY_ITEMS": "لا توجد عناصر للطلب.",
    "UNKNOWN_PRICE": "السعر الأصلي غير موجود أو غير قابل للاستنتاج.",
    "AMBIGUOUS_NEGATIVE_VALUE": "كمية أو مبلغ سالب لا يمكن تحديد معناه.",
    "DUPLICATE_ORDER_ID": "تكرار يحتاج سياسة دمج أو مراجعة.",
    "EMAIL_UNRECOVERABLE": "بريد إلكتروني غير صالح ولا يمكن إصلاحه تلقائيًا.",
    "MULTIPLE_CONFLICTING_ERRORS": "عدة أخطاء جوهرية تمنع التصحيح الآمن.",
}


def classify_and_clean(record_raw, seen_order_ids):
    """
    يطبّق كل القواعد على سجل خام واحد، ويقرر مصيره النهائي:
    Valid / Corrected / Quarantined، مع بناء Audit Trail كامل.
    """
    fields = normalize_record_keys(record_raw)
    corrections = []
    quarantine_codes = []

    def log_correction(field, original, corrected, rule_code):
        corrections.append({
            "field": field,
            "original_value": original,
            "corrected_value": corrected,
            "rule_code": rule_code,
        })

    # 1. التحقق من المعرفات الأساسية
    order_id = fields.get("order_id")
    if _is_empty(order_id):
        quarantine_codes.append("MISSING_ORDER_ID")

    customer_id = fields.get("customer_id")
    if _is_empty(customer_id):
        quarantine_codes.append("MISSING_CUSTOMER_ID")

    # 2. فحص تكرار المعرف
    if order_id and not _is_empty(order_id):
        if order_id in seen_order_ids:
            quarantine_codes.append("DUPLICATE_ORDER_ID")
        else:
            seen_order_ids.add(order_id)

    # 3. تنظيف الحقول المالية
    numeric_fields = ["payment_amount", "delivery_cost", "total_amount"]
    cleaned_numeric = {}
    for f in numeric_fields:
        val = fields.get(f)
        val, c1 = rule_arabic_numerals(val)
        if c1:
            log_correction(f, fields.get(f), val, "ARABIC_NUMERALS")
        val2, c3 = rule_thousands_separator(val)
        if c3:
            log_correction(f, val, val2, "THOUSANDS_SEPARATOR")
        val3, c4 = rule_price_words(val2)
        if c4:
            log_correction(f, val2, val3, "PRICE_WORDS")
        # فحص مرادفات التوصيل المجاني
        if f == "delivery_cost" and not _is_empty(val3):
            val_free, c_free = rule_trim_and_synonyms(val3, DELIVERY_COST_SYNONYMS)
            if c_free:
                log_correction(f, val3, val_free, "TRIM_SYNONYM")
                val3 = val_free
        cleaned_numeric[f] = val3

    # 4. فحص المبالغ المالية السالبة
    for f in numeric_fields:
        val_f = cleaned_numeric[f]
        parsed_f = _parse_float(val_f)
        if parsed_f is not None and parsed_f < 0:
            quarantine_codes.append("AMBIGUOUS_NEGATIVE_VALUE")

    # 5. العملة
    currency_val = fields.get("currency")
    currency_val, c2 = rule_currency(currency_val)
    if c2:
        log_correction("currency", fields.get("currency"), currency_val, "CURRENCY_SYNONYM")

    # 6. رقم الهاتف
    phone_raw = fields.get("customer_phone")
    phone_val, c5 = rule_phone(phone_raw)
    if c5:
        log_correction("customer_phone", phone_raw, phone_val, "PHONE_FORMAT")

    # 7. البريد الإلكتروني
    email_val, c6, email_valid = rule_email(fields.get("customer_email"))
    if c6:
        log_correction("customer_email", fields.get("customer_email"), email_val, "EMAIL_REPEATED_SYMBOLS")
    if not email_valid and not _is_empty(fields.get("customer_email")):
        quarantine_codes.append("EMAIL_UNRECOVERABLE")

    # 8. التاريخ
    date_raw = fields.get("order_date")
    date_val, c7, date_valid = rule_date(date_raw)
    if c7:
        log_correction("order_date", date_raw, date_val, "DATE_STANDARDIZED")
    if not date_valid:
        quarantine_codes.append("INVALID_IMPOSSIBLE_DATE")

    # 9. حالات الطلب والدفع
    status_val, c8a = rule_trim_and_synonyms(fields.get("status"), STATUS_SYNONYMS)
    if c8a:
        log_correction("status", fields.get("status"), status_val, "TRIM_SYNONYM")

    payment_status_val, c8b = rule_trim_and_synonyms(
        fields.get("payment_status"), PAYMENT_STATUS_SYNONYMS
    )
    if c8b:
        log_correction("payment_status", fields.get("payment_status"), payment_status_val, "TRIM_SYNONYM")

    # 10. عناصر الطلب (JSON) والتحقق من الكميات والأسعار السالبة
    items_json_raw = fields.get("items_json")
    items = None
    if _is_empty(items_json_raw):
        quarantine_codes.append("EMPTY_ITEMS")
    else:
        try:
            items = json.loads(items_json_raw)
            if not isinstance(items, list) or len(items) == 0:
                quarantine_codes.append("EMPTY_ITEMS")
            else:
                for item in items:
                    qty_parsed = _parse_float(item.get("qty"))
                    price_parsed = _parse_float(item.get("unit_price"))
                    if (qty_parsed is not None and qty_parsed < 0) or (price_parsed is not None and price_parsed < 0):
                        quarantine_codes.append("AMBIGUOUS_NEGATIVE_VALUE")
                        break
        except (json.JSONDecodeError, TypeError):
            quarantine_codes.append("CORRUPTED_ITEMS_JSON")
            items = None

    # 11. إعادة حساب الإجمالي
    total_amount_val = cleaned_numeric["total_amount"]
    if items and "CORRUPTED_ITEMS_JSON" not in quarantine_codes:
        new_total, c9 = rule_recompute_total(
            items, cleaned_numeric["delivery_cost"], total_amount_val
        )
        if c9:
            log_correction("total_amount", total_amount_val, new_total, "TOTAL_RECOMPUTED")
            total_amount_val = new_total

    # 12. التحقق من صلاحية الأسعار (UNKNOWN_PRICE)
    parsed_payment = _parse_float(cleaned_numeric["payment_amount"])
    if _is_empty(cleaned_numeric["payment_amount"]) or parsed_payment is None:
        quarantine_codes.append("UNKNOWN_PRICE")

    # إذا تعذر الحصول على total_amount سليم
    parsed_total = _parse_float(total_amount_val)
    if _is_empty(total_amount_val) or parsed_total is None:
        quarantine_codes.append("UNKNOWN_PRICE")

    # إذا كانت تكلفة التوصيل غير فارغة وغير رقمية
    if not _is_empty(cleaned_numeric["delivery_cost"]) and _parse_float(cleaned_numeric["delivery_cost"]) is None:
        quarantine_codes.append("UNKNOWN_PRICE")

    # إزالة التكرار من أكواد العزل وإضافة كود الأخطاء المتعددة
    quarantine_codes = list(dict.fromkeys(quarantine_codes))
    if len(quarantine_codes) >= 2:
        quarantine_codes.append("MULTIPLE_CONFLICTING_ERRORS")

    is_quarantined = len(quarantine_codes) > 0
    quality_status = "quarantined" if is_quarantined else ("corrected" if corrections else "valid")

    cleaned_fields = dict(fields)
    cleaned_fields.update(cleaned_numeric)
    cleaned_fields["currency"] = currency_val
    cleaned_fields["customer_phone"] = phone_val
    cleaned_fields["customer_email"] = email_val
    cleaned_fields["order_date"] = date_val
    cleaned_fields["status"] = status_val
    cleaned_fields["payment_status"] = payment_status_val
    cleaned_fields["total_amount"] = total_amount_val
    cleaned_fields["items"] = items if (items and isinstance(items, list)) else []

    return {
        "quality_status": quality_status,
        "corrections": corrections,
        "quarantine_codes": quarantine_codes,
        "quarantine_reasons": [QUARANTINE_REASONS.get(c, c) for c in quarantine_codes],
        "cleaned_fields": cleaned_fields,
        "order_id": order_id,
    }
