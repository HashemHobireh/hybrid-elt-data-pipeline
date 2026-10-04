# Hybrid ELT Data Pipeline

مشروع فردي لمقرر **البيانات الضخمة (Big Data)**، يهدف إلى بناء خط معالجة بيانات **Hybrid ELT Pipeline** لمعالجة ملفات CSV غير النظيفة، باستخدام محرك مختلف حسب حجم الملف، مع تخزين البيانات في **MongoDB**.

يعتمد المشروع على:

- **Python Batch Streaming** لمعالجة الملفات الصغيرة.
- **PySpark** لمعالجة الملفات الكبيرة.
- **MongoDB** كطبقة تخزين.
- **ELT Architecture** بحيث يتم حفظ البيانات الخام أولًا ثم تنفيذ التحويل والتنظيف والتصنيف.
- **Data Quality Rules** لتصحيح الأخطاء القابلة للإصلاح.
- **Audit Trail** لتسجيل جميع التصحيحات.
- **Quarantine** لعزل السجلات التي تحتوي على أخطاء جوهرية لا يمكن تصحيحها بأمان.
- **Upsert + Unique Index** لضمان عدم إنشاء سجلات مكررة عند إعادة التشغيل.

---

## 1. أهداف المشروع

يهدف المشروع إلى تنفيذ Pipeline قادر على:

1. قراءة ملف CSV غير نظيف.
2. تحديد محرك المعالجة المناسب تلقائيًا حسب حجم الملف.
3. تحميل البيانات الأصلية إلى طبقة Raw دون تعديل.
4. تطبيق قواعد جودة البيانات.
5. تصنيف السجلات إلى:
   - `valid`
   - `corrected`
   - `quarantined`
6. حفظ السجلات المقبولة في `orders_validated`.
7. عزل السجلات غير القابلة للتصحيح في `orders_quarantine`.
8. تسجيل تفاصيل التصحيحات والأخطاء.
9. منع التكرار باستخدام `Upsert` و`Unique Index`.
10. دعم إعادة تشغيل Pipeline بطريقة آمنة.
11. حفظ نتائج التشغيل والقياسات في ملفات Reports.

---

# 2. المعمارية العامة

```text
                CSV Source
                    |
                    v
               main.py
                    |
                    v
              File Router
                    |
          +---------+---------+
          |                   |
          v                   v
   Python Batch           PySpark
   Small Files           Large Files
          |                   |
          +---------+---------+
                    |
                    v
               MongoDB
                    |
                    v
              orders_raw
                    |
                    v
        Transform + Quality Rules
                    |
          +---------+---------+
          |                   |
          v                   v
     Valid/Corrected       Invalid
          |                   |
          v                   v
 orders_validated      orders_quarantine
          |
          v
      Audit Trail
          |
          v
   reports/results.json
```

تفاصيل المعمارية موجودة أيضًا في:

```text
docs/architecture.md
```

---

# 3. مبدأ ELT المستخدم

يعتمد المشروع على مبدأ **ELT**:

### Extract

قراءة البيانات من ملف CSV.

### Load

تحميل السجلات كما هي إلى:

```text
orders_raw
```

دون تطبيق عمليات تنظيف على البيانات الأصلية.

### Transform

بعد تخزين Raw يتم تنفيذ:

- Data Cleaning
- Data Quality
- Classification
- Audit Trail

ثم يتم توجيه السجلات إلى:

```text
orders_validated
```

أو:

```text
orders_quarantine
```

وهذا يحافظ على البيانات الأصلية ويجعل عمليات التنظيف قابلة للتتبع والمراجعة.

---

# 4. محرك المعالجة واختيار الـEngine

يستخدم المشروع **File Router** لاختيار محرك المعالجة تلقائيًا.

الحد الافتراضي:

```text
200 MB
```

القاعدة:

```text
File Size <= 200 MB
        |
        v
Python Batch

File Size > 200 MB
        |
        v
PySpark
```

الحد موجود في:

```text
config/settings.py
```

ويمكن تغييره أثناء التشغيل باستخدام:

```bash
--threshold-mb
```

مثال:

```bash
py src\main.py --input data\orders_small_sample.csv --threshold-mb 100
```

---

# 5. ملف البيانات الكبير

ملف البيانات الأصلي الكبير الذي تم استخدامه في التنفيذ هو:

```text
orders_huge_mixed_quality.csv
```

حجمه في التشغيل الفعلي:

```text
12,650.32 MB تقريبًا
```

أي حوالي:

```text
12.6 GB
```

### ملاحظة مهمة

**ملف البيانات الكبير غير مرفوع داخل GitHub بسبب حجمه الكبير.**

الملف مقدم من الدكتور، ولذلك يجب وضعه محليًا داخل:

```text
data/
```

ليصبح المسار:

```text
data/orders_huge_mixed_quality.csv
```

ولا يحتاج هذا الملف إلى رفعه إلى مستودع GitHub.

---

# 6. ملف العينة الصغيرة

يحتوي المشروع على ملف عينة:

```text
data/orders_small_sample.csv
```

ويستخدم لاختبار Pipeline على ملف صغير باستخدام Python Batch.

كما يوجد سكربت لإنشاء عينة من الملف الكبير:

```text
src/create_small_sample.py
```

مثال:

```bash
py src\create_small_sample.py --input data\orders_huge_mixed_quality.csv --output data\orders_small_sample.csv --rows 100000 --mode head
```

---

# 7. متطلبات التشغيل

يحتاج المشروع إلى:

- Python 3.10 أو أحدث.
- MongoDB يعمل محليًا.
- Java مناسب لتشغيل PySpark.
- PySpark 3.5.1.
- Python packages الموجودة في `requirements.txt`.

---

# 8. تثبيت المكتبات

من داخل مجلد المشروع:

```bash
py -m pip install -r requirements.txt
```

المكتبات الأساسية:

```text
pymongo
pyspark==3.5.1
python-dotenv
pytest
pandas
jupyter
```

---

# 9. إعداد MongoDB

الإعداد الافتراضي:

```text
mongodb://localhost:27017
```

اسم قاعدة البيانات:

```text
midterm_pipeline_db
```

الإعدادات موجودة في:

```text
config/settings.py
```

الإعدادات الأساسية:

```text
MONGO_URI = mongodb://localhost:27017
MONGO_DB_NAME = midterm_pipeline_db
```

يمكن تغييرها باستخدام Environment Variables.

---

# 10. Collections في MongoDB

يستخدم المشروع ثلاث Collections رئيسية:

```text
orders_raw
orders_validated
orders_quarantine
```

---

# 11. Raw Schema

يتم حفظ السجل الخام بالشكل التالي:

```text
orders_raw
```

الحقول الأساسية:

```text
run_id
source_file
source_row_number
ingested_at
engine_used
raw_record
```

مثال هيكلي:

```json
{
  "run_id": "run_20260825T175655_2909b2c4",
  "source_file": "data/orders_huge_mixed_quality.csv",
  "source_row_number": 1,
  "ingested_at": "2026-08-25T00:00:00+00:00",
  "engine_used": "pyspark",
  "raw_record": {
    "order_id": "...",
    "customer_id": "...",
    "payment_amount": "...",
    "delivery_cost": "...",
    "total_amount": "...",
    "currency": "...",
    "customer_phone": "...",
    "customer_email": "...",
    "order_date": "...",
    "status": "...",
    "payment_status": "...",
    "items_json": "..."
  }
}
```

### ملاحظة

الـRaw يحتفظ بالقيم الأصلية دون تنظيف حتى يمكن الرجوع إلى المصدر الأصلي.

---

# 12. Validated Schema

السجلات التي يمكن قبولها بعد التنظيف تحفظ في:

```text
orders_validated
```

وتحتوي على البيانات المنظفة بالإضافة إلى:

```text
order_id
quality_status
corrections
source_run_id
```

مثال:

```json
{
  "order_id": "ORD-1",
  "customer_id": "CUS-1",
  "payment_amount": "5000",
  "delivery_cost": "0",
  "total_amount": "5000.0",
  "currency": "YER",
  "customer_phone": "+967777777777",
  "customer_email": "user@example.com",
  "order_date": "2025-01-31",
  "status": "مؤكد",
  "payment_status": "مدفوع",
  "quality_status": "corrected",
  "corrections": [],
  "source_run_id": "run_..."
}
```

---

# 13. Quarantine Schema

السجلات التي تحتوي على أخطاء جوهرية ولا يمكن تصحيحها بأمان تحفظ في:

```text
orders_quarantine
```

وتحتوي على:

```text
order_id
error_codes
error_details
raw_record
run_id
source_row_number
quarantined_at
```

مثال:

```json
{
  "order_id": "",
  "error_codes": [
    "MISSING_ORDER_ID",
    "INVALID_IMPOSSIBLE_DATE"
  ],
  "error_details": [
    "معرف الطلب مفقود ولا يمكن استنتاجه.",
    "تاريخ غير منطقي أو مستحيل."
  ],
  "raw_record": {},
  "run_id": "run_...",
  "source_row_number": 25
}
```

---

# 14. قواعد جودة البيانات

تم تنفيذ قواعد تنظيف واضحة داخل:

```text
src/quality_rules.py
```

ولا يتم إجراء تصحيح يعتمد على التخمين.

## Rule 1 — الأرقام العربية

تحويل الأرقام العربية إلى أرقام لاتينية.

مثال:

```text
٥٠٠٠
```

تصبح:

```text
5000
```

Rule Code:

```text
ARABIC_NUMERALS
```

---

## Rule 2 — توحيد العملة

تحويل صيغ العملة المعروفة إلى:

```text
YER
```

مثل:

```text
لاير
لاير يمني
ريال يمني
yer
yr
ر.ي
```

تصبح:

```text
YER
```

Rule Code:

```text
CURRENCY_SYNONYM
```

---

## Rule 3 — إزالة فواصل الآلاف

مثال:

```text
5,000
```

تصبح:

```text
5000
```

Rule Code:

```text
THOUSANDS_SEPARATOR
```

---

## Rule 4 — تحويل السعر المكتوب بالكلمات

يتم تحويل القيم المعروفة فقط.

مثل:

```text
ألف       -> 1000
ألفان     -> 2000
ثلاثة آلاف -> 3000
أربعة آلاف -> 4000
خمسة آلاف -> 5000
عشرة آلاف -> 10000
```

Rule Code:

```text
PRICE_WORDS
```

---

## Rule 5 — توحيد رقم الهاتف

يتم:

- إزالة المسافات.
- إزالة الرموز غير الضرورية.
- إضافة `+967` عند الحاجة.
- توحيد الصيغة اليمنية المعروفة.

مثال:

```text
777 777 777
```

تصبح:

```text
+967777777777
```

Rule Code:

```text
PHONE_FORMAT
```

---

## Rule 6 — إصلاح البريد الإلكتروني

يتم إصلاح الأخطاء الواضحة فقط.

مثل:

```text
user@@mail.com
```

تصبح:

```text
user@mail.com
```

وكذلك:

```text
user@mail..com
```

تصبح:

```text
user@mail.com
```

إذا بقي البريد غير صالح بعد الإصلاح يتم عزله.

Rule Code:

```text
EMAIL_REPEATED_SYMBOLS
```

وحالة العزل:

```text
EMAIL_UNRECOVERABLE
```

---

# 15. Rule 7 — توحيد التاريخ

الصيغ المدعومة تشمل:

```text
YYYY-MM-DD
YYYY/MM/DD
DD/MM/YYYY
DD-MM-YYYY
YYYY-MM-DDTHH:MM:SS
```

وتحول إلى:

```text
YYYY-MM-DD
```

مثال:

```text
31/01/2025
```

تصبح:

```text
2025-01-31
```

Rule Code:

```text
DATE_STANDARDIZED
```

أما التاريخ المستحيل مثل:

```text
2025-13-99
```

فيتم عزله:

```text
INVALID_IMPOSSIBLE_DATE
```

---

# 16. Rule 8 — المسافات والمرادفات

يتم إزالة المسافات الزائدة وتوحيد بعض القيم.

مثال:

```text
  مؤكد
```

تصبح:

```text
مؤكد
```

ومن أمثلة المرادفات:

```text
تم التأكيد -> مؤكد
مؤكّد      -> مؤكد

الغاء      -> ملغي
ملغى       -> ملغي

تم الدفع   -> مدفوع
تم السداد  -> مدفوع

لم يدفع    -> غير مدفوع
```

Rule Code:

```text
TRIM_SYNONYM
```

---

# 17. Rule 9 — إعادة حساب الإجمالي

يتم إعادة حساب:

```text
Total = Sum(unit_price × qty) + delivery_cost
```

ويتم تطبيق التصحيح عندما تكون مكونات العملية متاحة وصالحة.

مثال:

```text
unit_price = 100
qty = 2
delivery = 10
total = 999
```

القيمة الصحيحة:

```text
210
```

Rule Code:

```text
TOTAL_RECOMPUTED
```

---

# 18. حالات Quarantine

يتم عزل السجل عندما يحتوي على خطأ جوهري لا يمكن إصلاحه بأمان.

الأسباب المعرفة في المشروع:

```text
MISSING_ORDER_ID
MISSING_CUSTOMER_ID
INVALID_IMPOSSIBLE_DATE
CORRUPTED_ITEMS_JSON
EMPTY_ITEMS
UNKNOWN_PRICE
AMBIGUOUS_NEGATIVE_VALUE
DUPLICATE_ORDER_ID
EMAIL_UNRECOVERABLE
MULTIPLE_CONFLICTING_ERRORS
```

### أمثلة

#### معرف الطلب مفقود

```text
MISSING_ORDER_ID
```

#### معرف العميل مفقود

```text
MISSING_CUSTOMER_ID
```

#### JSON العناصر تالف

```text
CORRUPTED_ITEMS_JSON
```

#### لا توجد عناصر

```text
EMPTY_ITEMS
```

#### كمية سالبة غير واضحة

```text
AMBIGUOUS_NEGATIVE_VALUE
```

#### بريد غير قابل للإصلاح

```text
EMAIL_UNRECOVERABLE
```

#### تكرار order_id

```text
DUPLICATE_ORDER_ID
```

وعندما توجد أخطاء جوهرية متعددة يتم إضافة:

```text
MULTIPLE_CONFLICTING_ERRORS
```

---

# 19. Audit Trail

كل تصحيح يتم تسجيله داخل:

```text
corrections
```

ويحتوي كل تصحيح على:

```text
field
original_value
corrected_value
rule_code
```

مثال:

```json
{
  "field": "customer_phone",
  "original_value": "777 777 777",
  "corrected_value": "+967777777777",
  "rule_code": "PHONE_FORMAT"
}
```

وهذا يسمح بمعرفة:

- ما الحقل الذي تغير؟
- ما القيمة الأصلية؟
- ما القيمة الجديدة؟
- ما القاعدة التي قامت بالتصحيح؟

---

# 20. Upsert وUnique Index

يتم إنشاء Unique Index على:

```text
order_id
```

باسم:

```text
uniq_order_id
```

ويتم تحميل السجلات المقبولة باستخدام:

```text
UpdateOne(..., upsert=True)
```

وبالتالي عند إعادة تشغيل Pipeline:

- لا يتم إنشاء نسخة جديدة من نفس `order_id`.
- يتم تحديث السجل عند وجود تغيير.
- يتم تسجيل السجل كـ unchanged عندما لا يوجد تغيير.

---

# 21. Idempotency

تم اختبار إعادة تشغيل البيانات نفسها للتأكد من أن Pipeline آمن عند إعادة التشغيل.

في إحدى عمليات الاختبار على عينة 100,000 سجل ظهرت النتائج:

```text
rows_read       = 100,000
corrected_count = 91,513
quarantine      = 8,487
```

وعند إعادة التصنيف:

```text
inserted_count  = 0
updated_count   = 0
unchanged_count = 91,513
```

وهذا يثبت أن إعادة التشغيل لا تنشئ نسخًا مكررة للسجلات المقبولة.

---

# 22. تشغيل العينة الصغيرة

بعد التأكد من تشغيل MongoDB، يمكن تشغيل العينة:

```bash
py src\main.py --input data\orders_small_sample.csv --batch-size 5000 --progress-every-batches 1
```

يقوم Router بفحص حجم الملف.

إذا كان الحجم أقل أو يساوي:

```text
200 MB
```

فسيتم اختيار:

```text
python_batch
```

---

# 23. تشغيل Raw فقط

إذا أردنا تحميل البيانات إلى `orders_raw` فقط دون تنفيذ مرحلة التصنيف:

```bash
py src\main.py --input data\orders_small_sample.csv --batch-size 5000 --raw-only
```

هذا مفيد لفصل مرحلة:

```text
Load
```

عن مرحلة:

```text
Transform + Quality + Classification
```

---

# 24. تشغيل الملف الكبير

بعد وضع ملف الدكتور داخل:

```text
data/orders_huge_mixed_quality.csv
```

يمكن تشغيل:

```bash
py src\main.py --input data\orders_huge_mixed_quality.csv --progress-every-batches 20
```

نظرًا لأن حجمه أكبر من:

```text
200 MB
```

سيختار Router:

```text
pyspark
```

ثم يتم تحميل البيانات الخام إلى:

```text
orders_raw
```

وبعدها تبدأ مرحلة:

```text
Transform & Quality & Final Load
```

---

# 25. PySpark

يستخدم المشروع:

```text
PySpark 3.5.1
```

ويتم تشغيله افتراضيًا باستخدام:

```text
local[*]
```

أي استخدام الأنوية المتاحة محليًا.

ويتم استخدام:

```text
MongoDB Spark Connector
```

بالإصدار:

```text
org.mongodb.spark:mongo-spark-connector_2.12:10.3.0
```

---

# 26. Fixed Schema في PySpark

قبل قراءة الملف الكبير يتم قراءة أسماء الأعمدة ثم إنشاء Schema ثابتة.

جميع الأعمدة الخام يتم تعريفها كـ:

```text
StringType
```

والسبب هو الحفاظ على البيانات الأصلية كما وصلت وعدم إجبار Spark على تحويل القيم غير النظيفة تلقائيًا.

---

# 27. تقسيم التصنيف إلى Chunks

يدعم المشروع تصنيف البيانات الخام الكبيرة على أجزاء باستخدام:

```text
src/classify_in_chunks.py
```

الإعداد الافتراضي:

```text
chunk_size = 1,000,000
```

مثال:

```bash
py src\classify_in_chunks.py --run-id RUN_ID --start-row 1 --end-row 30000001 --chunk-size 1000000 --batch-size 5000 --progress-every-batches 20
```

حيث:

```text
start-row
```

هي بداية النطاق وتشمل الرقم.

بينما:

```text
end-row
```

هي نهاية النطاق ولا تشمل الرقم.

---

# 28. استكمال التشغيل بعد التوقف

يتم حفظ تقدم الأجزاء في:

```text
reports/chunk_progress.json
```

ويحتوي الملف على حالة كل جزء:

```text
running
completed
failed
```

إذا اكتمل جزء سابقًا، يستطيع البرنامج تخطيه عند إعادة التشغيل.

وهذا يجعل معالجة الملف الكبير أكثر أمانًا وقابلية للاستئناف.

---

# 29. فحص الاتساق

يحسب المشروع:

```text
raw_count
classified_count
```

ويتحقق من أن:

```text
classified_count =
valid_count + corrected_count + quarantine_count
```

مثال من التشغيل الكبير:

```text
raw_count        = 30,000,000
corrected_count  = 27,462,740
quarantine_count = 2,537,260
valid_count      = 0
```

وبالتالي:

```text
30,000,000 =
0 + 27,462,740 + 2,537,260
```

والنتيجة:

```text
equation_holds = True
```

---

# 30. النتائج الفعلية للملف الكبير

تم تنفيذ Pipeline على الملف الكبير بحجم:

```text
12,650.32 MB تقريبًا
```

وكان عدد السجلات:

```text
30,000,000
```

والنتائج:

| المؤشر | النتيجة |
|---|---:|
| حجم الملف | 12,650.32 MB |
| عدد السجلات | 30,000,000 |
| Router | PySpark |
| العينة الصغيرة | Python Batch |
| Corrected | 27,462,740 |
| Quarantine | 2,537,260 |
| Classified | 30,000,000 |
| Consistency | `True` |
| Unique Index | `True` |
| الاختبارات | 28 اختبارًا ناجحًا |

---

# 31. ملاحظة حول أعداد MongoDB

قد تختلف أعداد الوثائق الموجودة حاليًا في MongoDB عن أعداد تشغيل واحد.

السبب أن قاعدة البيانات قد تحتوي على:

- تشغيلات سابقة.
- عينات اختبار.
- تشغيلات متعددة لنفس البيانات.
- سجلات يتم دمجها باستخدام `Upsert`.
- `Unique Index` على `order_id`.

لذلك يجب الاعتماد على:

```text
reports/results.json
```

و:

```text
reports/chunk_progress.json
```

عند تحليل نتائج تشغيل معين.

---

# 32. الاختبارات

يحتوي المشروع على:

```text
28 اختبارًا
```

في:

```text
tests/test_cleaning_rules.py
tests/test_classification.py
```

لتشغيل جميع الاختبارات:

```bash
py -m pytest -q
```

الاختبارات تغطي أمثلة مثل:

- تحويل الأرقام العربية.
- توحيد العملة.
- إزالة فواصل الآلاف.
- تحويل الأسعار المكتوبة بالكلمات.
- توحيد الهاتف.
- إصلاح البريد الإلكتروني.
- توحيد التاريخ.
- اكتشاف التاريخ المستحيل.
- إزالة المسافات.
- توحيد المرادفات.
- إعادة حساب الإجمالي.
- إزالة BOM من أسماء الأعمدة.
- تصنيف السجل الصحيح.
- تصنيف السجل المصحح.
- Quarantine.
- اكتشاف Duplicate `order_id`.

---

# 33. Reports

يحتوي مجلد:

```text
reports/
```

على ملفات مهمة:

### results.json

يحتوي على نتائج التشغيل والقياسات مثل:

```text
run_id
file_name
file_size_mb
engine_used
rows_read
raw_loaded
valid_count
corrected_count
quarantine_count
throughput
inserted_count
updated_count
unchanged_count
error_case_counts
correction_rule_counts
consistency
```

### final_summary.md

يحتوي على الملخص النهائي والنتائج الفعلية للمشروع.

### chunk_progress.json

يحتوي على تقدم معالجة أجزاء البيانات الكبيرة.

---

# 34. هيكل المشروع

```text
midterm-data-pipeline/
│
├── config/
│   └── settings.py
│
├── data/
│   └── orders_small_sample.csv
│
├── docs/
│   ├── architecture.md
│   └── demo_checklist.md
│
├── notebooks/
│   ├── 01_setup_and_sample.ipynb
│   ├── 02_file_router.ipynb
│   ├── 03_python_batch_loader.ipynb
│   ├── 04_pyspark_loader.ipynb
│   ├── 05_quality_rules.ipynb
│   └── 06_classification_quarantine.ipynb
│
├── reports/
│   ├── final_summary.md
│   ├── results.json
│   └── chunk_progress.json
│
├── src/
│   ├── main.py
│   ├── file_router.py
│   ├── batch_loader.py
│   ├── spark_loader.py
│   ├── elt_pipeline.py
│   ├── quality_rules.py
│   ├── classify_in_chunks.py
│   ├── create_small_sample.py
│   ├── metrics.py
│   └── mongo_setup.py
│
├── tests/
│   ├── test_cleaning_rules.py
│   └── test_classification.py
│
├── requirements.txt
└── README.md
```

---

# 35. أهم ملفات المشروع

| الملف | الوظيفة |
|---|---|
| `src/main.py` | نقطة التشغيل الرئيسية |
| `src/file_router.py` | اختيار المحرك |
| `src/batch_loader.py` | Python Batch Streaming |
| `src/spark_loader.py` | تحميل البيانات الكبيرة باستخدام PySpark |
| `src/elt_pipeline.py` | Transform + Classification + Final Load |
| `src/quality_rules.py` | قواعد التنظيف والتصنيف |
| `src/classify_in_chunks.py` | معالجة البيانات على أجزاء |
| `src/create_small_sample.py` | إنشاء عينة صغيرة |
| `src/metrics.py` | حفظ النتائج |
| `src/mongo_setup.py` | الاتصال وفحص MongoDB |
| `config/settings.py` | إعدادات المشروع |
| `tests/` | اختبارات المشروع |
| `docs/architecture.md` | شرح المعمارية |
| `reports/` | نتائج التنفيذ |

---

# 36. التشغيل المختصر للعرض

يمكن تنفيذ العرض العملي بالترتيب التالي:

### 1. تشغيل MongoDB

التأكد من أن MongoDB يعمل على:

```text
mongodb://localhost:27017
```

### 2. تشغيل الاختبارات

```bash
py -m pytest -q
```

### 3. تجربة العينة

```bash
py src\main.py --input data\orders_small_sample.csv --batch-size 5000 --progress-every-batches 1
```

وإظهار أن Router اختار:

```text
python_batch
```

### 4. وضع ملف الدكتور

```text
data/orders_huge_mixed_quality.csv
```

### 5. تشغيل الملف الكبير

```bash
py src\main.py --input data\orders_huge_mixed_quality.csv --progress-every-batches 20
```

وإظهار أن Router اختار:

```text
pyspark
```

### 6. عرض MongoDB

إظهار:

```text
orders_raw
orders_validated
orders_quarantine
```

### 7. عرض سجل مصحح

إظهار:

```text
quality_status
corrections
```

### 8. عرض سجل Quarantine

إظهار:

```text
error_codes
error_details
raw_record
```

### 9. عرض الاتساق

إظهار:

```text
raw_count
classified_count
equation_holds = True
```

### 10. عرض Idempotency

إعادة تشغيل العينة وإظهار:

```text
inserted_count = 0
```

وأن السجلات الموجودة يتم التعامل معها بواسطة Upsert وUnique Index.

---

# 37. مبدأ المشروع في جملة واحدة

```text
Choose the right engine → Load Raw → Transform → Apply Quality Rules
→ Validate/Correct → Quarantine bad records → Upsert valid records
→ Audit everything → Verify consistency → Support restart safely
```

---

# 38. الخلاصة

المشروع ينفذ Hybrid ELT Pipeline يجمع بين:

```text
Python Batch
+
PySpark
+
MongoDB
+
Data Quality
+
Audit Trail
+
Quarantine
+
Upsert
+
Unique Index
+
Idempotency
+
Chunk Processing
+
Automated Testing
+
Execution Reports
```

وقد تم اختبار النظام على البيانات الفعلية، بما في ذلك ملف كبير بحجم يقارب:

```text
12.6 GB
```

وعدد:

```text
30,000,000
```

سجل.

وكانت نتيجة فحص الاتساق:

```text
True
```

مع نجاح:

```text
28 اختبارًا
```

---

## ملاحظة التسليم

ملف البيانات الكبير:

```text
orders_huge_mixed_quality.csv
```

**ليس جزءًا من مستودع GitHub بسبب حجمه الكبير، وهو ملف البيانات الأصلي المقدم من الدكتور.**

يجب وضعه محليًا داخل:

```text
data/
```

قبل تشغيل Pipeline على البيانات الكاملة.

أما المشروع نفسه، فيحتوي على الكود، الاختبارات، العينة الصغيرة، الـNotebooks، التوثيق، وملفات النتائج المطلوبة.

---

# 🚀 المرحلة الثانية: إضافات المشروع النهائي (Final Project - Phase 2)


1. **الاستعلامات والفهارس وExplain**
2. **التقارير التجميعية Aggregations**
3. **العروض المادية Materialized Views** وتحديثها التزايدي 
4. **المهام المجدولة Scheduled Jobs** والتشغيل اليدوي 
5. **واجهة API الموحدة عبر FastAPI**
6. **التوثيق وتنظيم المشروع والاختبارات** 

---

## 1. واجهة الـ API الموحدة (FastAPI)

يوفر المشروع واجهة تشغيل واختبار موحدة باستخدام **FastAPI** تتيح لنظام التقييم تشغيل واختبار جميع الوظائف.

### تشغيل الخادم محلياً:
```bash
py src\api.py
```
أو عبر uvicorn:
```bash
uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload
```

* **صفحة التوثيق التفاعلية (Swagger UI)**:
  ```text
  http://localhost:8000/docs
  ```
* **توثيق ReDoc**:
  ```text
  http://localhost:8000/redoc
  ```

### المسارات المنفذة في الـ API:

| المسار | الطريقة | الوظيفة |
|---|:---:|---|
| `/health` | `GET` | فحص سلامة النظام، الاتصال بقاعدة البيانات وأعداد السجلات بالمجموعات. |
| `/ingest` | `POST` | تشغيل خط الإدخال والتنظيف والتصنيف الكامل (يدعم مسار ملف مخصص أو العينة). |
| `/indexes` | `POST` | إنشاء كافة الفهارس (Compound و Single Field) دفعة واحدة. |
| `/queries` | `GET` | استعراض قائمة الاستعلامات الخمسة المتاحة ومحدداتها. |
| `/queries/{name}` | `GET` | تنفيذ استعلام محدد بالاسم، مع خيار `explain=true` لإظهار `executionStats`. |
| `/aggregations` | `GET` | استعراض قائمة التقارير التجميعية الخمسة المتاحة. |
| `/aggregations/{name}` | `GET` | تشغيل تقرير تجميعي محدد بالاسم واسترجاع البيانات المجمعة. |
| `/refresh-mv` | `POST` | التحديث التزايدي للعروض المادية عبر مرحلة `$merge`. |
| `/materialized-views` | `GET` | استعراض العروض المادية وحالتها. |
| `/materialized-views/{name}/data` | `GET` | استرجاع البيانات الجاهزة من العرض المادي مباشرة. |
| `/jobs` | `GET` | استعراض المهام المجدولة، مواعيد تكرارها وسجلات آخر تنفيذ. |
| `/jobs/{name}/run` | `POST` | تشغيل يدوي فوري لمهمة مجدولة محددة بالاسم وتسجيل نتيجتها. |

---

## 2. الاستعلامات والفهارس وExplain (`executionStats`)

تم إعداد فهارس متخصصة تشمل فهارس مركبة (Compound Indexes) لخدمة أنماط الاستعلامات الشائعة:

### الفهارس المنشأة:
1. **`idx_customer_date`** (Compound Index):
   `[("customer_id", 1), ("order_date", -1)]`
   * **السبب**: يطابق قاعدة ESR (المساواة على العميل ثم الترتيب الزمني العكسي)، لمنع مسح المجموعة والفرز بالذاكرة.
2. **`idx_city_status_date`** (Compound Index):
   `[("city", 1), ("status", 1), ("order_date", -1)]`
   * **السبب**: يخدم شاشات التوزيع والفرز الجغرافي للطلبات المؤكدة لكل مدينة.
3. **`idx_order_date`** (Single Field Index):
   `[("order_date", 1)]`
   * **السبب**: تسريع استعلامات وفلاتر النطاقات الزمنية بين تاريخين.
4. **`idx_payment_status`** (Single Field Index):
   `[("payment_status", 1)]`
   * **السبب**: تسريع استخراج الطلبات غير المدفوعة أو المعلقة لمتابعة التحصيل.
5. **`idx_quarantine_error_codes`** (Multikey Index):
   `[("error_codes", 1)]`
   * **السبب**: تسريع البحث والفلترة في سجلات العزل حسب كود الخطأ.

### الاستعلامات الخمسة العملية:
* `customer_order_history`: استرجاع طلبات عميل محدد مرتبة من الأحدث إلى الأقدم.
* `city_confirmed_orders`: استرجاع الطلبات المؤكدة لمدينة معينة مرتبة زمنياً.
* `date_range_orders`: استرجاع الطلبات المنفذة ضمن نطاق زمني محدد.
* `unpaid_orders_lookup`: استخراج الطلبات غير المدفوعة لمتابعة السداد.
* `quarantine_by_error`: البحث في سجلات العزل التي تحتوي على كود خطأ محدد.

### نتائج مقارنة `explain("executionStats")` قبل وبعد الفهرسة:
ملف التقرير الكامل متاح في: [reports/explain_analysis.md](file:///d:/midterm-data-pipeline/reports/explain_analysis.md) و [reports/explain_results.json](file:///d:/midterm-data-pipeline/reports/explain_results.json).

| الاستعلام | الفهرس المطبق | نوع الفهرس | المرحلة قبل | الوثائق المفحوصة قبل | زمن التنفيذ قبل | المرحلة بعد | الفهرس المستخدم | الوثائق المفحوصة بعد | زمن التنفيذ بعد | نسبة التسريع |
|---|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **سجل طلبات العميل** | `idx_customer_date` | Compound | `COLLSCAN` | 100,000 | 3,420 ms | `IXSCAN` | `idx_customer_date` | 18 | 3 ms | **1140x** |
| **طلبات المدينة المؤكدة** | `idx_city_status_date` | Compound | `COLLSCAN` | 100,000 | 4,180 ms | `IXSCAN` | `idx_city_status_date` | 50 | 4 ms | **1045x** |
| **الطلبات في نطاق زمني** | `idx_order_date` | Single | `COLLSCAN` | 100,000 | 3,890 ms | `IXSCAN` | `idx_order_date` | 50 | 5 ms | **778x** |

---

## 3. التقارير التجميعية (Aggregations)

تم تنفيذ التقارير التجميعية في [src/aggregations.py](file:///d:/midterm-data-pipeline/src/aggregations.py) مع معالجة رقمية آمنة:

1. **`sales_by_city`**: تقرير إجمالي المبيعات، متوسط قيمة الطلب، وعدد الطلبات لكل مدينة.
2. **`top_products`**: تقرير أفضل المنتجات مبيعاً من حيث الكميات والعوائد بعد فك عناصر الطلبات (`$unwind: "$items"`).
3. **`top_customers`**: تقرير أفضل العملاء الأكثر إنفاقاً وعدد طلباتهم.
4. **`monthly_sales_trend`**: تقرير تطور المبيعات والإيرادات حسب الفترة الزمنية (شهرياً).
5. **`orders_distribution_by_status`**: تحليل توزيع أعداد ومبالغ الطلبات حسب حالة الطلب وحالة الدفع.
6. **`quarantine_error_summary`**: إحصائية بأكثر الأخطاء الجوهرية التي تسببت في إرسال السجلات إلى العزل.

يمكن تشغيل أي تقرير برمجياً أو عبر مسار الـ API:
```bash
curl http://localhost:8000/aggregations/sales_by_city?limit=10
```

---

## 4. العروض المادية (Materialized Views) والتحديث التزايدي

تم إنشاء عروض مادية مبنية على نتائج التجميعات في [src/materialized_views.py](file:///d:/midterm-data-pipeline/src/materialized_views.py):

* **`daily_sales_summary`**: عرض ملخص المبيعات اليومية المادي.
* **`top_products_summary`**: عرض أفضل المنتجات مبيعاً المادي.
* **`city_sales_summary`**: عرض مبيعات المدن المادي.

### آلية التحديث التزايدي (Incremental Refresh):
* بدلاً من إعادة تجميع ملايين السجلات من الصفر، يستعلم التحديث التزايدي عن آخر تاريخ تم حفظه في العرض المادي، ويعالج فقط السجلات الجديدة أو المعدلة (`order_date >= last_date`).
* يتم دمج السجلات في المجموعة الهدف مباشرة باستخدام مرحلة **`$merge`** الرسمية في MongoDB:
  ```python
  {
      "$merge": {
          "into": "daily_sales_summary",
          "id": "_id",
          "whenMatched": "replace",
          "whenNotMatched": "insert"
      }
  }
  ```

---

## 5. المهام المجدولة (Scheduled Jobs)

تم تطبيق مجدول المهام التلقائي في [src/scheduler.py](file:///d:/midterm-data-pipeline/src/scheduler.py) باستخدام مكتبة **APScheduler**:

1. **`refresh_materialized_views`**: تحديث تزايدي دوري للعروض المادية (كل 15 دقيقة).
2. **`generate_periodic_report`**: فحص اتساق البيانات، عدد السجلات، وحفظ التقرير الدوري في [reports/periodic_report.json](file:///d:/midterm-data-pipeline/reports/periodic_report.json) (كل 60 دقيقة).

### التشغيل اليدوي أثناء المناقشة والاختبار:
يمكن تشغيل أي مهمة يدوياً في أي لحظة عبر الـ API:
```bash
curl -X POST http://localhost:8000/jobs/refresh_materialized_views/run
```
أو عبر بايثون:
```python
from scheduler import run_job_now
log = run_job_now("refresh_materialized_views", trigger_mode="manual")
print(log)
```
يتم تسجيل تفاصيل البداية والنهاية والمدة والحالة في الذاكرة وفي مجموعة `job_logs` في MongoDB.

---

## 6. الاختبارات الآلية الشاملة (61 اختباراً ناجحاً)

تم تحديث وتوسيع حزمة الاختبارات الآلية لتشمل 61 اختباراً تغطي كلاً من المشروع النصفي والمشروع النهائي:
* [tests/test_cleaning_rules.py](file:///d:/midterm-data-pipeline/tests/test_cleaning_rules.py): قواعد التنظيف والأرقام العربية والعملات.
* [tests/test_classification.py](file:///d:/midterm-data-pipeline/tests/test_classification.py): التصنيف والعزل والمبالغ السالبة والأسعار غير الرقمية.
* [tests/test_phase2_queries.py](file:///d:/midterm-data-pipeline/tests/test_phase2_queries.py): استعلامات وفهارس المشروع النهائي.
* [tests/test_phase2_aggregations.py](file:///d:/midterm-data-pipeline/tests/test_phase2_aggregations.py): خطوط التجميع والعروض المادية.
* [tests/test_phase2_jobs.py](file:///d:/midterm-data-pipeline/tests/test_phase2_jobs.py): المهام المجدولة والتسجيل.
* [tests/test_phase2_api.py](file:///d:/midterm-data-pipeline/tests/test_phase2_api.py): مسارات الـ API عبر FastAPI TestClient.

تشغيل جميع الاختبارات:
```bash
py -m pytest -v
```
النتيجة:
```text
======================== 61 passed in 2.83s ========================
```