# Midterm Data Pipeline

مشروع فردي لمقرر **البيانات الضخمة – العملي** لبناء خط بيانات هجين لمعالجة ملف طلبات CSV غير نظيف باستخدام Python Batch وApache Spark وMongoDB وفق نمط ELT.

> المبدأ الأساسي: تصل كل السجلات أولًا إلى `orders_raw` كما وردت، ثم تُصنّف لاحقًا إلى `orders_validated` أو `orders_quarantine` مع سبب واضح للعزل.

## المتطلبات

يحتاج المشروع إلى Python 3.10 أو أحدث، وMongoDB يعمل محليًا على `mongodb://localhost:27017`. يعالج Python Batch الملفات الصغيرة، بينما يستخدم PySpark الملفات التي يتجاوز حجمها الحد المحدد في `config/settings.py`. يجب تثبيت Java المناسب لـPySpark عند تشغيل مسار Spark.

## التثبيت على Windows

افتح CMD أو Terminal داخل مجلد المشروع ثم نفّذ الأوامر التالية واحدًا بعد الآخر:

```cmd
py -m pip install -r requirements.txt
py -m pytest -q
```

النجاح المتوقع للاختبارات هو ظهور عدد الاختبارات مع كلمة `passed`.

## بنية المشروع

```text
midterm-data-pipeline/
|-- README.md
|-- requirements.txt
|-- config/settings.py
|-- data/
|-- src/
|   |-- main.py
|   |-- file_router.py
|   |-- create_small_sample.py
|   |-- batch_loader.py
|   |-- spark_loader.py
|   |-- quality_rules.py
|   |-- elt_pipeline.py
|   |-- mongo_setup.py
|   `-- metrics.py
|-- tests/
|   |-- test_cleaning_rules.py
|   `-- test_classification.py
|-- reports/
|   `-- results.json
|-- docs/architecture.md
`-- notebooks/
```

## إنشاء عينة قابلة لإعادة الإنتاج

لا تُعدّل ملف الدكتور يدويًا ولا تستخدم Excel. أنشئ العينة بالسكربت:

```cmd
py src\create_small_sample.py --input data\orders_huge_mixed_quality.csv --output data\orders_small_sample.csv --rows 100000 --mode head
```

يمكن تغيير عدد الصفوف. ووضع `head` يعيد أخذ أول الصفوف نفسها عند تكرار الأمر.

## نقطة التشغيل الموحدة والـRouter

يفحص `main.py` حجم الملف ويطبع القرار. الحد الافتراضي هو 200 MB، ويمكن تغييره من الإعدادات أو باستخدام `--threshold-mb`.

لتجربة الـRouter والتحميل الخام فقط على العينة:

```cmd
py src\main.py --input data\orders_small_sample.csv --batch-size 5000 --raw-only
```

سيُختار `python_batch` إذا كان حجم الملف أقل من أو يساوي 200 MB، وستظهر الدفعات والمعدل، ثم تُحفظ نتيجة التشغيل في `reports/results.json`.

لتشغيل خط كامل على ملف صغير:

```cmd
py src\main.py --input data\orders_small_sample.csv --batch-size 5000 --progress-every-batches 1
```

لتشغيل الملف الكبير:

```cmd
py src\main.py --input data\orders_huge_mixed_quality.csv --progress-every-batches 20
```

عند تجاوز الملف 200 MB يختار الـRouter `pyspark`. يستخدم هذا المسار `SparkSession` و`DataFrame API` وSchema ثابتة من نوع String للحقول الخام، ثم يكتب إلى MongoDB باستخدام MongoDB Spark Connector.

إذا تم تحميل Raw مسبقًا وتريد تشغيل التصنيف على تشغيل محدد دون إعادة تحميل الملف، استخدم:

```cmd
py src\main.py --classify-run-id RUN_ID --batch-size 5000 --progress-every-batches 1
```

استبدل `RUN_ID` بالمعرف الظاهر في نتيجة التحميل. وللملفات الكبيرة يمكن تحديد نطاق صفوف، مثل تشغيل الصفوف من 1 إلى 1,000,000:

```cmd
py src\main.py --classify-run-id RUN_ID --start-row 1 --end-row 1000001 --batch-size 5000 --progress-every-batches 1
```

النهاية `--end-row` غير شاملة. هذا الأسلوب يجعل كل جزء قابلًا للمراقبة وإعادة التشغيل بأمان باستخدام Upsert.

ولتقسيم التشغيل الكبير تلقائيًا إلى أجزاء مليونية مع حفظ الأجزاء المكتملة في `reports/chunk_progress.json`، استخدم:

```cmd
py src\classify_in_chunks.py --run-id RUN_ID --start-row 1 --end-row 30000001 --chunk-size 1000000 --batch-size 5000 --progress-every-batches 20
```

إذا انقطع جزء، يعاد تشغيل الأمر نفسه بعد معالجة السبب؛ الأجزاء المكتملة تُتخطى تلقائيًا، والجزء غير المكتمل يُعاد بأمان. يجب أن ينتهي السجل بـ `30/30` أجزاء مكتملة، وأن يثبت مجموع الصفوف المقروءة معالجة جميع السجلات.

## طبقات MongoDB

| المجموعة | الغرض |
|---|---|
| `orders_raw` | جميع السجلات الخام مع `run_id` و`source_file` و`source_row_number` و`ingested_at` و`engine_used` و`raw_record`. |
| `orders_validated` | السجلات السليمة أو المصححة، مع `quality_status` و`corrections` وفهرس فريد على `order_id`. |
| `orders_quarantine` | السجلات التي لا يمكن تصحيحها بأمان، مع `error_codes` و`error_details` و`raw_record`. |

تستخدم `orders_validated` عملية `UpdateOne(..., upsert=True)` على `order_id`. أما سجلات Quarantine فتستخدم مفتاح التشغيل ورقم الصف لمنع تكرار نفس سجل العزل عند إعادة التشغيل.

## قواعد الجودة

يطبق المشروع أكثر من ثماني قواعد واضحة، منها تحويل الأرقام العربية، توحيد العملة إلى YER، إزالة فواصل الآلاف، تحويل الكلمات السعرية المحددة، توحيد الهاتف، إصلاح التكرار الواضح في البريد، توحيد التاريخ، قص المسافات والمرادفات، وإعادة حساب الإجمالي من العناصر والتوصيل عند صلاحية المكونات. كل تصحيح يسجل `field` و`original_value` و`corrected_value` و`rule_code` داخل `corrections`.

السجلات ذات المعرفات المفقودة أو JSON التالف أو التاريخ المستحيل أو العناصر الفارغة أو السعر المجهول أو القيم السالبة الملتبسة أو تكرار `order_id` تنتقل إلى Quarantine مع رمز وشرح للخطأ.

## القياسات

يكتب `src/main.py` كل تشغيل في `reports/results.json`. تتضمن النتائج معرف التشغيل، اسم الملف وحجمه، المحرك، عدد الصفوف المقروءة، عدد السجلات الخام، السليم، المصحح، المعزول، الزمن، معدل المعالجة، إعدادات الدفعات أو التقسيمات، رموز الأخطاء، وعدادات `inserted_count` و`updated_count` و`unchanged_count`، إضافة إلى فحص معادلة الاتساق:

```text
raw_count = valid_count + corrected_count + quarantine_count
```

## الاختبارات وإثبات Idempotency

شغّل الاختبارات قبل العرض:

```cmd
py -m pytest -q
```

ولإثبات Idempotency، شغّل التصنيف على نفس `run_id` مرتين، ثم افحص أن عدد سجلات `orders_validated` لا يزيد وأن الفهرس الفريد على `order_id` يمنع تكرار Business Records. اعرض أيضًا سجلًا موجودًا بعد تحديث بياناته لإثبات أن العملية Upsert وليست Insert فقط.

## تسلسل العرض أمام الدكتور

ابدأ بتشغيل العينة وإظهار اختيار Python Batch. اعرض بعدها سجلًا من `orders_raw` لإثبات أن التحميل سبق التنظيف. ثم اعرض مثالًا سليمًا ومثالًا مصححًا يحتوي على Audit Trail ومثالًا معزولًا يحتوي على رموز الخطأ. بعد ذلك شغّل الملف الكبير لإظهار اختيار PySpark وعدد Input Partitions وSpark UI، ثم اعرض المجموعات النهائية و`reports/results.json`. اختم بإعادة التشغيل وإثبات عدم وجود Duplicate.
