# قائمة تحقق العرض والتسليم

## قبل العرض

- [ ] تشغيل `py -m pytest -q` وإظهار `28 passed`.
- [ ] التأكد من تشغيل MongoDB محليًا.
- [ ] إبقاء ملف الدكتور الكبير خارج GitHub وداخل مجلد `data` محليًا فقط.
- [ ] الاحتفاظ بنسخة من `reports/results.json` و`reports/chunk_progress.json`.

## دليل الـRouter

- [ ] تشغيل العينة الصغيرة وإظهار `file_size_mb` قريبًا من 41.765 و`engine_used: python_batch`.
- [ ] تشغيل الـRouter على الملف الكبير دون تحميله، وإظهار `file_size_mb` قريبًا من 12650.32 و`engine_used: pyspark`.

## دليل طبقة Raw

- [ ] إظهار أن العدد الكلي في `orders_raw` هو 30,200,000.
- [ ] إظهار توزيع `engine_used` الذي يثبت وجود 30,000,000 سجل بمحرك `pyspark` و200,000 بالعينة بمحرك `python_batch`.
- [ ] عرض سجل خام واحد يوضح `run_id` أو `id_run` و`source_file` أو `file_source` ورقم الصف.

## دليل الجودة

- [ ] عرض سجل `corrected` يحتوي على `corrections`، مع الحقول الأصلية والمصححة ورمز القاعدة.
- [ ] عرض سجل `quarantined` يحتوي على `error_codes` و`error_details` و`raw_record`.
- [ ] عرض عدادات الأخطاء وقواعد التصحيح من `results.json`.
- [ ] عرض معادلة الاتساق: `raw_count = valid_count + corrected_count + quarantine_count` مع `equation_holds: true`.

## دليل الأداء وELT

- [ ] عرض أن التشغيل الكبير عولج في 30 جزءًا مليونيا تقريبًا، إضافة إلى الأجزاء الصغيرة السابقة.
- [ ] عرض آخر سجل من `results.json` أو مجموع النتائج الذي يثبت قراءة 30,000,000 صف.
- [ ] عرض `reports/chunk_progress.json` وإظهار أن آخر نطاق ينتهي عند 30,000,001 كحد غير شامل.
- [ ] عرض `orders_validated` و`orders_quarantine` في MongoDB.
- [ ] عرض Unique Index على `orders_validated.order_id`.

## دليل Idempotency

- [ ] تشغيل التصنيف على عينة سبق تشغيلها.
- [ ] إظهار `inserted_count = 0` و`updated_count = 0` وظهور `unchanged_count`.
- [ ] توضيح أن Unique Index يمنع Duplicate Business Records.

## ملفات التسليم

- [ ] `README.md`.
- [ ] `requirements.txt`.
- [ ] مجلد `src` كامل.
- [ ] مجلد `tests` كامل.
- [ ] `docs/architecture.md`.
- [ ] `reports/final_summary.md`.
- [ ] `reports/results.json`.
- [ ] `reports/chunk_progress.json`.
- [ ] عدم رفع ملف CSV الكبير إلى GitHub.
