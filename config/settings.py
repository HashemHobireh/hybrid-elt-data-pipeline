"""
config/settings.py
-------------------
نقطة التحكم المركزية لكل إعدادات المشروع.
كل القيم القابلة للتغيير (المسارات، الحدود، إعدادات Mongo) توضع هنا
أو تُقرأ من متغيرات بيئة، بدل توزيعها داخل الكود (متطلب القسم 6.2 و 9).
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# مسارات المشروع
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
RESULTS_JSON_PATH = REPORTS_DIR / "results.json"

# اسم الملف الضخم الأصلي المتوقع (يوضع داخل data/ عند استلامه من الدكتور)
SOURCE_FILE_NAME = os.getenv("SOURCE_FILE_NAME", "orders_huge_mixed_quality.csv")
SOURCE_FILE_PATH = DATA_DIR / SOURCE_FILE_NAME

# ملف العينة الصغيرة الناتج عن create_small_sample.py
SAMPLE_FILE_NAME = os.getenv("SAMPLE_FILE_NAME", "orders_small_sample.csv")
SAMPLE_FILE_PATH = DATA_DIR / SAMPLE_FILE_NAME

# ---------------------------------------------------------------------------
# حد الفصل بين المحركين (Router) — القسم 6.2
# ---------------------------------------------------------------------------
# التبرير (يُنقل لاحقًا إلى التقرير النهائي):
# - نضع الحد عند 200MB لأن قراءة ملف بهذا الحجم بأسلوب Streaming عبر Python
#   العادي ما زالت عملية وسريعة على جهاز عادي (بدون تحميله كامل في الذاكرة)،
#   بينما الملفات الأكبر من ذلك تستفيد فعليًا من التوازي في Spark
#   (تعدد الأنوية/الـ partitions) لتقليل زمن التنفيذ.
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", 200))

# ---------------------------------------------------------------------------
# إعدادات Python Batch Loader — القسم 6.3
# ---------------------------------------------------------------------------
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))

# ---------------------------------------------------------------------------
# إعدادات PySpark — القسم 6.4
# ---------------------------------------------------------------------------
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "midterm-data-pipeline")
# في المسار A (عنقود Spark): spark://IP_MASTER:7077
# في التشغيل المحلي (الأساسي الإلزامي): local[*]
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")
SPARK_MONGO_CONNECTOR_PACKAGE = os.getenv(
    "SPARK_MONGO_CONNECTOR_PACKAGE",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
)

# ---------------------------------------------------------------------------
# إعدادات MongoDB — القسم 6.9
# ---------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "midterm_pipeline_db")
MONGO_TIMEOUT_MS = int(os.getenv("MONGO_TIMEOUT_MS", "30000"))

COLLECTION_RAW = "orders_raw"
COLLECTION_VALIDATED = "orders_validated"
COLLECTION_QUARANTINE = "orders_quarantine"

# العملة القياسية الموحدة (القسم 6.6)
STANDARD_CURRENCY = "YER"

# ---------------------------------------------------------------------------
# دالة مساعدة للتأكد من وجود المجلدات المطلوبة قبل أي تشغيل
# ---------------------------------------------------------------------------
def ensure_directories():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / "screenshots").mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    ensure_directories()
    print(f"BASE_DIR = {BASE_DIR}")
    print(f"SOURCE_FILE_PATH = {SOURCE_FILE_PATH}")
    print(f"SMALL_FILE_THRESHOLD_MB = {SMALL_FILE_THRESHOLD_MB}")
    print(f"MONGO_URI = {MONGO_URI}")
