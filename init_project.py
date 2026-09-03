"""
init_project.py
----------------
يبني هيكل مجلدات المشروع فارغة تلقائيًا (بدل إنشائها يدويًا واحدة واحدة).
شغّله مرة واحدة فقط داخل مجلد midterm-data-pipeline بعد ما تحط فيه
الملفات الأربعة الجاهزة (config/settings.py, src/create_small_sample.py,
notebooks/01_setup_and_sample.ipynb, requirements.txt).

الاستخدام:
    python init_project.py
"""

from pathlib import Path

# المجلدات المطلوبة حسب هيكلة المشروع (القسم 8 من وثيقة التكليف)
FOLDERS = [
    "config",
    "data",
    "src",
    "tests",
    "reports",
    "reports/screenshots",
    "docs",
    "notebooks",
]

def main():
    root = Path(__file__).resolve().parent
    print(f"جاري بناء الهيكل داخل: {root}\n")

    for folder in FOLDERS:
        path = root / folder
        path.mkdir(parents=True, exist_ok=True)
        # ملف .gitkeep فاضي عشان المجلدات الفاضية تنحفظ في git ولا تختفي
        keep_file = path / ".gitkeep"
        if not any(path.iterdir()):
            keep_file.touch()
        print(f"[OK] {folder}/")

    print("\nتم بناء الهيكل بنجاح.")
    print("تأكد الآن أن هذه الملفات موجودة في مكانها الصحيح:")
    print("  - config/settings.py")
    print("  - src/create_small_sample.py")
    print("  - notebooks/01_setup_and_sample.ipynb")
    print("  - requirements.txt (في جذر المشروع)")


if __name__ == "__main__":
    main()
