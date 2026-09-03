"""
الموجّه التلقائي للمحرك.
يختار Python Batch للملفات الصغيرة وPySpark للملفات الكبيرة من نقطة قرار واحدة.
"""

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from config import settings  # noqa: E402


def generate_run_id() -> str:
    """إنشاء معرف ثابت وفريد لكل تشغيل."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"run_{timestamp}_{uuid.uuid4().hex[:8]}"


def choose_engine(file_path: Path, threshold_mb: float | None = None) -> dict:
    """فحص حجم الملف واختيار المحرك مع تسجيل سبب القرار."""
    threshold_mb = (
        settings.SMALL_FILE_THRESHOLD_MB if threshold_mb is None else threshold_mb
    )
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"الملف غير موجود: {file_path}")
    if not file_path.is_file():
        raise IsADirectoryError(f"المسار ليس ملفًا: {file_path}")

    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    if file_size_mb <= threshold_mb:
        engine_used = "python_batch"
        reason = (
            f"حجم الملف ({file_size_mb:.2f} MB) أقل من أو يساوي الحد "
            f"({threshold_mb:.2f} MB)، لذلك Python Batch مناسب."
        )
    else:
        engine_used = "pyspark"
        reason = (
            f"حجم الملف ({file_size_mb:.2f} MB) أكبر من الحد "
            f"({threshold_mb:.2f} MB)، لذلك يلزم PySpark للتوازي."
        )

    return {
        "run_id": generate_run_id(),
        "file_path": str(file_path),
        "file_name": file_path.name,
        "file_size_mb": round(file_size_mb, 3),
        "threshold_mb": float(threshold_mb),
        "engine_used": engine_used,
        "reason": reason,
        "decided_at": datetime.now(timezone.utc).isoformat(),
    }


def print_decision(decision: dict) -> None:
    print("== قرار الـ Router ==")
    for key in (
        "run_id", "file_name", "file_size_mb", "threshold_mb", "engine_used", "reason"
    ):
        print(f"{key:<14}: {decision[key]}")


def main() -> None:
    parser = argparse.ArgumentParser(description="اختيار محرك المعالجة حسب حجم الملف")
    parser.add_argument("--file", required=True, help="مسار ملف CSV")
    parser.add_argument("--threshold-mb", type=float, default=None)
    args = parser.parse_args()
    decision = choose_engine(Path(args.file), args.threshold_mb)
    print_decision(decision)
    print("\nالمحرك جاهز للربط عبر src/main.py.")


if __name__ == "__main__":
    main()
