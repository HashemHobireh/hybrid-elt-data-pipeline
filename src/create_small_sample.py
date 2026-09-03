"""
create_small_sample.py
-----------------------
يستخرج عينة صغيرة قابلة لإعادة الإنتاج من الملف الضخم، دون تحميل الملف
كاملاً إلى الذاكرة (Streaming عبر csv module)، ودون أي تدخل يدوي (ممنوع Excel).

الاستخدام (حسب صيغة المشروع في القسم 6.1):
    python create_small_sample.py --input orders_huge_mixed_quality.csv --rows 100000

خيارات إضافية:
    --mode head     : يأخذ أول N صف بعد الهيدر (الافتراضي، حتمي 100%)
    --mode random   : عينة عشوائية موزعة عبر كامل الملف (Reservoir Sampling)
                       باستخدام --seed ثابت لضمان إعادة الإنتاج نفسها في كل مرة
    --output        : مسار ملف الإخراج (افتراضيًا data/orders_small_sample.csv)
"""

import argparse
import csv
import random
import sys
import time
from pathlib import Path

# السماح باستيراد config عند تشغيل السكربت مباشرة من src/
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import settings  # noqa: E402


def sample_head(input_path: Path, output_path: Path, rows: int) -> dict:
    """يأخذ أول N صف بعد الهيدر، بأسلوب Streaming (سطر بسطر)."""
    written = 0
    with open(input_path, "r", encoding="utf-8", errors="replace", newline="") as f_in, \
         open(output_path, "w", encoding="utf-8", newline="") as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)

        header = next(reader)
        writer.writerow(header)

        for row in reader:
            if written >= rows:
                break
            writer.writerow(row)
            written += 1

    return {"mode": "head", "rows_written": written}


def sample_random(input_path: Path, output_path: Path, rows: int, seed: int) -> dict:
    """
    Reservoir Sampling: عينة عشوائية موزعة على كامل الملف بمرور واحد فقط،
    دون معرفة عدد الصفوف مسبقًا ودون تحميل الملف كامل بالذاكرة.
    نفس seed => نفس النتيجة دائمًا (قابلية إعادة الإنتاج).
    """
    rng = random.Random(seed)
    reservoir = []

    with open(input_path, "r", encoding="utf-8", errors="replace", newline="") as f_in:
        reader = csv.reader(f_in)
        header = next(reader)

        for i, row in enumerate(reader):
            if i < rows:
                reservoir.append(row)
            else:
                j = rng.randint(0, i)
                if j < rows:
                    reservoir[j] = row

    with open(output_path, "w", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out)
        writer.writerow(header)
        writer.writerows(reservoir)

    return {"mode": "random", "seed": seed, "rows_written": len(reservoir)}


def create_small_sample(input_path: Path, output_path: Path, rows: int,
                         mode: str = "head", seed: int = 42) -> dict:
    if not input_path.exists():
        raise FileNotFoundError(f"الملف المصدر غير موجود: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    start = time.time()
    if mode == "head":
        result = sample_head(input_path, output_path, rows)
    elif mode == "random":
        result = sample_random(input_path, output_path, rows, seed)
    else:
        raise ValueError(f"mode غير معروف: {mode}")
    elapsed = time.time() - start

    result.update({
        "input_path": str(input_path),
        "output_path": str(output_path),
        "elapsed_seconds": round(elapsed, 3),
        "input_size_mb": round(input_path.stat().st_size / (1024 * 1024), 3),
        "output_size_mb": round(output_path.stat().st_size / (1024 * 1024), 3),
    })
    return result


def main():
    parser = argparse.ArgumentParser(description="استخراج عينة صغيرة قابلة لإعادة الإنتاج من ملف ضخم")
    parser.add_argument("--input", type=str, default=str(settings.SOURCE_FILE_PATH),
                         help="مسار الملف الضخم المصدر")
    parser.add_argument("--rows", type=int, default=100000,
                         help="عدد الصفوف المطلوبة في العينة")
    parser.add_argument("--output", type=str, default=str(settings.SAMPLE_FILE_PATH),
                         help="مسار ملف العينة الناتج")
    parser.add_argument("--mode", type=str, choices=["head", "random"], default="head",
                         help="head: أول N صف | random: عينة عشوائية موزعة (Reservoir Sampling)")
    parser.add_argument("--seed", type=int, default=42,
                         help="Seed ثابت لضمان إعادة إنتاج نفس العينة العشوائية")
    args = parser.parse_args()

    result = create_small_sample(
        input_path=Path(args.input),
        output_path=Path(args.output),
        rows=args.rows,
        mode=args.mode,
        seed=args.seed,
    )

    print("== تم إنشاء العينة الصغيرة ==")
    for k, v in result.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
