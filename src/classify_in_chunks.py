"""تشغيل تصنيف Raw الكبير على نطاقات صفوف مع سجل تقدم قابل للاستئناف."""

import argparse
import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings  # noqa: E402
from main import run_pipeline  # noqa: E402


def load_progress(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, list) else []
    except json.JSONDecodeError:
        return []


def save_progress(path: Path, progress: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(progress, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def classify_chunks(run_id: str, start_row: int, end_row: int, chunk_size: int,
                    batch_size: int, progress_every_batches: int, progress_path: Path) -> None:
    progress = load_progress(progress_path)
    completed = {
        (item.get("start_row"), item.get("end_row"))
        for item in progress if item.get("status") == "completed"
    }
    total_chunks = (end_row - start_row + chunk_size - 1) // chunk_size

    for index, chunk_start in enumerate(range(start_row, end_row, chunk_size), start=1):
        chunk_end = min(chunk_start + chunk_size, end_row)
        key = (chunk_start, chunk_end)
        if key in completed:
            print(f"[تخطي] الجزء {index}/{total_chunks}: {chunk_start} إلى {chunk_end - 1}")
            continue

        print(f"\n===== الجزء {index}/{total_chunks}: الصفوف {chunk_start} إلى {chunk_end - 1} =====")
        started_at = time.time()
        entry = {
            "run_id": run_id,
            "chunk_number": index,
            "start_row": chunk_start,
            "end_row": chunk_end,
            "status": "running",
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        progress = [
            item for item in progress
            if not (item.get("start_row") == chunk_start and item.get("end_row") == chunk_end)
        ]
        progress.append(entry)
        save_progress(progress_path, progress)

        try:
            result = run_pipeline(
                input_path=None,
                batch_size=batch_size,
                classify_run_id=run_id,
                progress_every_batches=progress_every_batches,
                start_row=chunk_start,
                end_row=chunk_end,
            )
            entry.update({
                "status": "completed",
                "elapsed_seconds": round(time.time() - started_at, 3),
                "rows_read": result["classification"]["rows_read"],
                "classification": result["classification"],
            })
            save_progress(progress_path, progress)
            print(f"[تم] الجزء {index}/{total_chunks} اكتمل وحُفظ في {progress_path}")
        except Exception as exc:
            entry.update({
                "status": "failed",
                "elapsed_seconds": round(time.time() - started_at, 3),
                "error_type": type(exc).__name__,
                "error": str(exc),
            })
            save_progress(progress_path, progress)
            print(f"[توقف] الجزء {index} فشل. أعد تشغيل نفس الأمر بعد معالجة السبب؛ الجزء سيُعاد بأمان.")
            raise

    completed_count = sum(1 for item in progress if item.get("status") == "completed")
    print(f"\nاكتملت الأجزاء: {completed_count}/{total_chunks}")
    print(f"ملف سجل التقدم: {progress_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="تصنيف Raw الكبير على أجزاء موثقة")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-row", type=int, default=1)
    parser.add_argument("--end-row", type=int, required=True)
    parser.add_argument("--chunk-size", type=int, default=1_000_000)
    parser.add_argument("--batch-size", type=int, default=settings.BATCH_SIZE)
    parser.add_argument("--progress-every-batches", type=int, default=20)
    parser.add_argument(
        "--progress-file", type=Path,
        default=settings.REPORTS_DIR / "chunk_progress.json",
    )
    args = parser.parse_args()
    if args.start_row < 1 or args.end_row <= args.start_row or args.chunk_size < 1:
        raise ValueError("يجب أن يكون start_row >= 1 وend_row أكبر منه وchunk_size موجبًا")
    settings.ensure_directories()
    classify_chunks(
        args.run_id, args.start_row, args.end_row, args.chunk_size,
        args.batch_size, args.progress_every_batches, args.progress_file,
    )


if __name__ == "__main__":
    main()
