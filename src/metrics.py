"""حفظ مقاييس التشغيل بصيغة JSON قابلة للمراجعة والتسليم."""

import json
from datetime import datetime, timezone
from pathlib import Path


def save_results(result: dict, output_path: Path) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    history = []
    if output_path.exists() and output_path.stat().st_size:
        try:
            existing = json.loads(output_path.read_text(encoding="utf-8"))
            history = existing if isinstance(existing, list) else [existing]
        except json.JSONDecodeError:
            history = []
    record = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        **result,
    }
    history.append(record)
    output_path.write_text(
        json.dumps(history, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return output_path
