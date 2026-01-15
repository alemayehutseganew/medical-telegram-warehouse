from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


def partition_path(root: Path, dt: datetime, channel: str) -> Path:
    partition = root / dt.strftime("%Y-%m-%d")
    partition.mkdir(parents=True, exist_ok=True)
    filename = f"{channel.lower().replace(' ', '_')}.json"
    return partition / filename


def write_jsonl(path: Path, records: Iterable[Dict[str, Any]]) -> None:
    rows: List[str] = []
    for record in records:
        rows.append(json.dumps(record, ensure_ascii=False))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[\n" + ",\n".join(rows) + "\n]\n", encoding="utf-8")
