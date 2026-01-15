from datetime import datetime
from pathlib import Path

from src.utils import partition_path


def test_partition_path(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    dt = datetime(2026, 1, 14)
    path = partition_path(root, dt, "CheMed")
    assert path.parent.name == "2026-01-14"
    assert path.name == "chemed.json"
