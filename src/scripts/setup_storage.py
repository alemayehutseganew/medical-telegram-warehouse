from __future__ import annotations

from pathlib import Path

from src.config import get_settings


def main() -> None:
    settings = get_settings()
    targets = [
        settings.data_root,
        settings.raw_json_root,
        settings.raw_image_root,
        settings.telegram_session_path.parent,
        settings.yolo_output_path.parent,
    ]
    for target in targets:
        target.mkdir(parents=True, exist_ok=True)
    print("Created storage folders:")
    for target in targets:
        print(" -", target)


if __name__ == "__main__":
    main()
