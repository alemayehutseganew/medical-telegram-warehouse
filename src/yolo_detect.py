from __future__ import annotations

import argparse
import inspect
import logging
import os
from pathlib import Path
from typing import List

os.environ.setdefault("TORCH_LOAD_WEIGHTS_ONLY", "0")

import pandas as pd
import torch
from torch.nn.modules.container import Sequential
from torch.serialization import add_safe_globals
from ultralytics import YOLO
from ultralytics.nn.tasks import DetectionModel

from .config import get_settings
from .logger import configure_logging

logger = logging.getLogger(__name__)

# Torch 2.6+ defaults to weights_only=True; allow required Ultralytics classes through torch's allow-list
# and patch torch.load to fall back to the legacy behavior when needed.
add_safe_globals([DetectionModel, Sequential])
if "weights_only" in inspect.signature(torch.load).parameters:
    _orig_torch_load = torch.load

    def _patched_torch_load(*args, **kwargs):
        kwargs.setdefault("weights_only", False)
        return _orig_torch_load(*args, **kwargs)

    torch.load = _patched_torch_load

PRODUCT_LABELS = {"bottle", "cup", "vase", "handbag", "backpack", "book", "laptop", "cell phone"}


def derive_category(labels: List[str]) -> str:
    has_person = "person" in labels
    has_product = any(label in PRODUCT_LABELS for label in labels)
    if has_person and has_product:
        return "promotional"
    if has_product:
        return "product_display"
    if has_person:
        return "lifestyle"
    return "other"


def detect(image_root: Path, output_path: Path, model_path: Path, conf: float) -> None:
    model = YOLO(str(model_path))
    rows: List[dict] = []
    for image_path in image_root.rglob("*.jpg"):
        try:
            message_id = int(image_path.stem)
        except ValueError:
            logger.warning("Skipping %s because filename is not numeric", image_path)
            continue
        channel_name = image_path.parent.name
        results = model.predict(source=str(image_path), conf=conf, verbose=False)
        labels: List[str] = []
        for result in results:
            names = result.names
            for box in result.boxes:
                label_idx = int(box.cls.item())
                label = names[label_idx]
                labels.append(label)
                rows.append(
                    {
                        "message_id": message_id,
                        "channel_name": channel_name,
                        "image_path": str(image_path),
                        "label": label,
                        "confidence": float(box.conf.item()),
                        "image_category": None,  # placeholder
                    }
                )
        if not labels:
            rows.append(
                {
                    "message_id": message_id,
                    "channel_name": channel_name,
                    "image_path": str(image_path),
                    "label": "none",
                    "confidence": 0.0,
                    "image_category": derive_category(labels),
                }
            )
        else:
            category = derive_category(labels)
            for row in rows:
                if row["message_id"] == message_id and row["image_category"] is None:
                    row["image_category"] = category
    if not rows:
        logger.warning("No detections generated")
        return
    df = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Saved %s detections to %s", len(df), output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run YOLO detections on scraped images")
    parser.add_argument("--image-root", type=str, help="Root folder with channel subdirectories")
    parser.add_argument("--output", type=str, help="CSV output path")
    parser.add_argument("--model", type=str, help="Path to YOLOv8 model weights", default=None)
    parser.add_argument("--conf", type=float, default=0.35, help="Confidence threshold")
    args = parser.parse_args()

    settings = get_settings()
    image_root = Path(args.image_root or settings.raw_image_root)
    output_path = Path(args.output or settings.yolo_output_path)
    model_path = Path(args.model or settings.yolo_model_path)

    configure_logging(Path("logs/yolo.log"))
    detect(image_root, output_path, model_path, args.conf)


if __name__ == "__main__":
    main()
