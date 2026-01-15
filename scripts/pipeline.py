from __future__ import annotations

import subprocess
from pathlib import Path

from dagster import Definitions, ScheduleDefinition, job, op

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run(cmd: list[str], *, cwd: Path | None = None, env: dict | None = None) -> None:
    subprocess.run(cmd, check=True, cwd=cwd or PROJECT_ROOT, env=env)


@op
def scrape_telegram_data() -> None:
    _run([
        "python",
        "-m",
        "src.scraper",
        "--channels",
        "data/config/channels.yml",
        "--days",
        "2",
    ])


@op
def load_raw_to_postgres() -> None:
    _run(
        [
            "python",
            "-m",
            "src.load_to_postgres",
            "--schema",
            "raw",
            "--table",
            "telegram_messages",
            "--mode",
            "json",
            "--source",
            "data/raw/telegram_messages",
        ]
    )


@op
def run_dbt_transformations() -> None:
    project_dir = PROJECT_ROOT / "medical_warehouse"
    _run(["dbt", "deps"], cwd=project_dir)
    _run(["dbt", "run"], cwd=project_dir)
    _run(["dbt", "test"], cwd=project_dir)


@op
def run_yolo_enrichment() -> None:
    _run(
        [
            "python",
            "-m",
            "src.yolo_detect",
            "--image-root",
            "data/raw/images",
            "--output",
            "data/yolo/detections.csv",
        ]
    )
    _run(
        [
            "python",
            "-m",
            "src.load_to_postgres",
            "--schema",
            "raw",
            "--table",
            "image_detections",
            "--mode",
            "csv",
            "--source",
            "data/yolo/detections.csv",
        ]
    )


@job
def medical_telegram_job() -> None:
    scrape_telegram_data()
    load_raw_to_postgres()
    run_dbt_transformations()
    run_yolo_enrichment()


definitions = Definitions(
    jobs=[medical_telegram_job],
    schedules=[
        ScheduleDefinition(
            job=medical_telegram_job,
            cron_schedule="0 4 * * *",
            execution_timezone="UTC",
            name="daily_medical_telegram",
        )
    ],
)
