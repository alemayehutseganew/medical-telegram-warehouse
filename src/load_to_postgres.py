from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd
from sqlalchemy import text

from .config import get_settings
from .db import get_engine
from .logger import configure_logging

logger = logging.getLogger(__name__)


def read_json_records(root: Path) -> Iterable[Dict]:
    for file in root.rglob("*.json"):
        if file.name.startswith("."):
            continue
        payload = json.loads(file.read_text(encoding="utf-8"))
        for record in payload:
            record["raw_file"] = str(file)
            yield record


def read_csv_records(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def ensure_table(schema: str, table: str, kind: str) -> None:
    ddl_messages = f"""
        create table if not exists {schema}.{table} (
            message_id bigint primary key,
            channel_name text,
            message_date timestamptz,
            message_text text,
            has_media boolean,
            image_path text,
            views integer,
            forwards integer,
            raw_payload jsonb,
            ingested_at timestamptz default now()
        );
    """
    ddl_detections = f"""
        create table if not exists {schema}.{table} (
            message_id bigint,
            channel_name text,
            image_path text not null,
            label text not null,
            confidence double precision,
            image_category text,
            ingested_at timestamptz default now(),
            primary key (message_id, label, image_path)
        );
    """
    ddl = ddl_messages if kind == "json" else ddl_detections
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(ddl))


def load_json(schema: str, table: str, source: Path) -> None:
    ensure_table(schema, table, "json")
    engine = get_engine()
    rows = list(read_json_records(source))
    if not rows:
        logger.warning("No JSON records found under %s", source)
        return
    for row in rows:
        row.setdefault("views", 0)
        row.setdefault("forwards", 0)
    with engine.begin() as conn:
        insert_sql = text(
            f"""
            insert into {schema}.{table} (
                message_id, channel_name, message_date, message_text,
                has_media, image_path, views, forwards, raw_payload
            ) values (
                :message_id, :channel_name, :message_date, :message_text,
                :has_media, :image_path, :views, :forwards, :raw_payload
            ) on conflict (message_id) do nothing
        """
        )
        for record in rows:
            message_date = record.get("message_date")
            if isinstance(message_date, str):
                message_date = datetime.fromisoformat(message_date)
            conn.execute(
                insert_sql,
                {
                    "message_id": record.get("message_id"),
                    "channel_name": record.get("channel_name"),
                    "message_date": message_date,
                    "message_text": record.get("message_text"),
                    "has_media": record.get("has_media", False),
                    "image_path": record.get("image_path"),
                    "views": record.get("views", 0),
                    "forwards": record.get("forwards", 0),
                    "raw_payload": json.dumps(record),
                },
            )
    logger.info("Loaded %s rows into %s.%s", len(rows), schema, table)


def load_csv(schema: str, table: str, csv_path: Path) -> None:
    ensure_table(schema, table, "csv")
    df = read_csv_records(csv_path)
    if df.empty:
        logger.warning("No detection rows in %s", csv_path)
        return
    engine = get_engine()
    with engine.begin() as conn:
        insert_sql = text(
            f"""
            insert into {schema}.{table} (
                message_id, channel_name, image_path, label, confidence, image_category
            ) values (
                :message_id, :channel_name, :image_path, :label, :confidence, :image_category
            ) on conflict (message_id, label, image_path) do nothing
        """
        )
        conn.execute(insert_sql, df.to_dict(orient="records"))
    logger.info("Loaded %s detection rows", len(df))


def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw data into PostgreSQL")
    parser.add_argument("--schema", default="raw")
    parser.add_argument("--table", default="telegram_messages")
    parser.add_argument("--source", type=str, help="Path to data source (JSON dir or CSV)")
    parser.add_argument("--mode", choices=["json", "csv"], default="json")
    args = parser.parse_args()

    configure_logging(Path("logs/loader.log"))
    settings = get_settings()
    source = Path(args.source) if args.source else settings.raw_json_root

    if args.mode == "json":
        load_json(args.schema, args.table, source)
    else:
        load_csv(args.schema, args.table, source)


if __name__ == "__main__":
    main()
