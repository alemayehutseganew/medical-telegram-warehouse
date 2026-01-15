from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List

import yaml
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError

from .config import get_settings
from .logger import configure_logging
from .utils import partition_path

logger = logging.getLogger(__name__)


def load_channels(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    channels: Iterable[str] = payload.get("channels", [])
    return [ch.strip() for ch in channels if ch]


class TelegramScraper:
    def __init__(self, session_path: Path) -> None:
        self.settings = get_settings()
        session_path.parent.mkdir(parents=True, exist_ok=True)
        self.client = TelegramClient(
            session_path,
            self.settings.telegram_api_id,
            self.settings.telegram_api_hash,
        )

    async def __aenter__(self) -> "TelegramScraper":
        await self.client.connect()
        if not await self.client.is_user_authorized():
            await self.client.send_code_request(self.settings.telegram_phone_number)
            raise RuntimeError("Phone verification required. Run the scraper interactively once.")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        await self.client.disconnect()

    async def scrape_channel(self, channel: str, days: int, limit: int) -> Path | None:
        horizon = datetime.now(timezone.utc) - timedelta(days=days)
        logger.info("Scraping %s", channel)
        records: List[dict] = []
        image_dir = get_settings().raw_image_root / channel.replace("https://t.me/", "")
        image_dir.mkdir(parents=True, exist_ok=True)

        try:
            async for message in self.client.iter_messages(channel, limit=limit):
                if message.date.tzinfo is None:
                    message_ts = message.date.replace(tzinfo=timezone.utc)
                else:
                    message_ts = message.date.astimezone(timezone.utc)
                if message_ts < horizon:
                    break

                has_media = bool(message.media)
                image_path = None
                if has_media:
                    dest = image_dir / f"{message.id}.jpg"
                    await self.client.download_media(message, file=str(dest))
                    image_path = str(dest)

                records.append(
                    {
                        "message_id": message.id,
                        "channel_name": channel if "http" not in channel else channel.split("/")[-1],
                        "message_date": message_ts.isoformat(),
                        "message_text": message.message,
                        "has_media": has_media,
                        "image_path": image_path,
                        "views": getattr(message, "views", 0) or 0,
                        "forwards": getattr(message, "forwards", 0) or 0,
                    }
                )
        except FloodWaitError as exc:
            logger.warning("Flood wait for %s seconds", exc.seconds)
            await asyncio.sleep(exc.seconds)
        except RPCError as exc:
            logger.error("Failed to scrape %s: %s", channel, exc)
            return None

        if not records:
            logger.info("No records for %s", channel)
            return None

        first_ts = datetime.fromisoformat(records[0]["message_date"])
        output_path = partition_path(get_settings().raw_json_root, first_ts, records[0]["channel_name"])
        output_path.write_text(json.dumps(records, indent=2), encoding="utf-8")
        logger.info("Wrote %s records to %s", len(records), output_path)
        return output_path


async def run(args: argparse.Namespace) -> None:
    settings = get_settings()
    configure_logging(Path("logs/scraper.log"))
    channels = load_channels(Path(args.channels))
    session_path = settings.telegram_session_path
    async with TelegramScraper(session_path) as scraper:
        tasks = [scraper.scrape_channel(ch, args.days, args.limit) for ch in channels]
        await asyncio.gather(*tasks)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Telegram medical commerce channels")
    parser.add_argument("--channels", type=str, required=True, help="Path to YAML file with channels list")
    parser.add_argument("--days", type=int, default=3, help="Lookback window in days")
    parser.add_argument("--limit", type=int, default=500, help="Message limit per channel")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
