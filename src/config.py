from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    telegram_api_id: int = Field(default=0, alias="TELEGRAM_API_ID")
    telegram_api_hash: str = Field(default="", alias="TELEGRAM_API_HASH")
    telegram_phone_number: str = Field(default="", alias="TELEGRAM_PHONE_NUMBER")

    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="telegram", alias="POSTGRES_DB")
    postgres_user: str = Field(default="warehouse", alias="POSTGRES_USER")
    postgres_password: str = Field(default="warehouse", alias="POSTGRES_PASSWORD")

    data_root: Path = Field(default=Path("data"), alias="DATA_ROOT")
    yolo_model_path: Path = Field(default=Path("weights/yolov8n.pt"), alias="YOLO_MODEL_PATH")

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    @property
    def telegram_session_path(self) -> Path:
        return self.data_root / "sessions" / "telegram"

    @property
    def raw_json_root(self) -> Path:
        return self.data_root / "raw" / "telegram_messages"

    @property
    def raw_image_root(self) -> Path:
        return self.data_root / "raw" / "images"

    @property
    def yolo_output_path(self) -> Path:
        return self.data_root / "yolo" / "detections.csv"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load environment variables once per process."""
    return Settings()  # type: ignore[arg-type]
