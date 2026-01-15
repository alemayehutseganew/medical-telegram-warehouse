from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .config import get_settings


def build_connection_uri() -> str:
    settings = get_settings()
    return (
        f"postgresql+psycopg2://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
    )


def get_engine(echo: bool = False) -> Engine:
    return create_engine(build_connection_uri(), echo=echo, future=True)
