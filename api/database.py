from __future__ import annotations

from sqlalchemy.orm import Session, sessionmaker

from src.db import get_engine

engine = get_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
