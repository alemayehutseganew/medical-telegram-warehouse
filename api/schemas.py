from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel


class TopProduct(BaseModel):
    term: str
    mentions: int


class ChannelActivityPoint(BaseModel):
    date: date
    posts: int
    avg_views: Optional[float]


class ChannelActivityResponse(BaseModel):
    channel_name: str
    total_posts: int
    avg_views: Optional[float]
    trend: List[ChannelActivityPoint]


class MessageOut(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
    message_date: datetime
    view_count: Optional[int]


class VisualContentStat(BaseModel):
    channel_name: str
    promotional_views: Optional[float]
    product_display_views: Optional[float]
    lifestyle_views: Optional[float]
    other_views: Optional[float]
