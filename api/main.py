from __future__ import annotations

from typing import List

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from .database import get_db
from .schemas import (
    ChannelActivityPoint,
    ChannelActivityResponse,
    MessageOut,
    TopProduct,
    VisualContentStat,
)

app = FastAPI(
    title="Medical Telegram Analytics API",
    description="REST endpoints backed by dbt marts for Week 8 challenge",
    version="0.1.0",
)


@app.get("/api/health")
def healthcheck() -> dict:
    return {"status": "ok"}


@app.get("/api/reports/top-products", response_model=List[TopProduct])
def top_products(limit: int = Query(10, ge=1, le=50), db: Session = Depends(get_db)) -> List[TopProduct]:
    sql = text(
        """
        with tokens as (
            select regexp_replace(lower(trim(token)), '[^a-z0-9]+', '', 'g') as token
            from (
                select regexp_split_to_table(coalesce(message_text, ''), '\\s+') as token
                from marts.fct_messages
            ) s
        )
        select token as term, count(*) as mentions
        from tokens
        where token <> ''
        group by token
        order by mentions desc
        limit :limit
        """
    )
    rows = db.execute(sql, {"limit": limit}).fetchall()
    return [TopProduct(term=row.term, mentions=row.mentions) for row in rows]


@app.get("/api/channels/{channel_name}/activity", response_model=ChannelActivityResponse)
def channel_activity(channel_name: str, db: Session = Depends(get_db)) -> ChannelActivityResponse:
    summary_sql = text(
        """
        select c.channel_name, count(*) as total_posts, avg(f.view_count) as avg_views
        from marts.fct_messages f
        join marts.dim_channels c on f.channel_key = c.channel_key
        where c.channel_name ilike :channel
        group by c.channel_name
        """
    )
    summary = db.execute(summary_sql, {"channel": channel_name}).fetchone()
    if not summary:
        raise HTTPException(status_code=404, detail="Channel not found")

    trend_sql = text(
        """
        select dd.full_date::date as dt, count(*) as posts, avg(f.view_count) as avg_views
        from marts.fct_messages f
        join marts.dim_channels c on f.channel_key = c.channel_key
        join marts.dim_dates dd on f.date_key = dd.date_key
        where c.channel_name ilike :channel
        group by dd.full_date
        order by dt desc
        limit 30
        """
    )
    trend_rows = db.execute(trend_sql, {"channel": channel_name}).fetchall()
    trend = [
        ChannelActivityPoint(date=row.dt, posts=row.posts, avg_views=row.avg_views)
        for row in trend_rows
    ]
    return ChannelActivityResponse(
        channel_name=summary.channel_name,
        total_posts=summary.total_posts,
        avg_views=summary.avg_views,
        trend=trend,
    )


@app.get("/api/search/messages", response_model=List[MessageOut])
def search_messages(
    query: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> List[MessageOut]:
    sql = text(
        """
        select f.message_id, c.channel_name, f.message_text, dd.full_date as message_date, f.view_count
        from marts.fct_messages f
        join marts.dim_channels c on f.channel_key = c.channel_key
        join marts.dim_dates dd on f.date_key = dd.date_key
        where f.message_text ilike :query
        order by f.view_count desc nulls last
        limit :limit
        """
    )
    rows = db.execute(sql, {"query": f"%{query}%", "limit": limit}).fetchall()
    return [
        MessageOut(
            message_id=row.message_id,
            channel_name=row.channel_name,
            message_text=row.message_text,
            message_date=row.message_date,
            view_count=row.view_count,
        )
        for row in rows
    ]


@app.get("/api/reports/visual-content", response_model=List[VisualContentStat])
def visual_content_stats(db: Session = Depends(get_db)) -> List[VisualContentStat]:
    sql = text(
        """
        select
            c.channel_name,
            sum(case when fid.image_category = 'promotional' then f.view_count end) as promotional_views,
            sum(case when fid.image_category = 'product_display' then f.view_count end) as product_display_views,
            sum(case when fid.image_category = 'lifestyle' then f.view_count end) as lifestyle_views,
            sum(case when fid.image_category = 'other' then f.view_count end) as other_views
        from marts.fct_image_detections fid
        join marts.fct_messages f on fid.message_id = f.message_id
        join marts.dim_channels c on f.channel_key = c.channel_key
        group by c.channel_name
        order by c.channel_name
        """
    )
    rows = db.execute(sql).fetchall()
    return [
        VisualContentStat(
            channel_name=row.channel_name,
            promotional_views=row.promotional_views,
            product_display_views=row.product_display_views,
            lifestyle_views=row.lifestyle_views,
            other_views=row.other_views,
        )
        for row in rows
    ]
