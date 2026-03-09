from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

DATE_FMT = "%Y-%m-%d"
INDIA_TZ = ZoneInfo("Asia/Kolkata")


def parse_date(value: str) -> date:
    return datetime.strptime(value, DATE_FMT).date()


def format_date(value: date) -> str:
    return value.strftime(DATE_FMT)


def default_business_dt() -> str:
    # 默认跑印度时区前一日
    india_now = datetime.now(INDIA_TZ)
    return format_date(india_now.date() - timedelta(days=1))


def iter_dates(start_dt: str, end_dt: str):
    start = parse_date(start_dt)
    end = parse_date(end_dt)
    cur = start
    while cur <= end:
        yield format_date(cur)
        cur += timedelta(days=1)
