"""Sales pipeline schedule helpers (IST)."""

from datetime import date, datetime, timedelta
from os import environ

from zoneinfo import ZoneInfo

# Every 30 minutes at :05 and :35 (48 runs/day). Snowflake only when a day needs transform.
TRANSFORMER_TIMER_SCHEDULE = "0 5/30 * * * *"


def pipeline_timezone() -> ZoneInfo:
    tz_name = environ.get("PIPELINE_TIMEZONE", "Asia/Kolkata").strip() or "Asia/Kolkata"
    return ZoneInfo(tz_name)


def now_pipeline() -> datetime:
    return datetime.now(pipeline_timezone())


def parse_target_days_override() -> list[date] | None:
    target_days_raw = environ.get("TARGET_DAYS", "").strip()
    if target_days_raw:
        return [date.fromisoformat(part.strip()) for part in target_days_raw.split(",") if part.strip()]

    target_day = environ.get("TARGET_DAY", "").strip()
    if target_day:
        return [date.fromisoformat(target_day)]
    return None


def discover_default_candidate_days(now_ist: datetime) -> list[date]:
    today = now_ist.date()
    return [today - timedelta(days=offset) for offset in range(7, -1, -1)]
