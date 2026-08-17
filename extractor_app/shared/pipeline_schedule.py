"""Sales extract schedule: Timeline 1 + Timeline 2 (IST)."""

from datetime import date, datetime, timedelta
from os import environ

from zoneinfo import ZoneInfo

EXTRACTOR_TIMER_SCHEDULE = "0 0/10 * * * *"
# Every 30 minutes at :05 and :35 (48 runs/day). Extractor stays on 10-minute chunks.
TRANSFORMER_TIMER_SCHEDULE = "0 5/30 * * * *"

TIMELINE2_JOB_BLOB_PATH = "rista/sales/jobs/timeline2.json"


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


def target_days_timeline1(now_ist: datetime) -> list[date]:
    today = now_ist.date()
    if now_ist.hour < 9:
        return [today - timedelta(days=1), today]
    return [today]


def target_days_timeline2(now_ist: datetime) -> list[date]:
    today = now_ist.date()
    return [today - timedelta(days=offset) for offset in range(6, -1, -1)]


def should_start_timeline2(now_ist: datetime, job: dict | None) -> bool:
    if now_ist.hour != 10 or now_ist.minute >= 10:
        return False
    if job and job.get("status") == "in_progress":
        return False
    started_on = (job or {}).get("startedOnDate")
    return started_on != now_ist.date().isoformat()


def timeline2_job_pending_days(job: dict) -> list[str]:
    pending = list(job.get("pendingDays", []))
    current = job.get("currentDay")
    if current and current not in pending:
        return [current] + pending
    return pending


def discover_default_candidate_days(now_ist: datetime) -> list[date]:
    today = now_ist.date()
    return [today - timedelta(days=offset) for offset in range(7, -1, -1)]


def should_force_refresh(checkpoint: dict | None, target_day: str) -> bool:
    """Re-extract only on first run, backfill override, or EXTRACTOR_FORCE_REFRESH=true.

    A completed day is not force-refreshed on every extractor tick (avoids endless
    re-extract → re-transform loops with the transformer).
    """
    if checkpoint is None:
        return True
    if checkpoint.get("status") != "completed":
        return False

    forced = environ.get("EXTRACTOR_FORCE_REFRESH", "").strip().lower()
    if forced in ("1", "true", "yes"):
        return True

    override = parse_target_days_override()
    if override is not None:
        return target_day in {day.isoformat() for day in override}
    return False
