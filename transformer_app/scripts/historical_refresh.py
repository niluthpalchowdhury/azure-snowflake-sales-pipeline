#!/usr/bin/env python3
"""Local-only: reload Snowflake sales data for a date range from existing blobs.

Deletes LAST_EXTRACTED_DAY rows and re-inserts the full blob snapshot per day.
Does not deploy to Azure — run from transformer_app with local.settings.json.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(APP_DIR))


def load_local_settings() -> None:
    settings_path = APP_DIR / "local.settings.json"
    if not settings_path.exists():
        logging.warning("local.settings.json not found at %s", settings_path)
        return
    values = json.loads(settings_path.read_text(encoding="utf-8")).get("Values", {})
    for key, value in values.items():
        os.environ.setdefault(key, str(value))


def iter_days(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError(f"end date {end} is before start date {start}")
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reload Snowflake sales data from blob for each day in a date range.",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="First day to reload (YYYY-MM-DD, inclusive).",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="Last day to reload (YYYY-MM-DD, inclusive).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List days and invoice counts only; no Snowflake writes.",
    )
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Skip days without a completed blob instead of failing.",
    )
    parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Do not update transform_checkpoint.json after reload.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    load_local_settings()

    from shared.blob_reader import BlobStorageReader
    from shared.day_reload import reload_day_from_blob
    from shared.snowflake_loader import SalesSnowflakeLoader

    start_day = date.fromisoformat(args.start)
    end_day = date.fromisoformat(args.end)
    target_days = [day.isoformat() for day in iter_days(start_day, end_day)]

    logging.info(
        "Historical refresh: %s to %s (%s days), dry_run=%s.",
        args.start,
        args.end,
        len(target_days),
        args.dry_run,
    )

    blob_reader = BlobStorageReader.from_env()
    loader = SalesSnowflakeLoader.from_env()
    reloaded = 0
    skipped = 0
    failed = 0

    def reload_one_day(target_day: str, connection: object | None) -> int:
        nonlocal reloaded, skipped, failed
        try:
            result = reload_day_from_blob(
                blob_reader,
                target_day,
                force=True,
                update_checkpoint=not args.no_checkpoint,
                dry_run=args.dry_run,
                loader=loader,
                connection=connection,
            )
        except Exception:
            logging.exception("Failed to reload %s.", target_day)
            failed += 1
            return 1 if not args.skip_missing else 0

        if result.status in ("reloaded", "dry_run"):
            reloaded += 1
        elif result.status == "skipped":
            skipped += 1
            logging.warning("Skipped %s: %s.", target_day, result.reason)
            if not args.skip_missing and result.reason and "extraction_not_complete" in result.reason:
                logging.error("Use --skip-missing to continue past incomplete extractions.")
                return 1
        else:
            logging.warning("Unexpected status for %s: %s", target_day, result.status)
            skipped += 1
        return 0

    if args.dry_run:
        for target_day in target_days:
            if reload_one_day(target_day, None):
                return 1
    else:
        logging.info("Using one shared Snowflake connection for %s days.", len(target_days))
        with loader.connect() as connection:
            for target_day in target_days:
                if reload_one_day(target_day, connection):
                    return 1

    logging.info(
        "Historical refresh finished: reloaded=%s skipped=%s failed=%s.",
        reloaded,
        skipped,
        failed,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
