"""Reload one extract day from blob into Snowflake (day delete + full insert)."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from shared.blob_reader import BlobStorageReader
from shared.snowflake_loader import SalesSnowflakeLoader
from transformers.sales_transformer import transform_sales


@dataclass
class DayReloadResult:
    status: str
    target_day: str
    reason: str | None = None
    record_count: int = 0
    metrics: dict | None = None


def reload_day_from_blob(
    blob_reader: BlobStorageReader,
    target_day: str,
    *,
    force: bool = False,
    update_checkpoint: bool = True,
    dry_run: bool = False,
    loader: SalesSnowflakeLoader | None = None,
    connection: Any | None = None,
) -> DayReloadResult:
    """Delete Snowflake rows for target_day and reload the full blob snapshot.

    When force=False (scheduled transformer), respects extraction status, transform
    checkpoint, and transform lease. When force=True (historical local refresh),
    always reloads if extraction is complete.
    """
    if not blob_reader.is_extraction_complete(target_day):
        checkpoint = blob_reader.read_checkpoint(target_day)
        pending_count = len((checkpoint or {}).get("pendingBranchCodes", []))
        return DayReloadResult(
            status="skipped",
            target_day=target_day,
            reason=f"extraction_not_complete ({pending_count} branches pending)",
        )

    extracted_at = blob_reader.snapshot_extracted_at(target_day)

    if not force:
        transform_checkpoint = blob_reader.read_transform_checkpoint(target_day)
        if transform_checkpoint and transform_checkpoint.get("extractedAt") == extracted_at:
            return DayReloadResult(
                status="skipped",
                target_day=target_day,
                reason=f"already_transformed extractedAt={extracted_at}",
            )

        if not blob_reader.try_begin_transform(target_day, extracted_at):
            return DayReloadResult(
                status="skipped",
                target_day=target_day,
                reason=f"transform_in_progress extractedAt={extracted_at}",
            )

    lease_acquired = not force
    try:
        if not force:
            transform_checkpoint = blob_reader.read_transform_checkpoint(target_day)
            if transform_checkpoint and transform_checkpoint.get("extractedAt") == extracted_at:
                return DayReloadResult(
                    status="skipped",
                    target_day=target_day,
                    reason=f"already_transformed extractedAt={extracted_at}",
                )

        if dry_run:
            raw_payload = blob_reader.read_final_snapshot(target_day)
            record_count = len(raw_payload.get("data", []))
            logging.info(
                "DRY RUN: would reload %s (%s invoices, extractedAt=%s).",
                target_day,
                record_count,
                extracted_at,
            )
            return DayReloadResult(
                status="dry_run",
                target_day=target_day,
                record_count=record_count,
            )

        raw_payload = blob_reader.read_final_snapshot(target_day)
        transformed = transform_sales(raw_payload)
        snowflake_loader = loader or SalesSnowflakeLoader.from_env()
        metrics = snowflake_loader.load(target_day, transformed, connection=connection)
        record_count = len(transformed.headers)

        if update_checkpoint:
            blob_reader.write_transform_checkpoint(
                target_day,
                {
                    "targetDay": target_day,
                    "businessDay": raw_payload.get("businessDay", target_day),
                    "extractedAt": extracted_at,
                    "transformedAt": datetime.now(timezone.utc).isoformat(),
                    "recordCount": record_count,
                    "metrics": metrics,
                },
            )

        logging.info(
            "Day reload for %s: %s invoices (mode=%s).",
            target_day,
            record_count,
            metrics.get("reload_mode", "day_delete_insert"),
        )
        return DayReloadResult(
            status="reloaded",
            target_day=target_day,
            record_count=record_count,
            metrics=metrics,
        )
    finally:
        if lease_acquired:
            blob_reader.clear_transform_lease(target_day)
