import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from shared.api_call_logger import ApiCallLogger
from shared.blob_storage import BlobStorageWriter, build_sales_blob_path
from shared.rista_client import RistaClient, get_all_branches


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _lease_id(lease_client) -> str | None:
    return lease_client.id if lease_client is not None else None


def _invoice_key(record: dict) -> tuple[str, str]:
    branch_code = (record.get("branchCode") or "").strip()
    invoice_number = (record.get("invoiceNumber") or "").strip()
    return branch_code, invoice_number


def _dedupe_sales_records(records: list[dict]) -> list[dict]:
    deduped: dict[tuple[str, str], dict] = {}
    for record in records:
        branch_code, invoice_number = _invoice_key(record)
        if not invoice_number:
            continue
        deduped[(branch_code, invoice_number)] = record
    return list(deduped.values())


def _write_checkpoint_heartbeat(
    blob_writer: BlobStorageWriter,
    target_day: str,
    checkpoint: dict[str, Any],
    lease_client,
) -> None:
    checkpoint["lastHeartbeatAt"] = _utc_now_iso()
    blob_writer.write_checkpoint(target_day, checkpoint, lease_id=_lease_id(lease_client))


def _empty_partial(target_day: str) -> dict[str, Any]:
    return {
        "extractType": "sales",
        "businessDay": target_day,
        "data": [],
        "branches": [],
    }


def _branch_lookup(branch_catalog: list[dict]) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for branch in branch_catalog:
        branch_code = (branch.get("branchCode") or "").strip()
        if branch_code:
            lookup[branch_code] = branch
    return lookup


def _summaries_by_code(branch_summaries: list[dict]) -> dict[str, dict]:
    return {
        (summary.get("branchCode") or "").strip(): summary
        for summary in branch_summaries
        if (summary.get("branchCode") or "").strip()
    }


def _initialize_checkpoint(target_day: str, all_branches: list[dict], timeline: int) -> dict[str, Any]:
    pending_codes = [(branch.get("branchCode") or "").strip() for branch in all_branches]
    pending_codes = [code for code in pending_codes if code]
    return {
        "targetDay": target_day,
        "status": "in_progress",
        "timeline": timeline,
        "activeBranchCount": len(pending_codes),
        "pendingBranchCodes": pending_codes,
        "completedBranchCodes": [],
        "failedBranches": [],
        "branchAttemptCounts": {},
        "branchCatalog": all_branches,
        "runCount": 0,
        "recordCount": 0,
        "refreshGeneration": int(time.time()),
        "createdAt": _utc_now_iso(),
        "updatedAt": _utc_now_iso(),
    }


def _count_successful_branches(summaries_by_code: dict[str, dict], failed_codes: set[str]) -> int:
    return sum(
        1
        for code, summary in summaries_by_code.items()
        if summary.get("fetchStatus") == "success" and code not in failed_codes
    )


def _log_branch_progress(
    target_day: str,
    *,
    active_total: int,
    completed_overall: int,
    failed_count: int,
    pending_this_run: int,
    processed_this_run: int,
    remaining_this_run: int,
    event: str,
    branch_code: str = "",
) -> None:
    left_overall = max(active_total - completed_overall - failed_count, 0)
    branch_hint = f" branch={branch_code}" if branch_code else ""
    logging.info(
        "Branch progress [%s]%s: %s/%s done overall | %s failed | "
        "%s processed this run | %s left this run | ~%s left overall",
        target_day,
        branch_hint,
        completed_overall,
        active_total,
        failed_count,
        processed_this_run,
        remaining_this_run,
        left_overall,
    )
    if event:
        logging.info("  -> %s", event)


def _build_final_snapshot(
    target_day: str,
    checkpoint: dict[str, Any],
    partial: dict[str, Any],
) -> dict[str, Any]:
    failed_branches = list(checkpoint.get("failedBranches", []))
    completed_count = len(checkpoint.get("completedBranchCodes", []))
    active_count = int(checkpoint.get("activeBranchCount", 0))
    branch_summaries = list(partial.get("branches", []))
    summary_codes = {
        (summary.get("branchCode") or "").strip()
        for summary in branch_summaries
        if (summary.get("branchCode") or "").strip()
    }
    missing_branch_codes = [
        code
        for code in checkpoint.get("pendingBranchCodes", [])
        if code not in summary_codes and code not in checkpoint.get("completedBranchCodes", [])
    ]

    return {
        "extractType": "sales",
        "businessDay": target_day,
        "extractedAt": _utc_now_iso(),
        "activeBranchCount": active_count,
        "successfulBranchCount": completed_count,
        "failedBranchCount": len(failed_branches),
        "missingBranchCodes": missing_branch_codes,
        "recordCount": len(partial.get("data", [])),
        "branches": branch_summaries,
        "failedBranches": failed_branches,
        "data": list(partial.get("data", [])),
        "extractionRunCount": checkpoint.get("runCount", 0),
        "refreshGeneration": checkpoint.get("refreshGeneration"),
    }


def run_checkpointed_extraction(
    client: RistaClient,
    blob_writer: BlobStorageWriter,
    target_day: str,
    run_budget_seconds: int,
    lease_seconds: int,
    timeline: int = 1,
    force_refresh: bool = False,
) -> dict[str, Any]:
    checkpoint = blob_writer.read_checkpoint(target_day)
    if checkpoint and checkpoint.get("status") == "completed" and not force_refresh:
        logging.info("Extraction for %s is already completed. Skipping run.", target_day)
        return {"status": "completed", "skipped": True, "targetDay": target_day}

    stale_after_seconds = max(lease_seconds * 3, 120)
    lease_client = blob_writer.try_acquire_checkpoint_lease(
        target_day,
        lease_seconds,
        stale_after_seconds=stale_after_seconds,
    )
    if lease_client is None:
        checkpoint = blob_writer.read_checkpoint(target_day)
        heartbeat = (checkpoint or {}).get("lastHeartbeatAt") or (checkpoint or {}).get(
            "lastRunStartedAt"
        )
        logging.info(
            "Could not acquire checkpoint lease for %s. Another run is active (heartbeat=%s).",
            target_day,
            heartbeat or "unknown",
        )
        return {"status": "locked", "skipped": True, "targetDay": target_day}

    run_id = str(uuid.uuid4())
    run_started = time.monotonic()
    api_logger = ApiCallLogger.for_run(target_day, run_id)
    client.api_logger = api_logger

    try:
        checkpoint = blob_writer.read_checkpoint(target_day) or {}
        partial = blob_writer.read_partial_data(target_day) or _empty_partial(target_day)

        needs_init = (
            checkpoint.get("status") != "in_progress"
            or force_refresh
            or checkpoint.get("status") == "completed"
        )

        if needs_init:
            branch_payload = client.fetch_branch_list()
            all_branches = get_all_branches(branch_payload)
            if not all_branches:
                raise RuntimeError(f"No branches returned for {target_day}.")

            checkpoint = _initialize_checkpoint(target_day, all_branches, timeline)
            partial = _empty_partial(target_day)
            logging.info(
                "Initialized sales checkpoint for %s with %s branches (timeline=%s, refresh=%s).",
                target_day,
                checkpoint["activeBranchCount"],
                timeline,
                force_refresh,
            )

        checkpoint["status"] = "in_progress"
        checkpoint["runCount"] = int(checkpoint.get("runCount", 0)) + 1
        checkpoint["lastRunId"] = run_id
        checkpoint["lastRunStartedAt"] = _utc_now_iso()
        _write_checkpoint_heartbeat(blob_writer, target_day, checkpoint, lease_client)

        branch_catalog = _branch_lookup(checkpoint.get("branchCatalog", []))
        pending_codes: list[str] = list(checkpoint.get("pendingBranchCodes", []))
        attempt_counts: dict[str, int] = dict(checkpoint.get("branchAttemptCounts", {}))
        failed_branches: list[dict] = list(checkpoint.get("failedBranches", []))
        failed_codes = {
            (entry.get("branchCode") or "").strip()
            for entry in failed_branches
            if (entry.get("branchCode") or "").strip()
        }

        summaries = list(partial.get("branches", []))
        summaries_by_code = _summaries_by_code(summaries)
        rows = _dedupe_sales_records(list(partial.get("data", [])))

        processed_this_run = 0
        budget_reached = False
        next_pending: list[str] = []
        last_lease_renew = time.monotonic()
        lease_renew_interval = max(lease_seconds - 15, 30)
        active_total = int(checkpoint.get("activeBranchCount", 0)) or len(pending_codes)
        chunk_total = len(pending_codes)
        completed_before_run = _count_successful_branches(summaries_by_code, failed_codes)

        _log_branch_progress(
            target_day,
            active_total=active_total,
            completed_overall=completed_before_run,
            failed_count=len(failed_branches),
            pending_this_run=chunk_total,
            processed_this_run=0,
            remaining_this_run=chunk_total,
            event=f"Run {checkpoint['runCount']} started — {chunk_total} branches queued",
        )

        for index, branch_code in enumerate(pending_codes):
            if time.monotonic() - last_lease_renew >= lease_renew_interval:
                BlobStorageWriter.renew_lease(lease_client)
                last_lease_renew = time.monotonic()
                _write_checkpoint_heartbeat(blob_writer, target_day, checkpoint, lease_client)

            elapsed = time.monotonic() - run_started
            if elapsed >= run_budget_seconds:
                budget_reached = True
                deferred_codes = pending_codes[index:]
                next_pending = list(dict.fromkeys(next_pending + deferred_codes))
                logging.warning(
                    "Time budget reached for %s after %ss. %s/%s branches processed; %s deferred.",
                    target_day,
                    int(elapsed),
                    index,
                    chunk_total,
                    len(next_pending),
                )
                break

            branch = branch_catalog.get(branch_code)
            if not branch:
                failed_branches.append(
                    {
                        "branchCode": branch_code,
                        "error": "Branch missing from checkpoint catalog.",
                    }
                )
                failed_codes.add(branch_code)
                continue

            branch_name = (branch.get("branchName") or branch_code).strip()
            current_attempt = int(attempt_counts.get(branch_code, 0)) + 1

            remaining_this_run = chunk_total - index
            logging.info(
                "Fetching sales for branch %s on %s (attempt %s/%s, run %s, slot %s/%s this run).",
                branch_code,
                target_day,
                current_attempt,
                client.max_branch_attempts,
                checkpoint["runCount"],
                index + 1,
                chunk_total,
            )

            try:
                branch_rows, page_count = client.fetch_branch_sales(branch_code, target_day)
                if branch_rows:
                    rows = _dedupe_sales_records(rows + branch_rows)
                summaries_by_code[branch_code] = {
                    "branchCode": branch_code,
                    "branchName": branch_name,
                    "branchListStatus": branch.get("status"),
                    "recordCount": len(branch_rows),
                    "pageCount": page_count,
                    "fetchStatus": "success",
                    "attemptCount": current_attempt,
                }
                attempt_counts.pop(branch_code, None)
                processed_this_run += 1
                completed_overall = _count_successful_branches(summaries_by_code, failed_codes)
                _log_branch_progress(
                    target_day,
                    active_total=active_total,
                    completed_overall=completed_overall,
                    failed_count=len(failed_branches),
                    pending_this_run=chunk_total,
                    processed_this_run=processed_this_run,
                    remaining_this_run=max(remaining_this_run - 1, 0) + len(next_pending),
                    event=f"OK {branch_code}: {len(branch_rows)} invoices, {page_count} pages",
                    branch_code=branch_code,
                )
                if processed_this_run % 5 == 0:
                    _write_checkpoint_heartbeat(blob_writer, target_day, checkpoint, lease_client)
            except Exception as exc:
                error_message = str(exc)
                attempt_counts[branch_code] = current_attempt
                summaries_by_code[branch_code] = {
                    "branchCode": branch_code,
                    "branchName": branch_name,
                    "branchListStatus": branch.get("status"),
                    "recordCount": 0,
                    "pageCount": 0,
                    "fetchStatus": "failed",
                    "attemptCount": current_attempt,
                    "error": error_message,
                }

                if current_attempt < client.max_branch_attempts:
                    next_pending.append(branch_code)
                    logging.warning(
                        "Branch %s failed attempt %s/%s for %s: %s",
                        branch_code,
                        current_attempt,
                        client.max_branch_attempts,
                        target_day,
                        error_message,
                    )
                    event = f"RETRY scheduled for {branch_code}"
                else:
                    failed_branches.append(
                        {
                            "branchCode": branch_code,
                            "branchName": branch_name,
                            "error": error_message,
                            "attemptCount": current_attempt,
                        }
                    )
                    failed_codes.add(branch_code)
                    logging.exception(
                        "Branch %s permanently failed for %s after %s attempts.",
                        branch_code,
                        target_day,
                        client.max_branch_attempts,
                    )
                    event = f"FAILED permanently {branch_code}"

                completed_overall = _count_successful_branches(summaries_by_code, failed_codes)
                _log_branch_progress(
                    target_day,
                    active_total=active_total,
                    completed_overall=completed_overall,
                    failed_count=len(failed_branches),
                    pending_this_run=chunk_total,
                    processed_this_run=processed_this_run,
                    remaining_this_run=max(remaining_this_run - 1, 0) + len(next_pending),
                    event=event,
                    branch_code=branch_code,
                )

        if not budget_reached:
            next_pending = list(dict.fromkeys(next_pending))

        completed_codes = [
            code
            for code, summary in summaries_by_code.items()
            if summary.get("fetchStatus") == "success" and code not in failed_codes
        ]

        checkpoint["pendingBranchCodes"] = next_pending
        checkpoint["completedBranchCodes"] = completed_codes
        checkpoint["failedBranches"] = failed_branches
        checkpoint["branchAttemptCounts"] = attempt_counts
        checkpoint["recordCount"] = len(rows)
        checkpoint["processedThisRun"] = processed_this_run
        checkpoint["budgetReached"] = budget_reached
        checkpoint["updatedAt"] = _utc_now_iso()
        checkpoint["lastRunEndedAt"] = _utc_now_iso()

        partial["data"] = rows
        partial["branches"] = list(summaries_by_code.values())

        blob_writer.write_partial_data(target_day, partial)
        blob_writer.write_checkpoint(target_day, checkpoint, lease_id=_lease_id(lease_client))

        completed_overall = len(completed_codes)
        _log_branch_progress(
            target_day,
            active_total=active_total,
            completed_overall=completed_overall,
            failed_count=len(failed_branches),
            pending_this_run=chunk_total,
            processed_this_run=processed_this_run,
            remaining_this_run=len(next_pending),
            event=(
                f"Run {checkpoint['runCount']} ended — "
                f"{processed_this_run} branches processed this run, "
                f"{len(next_pending)} still pending, {len(rows)} total invoices"
            ),
        )

        if next_pending:
            return {
                "status": "in_progress",
                "targetDay": target_day,
                "pendingBranchCount": len(next_pending),
                "completedBranchCount": len(completed_codes),
                "failedBranchCount": len(failed_branches),
                "recordCount": len(rows),
                "budgetReached": budget_reached,
                "processedThisRun": processed_this_run,
            }

        if not completed_codes and checkpoint.get("activeBranchCount", 0) > 0:
            raise RuntimeError(f"Sales extraction failed for all branches on {target_day}.")

        final_snapshot = _build_final_snapshot(target_day, checkpoint, partial)
        blob_writer.upload_gzip_json(build_sales_blob_path(target_day), final_snapshot)

        checkpoint["status"] = "completed"
        checkpoint["completedAt"] = _utc_now_iso()
        checkpoint["snapshotExtractedAt"] = final_snapshot["extractedAt"]
        checkpoint["updatedAt"] = _utc_now_iso()
        blob_writer.write_checkpoint(target_day, checkpoint, lease_id=_lease_id(lease_client))

        logging.info(
            "Extraction completed for %s: %s records, %s/%s branches (%s failed).",
            target_day,
            final_snapshot["recordCount"],
            final_snapshot["successfulBranchCount"],
            final_snapshot["activeBranchCount"],
            final_snapshot["failedBranchCount"],
        )

        return {
            "status": "completed",
            "targetDay": target_day,
            "snapshot": final_snapshot,
            "runCount": checkpoint.get("runCount", 0),
        }
    finally:
        client.api_logger = None
        BlobStorageWriter.release_lease(lease_client)


def upload_failed_branch_artifact(
    blob_writer: BlobStorageWriter,
    target_day: str,
    snapshot: dict[str, Any],
) -> None:
    if not snapshot.get("failedBranchCount"):
        return

    blob_writer.upload_failed_branches_json(
        target_day,
        {
            "component": "extractor",
            "targetDay": target_day,
            "generatedAt": _utc_now_iso(),
            "activeBranchCount": snapshot.get("activeBranchCount", 0),
            "successfulBranchCount": snapshot.get("successfulBranchCount", 0),
            "failedBranchCount": snapshot.get("failedBranchCount", 0),
            "failedBranches": snapshot.get("failedBranches", []),
            "missingBranchCodes": snapshot.get("missingBranchCodes", []),
        },
    )
