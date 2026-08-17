"""Resolve which business days to extract on this invocation."""

from shared.blob_storage import BlobStorageWriter
from shared.pipeline_schedule import (
    TIMELINE2_JOB_BLOB_PATH,
    now_pipeline,
    parse_target_days_override,
    should_start_timeline2,
    target_days_timeline1,
    target_days_timeline2,
    timeline2_job_pending_days,
)


def read_timeline2_job(blob_writer: BlobStorageWriter) -> dict | None:
    return blob_writer.read_json_blob(TIMELINE2_JOB_BLOB_PATH)


def write_timeline2_job(blob_writer: BlobStorageWriter, job: dict) -> None:
    blob_writer.upload_json_blob(TIMELINE2_JOB_BLOB_PATH, job)


def start_timeline2_job(blob_writer: BlobStorageWriter, now_ist) -> dict:
    pending_days = [day.isoformat() for day in target_days_timeline2(now_ist)]
    job = {
        "status": "in_progress",
        "timeline": 2,
        "startedAt": now_ist.isoformat(),
        "startedOnDate": now_ist.date().isoformat(),
        "pendingDays": pending_days,
        "currentDay": pending_days[0] if pending_days else None,
    }
    write_timeline2_job(blob_writer, job)
    return job


def complete_timeline2_job_if_done(blob_writer: BlobStorageWriter, job: dict) -> None:
    if not job.get("pendingDays"):
        job["status"] = "completed"
        job["completedAt"] = now_pipeline().isoformat()
        write_timeline2_job(blob_writer, job)


def mark_timeline2_day_complete(blob_writer: BlobStorageWriter, job: dict, target_day: str) -> dict:
    pending = [day for day in job.get("pendingDays", []) if day != target_day]
    job["pendingDays"] = pending
    job["currentDay"] = pending[0] if pending else None
    if not pending:
        job["status"] = "completed"
        job["completedAt"] = now_pipeline().isoformat()
    write_timeline2_job(blob_writer, job)
    return job


def resolve_work_days(blob_writer: BlobStorageWriter) -> list[str]:
    override = parse_target_days_override()
    if override is not None:
        return [day.isoformat() for day in override]

    now_ist = now_pipeline()
    job = read_timeline2_job(blob_writer)

    if should_start_timeline2(now_ist, job):
        job = start_timeline2_job(blob_writer, now_ist)

    if job and job.get("status") == "in_progress":
        pending = timeline2_job_pending_days(job)
        if pending:
            return pending

    return [day.isoformat() for day in target_days_timeline1(now_ist)]
