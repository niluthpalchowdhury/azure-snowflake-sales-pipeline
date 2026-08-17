import logging
import traceback

import azure.functions as func

from shared.blob_storage import BlobStorageWriter
from shared.extraction_runner import run_checkpointed_extraction, upload_failed_branch_artifact
from shared.pipeline_schedule import EXTRACTOR_TIMER_SCHEDULE, should_force_refresh
from shared.rista_client import RistaClient
from shared.work_plan import (
    complete_timeline2_job_if_done,
    mark_timeline2_day_complete,
    read_timeline2_job,
    resolve_work_days,
)

app = func.FunctionApp()


def _run_budget_seconds() -> int:
    from os import environ

    return int(environ.get("EXTRACTOR_RUN_BUDGET_SECONDS", "480"))


def _lease_seconds() -> int:
    from os import environ

    configured = int(environ.get("EXTRACTOR_LEASE_SECONDS", "60"))
    return max(15, min(configured, 60))


@app.function_name(name="rista_sales_extractor")
@app.schedule(
    schedule=EXTRACTOR_TIMER_SCHEDULE,
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
def rista_sales_extractor(timer: func.TimerRequest) -> None:
    del timer

    blob_writer = BlobStorageWriter.from_env()
    client = RistaClient.from_env()
    work_days = resolve_work_days(blob_writer)
    timeline2_job = read_timeline2_job(blob_writer)
    timeline = 2 if timeline2_job and timeline2_job.get("status") == "in_progress" else 1

    logging.info("Sales extractor starting for days: %s (timeline=%s).", work_days, timeline)

    for target_day in work_days:
        try:
            checkpoint = blob_writer.read_checkpoint(target_day)
            force_refresh = should_force_refresh(checkpoint, target_day)

            result = run_checkpointed_extraction(
                client=client,
                blob_writer=blob_writer,
                target_day=target_day,
                run_budget_seconds=_run_budget_seconds(),
                lease_seconds=_lease_seconds(),
                timeline=timeline,
                force_refresh=force_refresh,
            )

            if result.get("skipped") and result.get("status") != "locked":
                continue

            if result.get("status") == "completed":
                snapshot = result.get("snapshot", {})
                if snapshot.get("failedBranchCount"):
                    upload_failed_branch_artifact(blob_writer, target_day, snapshot)

                if timeline == 2 and timeline2_job:
                    timeline2_job = mark_timeline2_day_complete(blob_writer, timeline2_job, target_day)
                    complete_timeline2_job_if_done(blob_writer, timeline2_job)
                continue

            if result.get("status") == "in_progress":
                logging.info(
                    "Sales extraction in progress for %s: %s pending branches, %s records.",
                    target_day,
                    result.get("pendingBranchCount", 0),
                    result.get("recordCount", 0),
                )
        except Exception as exc:
            logging.exception("Sales extraction failed for %s.", target_day)
            blob_writer.upload_error_json(
                target_day,
                "extractor",
                {
                    "component": "extractor",
                    "targetDay": target_day,
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
            )
            raise
