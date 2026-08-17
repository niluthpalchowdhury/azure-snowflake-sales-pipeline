import logging
import traceback

import azure.functions as func

from shared.blob_reader import BlobStorageReader
from shared.day_reload import reload_day_from_blob
from shared.pipeline_schedule import (
    TRANSFORMER_TIMER_SCHEDULE,
    discover_default_candidate_days,
    now_pipeline,
    parse_target_days_override,
)

app = func.FunctionApp()


def resolve_transform_days(blob_reader: BlobStorageReader) -> list[str]:
    override = parse_target_days_override()
    if override is not None:
        return [day.isoformat() for day in override]

    now_ist = now_pipeline()
    candidates = [day.isoformat() for day in discover_default_candidate_days(now_ist)]
    return [day for day in candidates if blob_reader.needs_transform(day)]


def process_target_day(blob_reader: BlobStorageReader, target_day: str) -> None:
    result = reload_day_from_blob(blob_reader, target_day, force=False)
    if result.status == "skipped":
        logging.info("Transform for %s skipped: %s.", target_day, result.reason)


@app.function_name(name="rista_sales_transformer")
@app.schedule(
    schedule=TRANSFORMER_TIMER_SCHEDULE,
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
def rista_sales_transformer(timer: func.TimerRequest) -> None:
    del timer

    blob_reader = BlobStorageReader.from_env()
    target_days = resolve_transform_days(blob_reader)

    if not target_days:
        logging.info("Sales transformer: no days pending transform.")
        return

    logging.info("Sales transformer starting for days: %s.", target_days)

    for target_day in target_days:
        try:
            process_target_day(blob_reader, target_day)
        except Exception as exc:
            logging.exception("Sales transform failed for %s.", target_day)
            blob_reader.upload_error_json(
                target_day,
                "transformer",
                {
                    "component": "transformer",
                    "targetDay": target_day,
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
            )
            raise
