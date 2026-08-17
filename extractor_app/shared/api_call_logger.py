import csv
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CSV_COLUMNS = [
    "timestamp_utc",
    "target_day",
    "run_id",
    "endpoint",
    "http_method",
    "url",
    "branch_code",
    "page_number",
    "query_params",
    "http_status",
    "duration_ms",
    "records_count",
    "attempt",
    "outcome",
    "error_message",
]


def _default_log_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "logs" / "api_calls"


@dataclass
class ApiCallLogger:
    target_day: str
    run_id: str
    log_path: Path
    _file_initialized: bool = field(default=False, init=False)

    @classmethod
    def for_run(cls, target_day: str, run_id: str) -> "ApiCallLogger":
        from os import environ

        log_dir = Path(environ.get("API_CALL_LOG_DIR", "").strip() or _default_log_dir())
        log_dir.mkdir(parents=True, exist_ok=True)
        safe_day = target_day.replace("/", "-")
        log_path = log_dir / f"{safe_day}_api_calls.csv"
        return cls(target_day=target_day, run_id=run_id, log_path=log_path)

    def _ensure_header(self) -> None:
        if self._file_initialized:
            return
        write_header = not self.log_path.exists() or self.log_path.stat().st_size == 0
        if write_header:
            with self.log_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
                writer.writeheader()
        self._file_initialized = True
        logging.info("API call log file: %s", self.log_path.resolve())

    def log_call(
        self,
        *,
        endpoint: str,
        http_method: str,
        url: str,
        query_params: dict[str, Any] | None,
        http_status: int | None,
        duration_ms: float,
        records_count: int | None,
        attempt: int,
        outcome: str,
        error_message: str = "",
        branch_code: str = "",
        page_number: int | None = None,
    ) -> None:
        self._ensure_header()
        row = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "target_day": self.target_day,
            "run_id": self.run_id,
            "endpoint": endpoint,
            "http_method": http_method,
            "url": url,
            "branch_code": branch_code,
            "page_number": page_number if page_number is not None else "",
            "query_params": json.dumps(query_params or {}, ensure_ascii=True, sort_keys=True),
            "http_status": http_status if http_status is not None else "",
            "duration_ms": round(duration_ms, 2),
            "records_count": records_count if records_count is not None else "",
            "attempt": attempt,
            "outcome": outcome,
            "error_message": error_message,
        }
        with self.log_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
            writer.writerow(row)
