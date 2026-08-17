import logging
import time
import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import jwt
import requests

if TYPE_CHECKING:
    from shared.api_call_logger import ApiCallLogger

RETRYABLE_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}


def _normalize_records(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [record for record in payload if isinstance(record, dict)]
    if isinstance(payload, dict):
        data = payload.get("data", payload)
        if isinstance(data, list):
            return [record for record in data if isinstance(record, dict)]
    return []


def get_all_branches(branch_payload: Any) -> list[dict]:
    branches: list[dict] = []
    for branch in _normalize_records(branch_payload):
        branch_code = (branch.get("branchCode") or "").strip()
        if branch_code:
            branches.append(branch)
    return branches


@dataclass
class RistaClient:
    api_key: str
    secret_key: str
    base_url: str
    max_pages: int = 500
    timeout_seconds: int = 120
    max_retries: int = 3
    retry_backoff_seconds: float = 2.0
    max_branch_attempts: int = 2
    branch_retry_backoff_seconds: float = 5.0
    session: requests.Session = field(default_factory=requests.Session)
    api_logger: "ApiCallLogger | None" = None

    @classmethod
    def from_env(cls) -> "RistaClient":
        from os import environ

        return cls(
            api_key=environ["RISTA_API_KEY"].strip(),
            secret_key=environ["RISTA_SECRET_KEY"].strip(),
            base_url=environ["RISTA_BASE_URL"].strip(),
            max_pages=int(environ.get("RISTA_MAX_PAGES", "500")),
            timeout_seconds=int(environ.get("RISTA_REQUEST_TIMEOUT", "120")),
            max_retries=int(environ.get("RISTA_MAX_RETRIES", "3")),
            retry_backoff_seconds=float(environ.get("RISTA_RETRY_BACKOFF_SECONDS", "2")),
            max_branch_attempts=int(environ.get("RISTA_BRANCH_ATTEMPTS", "2")),
            branch_retry_backoff_seconds=float(
                environ.get("RISTA_BRANCH_RETRY_BACKOFF_SECONDS", "5")
            ),
        )

    def _build_url(self, endpoint_path: str) -> str:
        return f"{self.base_url.rstrip('/')}/{endpoint_path.lstrip('/')}"

    def _build_headers(self) -> dict[str, str]:
        issued_at = int(time.time())
        token = jwt.encode(
            {
                "iss": self.api_key,
                "iat": issued_at,
                "exp": issued_at + 300,
                "jti": str(uuid.uuid4()),
            },
            self.secret_key,
            algorithm="HS256",
        )
        return {
            "x-api-key": self.api_key,
            "x-api-token": token,
            "content-type": "application/json",
        }

    def _is_retryable(self, exc: Exception) -> bool:
        if isinstance(exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
            return True
        if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
            return exc.response.status_code in RETRYABLE_HTTP_STATUS_CODES
        return False

    def _get_json(
        self,
        endpoint_path: str,
        params: dict[str, Any] | None = None,
        *,
        branch_code: str = "",
        page_number: int | None = None,
    ) -> Any:
        url = self._build_url(endpoint_path)
        last_error: Exception | None = None
        safe_params = dict(params or {})

        for attempt in range(1, self.max_retries + 1):
            started = time.monotonic()
            http_status: int | None = None
            try:
                response = self.session.get(
                    url,
                    headers=self._build_headers(),
                    params=safe_params,
                    timeout=self.timeout_seconds,
                )
                http_status = response.status_code
                response.raise_for_status()
                payload = response.json()
                duration_ms = (time.monotonic() - started) * 1000
                records_count = len(_normalize_records(payload))
                if self.api_logger:
                    self.api_logger.log_call(
                        endpoint=endpoint_path,
                        http_method="GET",
                        url=url,
                        query_params=safe_params,
                        http_status=http_status,
                        duration_ms=duration_ms,
                        records_count=records_count,
                        attempt=attempt,
                        outcome="success",
                        branch_code=branch_code,
                        page_number=page_number,
                    )
                return payload
            except Exception as exc:
                duration_ms = (time.monotonic() - started) * 1000
                if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
                    http_status = exc.response.status_code
                will_retry = attempt < self.max_retries and self._is_retryable(exc)
                if self.api_logger:
                    self.api_logger.log_call(
                        endpoint=endpoint_path,
                        http_method="GET",
                        url=url,
                        query_params=safe_params,
                        http_status=http_status,
                        duration_ms=duration_ms,
                        records_count=None,
                        attempt=attempt,
                        outcome="retry" if will_retry else "failed",
                        error_message=str(exc),
                        branch_code=branch_code,
                        page_number=page_number,
                    )
                last_error = exc
                if not will_retry:
                    raise
                wait_seconds = self.retry_backoff_seconds * attempt
                logging.warning(
                    "Rista request failed for %s (attempt %s/%s): %s. Retrying in %ss.",
                    endpoint_path,
                    attempt,
                    self.max_retries,
                    exc,
                    wait_seconds,
                )
                time.sleep(wait_seconds)

        if last_error:
            raise last_error
        raise RuntimeError(f"Rista request failed for {endpoint_path}")

    def fetch_branch_list(self) -> Any:
        return self._get_json("/v1/branch/list")

    def fetch_branch_sales(
        self,
        branch_code: str,
        target_day: str,
    ) -> tuple[list[dict], int]:
        rows: list[dict] = []
        page_count = 0
        last_key: str | None = None
        previous_page: list[dict] | None = None

        while page_count < self.max_pages:
            params: dict[str, Any] = {
                "branch": branch_code,
                "day": target_day,
            }
            if last_key:
                params["lastKey"] = last_key

            payload = self._get_json(
                "/v1/sales/page",
                params=params,
                branch_code=branch_code,
                page_number=page_count + 1,
            )
            page_rows = _normalize_records(payload)
            if not page_rows:
                break

            if previous_page is not None and page_rows == previous_page:
                logging.warning(
                    "Sales page %s for branch %s on %s identical to previous page. Stopping.",
                    page_count + 1,
                    branch_code,
                    target_day,
                )
                break

            for record in page_rows:
                enriched = deepcopy(record)
                enriched["requestedDay"] = target_day
                enriched["requestedStatus"] = record.get("status")
                rows.append(enriched)

            page_count += 1
            previous_page = page_rows
            last_key = payload.get("lastKey") if isinstance(payload, dict) else None
            if not last_key:
                break

        return rows, page_count
