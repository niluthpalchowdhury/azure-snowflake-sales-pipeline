import gzip
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.storage.blob import BlobLeaseClient, BlobServiceClient, ContentSettings


def build_sales_blob_path(target_day: str) -> str:
    year, month, day = target_day.split("-")
    return f"rista/sales/{year}/{month}/{day}/data.json.gz"


def build_checkpoint_path(target_day: str) -> str:
    year, month, day = target_day.split("-")
    return f"rista/sales/{year}/{month}/{day}/checkpoint.json"


def build_partial_data_path(target_day: str) -> str:
    year, month, day = target_day.split("-")
    return f"rista/sales/{year}/{month}/{day}/partial_data.json.gz"


def build_transform_checkpoint_path(target_day: str) -> str:
    year, month, day = target_day.split("-")
    return f"rista/sales/{year}/{month}/{day}/transform_checkpoint.json"


def decode_blob_json_payload(payload: bytes) -> dict:
    try:
        return json.loads(gzip.decompress(payload).decode("utf-8"))
    except gzip.BadGzipFile:
        return json.loads(payload.decode("utf-8"))


@dataclass
class BlobStorageWriter:
    account_name: str
    account_key: str
    raw_container: str
    error_container: str

    @classmethod
    def from_env(cls) -> "BlobStorageWriter":
        from os import environ

        return cls(
            account_name=environ["AZURE_STORAGE_ACCOUNT"].strip(),
            account_key=environ["AZURE_STORAGE_KEY"].strip(),
            raw_container=environ.get("AZURE_RAW_CONTAINER", "raw").strip() or "raw",
            error_container=environ.get("AZURE_ERROR_CONTAINER", "errors").strip() or "errors",
        )

    @property
    def _service_client(self) -> BlobServiceClient:
        account_url = f"https://{self.account_name}.blob.core.windows.net"
        return BlobServiceClient(account_url=account_url, credential=self.account_key)

    def upload_gzip_json(self, blob_path: str, payload: dict) -> None:
        raw_bytes = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        gzipped = gzip.compress(raw_bytes)
        self._service_client.get_blob_client(self.raw_container, blob_path).upload_blob(
            gzipped,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/gzip"),
        )

    def upload_error_json(self, target_day: str, component: str, payload: dict) -> None:
        year, month, day = target_day.split("-")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        blob_path = f"rista/sales/errors/{year}/{month}/{day}/{component}_{stamp}.json"
        error_bytes = json.dumps(payload, indent=2, ensure_ascii=True).encode("utf-8")
        self._service_client.get_blob_client(self.error_container, blob_path).upload_blob(
            error_bytes,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )

    def upload_failed_branches_json(self, target_day: str, payload: dict) -> None:
        year, month, day = target_day.split("-")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        blob_path = f"rista/sales/failed_branches/{year}/{month}/{day}/failed_{stamp}.json"
        body = json.dumps(payload, indent=2, ensure_ascii=True).encode("utf-8")
        self._service_client.get_blob_client(self.error_container, blob_path).upload_blob(
            body,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )

    def read_json_blob(self, blob_path: str) -> dict[str, Any] | None:
        blob_client = self._service_client.get_blob_client(self.raw_container, blob_path)
        try:
            payload = blob_client.download_blob().readall()
        except ResourceNotFoundError:
            return None
        return json.loads(payload.decode("utf-8"))

    def upload_json_blob(self, blob_path: str, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, indent=2, ensure_ascii=True).encode("utf-8")
        self._service_client.get_blob_client(self.raw_container, blob_path).upload_blob(
            body,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )

    def read_gzip_json_blob(self, blob_path: str) -> dict[str, Any] | None:
        blob_client = self._service_client.get_blob_client(self.raw_container, blob_path)
        try:
            payload = blob_client.download_blob(decompress=False).readall()
        except ResourceNotFoundError:
            return None
        return decode_blob_json_payload(payload)

    def blob_exists(self, blob_path: str) -> bool:
        return self._service_client.get_blob_client(self.raw_container, blob_path).exists()

    def read_checkpoint(self, target_day: str) -> dict[str, Any] | None:
        return self.read_json_blob(build_checkpoint_path(target_day))

    def write_checkpoint(
        self,
        target_day: str,
        payload: dict[str, Any],
        lease_id: str | None = None,
    ) -> None:
        blob_path = build_checkpoint_path(target_day)
        body = json.dumps(payload, indent=2, ensure_ascii=True).encode("utf-8")
        blob_client = self._service_client.get_blob_client(self.raw_container, blob_path)
        upload_kwargs: dict[str, Any] = {
            "overwrite": True,
            "content_settings": ContentSettings(content_type="application/json"),
        }
        if lease_id:
            upload_kwargs["lease"] = lease_id
        blob_client.upload_blob(body, **upload_kwargs)

    @staticmethod
    def _parse_iso_timestamp(value: str) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    @classmethod
    def is_checkpoint_heartbeat_stale(
        cls,
        checkpoint: dict[str, Any],
        stale_after_seconds: int,
    ) -> bool:
        heartbeat_at = cls._parse_iso_timestamp(checkpoint.get("lastHeartbeatAt", ""))
        if heartbeat_at is None:
            heartbeat_at = cls._parse_iso_timestamp(checkpoint.get("lastRunStartedAt", ""))
        if heartbeat_at is None:
            return True
        age_seconds = (datetime.now(timezone.utc) - heartbeat_at).total_seconds()
        return age_seconds > stale_after_seconds

    def read_partial_data(self, target_day: str) -> dict[str, Any] | None:
        return self.read_gzip_json_blob(build_partial_data_path(target_day))

    def write_partial_data(self, target_day: str, payload: dict[str, Any]) -> None:
        self.upload_gzip_json(build_partial_data_path(target_day), payload)

    def try_acquire_checkpoint_lease(
        self,
        target_day: str,
        lease_seconds: int,
        stale_after_seconds: int | None = None,
    ) -> BlobLeaseClient | None:
        blob_path = build_checkpoint_path(target_day)
        blob_client = self._service_client.get_blob_client(self.raw_container, blob_path)
        if not blob_client.exists():
            self.write_checkpoint(
                target_day,
                {
                    "targetDay": target_day,
                    "status": "initializing",
                },
            )

        heartbeat_stale_after = stale_after_seconds or max(lease_seconds * 3, 120)

        try:
            return blob_client.acquire_lease(lease_duration=lease_seconds)
        except HttpResponseError as exc:
            if exc.status_code != 409:
                raise

            checkpoint = self.read_checkpoint(target_day)
            if checkpoint and self.is_checkpoint_heartbeat_stale(checkpoint, heartbeat_stale_after):
                logging.warning(
                    "Breaking stale checkpoint lease for %s (no heartbeat for >%ss).",
                    target_day,
                    heartbeat_stale_after,
                )
                try:
                    blob_client.break_lease()
                    return blob_client.acquire_lease(lease_duration=lease_seconds)
                except HttpResponseError:
                    return None
            return None

    @staticmethod
    def renew_lease(lease_client: BlobLeaseClient | None) -> None:
        if lease_client is None:
            return
        try:
            lease_client.renew()
        except Exception:
            pass

    @staticmethod
    def release_lease(lease_client: BlobLeaseClient | None) -> None:
        if lease_client is None:
            return
        try:
            lease_client.release()
        except Exception:
            pass
