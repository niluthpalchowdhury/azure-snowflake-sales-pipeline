import gzip
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings


def build_sales_blob_path(target_day: str) -> str:
    year, month, day = target_day.split("-")
    return f"rista/sales/{year}/{month}/{day}/data.json.gz"


def build_checkpoint_path(target_day: str) -> str:
    year, month, day = target_day.split("-")
    return f"rista/sales/{year}/{month}/{day}/checkpoint.json"


def build_transform_checkpoint_path(target_day: str) -> str:
    year, month, day = target_day.split("-")
    return f"rista/sales/{year}/{month}/{day}/transform_checkpoint.json"


def build_transform_lease_path(target_day: str) -> str:
    year, month, day = target_day.split("-")
    return f"rista/sales/{year}/{month}/{day}/transform_lease.json"


def _transform_lease_ttl_seconds() -> int:
    from os import environ

    configured = environ.get("TRANSFORM_LEASE_SECONDS", "900").strip()
    try:
        return max(60, int(configured))
    except ValueError:
        return 900


def decode_blob_json_payload(payload: bytes) -> dict:
    try:
        return json.loads(gzip.decompress(payload).decode("utf-8"))
    except gzip.BadGzipFile:
        return json.loads(payload.decode("utf-8"))


@dataclass
class BlobStorageReader:
    account_name: str
    account_key: str
    raw_container: str
    error_container: str

    @classmethod
    def from_env(cls) -> "BlobStorageReader":
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

    def read_gzip_json(self, blob_path: str) -> dict:
        blob = self._service_client.get_blob_client(self.raw_container, blob_path)
        payload = blob.download_blob(decompress=False).readall()
        return decode_blob_json_payload(payload)

    def read_json_blob(self, blob_path: str) -> dict | None:
        blob = self._service_client.get_blob_client(self.raw_container, blob_path)
        try:
            payload = blob.download_blob().readall()
        except ResourceNotFoundError:
            return None
        return json.loads(payload.decode("utf-8"))

    def read_checkpoint(self, target_day: str) -> dict | None:
        return self.read_json_blob(build_checkpoint_path(target_day))

    def read_transform_checkpoint(self, target_day: str) -> dict | None:
        return self.read_json_blob(build_transform_checkpoint_path(target_day))

    def write_transform_checkpoint(self, target_day: str, payload: dict) -> None:
        blob_path = build_transform_checkpoint_path(target_day)
        body = json.dumps(payload, indent=2, ensure_ascii=True).encode("utf-8")
        self._service_client.get_blob_client(self.raw_container, blob_path).upload_blob(
            body,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )

    def read_transform_lease(self, target_day: str) -> dict | None:
        return self.read_json_blob(build_transform_lease_path(target_day))

    def try_begin_transform(self, target_day: str, extracted_at: str) -> bool:
        """Claim transform for this snapshot; return False if already done or in progress."""
        transform_checkpoint = self.read_transform_checkpoint(target_day)
        if transform_checkpoint and transform_checkpoint.get("extractedAt") == extracted_at:
            return False

        now = datetime.now(timezone.utc)
        lease = self.read_transform_lease(target_day)
        if lease and lease.get("extractedAt") == extracted_at and lease.get("status") == "in_progress":
            started_raw = lease.get("startedAt")
            if started_raw:
                started_at = datetime.fromisoformat(started_raw.replace("Z", "+00:00"))
                if started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=timezone.utc)
                age_seconds = (now - started_at).total_seconds()
                if age_seconds < _transform_lease_ttl_seconds():
                    return False

        self.write_json_blob(
            build_transform_lease_path(target_day),
            {
                "targetDay": target_day,
                "extractedAt": extracted_at,
                "status": "in_progress",
                "runId": str(uuid.uuid4()),
                "startedAt": now.isoformat(),
            },
        )
        return True

    def clear_transform_lease(self, target_day: str) -> None:
        blob_path = build_transform_lease_path(target_day)
        blob = self._service_client.get_blob_client(self.raw_container, blob_path)
        try:
            blob.delete_blob()
        except ResourceNotFoundError:
            pass

    def write_json_blob(self, blob_path: str, payload: dict) -> None:
        body = json.dumps(payload, indent=2, ensure_ascii=True).encode("utf-8")
        self._service_client.get_blob_client(self.raw_container, blob_path).upload_blob(
            body,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )

    def is_extraction_complete(self, target_day: str) -> bool:
        checkpoint = self.read_checkpoint(target_day)
        if checkpoint is None:
            blob_path = build_sales_blob_path(target_day)
            return self._service_client.get_blob_client(self.raw_container, blob_path).exists()
        return checkpoint.get("status") == "completed"

    def read_final_snapshot(self, target_day: str) -> dict:
        return self.read_gzip_json(build_sales_blob_path(target_day))

    def snapshot_extracted_at(self, target_day: str) -> str | None:
        if not self.is_extraction_complete(target_day):
            return None
        checkpoint = self.read_checkpoint(target_day)
        if checkpoint and checkpoint.get("snapshotExtractedAt"):
            return checkpoint["snapshotExtractedAt"]
        snapshot = self.read_final_snapshot(target_day)
        return snapshot.get("extractedAt")

    def needs_transform(self, target_day: str) -> bool:
        extracted_at = self.snapshot_extracted_at(target_day)
        if not extracted_at:
            return False

        transform_checkpoint = self.read_transform_checkpoint(target_day)
        if transform_checkpoint is None:
            return True
        return transform_checkpoint.get("extractedAt") != extracted_at

    def upload_error_json(self, target_day: str, component: str, payload: dict) -> None:
        year, month, day = target_day.split("-")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        blob_path = f"rista/sales/errors/{year}/{month}/{day}/{component}_{stamp}.json"
        self._service_client.get_blob_client(self.error_container, blob_path).upload_blob(
            json.dumps(payload, indent=2, ensure_ascii=True).encode("utf-8"),
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )
