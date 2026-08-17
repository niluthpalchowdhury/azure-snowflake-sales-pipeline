import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector.pandas_tools import write_pandas

from shared.invoice_keys import normalize_invoice_number
from transformers.sales_transformer import SalesTransformResult


def _normalize_private_key_body(raw: str) -> str:
    """Normalize PEM body from env vars (Azure may store literal \\n, spaces, or headers)."""
    s = raw.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        s = s[1:-1].strip()

    s = re.sub(r"-----BEGIN [^-]+-----", "", s, flags=re.IGNORECASE)
    s = re.sub(r"-----END [^-]+-----", "", s, flags=re.IGNORECASE)

    # Literal escape sequences from portal / JSON (InvalidByte often = backslash 0x5C)
    s = s.replace("\\r\\n", "").replace("\\r", "").replace("\\n", "")
    s = s.replace("\r\n", "").replace("\r", "").replace("\n", "")
    s = s.strip()

    if " " in s:
        s = s.replace(" ", "+")
    if "\\" in s:
        s = s.replace("\\", "")

    # PKCS#8 body is base64 only
    s = re.sub(r"[^A-Za-z0-9+/=]", "", s)
    return s


def _pem_bytes_from_env_body(body: str) -> bytes:
    normalized = _normalize_private_key_body(body)
    if not normalized:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY is empty after normalization.")
    return (
        "-----BEGIN PRIVATE KEY-----\n"
        f"{normalized}\n"
        "-----END PRIVATE KEY-----"
    ).encode("utf-8")


def load_private_key_der_from_env() -> bytes:
    """Load Snowflake key-pair private key from SNOWFLAKE_PRIVATE_KEY (PEM body, normalized)."""
    from os import environ

    body = environ.get("SNOWFLAKE_PRIVATE_KEY", "").strip()
    if not body:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY is required (PEM body only, no BEGIN/END lines).")

    passphrase = environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").strip() or None
    password = passphrase.encode("utf-8") if passphrase else None
    pem_data = _pem_bytes_from_env_body(body)

    try:
        private_key = serialization.load_pem_private_key(
            pem_data,
            password=password,
            backend=default_backend(),
        )
    except ValueError as exc:
        normalized_len = len(_normalize_private_key_body(body))
        raise ValueError(
            "Could not parse Snowflake private key after normalization "
            f"(normalized body length={normalized_len}, expected ~1624). "
            "Check SNOWFLAKE_PRIVATE_KEY on the Function App."
        ) from exc
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


_private_key_der_cache: bytes | None = None


def get_private_key_der_from_env() -> bytes:
    """Return cached DER private key (parse PEM once per process)."""
    global _private_key_der_cache
    if _private_key_der_cache is None:
        _private_key_der_cache = load_private_key_der_from_env()
    return _private_key_der_cache


def _dedupe_header_rows(rows: list[dict]) -> list[dict]:
    by_invoice: dict[str, dict] = {}
    for row in rows:
        invoice_number = normalize_invoice_number(row.get("invoice_number"))
        if not invoice_number:
            continue
        row["invoice_number"] = invoice_number
        if invoice_number in by_invoice:
            logging.warning(
                "Duplicate SALES_HEADER row for invoice %s in load batch; keeping latest.",
                invoice_number,
            )
        by_invoice[invoice_number] = row
    return list(by_invoice.values())


def _normalize_headers_by_invoice(headers: list[dict]) -> dict[str, dict]:
    by_invoice: dict[str, dict] = {}
    for row in headers:
        invoice_number = normalize_invoice_number(row.get("invoice_number"))
        if not invoice_number:
            continue
        row["invoice_number"] = invoice_number
        if invoice_number in by_invoice:
            logging.warning(
                "Duplicate invoice %s in transform batch; keeping latest row.",
                invoice_number,
            )
        by_invoice[invoice_number] = row
    return by_invoice


CHILD_TABLES = [
    "SALES_ITEM_DISCOUNTS",
    "SALES_ITEM_EVENT_LOG",
    "SALES_ITEM_OPTIONS",
    "SALES_ITEMS",
    "SALES_EVENT_LOG",
    "SALES_PAYMENTS",
    "SALES_DISCOUNTS",
    "SALES_CHARGES",
    "SALES_CUSTOMER",
    "SALES_SOURCE_CUSTOMER_DETAIL",
    "SALES_SOURCE_INFO",
    "SALES_DELIVERY",
]

VARIANT_COLUMNS = {
    "SALES_HEADER": [
        "TAGS",
        "TAXES_DETAIL",
        "OVERALL_REFUNDS_DETAIL",
        "REPRINT_EVENTS_DETAIL",
    ],
    "SALES_CHARGES": ["CHARGE_TAXES_DETAIL"],
    "SALES_ITEMS": ["LINE_TAXES_DETAIL", "ITEM_LOG_DETAIL", "BATCHES_DETAIL"],
    "SALES_ITEM_OPTIONS": ["OPTION_TAXES_DETAIL"],
}

DATE_COLUMNS_BY_TABLE: dict[str, list[str]] = {}

DECIMAL_COLUMNS_BY_TABLE: dict[str, list[str]] = {}


def _coerce_date(value: Any) -> date | None:
    """Normalize Snowflake DATE values (date, datetime, or ISO string) for write_pandas."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])


def _coerce_dataframe_date_columns(dataframe: pd.DataFrame, table_name: str) -> pd.DataFrame:
    for column in DATE_COLUMNS_BY_TABLE.get(table_name, []):
        if column not in dataframe.columns:
            continue
        dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce").dt.date
    return dataframe


def _coerce_decimal(value: Any) -> Decimal | None:
    """Normalize NUMBER columns for write_pandas (Snowflake Decimal vs Python float)."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _coerce_dataframe_decimal_columns(dataframe: pd.DataFrame, table_name: str) -> pd.DataFrame:
    for column in DECIMAL_COLUMNS_BY_TABLE.get(table_name, []):
        if column not in dataframe.columns:
            continue
        dataframe[column] = dataframe[column].map(_coerce_decimal)
    return dataframe


@dataclass
class SalesSnowflakeLoader:
    account: str
    user: str
    database: str
    schema: str
    warehouse: str
    role: str | None = None

    @classmethod
    def from_env(cls) -> "SalesSnowflakeLoader":
        from os import environ

        return cls(
            account=environ["SNOWFLAKE_ACCOUNT"].strip(),
            user=environ["SNOWFLAKE_USER"].strip(),
            database=environ["SNOWFLAKE_DATABASE"].strip(),
            schema=environ["SNOWFLAKE_SCHEMA"].strip(),
            warehouse=environ["SNOWFLAKE_WAREHOUSE"].strip(),
            role=environ.get("SNOWFLAKE_ROLE", "").strip() or None,
        )

    def connect(self):
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            private_key=get_private_key_der_from_env(),
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
            role=self.role,
            autocommit=False,
        )

    def _execute_day_load(
        self,
        connection,
        extracted_day: date,
        transformed: SalesTransformResult,
        headers_by_invoice: dict[str, dict],
    ) -> None:
        cursor = connection.cursor()
        try:
            self._delete_day_data(cursor, extracted_day)

            self._write_rows(connection, list(headers_by_invoice.values()), "SALES_HEADER")
            self._write_rows(connection, transformed.deliveries, "SALES_DELIVERY")
            self._write_rows(connection, transformed.source_infos, "SALES_SOURCE_INFO")
            self._write_rows(connection, transformed.source_customers, "SALES_SOURCE_CUSTOMER_DETAIL")
            self._write_rows(connection, transformed.customers, "SALES_CUSTOMER")
            self._write_rows(connection, transformed.charges, "SALES_CHARGES")
            self._write_rows(connection, transformed.discounts, "SALES_DISCOUNTS")
            self._write_rows(connection, transformed.payments, "SALES_PAYMENTS")
            self._write_rows(connection, transformed.event_logs, "SALES_EVENT_LOG")
            self._write_rows(connection, transformed.items, "SALES_ITEMS")
            self._write_rows(connection, transformed.item_options, "SALES_ITEM_OPTIONS")
            self._write_rows(connection, transformed.item_event_logs, "SALES_ITEM_EVENT_LOG")
            self._write_rows(connection, transformed.item_discounts, "SALES_ITEM_DISCOUNTS")

            self._parse_variant_columns_for_day(cursor, "SALES_HEADER", extracted_day)
            self._parse_variant_columns_for_day(cursor, "SALES_CHARGES", extracted_day)
            self._parse_variant_columns_for_day(cursor, "SALES_ITEMS", extracted_day)
            self._parse_variant_columns_for_day(cursor, "SALES_ITEM_OPTIONS", extracted_day)
        finally:
            cursor.close()

    def load(
        self,
        target_day: str,
        transformed: SalesTransformResult,
        connection: Any | None = None,
    ) -> dict[str, int]:
        extracted_day = _coerce_date(target_day)
        if extracted_day is None:
            raise ValueError(f"Invalid target_day: {target_day!r}")

        headers_by_invoice = _normalize_headers_by_invoice(transformed.headers)
        header_count = len(headers_by_invoice)

        metrics = {
            "inserted": header_count,
            "skipped": 0,
            "replaced": 0,
            "history_rows": 0,
            "reload_mode": "day_delete_insert",
        }

        if connection is not None:
            try:
                self._execute_day_load(connection, extracted_day, transformed, headers_by_invoice)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        else:
            with self.connect() as owned_connection:
                try:
                    self._execute_day_load(
                        owned_connection,
                        extracted_day,
                        transformed,
                        headers_by_invoice,
                    )
                    owned_connection.commit()
                except Exception:
                    owned_connection.rollback()
                    raise

        logging.info(
            "Sales day reload for %s: deleted and inserted %s invoices.",
            target_day,
            header_count,
        )
        return metrics

    def _delete_day_data(self, cursor, target_day: date) -> None:
        for table in CHILD_TABLES:
            cursor.execute(
                f"DELETE FROM {self.database}.{self.schema}.{table} "
                "WHERE LAST_EXTRACTED_DAY = %s",
                (target_day,),
            )
        cursor.execute(
            f"DELETE FROM {self.database}.{self.schema}.SALES_HEADER "
            "WHERE LAST_EXTRACTED_DAY = %s",
            (target_day,),
        )

    def _write_rows(self, connection, rows: list[dict], table_name: str) -> None:
        if not rows:
            return
        if table_name == "SALES_HEADER":
            rows = _dedupe_header_rows(rows)
            if not rows:
                return
        dataframe = pd.DataFrame(rows)
        dataframe.columns = [column.upper() for column in dataframe.columns]
        dataframe = _coerce_dataframe_date_columns(dataframe, table_name)
        dataframe = _coerce_dataframe_decimal_columns(dataframe, table_name)
        write_pandas(
            connection,
            dataframe,
            table_name,
            database=self.database,
            schema=self.schema,
            auto_create_table=False,
        )

    def _parse_variant_columns_for_day(self, cursor, table_name: str, target_day: date) -> None:
        columns = VARIANT_COLUMNS.get(table_name, [])
        if not columns:
            return

        for column in columns:
            cursor.execute(
                f"UPDATE {self.database}.{self.schema}.{table_name} "
                f"SET {column} = PARSE_JSON({column}::STRING) "
                f"WHERE LAST_EXTRACTED_DAY = %s AND {column} IS NOT NULL",
                (target_day,),
            )
