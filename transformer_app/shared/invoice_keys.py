"""Shared invoice key normalization for transform + Snowflake load."""


def normalize_invoice_number(value: object) -> str:
    return str(value or "").strip().upper()
