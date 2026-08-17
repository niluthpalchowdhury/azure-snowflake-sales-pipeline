# Section 5 — Sales load and re-extraction merge rules

> **⚠️ DESIGNED BUT NOT IMPLEMENTED**
>
> The per-invoice merge algorithm described in sections 1–9 of this document **was never built**.
> The transformer performs a day-level delete-then-insert instead. No `NET_AMOUNT` comparison
> exists anywhere in the code, and `SALES_INVOICE_HISTORY` — though created by
> `sql/003_sales_tables.sql` — is never written.
>
> This file is kept as the design record for that unbuilt feature. **For what the pipeline
> actually does, read §0 below.** Treat everything after §0 as a proposal, not documentation.

---

## 0. Implemented behaviour

`transformer_app/shared/snowflake_loader.py` loads one whole business day atomically:

| Step | Statement |
|------|-----------|
| 1 | `DELETE FROM <each of the 12 child tables> WHERE LAST_EXTRACTED_DAY = :target_day` |
| 2 | `DELETE FROM SALES_HEADER WHERE LAST_EXTRACTED_DAY = :target_day` |
| 3 | `write_pandas` bulk insert of every row list produced from the snapshot |
| 4 | `UPDATE ... SET <col> = PARSE_JSON(<col>::STRING)` for the 9 VARIANT columns |
| 5 | `COMMIT` — or `ROLLBACK` and re-raise on any failure |

What follows from that:

- **Idempotent per day.** Re-running the same snapshot yields the same rows.
- **No comparison, no skipping.** Incoming data always replaces the day. `NET_AMOUNT` is never
  compared against the existing row.
- **No audit trail.** Prior values of a changed invoice are not retained anywhere, because
  `SALES_INVOICE_HISTORY` is never populated.
- **The delete key is not the primary key.** Deletes target `LAST_EXTRACTED_DAY`, while the
  declared primary key is `INVOICE_NUMBER`. An invoice that previously arrived under a different
  `target_day` is not removed by the delete, and Snowflake does not enforce primary keys.
- **Metrics are placeholders.** `load()` returns
  `{"inserted": <header count>, "skipped": 0, "replaced": 0, "history_rows": 0, "reload_mode": "day_delete_insert"}`
  — the three zeros are hardcoded, not measured.

---

## The unbuilt design

Everything below describes how the transformer *would* load `/v1/sales/page` data under a
per-invoice merge. It is not the current behaviour.

---

## 1. Table roles

| Table | Role |
|--------|------|
| `SALES_HEADER` (+ child tables) | **Current state** — latest accepted data per `INVOICE_NUMBER` |
| `SALES_INVOICE_HISTORY` | **Audit** — paired snapshots when re-extract changes `NET_AMOUNT` |

`LAST_EXTRACTED_DAY` on current tables = `requestedDay` from the extract that last wrote the row.  
`REEXTRACTION_REQUESTED_DAY` on history = the `TARGET_DAY` of the re-extract run that detected the conflict.

---

## 2. Compare field

Re-extraction decision uses **`NET_AMOUNT`** only (API field `netAmount` → column `NET_AMOUNT`).

Comparison: treat as equal if `ABS(existing - incoming) < 0.01` (one paisa tolerance) unless you standardize on exact decimal match.

---

## 3. Per-invoice algorithm

For each invoice in the daily blob (`requestedDay` = run’s `target_day`):

```
existing = SELECT * FROM SALES_HEADER WHERE INVOICE_NUMBER = :invoice

IF existing IS NULL:
    INSERT full invoice graph (header + all child tables)
    STOP

IF existing.NET_AMOUNT matches incoming.NET_AMOUNT:
    SKIP — no update to current tables, no history row
    STOP

-- NET_AMOUNT mismatch: archive both snapshots, then replace current
conflict_id = new UUID
next_seq = MAX(HISTORY_ENTRY_SEQ) + 1 for this invoice (or 1)

INSERT SALES_INVOICE_HISTORY (
  SNAPSHOT_ROLE = 'SUPERSEDED',
  EXTRACTED_DAY = existing.LAST_EXTRACTED_DAY,
  NET_AMOUNT = existing.NET_AMOUNT,
  HEADER_SNAPSHOT = full existing header as JSON,
  ...
)
INSERT SALES_INVOICE_HISTORY (
  SNAPSHOT_ROLE = 'INCOMING',
  EXTRACTED_DAY = incoming.REQUESTED_DAY,
  NET_AMOUNT = incoming.NET_AMOUNT,
  HEADER_SNAPSHOT = full incoming header as JSON,
  ...
)

DELETE all child rows for INVOICE_NUMBER (see list below)
UPDATE or DELETE+INSERT SALES_HEADER with incoming row
INSERT all child rows from incoming payload
```

### Child tables to replace on conflict (same `INVOICE_NUMBER`)

Delete before re-insert (order avoids orphan checks in app logic):

```text
SALES_ITEM_DISCOUNTS
SALES_ITEM_EVENT_LOG
SALES_ITEM_OPTIONS
SALES_ITEMS
SALES_EVENT_LOG
SALES_PAYMENTS
SALES_DISCOUNTS
SALES_CHARGES
SALES_CUSTOMER
SALES_SOURCE_CUSTOMER_DETAIL
SALES_SOURCE_INFO
SALES_DELIVERY
```

Satellite tables (0..1 row): omit row if incoming JSON has no object; delete existing row if incoming has none.

---

## 4. History table columns

| Column | Purpose |
|--------|---------|
| `INVOICE_NUMBER` | Invoice id |
| `HISTORY_ENTRY_SEQ` | Monotonic per invoice (`MAX+1` for each insert) |
| `CONFLICT_GROUP_ID` | Same UUID on **both** rows for one conflict event |
| `SNAPSHOT_ROLE` | `SUPERSEDED` = old current row; `INCOMING` = new extract |
| `EXTRACTED_DAY` | `LAST_EXTRACTED_DAY` (superseded) or new `REQUESTED_DAY` (incoming) |
| `REEXTRACTION_REQUESTED_DAY` | Run’s `target_day` |
| `NET_AMOUNT` | Amount for that snapshot |
| `HEADER_SNAPSHOT` | Optional full header VARIANT for forensics |
| `RECORDED_AT` | When conflict was logged |

---

## 5. Primary keys (current tables)

| Table | Primary key |
|--------|-------------|
| `SALES_HEADER` | `INVOICE_NUMBER` |
| `SALES_DELIVERY` | `INVOICE_NUMBER` |
| `SALES_SOURCE_INFO` | `INVOICE_NUMBER` |
| `SALES_SOURCE_CUSTOMER_DETAIL` | `INVOICE_NUMBER` |
| `SALES_CUSTOMER` | `INVOICE_NUMBER` |
| `SALES_CHARGES` | `INVOICE_NUMBER`, `CHARGE_SEQ` |
| `SALES_DISCOUNTS` | `INVOICE_NUMBER`, `DISCOUNT_SEQ` |
| `SALES_PAYMENTS` | `INVOICE_NUMBER`, `PAYMENT_SEQ` |
| `SALES_EVENT_LOG` | `INVOICE_NUMBER`, `EVENT_SEQ` |
| `SALES_ITEMS` | `INVOICE_NUMBER`, `ITEM_NUMBER` |
| `SALES_ITEM_OPTIONS` | `INVOICE_NUMBER`, `ITEM_NUMBER`, `OPTION_SEQ` |
| `SALES_ITEM_EVENT_LOG` | `INVOICE_NUMBER`, `ITEM_NUMBER`, `EVENT_SEQ` |
| `SALES_ITEM_DISCOUNTS` | `INVOICE_NUMBER`, `ITEM_NUMBER`, `DISCOUNT_SEQ` |
| `SALES_INVOICE_HISTORY` | `INVOICE_NUMBER`, `HISTORY_ENTRY_SEQ` |

`BRANCH_CODE` is **not** part of any PK (invoice number is globally unique).

---

## 6. Join pattern

All children join to header on:

```sql
child.INVOICE_NUMBER = header.INVOICE_NUMBER
```

---

## 7. First-time vs re-extract of a calendar day

> **Not implemented.** The transformer *does* delete `WHERE LAST_EXTRACTED_DAY = target_day` for
> the whole day — see §0. The bullets below describe the unbuilt design.

- **Extractor** can still write blob `raw/rista/sales/YYYY/MM/DD/data.json.gz` per `target_day`.
- **Transformer** no longer deletes `WHERE LAST_EXTRACTED_DAY = target_day` for the whole day.
- It processes **each invoice** in the blob with the merge rules above.

Re-running transform for the same blob day is safe: matching `NET_AMOUNT` → no-op; mismatch → another history pair + replace.

**Which days to extract/load:** see `docs/section-06-sales-extract-schedule.md` (10-minute extractor, branch checkpoint resume, Timeline 1 + 10 AM 7-day Timeline 2).

---

## 8. Metrics to log per run

> **Not implemented.** The loader returns `skipped`, `replaced` and `history_rows` hardcoded to
> `0`; only `inserted` (a header count) is real. See §0.

| Metric | Meaning |
|--------|---------|
| `inserted_count` | New invoices |
| `skipped_count` | Existing, same `NET_AMOUNT` |
| `replaced_count` | Existing, different `NET_AMOUNT`, current updated |
| `history_rows_written` | `2 * replaced_count` |

---

## 9. Verification SQL

```sql
-- Current invoices updated on a given extract day
SELECT COUNT(*) FROM SALES_DB.RAW.SALES_HEADER
WHERE LAST_EXTRACTED_DAY = '2026-05-12';

-- Conflicts from a re-extract run
SELECT REEXTRACTION_REQUESTED_DAY, COUNT(DISTINCT CONFLICT_GROUP_ID) AS conflicts
FROM SALES_DB.RAW.SALES_INVOICE_HISTORY
GROUP BY 1;

-- Paired history for one invoice
SELECT *
FROM SALES_DB.RAW.SALES_INVOICE_HISTORY
WHERE INVOICE_NUMBER = 'INV-1001'
ORDER BY HISTORY_ENTRY_SEQ;
```
