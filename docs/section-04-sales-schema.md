# Section 4 — Sales Snowflake Schema

Schema for Rista `/v1/sales/page` data loaded into `SALES_DB.RAW`.  
Aligned with the nested-JSON category SOP (flatten / child table / VARIANT + summary).

**SQL files**

| File | Purpose |
|------|---------|
| `sql/003_sales_tables.sql` | `CREATE TABLE` + grants (current + history) |
| `sql/004_sales_variant_parse.sql` | Post-load `PARSE_JSON` on VARIANT columns |
| `docs/section-05-sales-load-merge.md` | Per-invoice merge and re-extract rules |

**Prerequisite:** Snowflake warehouse, database, schema, and role from `sql/001_setup_env.sql`.

---

## 1. Load grain and keys (v2 — one row per invoice)

| Concept | Value |
|---------|--------|
| API | `GET /v1/sales/page` (`branch`, `day`, `lastKey`) |
| Blob type | `sales` → `raw/rista/sales/YYYY/MM/DD/data.json.gz` |
| Invoice primary key | **`INVOICE_NUMBER`** (globally unique) |
| Line primary key | **`INVOICE_NUMBER`** + **`ITEM_NUMBER`** |
| Last extract marker | `LAST_EXTRACTED_DAY` = `requestedDay` of the load that last wrote the row (not part of PK) |

**Merge / re-extract:** the transformer deletes and re-inserts the whole business day
(`WHERE LAST_EXTRACTED_DAY = :target_day`) inside one transaction. See
**`docs/section-05-sales-load-merge.md` §0**.

> **Designed but not implemented.** The two rules below were specified and never built. There is
> no `NET_AMOUNT` comparison in the code and no row is ever written to `SALES_INVOICE_HISTORY`.
>
> - Same `NET_AMOUNT` on re-extract → **skip** (no change).
> - Different `NET_AMOUNT` → write **two rows** to `SALES_INVOICE_HISTORY` (`SUPERSEDED` + `INCOMING`), then **replace** current header and all child rows.

---

## 2. Nested JSON category SOP

| Category | Rule |
|----------|------|
| **1 — Flatten to parent columns** | Scalar sub-fields become typed columns on the parent table |
| **2 — Child table** | One row per array element; FK = invoice keys (+ seq + line keys where needed) |
| **3 — VARIANT + summary** | Full JSON in `VARIANT`; optional scalar summary column |

---

## 3. Nested JSON → table mapping

| Nested JSON | Category | Snowflake target |
|-------------|----------|------------------|
| Pipeline: `requestedDay`, `requestedStatus` | 1 | `SALES_HEADER` |
| `delivery` | 2 | `SALES_DELIVERY` |
| `delivery.address` | 1 (parent = delivery table) | `SALES_DELIVERY` address columns |
| `deliveryBy` | 1 | `SALES_HEADER` |
| `resourceInfo` | 1 | `SALES_HEADER` |
| `customer` | 2 | `SALES_CUSTOMER` (0..1 row / invoice) |
| `sourceInfo` | 2 | `SALES_SOURCE_INFO` (0..1 row / invoice) |
| `sourceInfo.sourceCustomerDetail` | 2 | `SALES_SOURCE_CUSTOMER_DETAIL` (0..1 row / invoice) |
| `statusInfo` | 1 | `SALES_HEADER` |
| `charges[]` | 2 | `SALES_CHARGES` |
| `charges[i].taxes[]` | 3 | `SALES_CHARGES.CHARGE_TAXES_DETAIL` + `CHARGE_TAXES_TOTAL_AMOUNT` |
| `discounts[]` (header) | 2 | `SALES_DISCOUNTS` |
| `taxes[]` (header) | 3 | `SALES_HEADER.TAXES_DETAIL` + `TAXES_TOTAL_AMOUNT` |
| `payments[]` | 2 | `SALES_PAYMENTS` |
| `overallRefunds[]` | 3 | `SALES_HEADER.OVERALL_REFUNDS_DETAIL` + `OVERALL_REFUNDS_TOTAL_AMOUNT` |
| `eventLog[]` (header) | 2 | `SALES_EVENT_LOG` |
| `tags[]` | 1 | `SALES_HEADER.TAGS` (VARIANT array) + `TAG_COUNT` |
| `items[]` | 2 | `SALES_ITEMS` (+ all line scalars) |
| `reprintEvents[]` | 3 | `SALES_HEADER.REPRINT_EVENTS_DETAIL` + `REPRINT_EVENTS_COUNT` |
| `items[i].options[]` | 2 | `SALES_ITEM_OPTIONS` |
| `items[i].options[j].taxes[]` | 3 | `SALES_ITEM_OPTIONS.OPTION_TAXES_DETAIL` + `OPTION_TAXES_TOTAL_AMOUNT` |
| `items[i].taxes[]` | 3 | `SALES_ITEMS.LINE_TAXES_DETAIL` + `LINE_TAXES_TOTAL_AMOUNT` |
| `items[i].eventLog[]` | 2 | `SALES_ITEM_EVENT_LOG` |
| `items[i].discounts[]` | 2 | `SALES_ITEM_DISCOUNTS` |
| `items[i].itemLog` | 3 | `SALES_ITEMS.ITEM_LOG_DETAIL` only (no summary column) |
| `items[i].batches` | 3 | `SALES_ITEMS.BATCHES_DETAIL` only (no summary column) |

---

## 4. Table catalog

### 4.1 `SALES_HEADER`

**One current row per `INVOICE_NUMBER`.** Primary key: `INVOICE_NUMBER` only.

### 4.1b `SALES_INVOICE_HISTORY` — created, never written

> **Designed but not implemented.** This table is created by `sql/003_sales_tables.sql` and is
> **always empty**; the pipeline has no code path that inserts into it. It is retained so the
> merge feature can be added later without a schema migration.

Intended: paired audit rows when re-extraction changes `NET_AMOUNT`. Primary key: `INVOICE_NUMBER`, `HISTORY_ENTRY_SEQ`.  
See section 5 for `SNAPSHOT_ROLE` = `SUPERSEDED` | `INCOMING`.

**Includes:** all top-level invoice scalars, Category 1 nested fields, Category 3 header arrays.

**Notable columns**

| Column | Source |
|--------|--------|
| `REQUESTED_DAY` / `REQUESTED_STATUS` | Extractor enrichment |
| `DIRECT_CHARGE_AMOUNT` | API field `directChargeAmout` (typo preserved in API) |
| `DELIVERY_BY_*` | `deliveryBy` |
| `RESOURCE_*` | `resourceInfo` |
| `STATUS_INFO_*` | `statusInfo` (`remarks` may be JSON `null`) |
| `TAGS` | `tags[]` as JSON array in VARIANT |
| `TAG_COUNT` | `len(tags)` |
| `TAXES_DETAIL` / `TAXES_TOTAL_AMOUNT` | `taxes[]` / sum of `amount` |
| `OVERALL_REFUNDS_DETAIL` / `OVERALL_REFUNDS_TOTAL_AMOUNT` | `overallRefunds[]` / sum of `refundAmount` |
| `REPRINT_EVENTS_DETAIL` / `REPRINT_EVENTS_COUNT` | `reprintEvents[]` / array length |

### 4.2 `SALES_DELIVERY`

One row per invoice when `delivery` exists (0..1).

Address sub-object flattened: `ADDRESS_LINE`, `ADDRESS_CITY`, `ADDRESS_STATE`, `ADDRESS_COUNTRY`, `ADDRESS_ZIP`, `ADDRESS_LANDMARK`, `ADDRESS_LABEL` (landmark/label may be `null` in API).

Optional delivery fields: `email`, `phoneNumber`, `title`, `paymentMode`; `deliveryDate` may be empty string.

### 4.3 `SALES_SOURCE_INFO`

One row per invoice when `sourceInfo` exists.  
`IS_ECOM_ORDER` nullable (absent on some aggregator rows). `COMPANY_NAME` used on API/webhook sources.

### 4.4 `SALES_SOURCE_CUSTOMER_DETAIL`

One row per invoice when `sourceInfo.sourceCustomerDetail` exists (typically ecom).

### 4.5 `SALES_CUSTOMER`

One row per invoice when `customer` exists (dine-in / POS).

### 4.6 `SALES_CHARGES`

Grain: `CHARGE_SEQ` (1..n per invoice).

Category 3 on each charge: `CHARGE_TAXES_DETAIL`, `CHARGE_TAXES_TOTAL_AMOUNT` = sum of `taxes[].amount`.

### 4.7 `SALES_DISCOUNTS`

Header-level `discounts[]`; grain `DISCOUNT_SEQ`.

### 4.8 `SALES_PAYMENTS`

Grain: `PAYMENT_SEQ`. Fields: `mode`, `amount`, `reference` (optional / empty string).

### 4.9 `SALES_EVENT_LOG`

Header `eventLog[]`; grain `EVENT_SEQ`. Optional `note` on some events.

### 4.10 `SALES_ITEMS`

Line scalars + Category 3:

| Column | Summary rule |
|--------|----------------|
| `LINE_TAXES_DETAIL` / `LINE_TAXES_TOTAL_AMOUNT` | `taxes[]` / sum of `amount` |
| `ITEM_LOG_DETAIL` | Full `itemLog` JSON; **no summary column** |
| `BATCHES_DETAIL` | Full `batches` JSON; **no summary column** |

> `itemLog` and `batches` were not present in the sample payload; columns are reserved for when the API returns them.

### 4.11 `SALES_ITEM_OPTIONS`

Grain: `ITEM_NUMBER` + `OPTION_SEQ`. Option tax Category 3: sum of `taxes[].amount`.

### 4.12 `SALES_ITEM_EVENT_LOG` / `SALES_ITEM_DISCOUNTS`

Line-level `eventLog[]` and `discounts[]` with `EVENT_SEQ` / `DISCOUNT_SEQ`.

---

## 5. Transformer summary rules (Category 3)

| Source path | Summary column | Rule |
|-------------|----------------|------|
| `taxes[]` (header) | `TAXES_TOTAL_AMOUNT` | `SUM(element.amount)` |
| `charges[i].taxes[]` | `CHARGE_TAXES_TOTAL_AMOUNT` | `SUM(element.amount)` |
| `overallRefunds[]` | `OVERALL_REFUNDS_TOTAL_AMOUNT` | `SUM(element.refundAmount)` |
| `reprintEvents[]` | `REPRINT_EVENTS_COUNT` | `COUNT(elements)` |
| `items[i].taxes[]` | `LINE_TAXES_TOTAL_AMOUNT` | `SUM(element.amount)` |
| `options[j].taxes[]` | `OPTION_TAXES_TOTAL_AMOUNT` | `SUM(element.amount)` |
| `tags[]` | `TAG_COUNT` | `COUNT(elements)`; store array in `TAGS` VARIANT |
| `itemLog`, `batches` | — | VARIANT only; no summary |

Empty arrays: `VARIANT` = `[]` or `NULL` per transformer convention; summary numeric = `0` or `NULL` (transformer should be consistent).

---

## 6. Data types

| Kind | Snowflake type |
|------|----------------|
| IDs, names, status | `VARCHAR` |
| Money | `NUMBER(18, 2)` |
| Quantity | `NUMBER(18, 4)` |
| Counts | `NUMBER(10, 0)` |
| Dates | `DATE` |
| Timestamps | `TIMESTAMP_NTZ` |
| Boolean | `BOOLEAN` |
| Nested JSON / arrays | `VARIANT` |
| Audit | `LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()` |

---

## 7. Clustering

Current tables: `CLUSTER BY (INVOICE_NUMBER)` or `(INVOICE_DAY, BRANCH_CODE)` on header.  
History: `CLUSTER BY (INVOICE_NUMBER, REEXTRACTION_REQUESTED_DAY)`.

---

## 8. Post-load VARIANT parsing

After `write_pandas`, run statements in `sql/004_sales_variant_parse.sql` for the target day so VARIANT columns hold parsed JSON, not string literals.

---

## 9. Verification queries

```sql
SELECT LAST_EXTRACTED_DAY, COUNT(*) AS invoices
FROM SALES_DB.RAW.SALES_HEADER
GROUP BY 1 ORDER BY 1 DESC;

SELECT LAST_EXTRACTED_DAY, COUNT(*) AS lines
FROM SALES_DB.RAW.SALES_ITEMS
GROUP BY 1 ORDER BY 1 DESC;
```

---

## 10. Gaps / forward compatibility

| Item | Notes |
|------|--------|
| `itemLog` | Not in the sample payload; `ITEM_LOG_DETAIL` VARIANT ready |
| `batches` | Not in the sample payload; `BATCHES_DETAIL` VARIANT ready |
| `overallRefunds[]` | Sample always `[]`; total uses `refundAmount` when populated |
| `IGST` | Not in the sample payload; stored inside `TAXES_DETAIL` VARIANT when present |

---

## 11. Related pipeline docs

- Extractor: `/v1/branch/list` + `/v1/sales/page`, checkpointed multi-run, dedupe `branchCode` + `invoiceNumber`
- Enrichment: `requestedDay`, `requestedStatus` (= sale `status`)
- Base connector patterns: `rista_to_snowflake_connector_instructions.md`
