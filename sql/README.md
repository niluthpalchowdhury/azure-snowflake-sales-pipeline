# Sales pipeline — Snowflake SQL

## Run order

1. **One-time environment setup:** `001_setup_env.sql`
   Creates the warehouse, database, `RAW` schema, loader role and service user, plus grants.

2. **Sales tables:** `003_sales_tables.sql`
   Creates the `SALES_*` current-state tables (one row per invoice) and `SALES_INVOICE_HISTORY`.

3. **After a transformer load (optional):** `004_sales_variant_parse.sql`
   Standalone equivalent of the `PARSE_JSON` pass the loader already performs. Use
   `LAST_EXTRACTED_DAY = :target_day` for the rows touched in that run.

4. **Validate load & relationships:** `005_sales_validation.sql`
   Set `VALIDATION_LAST_EXTRACTED_DAY` in §0; §10 should show all `PASS`.

Numbering starts at `001` and skips `002`, which was retired.

## Documentation

| Doc | Topic |
|-----|--------|
| `../docs/section-04-sales-schema.md` | Tables, nested JSON mapping, column catalog |
| `../docs/section-05-sales-load-merge.md` | Re-extract merge design — **not implemented**; see the banner in that file |
| `../docs/section-06-sales-extract-schedule.md` | 10-min extractor, checkpoint resume, Timeline 1 + 7-day sweep |

## Implemented load behaviour

The transformer performs a **day-level delete-then-insert**, not a per-invoice merge:

```sql
DELETE FROM SALES_DB.RAW.<each child table> WHERE LAST_EXTRACTED_DAY = :target_day;
DELETE FROM SALES_DB.RAW.SALES_HEADER      WHERE LAST_EXTRACTED_DAY = :target_day;
-- then bulk insert the full snapshot, then PARSE_JSON the VARIANT columns
```

All of it runs in one transaction, so re-running a day is idempotent.

`SALES_INVOICE_HISTORY` is created by `003` but **never written** by the pipeline. The
`NET_AMOUNT` comparison and audit-pair logic described in `section-05` was designed and never
built. Statements and checks that target that table (in `004` and `005` §8) are retained for when
the merge is implemented, and currently match zero rows.

## Schema history

An earlier iteration keyed the current tables on `(BRANCH_CODE, INVOICE_NUMBER, SALES_BUSINESS_DAY)`.
The present schema keys them on `INVOICE_NUMBER` alone. Migrating from the earlier form requires new
tables or a `DROP` and recreate — do not run `003` blindly against an existing deployment without a
migration plan.
