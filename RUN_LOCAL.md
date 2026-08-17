# Running the pipeline locally

Two Azure Functions apps:

- `extractor_app` — Rista Sales API → Azure Blob (`rista/sales/...`)
- `transformer_app` — Blob snapshot → Snowflake (day delete + full reinsert by `LAST_EXTRACTED_DAY`)

Windows + PowerShell assumed. Paths below are relative to the repository root.

## Prerequisites

- Python 3.11+
- Azure Functions Core Tools (`func`)
- An Azure Storage account with two containers: `raw` and `errors`
- Snowflake environment from `sql/001_setup_env.sql`, then tables from `sql/003_sales_tables.sql`
- An RSA key pair for Snowflake key-pair authentication (see below)

### Snowflake key pair

Generate an unencrypted PKCS#8 key pair:

```powershell
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
```

Register the public key on the service user once (body only, no `BEGIN`/`END` lines):

```sql
ALTER USER <SNOWFLAKE_USER> SET RSA_PUBLIC_KEY='<public key body without headers>';
```

The transformer reads the private key only from **`SNOWFLAKE_PRIVATE_KEY`** (PEM body only, no
`BEGIN`/`END` lines). The same setting is used for local `local.settings.json` and for Azure app
settings.

```json
"SNOWFLAKE_PRIVATE_KEY": "MIIEvg...<body>...YIIH"
```

If the `.p8` file is encrypted, also set `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`.

Key files are covered by `.gitignore` — never commit them.

## Common

- Copy `local.settings.example.json` → `local.settings.json` in each app folder and fill in values.
- **Restart `func start`** after changing settings; they are read at startup.
- Optional backfill: set `TARGET_DAY` or comma-separated `TARGET_DAYS`. Clear them afterwards —
  see the warning in the README.
- Production schedules: extractor `0 0/10 * * * *` (every 10 min), transformer `0 5/30 * * * *`
  (every 30 min at :05 and :35). Set `WEBSITE_TIME_ZONE=India Standard Time` on Azure.

---

## 1) Extractor

```powershell
cd extractor_app
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy local.settings.example.json local.settings.json
func start --port 7071
```

Trigger manually:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:7071/admin/functions/rista_sales_extractor" `
  -ContentType "application/json" `
  -Body '{"input":""}'
```

Blobs per day (`raw` container):

| Path | Purpose |
|------|---------|
| `rista/sales/YYYY/MM/DD/checkpoint.json` | Branch progress |
| `rista/sales/YYYY/MM/DD/partial_data.json.gz` | In-progress rows |
| `rista/sales/YYYY/MM/DD/data.json.gz` | Final snapshot when `status=completed` |

Re-run a completed day: delete the three blobs above, or set `TARGET_DAY` (the backfill override
re-initializes that day on the next extractor tick).

Because one invocation only advances the work, repeat the trigger until the logs show
`Extraction completed for <day>`.

**Branch progress logs** appear as:

```text
Branch progress [2026-05-28]: 12/150 done overall | 1 failed | 5 processed this run | 45 left this run | ~137 left overall
```

**API call CSV** (one file per business day, appended per run):

- Default path: `extractor_app/logs/api_calls/<TARGET_DAY>_api_calls.csv`
- Override directory: `API_CALL_LOG_DIR`
- Columns: timestamp, endpoint, URL, branch, page, status, duration, record count, outcome, errors

> On Azure this writes inside the deployment directory, which is read-only when running from
> package. Set `API_CALL_LOG_DIR` to a writable path, or treat this as a local-only tool. See
> "Data quality & known limitations" in the README.

---

## 2) Transformer

```powershell
cd transformer_app
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy local.settings.example.json local.settings.json
func start --port 7072
```

Trigger manually:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:7072/admin/functions/rista_sales_transformer" `
  -ContentType "application/json" `
  -Body '{"input":""}'
```

The transformer:

- Skips days where extraction is not `completed`.
- Skips when `transform_checkpoint.json` already has the same `extractedAt` as the final snapshot.
- Without `TARGET_DAY` / `TARGET_DAYS`, scans the last 8 calendar days for pending transforms.
- Each load **deletes** all rows for that `LAST_EXTRACTED_DAY` in the current tables, then
  **re-inserts** the full blob snapshot, inside one transaction.

Success log example: `Day reload for 2026-05-28: N invoices (mode=day_delete_insert).`

Errors: `errors/rista/sales/errors/YYYY/MM/DD/transformer_<timestamp>.json`

### Historical refresh (local script only)

Re-load Snowflake from existing blobs for a date range. Use when backfilling or repairing
historical RAW data. Requires a completed `data.json.gz` per day — run the extractor first if one
is missing.

```powershell
cd transformer_app
.venv\Scripts\Activate.ps1
python scripts/historical_refresh.py --start 2026-05-01 --end 2026-05-28
```

| Flag | Purpose |
|------|---------|
| `--dry-run` | Show days and invoice counts only; no Snowflake writes |
| `--skip-missing` | Skip days without a completed blob instead of stopping |
| `--no-checkpoint` | Do not update `transform_checkpoint.json` after reload |

Uses the same `local.settings.json` as the transformer. Opens **one** Snowflake connection for the
whole date range, committing per day.

---

## 3) Tests

The JSON→tables transform is a pure function and is covered by unit tests that need no network,
storage, or database:

```powershell
cd transformer_app
.venv\Scripts\Activate.ps1
pip install pytest
python -m pytest tests -v
```

---

## 4) Verify in Snowflake

```sql
SELECT COUNT(*) AS header_rows
FROM SALES_DB.RAW.SALES_HEADER
WHERE LAST_EXTRACTED_DAY = '2026-05-28';

SELECT LAST_EXTRACTED_DAY, COUNT(*) AS invoices
FROM SALES_DB.RAW.SALES_HEADER
GROUP BY 1 ORDER BY 1 DESC;
```

VARIANT columns are normalized with `PARSE_JSON` by the loader itself; see
`sql/004_sales_variant_parse.sql` for the standalone equivalent.

Full validation suite: `sql/005_sales_validation.sql` (set `VALIDATION_LAST_EXTRACTED_DAY` first).

> `SALES_INVOICE_HISTORY` is created by the DDL but **never written** by this pipeline — the
> per-invoice merge that would populate it was designed and never implemented. Any query against it
> returns zero rows. See "Data quality & known limitations" in the README.
