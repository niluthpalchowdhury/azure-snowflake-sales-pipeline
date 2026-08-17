# Section 6 — Sales extract schedule (Timeline 1 & 2)

Defines **which calendar days** to pull, **how often** the extractor runs, and **checkpoint resume** when a single Azure Function invocation would time out.

**Timezone:** `Asia/Kolkata` (IST) via `PIPELINE_TIMEZONE` / `WEBSITE_TIME_ZONE=India Standard Time`.

**Merge rules:** `docs/section-05-sales-load-merge.md`  
**Implementation:** `extractor_app/shared/extraction_runner.py`.

---

## 1. Overview

| Timeline | Which days to load | When that day list applies |
|----------|-------------------|----------------------------|
| **Timeline 1** | See §2 (by IST hour) | All day, every 10 minutes |
| **Timeline 2** | Last **7** days `T-6 … T` | Started at **10:00 AM**; runs until **complete** (§3) |

| Runtime (both timelines) | Value |
|--------------------------|--------|
| Extractor timer | **Every 10 minutes** |
| Resume | **Pending branches** from checkpoint (per day) |
| Run budget | Stop before function timeout; next tick continues |

---

## 2. Timeline 1 — day selection (IST hour)

Unchanged business rules; only the **timer** is every 10 minutes (not hourly).

Let `T` = today (IST), `T-1` = yesterday.

| IST hour | `target_days` for new / refreshed work |
|----------|----------------------------------------|
| **0 – 8** | `T-1`, `T` |
| **9 – 23** | `T` only |

**Exception at 10:00 AM:** while a Timeline 2 seven-day job is **in progress**, the extractor **finishes Timeline 2 first** (§3.3). Timeline 1 days for that period are not started until T2 completes (or share budget only if you later change policy).

### Pseudocode — day list only

```python
def target_days_timeline1(now_ist: datetime) -> list[date]:
    today = now_ist.date()
    if now_ist.hour < 9:
        return [today - timedelta(days=1), today]
    return [today]
```

---

## 3. Timeline 2 — seven-day extract at 10:00 AM

### 3.1 Day list

At **10:00 AM IST** (first tick of the hour), start a job with:

```text
target_days = [T-6, T-5, T-4, T-3, T-2, T-1, T]
```

### 3.2 Completion window (not one shot)

The seven-day pull **does not** finish in a single invocation. It uses the **same 10-minute extractor** as Timeline 1:

- **10:00, 10:10, 10:20, …** until every branch for every queued day is done or marked failed.
- State is stored in checkpoint (§4). Later hours (11:00+) still run Timeline 1 for today, but **pending Timeline 2 work takes priority** until the seven-day checkpoint reports `status=completed`.

### 3.3 Starting Timeline 2

As implemented in `extractor_app/shared/pipeline_schedule.py`:

```python
def should_start_timeline2(now_ist: datetime, job: dict | None) -> bool:
    if now_ist.hour != 10 or now_ist.minute >= 10:
        return False
    if job and job.get("status") == "in_progress":
        return False
    started_on = (job or {}).get("startedOnDate")
    return started_on != now_ist.date().isoformat()
```

On start, `work_plan.start_timeline2_job()` writes a job manifest to
`rista/sales/jobs/timeline2.json` with `status="in_progress"`, `pendingDays=[T-6..T]` and
`currentDay=<first pending>`. Per-day branch state still lives in that day's own
`checkpoint.json`.

> **Known bug.** A day whose checkpoint is already `completed` is skipped without being removed
> from `pendingDays`, so the job can never finish and Timeline 1 becomes unreachable. See
> "Data quality & known limitations" in the README.

---

## 4. Checkpoint and resume (both timelines)

### 4.1 Why

Azure Functions can **timeout** before all **~352 branches** × pages × (1 or 2 or 7 days) finish. Each **10-minute** run processes as many branches as the **run budget** allows, then saves progress.

### 4.2 Scope

| Level | Checkpoint path (per day) |
|-------|---------------------------|
| Day | `rista/sales/YYYY/MM/DD/checkpoint.json` |
| Partial data | `rista/sales/YYYY/MM/DD/partial_data.json.gz` |
| Final blob | `rista/sales/YYYY/MM/DD/data.json.gz` |

For **Timeline 2**, use either:

- **Option A (recommended):** one checkpoint **per day** (same paths as Timeline 1). Queue `pendingDays` in a small **job manifest** blob, e.g. `rista/sales/jobs/timeline2_YYYY-MM-DD.json`.
- **Option B:** single multi-day checkpoint under `rista/sales/jobs/timeline2_active.json`.

### 4.3 Checkpoint fields (per day)

Checkpoint fields written per business day:

| Field | Purpose |
|-------|---------|
| `status` | `in_progress` \| `completed` |
| `timeline` | `1` \| `2` |
| `targetDay` | API `day` parameter |
| `pendingBranchCodes` | Branches not yet successful this day |
| `completedBranchCodes` | Branches done |
| `failedBranches` | Branches exceeded retry limit |
| `branchCatalog` | Snapshot from `/v1/branch/list` |
| `storeAttemptCounts` / `branchAttemptCounts` | Per-branch retries |
| `recordCount` | Rows in partial snapshot |
| `runCount` | Number of function invocations |
| `budgetReached` | Last run stopped due to time budget |
| `lastHeartbeatAt` | Lease heartbeat |

### 4.4 One invocation (both timelines)

```text
1. Acquire blob lease on checkpoint (skip if another instance holds lease).
2. Load checkpoint + partial_data for current target_day.
3. If no checkpoint: fetch /v1/branch/list → all branchCode → init pending list.
4. Loop pendingBranchCodes:
     - If run_budget_seconds exceeded → save pending, exit (resume next 10 min).
     - GET /v1/sales/page?branch=&day=&lastKey=…
     - Dedupe invoices; append to partial data.
     - On failure: retry count; skip branch after max attempts.
5. When pending empty for this day → write data.json.gz, mark day completed.
6. Timeline 2: advance to next pending day and continue in same run if budget allows.
7. Release lease.
```

### 4.5 Branch list

- **All** branches from `/v1/branch/list` (Active + Inactive), same as agreed earlier.
- Resume order: **pending only** — completed branches are not re-fetched that day unless you add a separate “full refresh” flag.

### 4.6 Environment variables

| Variable | Typical value | Purpose |
|----------|---------------|---------|
| `EXTRACTOR_RUN_BUDGET_SECONDS` | `480` | Stop before timeout (leave buffer below 10 min max) |
| `EXTRACTOR_LEASE_SECONDS` | `60` | Blob lease TTL (clamped to 15–60 by the code) |
| `RISTA_BRANCH_ATTEMPTS` | `2` | Max tries per branch per day |
| `RISTA_REQUEST_TIMEOUT` | `120` | HTTP timeout |
| `TARGET_DAY` / `TARGET_DAYS` | optional | Manual override; bypass timeline logic |

---

## 5. Azure Function timers

Set **`WEBSITE_TIME_ZONE=India Standard Time`**.

| Function | Cron (6-field) | Meaning |
|----------|----------------|---------|
| **Sales extractor** | `0 0/10 * * * *` | Every 10 minutes at :00, :10, :20, … (chunked branch resume) |
| **Sales transformer** | `0 5/30 * * * *` | Every **30** minutes at :05 and :35 (~48 runs/day) |

Extractor keeps the 10-minute cadence for **Timeline 1 and Timeline 2** — only the **day queue** and **checkpoint state** differ.

### Completed-day refresh policy

After `checkpoint.status=completed`, the extractor **does not** start a full re-extract on the next 10-minute tick. Re-extract only when:

- No checkpoint yet (first run for that day)
- `TARGET_DAY` / `TARGET_DAYS` is set (backfill override for that day)
- `EXTRACTOR_FORCE_REFRESH=true` on the extractor app

To re-run a day manually without env overrides, delete that day’s blobs under `rista/sales/YYYY/MM/DD/`.

### Choosing work each tick

```python
def work_plan(now_ist, t2_job) -> list[date]:
    if t2_job and t2_job.status == "in_progress":
        return t2_job.pending_days_including_current()
    if now_ist.hour == 10 and now_ist.minute < 10 and not t2_job.completed_today():
        start_timeline2_job()
        return t2_job.pending_days_including_current()
    return target_days_timeline1(now_ist)
```

Transformer: on each 30-minute tick, scan pending days (last 8 calendar days). For any day where extraction is `completed` and `transform_checkpoint.json` does not match `snapshotExtractedAt` on the checkpoint, read `data.json.gz` and run merge load. Snowflake is skipped when already transformed for the current snapshot.

---

## 6. Summary — time vs days vs runtime

| Time (IST) | Days queued (new work) | Extractor cadence |
|------------|------------------------|-------------------|
| 00:00 – 08:59 | `T-1`, `T` | Every 10 min, resume branches |
| 09:00 – 09:59 | `T` | Every 10 min, resume branches |
| **10:00 – until T2 done** | **`T-6` … `T`** | Every 10 min, resume branches/days |
| After T2 done, 10:xx – 23:59 | `T` (Timeline 1) | Every 10 min, resume branches |

---

## 7. Transformer interaction

- Transformer does **not** need 10-minute checkpointing for branches; it reads **completed** daily blobs.
- Loads **per completed day** when `data.json.gz` exists and checkpoint `status=completed`.
- Skipping is decided by snapshot identity, not by row values: a day is reloaded only when the
  snapshot's `extractedAt` differs from the `extractedAt` recorded in `transform_checkpoint.json`.
  There is no per-invoice `NET_AMOUNT` comparison (see §5 §0).

---

## 8. Manual override

| Env var | Behavior |
|---------|----------|
| `TARGET_DAY` | Single day; checkpoint under that day only |
| `TARGET_DAYS` | Comma-separated list; process in order with resume per day |
| ~~`PIPELINE_ENFORCE_WINDOW`~~ | **Not implemented** — no code reads this variable |

---

## 9. Related docs

- `docs/section-05-sales-load-merge.md`
- `docs/section-04-sales-schema.md`
- Implementation: `extractor_app/shared/extraction_runner.py`
