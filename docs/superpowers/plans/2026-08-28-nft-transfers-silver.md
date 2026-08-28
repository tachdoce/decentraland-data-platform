# Silver NFT Transfers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Build `silver.nft_transfers` from `staging.ethereum_nft_transfers`: deduplicated to the latest bronze extraction run, partitioned by `(chain_id, dt)`, tested, and backfilled from the 2019-01-01 seed boundary to the present.

**Architecture:** dbt-athena incremental model (`insert_overwrite`, partitioned by `chain_id, dt`). First build seeds `dt < '2019-01-01'`; each incremental run advances `(max(dt), max(dt) + N]` with `N = var('nft_transfers_incremental_days', 10)`, both bounds zero-scan via `$partitions` macros (`max_partition_dt`, new `max_partition_dt_offset`).

**Tech Stack:** dbt-athena (venv at `.venv/bin/dbt`, run from `dbt/`), Athena engine v3, AWS CLI (workgroup `decentraland-data-platform`).

**Spec:** `docs/superpowers/specs/2026-08-28-nft-transfers-silver-design.md`

## Global Constraints

- Athena workgroup cap: `bytes_scanned_cutoff_per_query = 1 GB`; if a catch-up batch approaches it, shrink `nft_transfers_incremental_days`.
- SQL style: keywords UPPERCASE; explicit `INNER JOIN`.
- All artifacts in English; commit messages in English.
- Partition column is `dt` (string, `YYYY-MM-DD`); never `date`.
- `max(dt)`/`min(dt)` of partition columns ALWAYS via `"<table>$partitions"`.
- Feature branch + PR with squash merge; never `git push` without the user's explicit OK.

## Current state (already implemented and parse-verified, NOT yet committed or built)

- `dbt/macros/max_partition_dt_offset.sql` — `MAX(dt) + N days` over `$partitions`.
- `dbt/models/silver/nft_transfers.sql` — model per the spec.
- `dbt/models/silver/sources.yml` — `staging.ethereum_nft_transfers` source added.
- `dbt/models/silver/schema.yml` — model block with not_null + accepted_values tests.
- `dbt/tests/assert_nft_transfers_single_run.sql` — single-extraction-run guard.
- `dbt parse` and `dbt compile --select nft_transfers` pass (bootstrap branch compiles with `WHERE t.dt < '2019-01-01'`).

---

### Task 1: Branch and commit the implementation

**Files:**
- Commit (already on disk): the five files listed under Current state.

**Interfaces:**
- Produces: macro `max_partition_dt_offset(relation, days)` — parenthesized scalar SQL subquery, callable with `this`/`ref()`/`source()`. Model `nft_transfers` with columns `transaction_hash` (string), `log_index` (bigint), `block_timestamp` (timestamp), `contract_address` (string), `token_id` (string), `quantity` (decimal(38,0)), `from_address` (string), `to_address` (string), `bronze_extracted_at` (timestamp), `decoded_at` (timestamp), `silver_processed_at` (timestamp), `chain_id` (int, partition), `dt` (string, partition).

- [x] **Step 1: Create the feature branch**

```bash
git checkout -b feat/nft-transfers-silver
```

- [x] **Step 2: Re-verify the project parses**

Run: `cd dbt && ../.venv/bin/dbt parse`
Expected: `Completed successfully` (the unused `models.decentraland.gold` config warning is pre-existing and acceptable).

- [x] **Step 3: Commit**

```bash
git add dbt/macros/max_partition_dt_offset.sql dbt/models/silver/nft_transfers.sql dbt/models/silver/sources.yml dbt/models/silver/schema.yml dbt/tests/assert_nft_transfers_single_run.sql docs/superpowers/specs/2026-08-28-nft-transfers-silver-design.md docs/superpowers/plans/2026-08-28-nft-transfers-silver.md
git commit -m "feat: silver.nft_transfers (deduped NFT transfers, capped incremental by (chain_id, dt))"
```

---

### Task 2: Initial build (2018 seed) and tests

**Files:** none (runs only)

**Interfaces:**
- Consumes: Task 1. Table `silver.nft_transfers` does not exist yet, so the bootstrap branch (`dt < '2019-01-01'`) runs.

- [x] **Step 1: First build**

Run: `cd dbt && ../.venv/bin/dbt build --select nft_transfers`
Expected: 1 incremental model PASS + 11 tests PASS (10 schema + 1 singular). If `assert_nft_transfers_single_run` fails, STOP and report the offending keys — do not delete data.

- [x] **Step 2: Verify seed coverage in Athena**

```bash
QID=$(aws athena start-query-execution --work-group decentraland-data-platform \
  --query-string "SELECT MIN(dt), MAX(dt), COUNT(DISTINCT dt), COUNT(*) FROM silver.nft_transfers" \
  --query QueryExecutionId --output text)
sleep 5
aws athena get-query-results --query-execution-id $QID --output text
```

Expected: `MAX(dt) <= 2018-12-31`, `MIN(dt)` = first staging partition, non-trivial row count. Compare `COUNT(*)` against staging over the same range (should be <= staging's count; smaller only if double extractions were deduped). Report numbers to the user.

---

### Task 3: Catch-up backfill to the present

**Files:** none (repeated runs)

**Interfaces:**
- Consumes: Tasks 1–2. Each run advances up to 180 days; from 2019-01-01 to today (~2,800 days) that is ~16 runs.

- [x] **Step 1: Run the catch-up loop**

```bash
cd dbt
for i in $(seq 1 16); do
  ../.venv/bin/dbt build --select nft_transfers --vars '{nft_transfers_incremental_days: 180}' || break
done
```

Expected: every run PASS (model + tests). Stop immediately on any failure. Runs after `max(dt)` reaches staging's last partition load zero new rows — harmless, stop looping there.

- [x] **Step 2: Verify scans stayed under the cap**

```bash
aws athena list-query-executions --work-group decentraland-data-platform --max-results 25 --output json | python3 -c "
import json, subprocess, sys
ids = json.load(sys.stdin)['QueryExecutionIds']
for qid in ids:
    q = json.loads(subprocess.check_output(['aws','athena','get-query-execution','--query-execution-id',qid]))['QueryExecution']
    print(q['Status']['State'], q.get('Statistics',{}).get('DataScannedInBytes'))"
```

Expected: every query well under 1 GB. If any batch approached the cap, rerun the remainder with a smaller `nft_transfers_incremental_days` and tell the user.

- [x] **Step 3: Final coverage check**

Same Athena query as Task 2 Step 2. Expected: `MAX(dt)` equals staging's max partition (`SELECT MAX(dt) FROM "staging"."ethereum_nft_transfers$partitions"`), no gaps in `COUNT(DISTINCT dt)` vs staging's partition count. Report to the user.

---

### Task 4: PR

- [ ] **Step 1: Ask the user for the OK to push, then open the PR**

```bash
git push -u origin feat/nft-transfers-silver
gh pr create --title "feat: silver.nft_transfers" --body "..."
```

Squash merge per repo convention. Do NOT push without explicit user confirmation.
