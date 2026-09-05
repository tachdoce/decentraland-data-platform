# gold.fct_nft_transfers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `gold.fct_nft_transfers` — the NFT transfers fact keyed by `sk_contract` — and backfill it from 2018 to silver's frontier (2026-08-20).

**Architecture:** One incremental dbt model (`insert_overwrite`, partitioned by `dt` only) that INNER JOINs `silver.nft_transfers` to `gold.dim_nft_contracts`. No dedup (silver guarantees one copy per run) and no pricing joins. Backfill advances the incremental front in ≤100-day windows (Athena caps INSERT at 100 partitions per query).

**Tech Stack:** dbt-athena, Athena SQL, AWS S3/Glue. Branch: `feat/fct-nft-transfers` (already created; spec committed there).

**Spec:** `docs/superpowers/specs/2026-09-05-fct-nft-transfers-design.md`

## Global Constraints

- All artifacts in English; SQL keywords UPPERCASE; explicit `INNER JOIN` (never bare `JOIN`).
- Partition column is `dt` (never `date`); max/min of a partition column ALWAYS via the `"<table>$partitions"` metadata table (the `max_partition_dt` / `max_partition_dt_offset` macros already do this).
- `sk_contract` comes from `dim_nft_contracts` (built with `dbt_utils.generate_surrogate_key(['chain_id', 'contract_address'])`); never build keys manually.
- Grain `(chain_id, transaction_hash, log_index)` is NOT unique (ERC-1155 TransferBatch) — no uniqueness test on it.
- Commit locally freely; NEVER `git push` without explicit user confirmation.
- All dbt commands run from `/Users/tachone/proyectos/Decentraland/dbt`.

---

### Task 1: Model + tests

**Files:**
- Create: `dbt/models/gold/fct_nft_transfers.sql`
- Modify: `dbt/models/gold/schema.yml` (append a model entry)

**Interfaces:**
- Consumes: `silver.nft_transfers` (columns: `transaction_hash`, `log_index`, `block_timestamp`, `contract_address`, `token_id`, `quantity`, `from_address`, `to_address`, `silver_processed_at`, `chain_id`, `dt`), `gold.dim_nft_contracts` (`sk_contract`, `chain_id`, `contract_address`), macros `max_partition_dt(this)` / `max_partition_dt_offset(this, n)`, var `nft_transfers_incremental_days` (shared with silver, default 10).
- Produces: table `gold.fct_nft_transfers` partitioned by `dt`, used by Task 2 (seed build) and Task 3 (backfill).

- [x] **Step 1: Write the model**

Create `dbt/models/gold/fct_nft_transfers.sql`:

```sql
-- NFT transfers fact for the gold star schema. Grain: one row per
-- transferred token position — a 1155 TransferBatch explodes into one
-- row per array element, so (chain_id, transaction_hash, log_index)
-- can legitimately repeat, even with identical token_id. The INNER
-- JOIN to dim_nft_contracts is intentional: only ERC-721/1155 curated
-- contracts make it into gold (EstateProxy, erc_type = 0, drops out).
-- Addresses travel via sk_contract; join the dim to recover them.
--
-- No dedup here: silver.nft_transfers already keeps one copy per log
-- and run (latest bronze_extracted_at, insert_overwrite partitions).
-- Partitioned by dt only (chain_id is a regular column), so one dt
-- overwrite covers every chain once the polygon decode lands.
--
-- The initial build seeds dt < '2019-01-01' (no price dependency,
-- unlike fct_sales); incremental runs advance at most
-- nft_transfers_incremental_days (default 10, same var and cadence as
-- silver) past the current max dt, e.g.
-- --vars '{nft_transfers_incremental_days: 100}' to catch up.
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['dt']
) }}

SELECT t.transaction_hash,
    t.log_index,
    t.block_timestamp,
    t.from_address,
    t.to_address,
    c.sk_contract,
    t.token_id,
    t.quantity,
    t.chain_id,
    t.silver_processed_at,
    CAST(current_timestamp AS timestamp) AS gold_processed_at,
    t.dt
FROM {{ ref('nft_transfers') }} AS t
    INNER JOIN {{ ref('dim_nft_contracts') }} AS c
        ON t.chain_id = c.chain_id
        AND t.contract_address = c.contract_address
{% if is_incremental() %}
WHERE t.dt > {{ max_partition_dt(this) }}
  AND t.dt <= {{ max_partition_dt_offset(this, var('nft_transfers_incremental_days', 10)) }}
{% else %}
WHERE t.dt < '2019-01-01'
{% endif %}
```

- [x] **Step 2: Add the schema.yml entry**

Append to `dbt/models/gold/schema.yml` (after `fct_monthly_nft_prices`, before `dim_currency`, same indentation as the sibling models):

```yaml
  - name: fct_nft_transfers
    description: >
      NFT transfers fact: one row per transferred token position for
      curated ERC-721/1155 contracts, keyed by sk_contract. NOT unique
      on (chain_id, transaction_hash, log_index): 1155 TransferBatch
      explodes into one row per array element. INNER JOIN to
      dim_nft_contracts is intentional (EstateProxy, erc_type 0, drops
      out). Full history from 2018 — no price dependency. Incremental,
      partitioned by dt; runs advance at most
      nft_transfers_incremental_days (default 10) past max dt.
    data_tests:
      - dbt_utils.expression_is_true:
          name: fct_nft_transfers_quantity_positive
          arguments:
            expression: "quantity > 0"
    columns:
      - name: transaction_hash
        data_tests:
          - not_null
      - name: log_index
        data_tests:
          - not_null
      - name: block_timestamp
        data_tests:
          - not_null
      - name: from_address
        data_tests:
          - not_null
      - name: to_address
        data_tests:
          - not_null
      - name: sk_contract
        data_tests:
          - not_null
      - name: token_id
        data_tests:
          - not_null
      - name: quantity
        data_tests:
          - not_null
      - name: chain_id
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: [1]
                quote: false
      - name: dt
        data_tests:
          - not_null
```

- [x] **Step 3: Verify the project parses and the compiled SQL looks right**

Run:
```bash
cd dbt && dbt parse && dbt compile --select fct_nft_transfers
```
Expected: both succeed. Open `dbt/target/compiled/**/gold/fct_nft_transfers.sql` and confirm the non-incremental branch ends in `WHERE t.dt < '2019-01-01'` (first build → no `$partitions` subquery yet).

- [x] **Step 4: Commit**

```bash
git add dbt/models/gold/fct_nft_transfers.sql dbt/models/gold/schema.yml
git commit -m "feat: gold.fct_nft_transfers model and tests"
```

### Task 2: Initial seed build (2018) and verification

**Files:**
- None (runs dbt against Athena; no code changes).

**Interfaces:**
- Consumes: the model from Task 1.
- Produces: `gold.fct_nft_transfers` seeded with every `dt < '2019-01-01'`, tests green — the base the Task 3 backfill advances from.

- [x] **Step 1: Run the initial build (model + tests)**

```bash
cd dbt && dbt build --select fct_nft_transfers
```
Expected: model builds and all 12 tests PASS. If a test fails, STOP and investigate (superpowers:systematic-debugging) — do not proceed to backfill.

- [x] **Step 2: Verify the seed against silver in Athena**

Run both queries in the tagged workgroup (as usual via the Athena console or `aws athena start-query-execution`):

```sql
-- gold row count and frontier (partitions metadata, zero bytes scanned)
SELECT MAX(dt) AS max_dt, COUNT(*) AS partition_count
FROM "gold"."fct_nft_transfers$partitions";
```
Expected: `max_dt = 2018-12-31` (or silver's last 2018 partition).

```sql
-- gold must equal silver joined to the dim, row for row
SELECT
    (SELECT COUNT(*) FROM "gold"."fct_nft_transfers") AS gold_rows,
    (SELECT COUNT(*)
     FROM "silver"."nft_transfers" AS t
         INNER JOIN "gold"."dim_nft_contracts" AS c
             ON t.chain_id = c.chain_id
             AND t.contract_address = c.contract_address
     WHERE t.dt < '2019-01-01'
       AND t.quantity > 0) AS expected_rows;
```
Expected: `gold_rows = expected_rows`. If they differ, STOP and investigate.

- [x] **Step 3: Record the silver frontier for the backfill**

```sql
SELECT MAX(dt) AS silver_max_dt FROM "silver"."nft_transfers$partitions";
```
Expected: `2026-08-20` (spec-time value; use whatever this returns as the Task 3 target).

### Task 3: Backfill to silver's frontier

**Files:**
- None (repeated dbt runs; no code changes).

**Interfaces:**
- Consumes: seeded table from Task 2; var `nft_transfers_incremental_days`.
- Produces: `gold.fct_nft_transfers` complete up to silver's max dt.

- [x] **Step 1: Advance in 100-day windows until reaching the frontier**

Athena INSERT writes at most 100 partitions per query, so the window var must stay ≤ 100. From 2018-12-31 to 2026-08-20 is ~2790 days → ~28 runs. Run sequentially (each run reads the previous max dt from `$partitions`):

```bash
cd dbt
for i in {1..30}; do
  echo "=== run $i ==="
  dbt build --select fct_nft_transfers \
    --vars '{nft_transfers_incremental_days: 100}' || break
done
```
Expected: each run loads ≤100 new partitions and its tests PASS. Once the front passes silver's max dt, runs load 0 new partitions (the window compiles past the data) — stop the loop then. If any run fails, STOP: fix before re-running (insert_overwrite makes re-running the same window safe, but a partial INSERT retry can duplicate partitions — check `$partitions` for the failed window before retrying).

- [x] **Step 2: Verify completeness in Athena**

```sql
SELECT MAX(dt) AS max_dt FROM "gold"."fct_nft_transfers$partitions";
```
Expected: equals silver's max dt from Task 2 Step 3.

```sql
-- yearly reconciliation, gold vs silver ⋈ dim
SELECT YEAR(CAST(t.dt AS date)) AS yr,
    COUNT(*) AS expected_rows
FROM "silver"."nft_transfers" AS t
    INNER JOIN "gold"."dim_nft_contracts" AS c
        ON t.chain_id = c.chain_id
        AND t.contract_address = c.contract_address
WHERE t.quantity > 0
GROUP BY 1 ORDER BY 1;
```
```sql
SELECT YEAR(CAST(dt AS date)) AS yr,
    COUNT(*) AS gold_rows
FROM "gold"."fct_nft_transfers"
GROUP BY 1 ORDER BY 1;
```
Expected: identical counts per year. If a year differs, find the offending dt range with the same query at month grain and re-run that window.

- [x] **Step 3: Run the full test suite once over the finished table**

```bash
cd dbt && dbt test --select fct_nft_transfers
```
Expected: all 12 tests PASS.

- [x] **Step 4: Commit plan checkboxes and report**

```bash
git add docs/superpowers/plans/2026-09-05-fct-nft-transfers.md
git commit -m "docs: fct_nft_transfers backfill complete"
```
Report to the user: total rows, max dt, number of backfill runs, and approximate MB scanned (from the Athena console history) before moving to the finishing-a-development-branch skill (PR + squash merge; push only with explicit user confirmation).
