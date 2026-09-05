# gold.fct_mana_transfers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `gold.fct_mana_transfers` — MANA transfers with exact whole-token amounts — and backfill it from 2017 to silver's frontier (2026-08-20).

**Architecture:** One incremental dbt model (`insert_overwrite`, partitioned by `dt` only) reading `silver.mana_transfers` with no joins and no dedup; `amount_raw > 0` filtered; wei→MANA conversion via exact decimal multiplication (never `POW`, which routes through `double`). Backfill advances in ≤100-day windows (Athena's 100-partition INSERT cap).

**Tech Stack:** dbt-athena, Athena SQL, AWS S3/Glue. Branch: `feat/fct-mana-transfers` (already created; spec committed there).

**Spec:** `docs/superpowers/specs/2026-09-05-fct-mana-transfers-design.md`

## Global Constraints

- All artifacts in English; SQL keywords UPPERCASE; explicit `INNER JOIN` (never bare `JOIN`) — this model has no joins at all.
- Partition column is `dt` (never `date`); max/min of a partition column ALWAYS via the `"<table>$partitions"` metadata table (the `max_partition_dt` / `max_partition_dt_offset` macros already do this).
- No `usd_amount`, no `token_address`, no surrogate key: single-token fact by user decision.
- Amount conversion MUST be `CAST(t.amount_raw * DECIMAL '0.000000000000000001' AS decimal(30, 18))` — never `POW(10, 18)` (double, loses wei precision) and never plain decimal division (integer division: Trino keeps `max(s1, s2)` as the result scale).
- Commit locally freely; NEVER `git push` without explicit user confirmation.
- All dbt commands run from `/Users/tachone/proyectos/Decentraland/dbt` using `../.venv/bin/dbt`.

---

### Task 1: Model + tests

**Files:**
- Create: `dbt/models/gold/fct_mana_transfers.sql`
- Modify: `dbt/models/gold/schema.yml` (append a model entry)

**Interfaces:**
- Consumes: `silver.mana_transfers` (columns: `transaction_hash`, `log_index`, `block_timestamp`, `from_address`, `to_address`, `amount_raw` decimal(38,0), `silver_processed_at`, `chain_id`, `dt`), macros `max_partition_dt(this)` / `max_partition_dt_offset(this, n)`, var `mana_transfers_incremental_days` (shared with silver, default 10).
- Produces: table `gold.fct_mana_transfers` partitioned by `dt`, used by Task 2 (seed build) and Task 3 (backfill).

- [ ] **Step 1: Write the model**

Create `dbt/models/gold/fct_mana_transfers.sql`:

```sql
-- MANA transfers fact for the gold star schema. Grain: one row per
-- (chain_id, transaction_hash, log_index) — unique, ERC-20 Transfer
-- emits one row per log. Single-token fact: no token_address, no
-- surrogate key, decimals = 18 hardcoded in the conversion below.
--
-- amount converts wei exactly: DECIMAL '0.000000000000000001' is
-- decimal(18,18) and represents 1e-18 exactly; decimal multiplication
-- adds scales (38,0 x 18,18 -> scale 18, precision capped 38), so no
-- value ever routes through double (POW(10,18) would, losing wei-level
-- digits). The CAST to decimal(30,18) keeps scale 18 intact and leaves
-- 12 integer digits — MANA supply (~2,193M) is ~456x below that limit.
--
-- No dedup here: silver.mana_transfers already keeps one copy per
-- grain (dual decoded_at + bronze_extracted_at dedup, insert_overwrite
-- partitions, uniqueness test). amount_raw = 0 no-op transfers stay in
-- silver but are dropped from gold, matching fct_nft_transfers; mints
-- and burns (zero address) are kept — real supply movements.
--
-- The initial build seeds dt < '2019-01-01' (data starts 2017-09-06);
-- incremental runs advance at most mana_transfers_incremental_days
-- (default 10, same var and cadence as silver) past the current max
-- dt, e.g. --vars '{mana_transfers_incremental_days: 100}' to catch up.
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
    CAST(t.amount_raw * DECIMAL '0.000000000000000001' AS decimal(30, 18)) AS amount,
    t.chain_id,
    t.silver_processed_at,
    CAST(current_timestamp AS timestamp) AS gold_processed_at,
    t.dt
FROM {{ ref('mana_transfers') }} AS t
WHERE t.amount_raw > 0
{% if is_incremental() %}
  AND t.dt > {{ max_partition_dt(this) }}
  AND t.dt <= {{ max_partition_dt_offset(this, var('mana_transfers_incremental_days', 10)) }}
{% else %}
  AND t.dt < '2019-01-01'
{% endif %}
```

- [ ] **Step 2: Add the schema.yml entry**

Append to `dbt/models/gold/schema.yml`, after the `fct_nft_transfers` entry and before `dim_currency`, same indentation as the sibling models:

```yaml
  - name: fct_mana_transfers
    description: >
      MANA transfers fact: one row per (chain_id, transaction_hash,
      log_index) with the amount converted exactly from wei
      (decimal multiplication, never POW — see model comments).
      Single-token fact: no token_address, no surrogate key, no USD
      valuation by design. amount_raw = 0 no-ops are dropped; mints and
      burns (zero address) are kept. Full history from 2017-09-06.
      Incremental, partitioned by dt; runs advance at most
      mana_transfers_incremental_days (default 10) past max dt.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns:
              - transaction_hash
              - log_index
      - dbt_utils.expression_is_true:
          name: fct_mana_transfers_amount_positive
          arguments:
            expression: "amount > 0"
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
      - name: amount
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

- [ ] **Step 3: Verify the project parses and the compiled SQL looks right**

Run:
```bash
cd dbt && ../.venv/bin/dbt parse && ../.venv/bin/dbt compile --select fct_mana_transfers
```
Expected: both succeed. Open `dbt/target/compiled/**/gold/fct_mana_transfers.sql` and confirm the WHERE ends in `AND t.dt < '2019-01-01'` (first build) and the amount line multiplies by `DECIMAL '0.000000000000000001'`.

- [ ] **Step 4: Commit**

```bash
git add dbt/models/gold/fct_mana_transfers.sql dbt/models/gold/schema.yml
git commit -m "feat: gold.fct_mana_transfers model and tests"
```

### Task 2: Initial seed build (2017–2018) and verification

**Files:**
- None (runs dbt against Athena; no code changes).

**Interfaces:**
- Consumes: the model from Task 1.
- Produces: `gold.fct_mana_transfers` seeded with every `dt < '2019-01-01'`, tests green — the base the Task 3 backfill advances from.

- [ ] **Step 1: Run the initial build (model + tests)**

```bash
cd dbt && ../.venv/bin/dbt build --select fct_mana_transfers
```
Expected: model builds and all 11 tests PASS. If a test fails, STOP and investigate (superpowers:systematic-debugging) — do not proceed to backfill.

- [ ] **Step 2: Verify the seed against silver in Athena**

Run in the tagged workgroup (`decentraland-data-platform`):

```sql
SELECT MAX(dt) AS max_dt, COUNT(*) AS partition_count
FROM "gold"."fct_mana_transfers$partitions";
```
Expected: `max_dt = 2018-12-31` (or silver's last 2018 partition).

```sql
SELECT
    (SELECT COUNT(*) FROM "gold"."fct_mana_transfers") AS gold_rows,
    (SELECT COUNT(*)
     FROM "silver"."mana_transfers"
     WHERE dt < '2019-01-01'
       AND amount_raw > 0) AS expected_rows;
```
Expected: `gold_rows = expected_rows`. If they differ, STOP and investigate.

- [ ] **Step 3: Verify the conversion is exact on the seed**

```sql
SELECT
    (SELECT SUM(amount) FROM "gold"."fct_mana_transfers") AS gold_mana,
    (SELECT CAST(SUM(amount_raw) AS decimal(38, 0)) * DECIMAL '0.000000000000000001'
     FROM "silver"."mana_transfers"
     WHERE dt < '2019-01-01'
       AND amount_raw > 0) AS silver_mana;
```
Expected: identical values, digit for digit. A mismatch means the conversion lost precision — STOP.

### Task 3: Backfill to silver's frontier

**Files:**
- None (repeated dbt runs; no code changes).

**Interfaces:**
- Consumes: seeded table from Task 2; var `mana_transfers_incremental_days`.
- Produces: `gold.fct_mana_transfers` complete up to silver's max dt (2026-08-20 at spec time).

- [ ] **Step 1: Advance in 100-day windows until reaching the frontier**

~2790 days from 2018-12-31 to 2026-08-20 → ~28 runs (window capped at 100 by Athena's 100-partition INSERT limit). Run sequentially from `dbt/`:

```bash
for i in {1..30}; do
  echo "=== run $i ==="
  ../.venv/bin/dbt build --select fct_mana_transfers \
    --vars '{mana_transfers_incremental_days: 100}' || break
done
```
Expected: each run loads ≤100 new partitions and its tests PASS. If any run fails, STOP: `dbt build` inserts BEFORE tests run, so a failed run may leave its window loaded — fix the cause, then recover with `--full-refresh` + re-backfill (the proven recipe from fct_nft_transfers) rather than manual partition surgery.

- [ ] **Step 2: Verify completeness in Athena**

```sql
SELECT MAX(dt) AS max_dt FROM "gold"."fct_mana_transfers$partitions";
```
Expected: equals `MAX(dt)` of `"silver"."mana_transfers$partitions"`.

```sql
WITH s AS (
    SELECT YEAR(CAST(dt AS date)) AS yr,
        COUNT(*) AS expected_rows,
        CAST(SUM(amount_raw) AS decimal(38, 0)) * DECIMAL '0.000000000000000001' AS expected_mana
    FROM "silver"."mana_transfers"
    WHERE amount_raw > 0
    GROUP BY 1
),

g AS (
    SELECT YEAR(CAST(dt AS date)) AS yr,
        COUNT(*) AS gold_rows,
        SUM(amount) AS gold_mana
    FROM "gold"."fct_mana_transfers"
    GROUP BY 1
)

SELECT s.yr,
    s.expected_rows - g.gold_rows AS row_diff,
    s.expected_mana - g.gold_mana AS mana_diff
FROM s
    INNER JOIN g ON s.yr = g.yr
ORDER BY s.yr;
```
Expected: `row_diff = 0` AND `mana_diff = 0` for every year 2017–2026. A non-zero `mana_diff` with zero `row_diff` means precision loss — STOP.

- [ ] **Step 3: Run the full test suite once over the finished table**

```bash
cd dbt && ../.venv/bin/dbt test --select fct_mana_transfers
```
Expected: all 11 tests PASS.

- [ ] **Step 4: Commit plan checkboxes and report**

```bash
git add docs/superpowers/plans/2026-09-05-fct-mana-transfers.md
git commit -m "docs: fct_mana_transfers backfill complete"
```
Report to the user: total rows, max dt, number of backfill runs, and the yearly reconciliation result, then move to the finishing-a-development-branch skill (PR + squash merge; push only with explicit user confirmation).
