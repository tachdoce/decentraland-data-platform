# gold.fct_sales Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish `gold.fct_sales`, a USD-priced sales fact joining `silver.sales` to the gold dims and daily token prices.

**Architecture:** One incremental dbt-athena model (`insert_overwrite`, partitioned by `dt`) selecting from `silver.sales` windowed by the shared `sales_dt_window` macro (extended with a `seed_end_dt` arg), INNER-joined to `dim_currency`, `token_prices` and `dim_nft_contracts`. Plus grain-uniqueness and schema tests.

**Tech Stack:** dbt 1.12 + dbt-athena adapter; Athena SQL (Trino dialect); project venv at `../.venv/bin/dbt` relative to `dbt/`.

**Spec:** `docs/superpowers/specs/2026-09-05-gold-fct-sales-design.md`

## Global Constraints

- All artifacts in English; SQL keywords UPPERCASE; explicit `INNER JOIN`, never bare `JOIN`.
- Partition column is `dt` (varchar `YYYY-MM-DD`), never named `date`; max/min of a partition column always via the `"<table>$partitions"` metadata table.
- Surrogate keys via `dbt_utils.generate_surrogate_key`; never ROW_NUMBER or manual ids.
- Work happens on branch `gold-fct-sales` (already created; holds the spec commit). Commit locally; NEVER `git push` without explicit user confirmation.
- All dbt commands run from `dbt/`: `cd /Users/tachone/proyectos/Decentraland/dbt`.
- Athena may already hold a `gold.fct_sales` table from design exploration; the `--full-refresh` build in Task 2 replaces it — do not drop it manually.

---

### Task 1: `seed_end_dt` argument on `sales_dt_window`

**Files:**
- Modify: `dbt/macros/sales_dt_window.sql`

**Interfaces:**
- Consumes: nothing new.
- Produces: `sales_dt_window(start_dt, end_dt=none, seed_end_dt='2019-01-01')` — same emitted SQL as today for every existing caller (silver.sales passes only `start_dt`/`end_dt`), but the non-incremental seed cap becomes parameterizable. Task 2 calls it as `sales_dt_window('2019-01-01', seed_end_dt='2019-02-01')`.

- [ ] **Step 1: Edit the macro**

Replace the full contents of `dbt/macros/sales_dt_window.sql` with:

```sql
-- Shared dt window for every staging scan in silver.sales and for the
-- silver.sales scan in gold.fct_sales (bounds read each model's own
-- "$partitions" via {{ this }}).
-- start_dt: first day the marketplace traded (constant, prunes scans
-- before it existed). end_dt: last day of a dead marketplace — a scan
-- cap, not a semantic filter; once the incremental front passes it the
-- branch compiles to an empty zero-cost scan. Initial build seeds
-- dt < seed_end_dt; incremental runs advance at most
-- sales_incremental_days (default 10) past the table's max dt,
-- both bounds zero-scan via "$partitions". seed_end_dt defaults to
-- '2019-01-01' (silver's seed); fct_sales starts at 2019-01-01 (first
-- priced day in token_prices) and seeds one month instead.
{% macro sales_dt_window(start_dt, end_dt=none, seed_end_dt='2019-01-01') %}
    dt >= '{{ start_dt }}'
    {% if end_dt %}
    AND dt <= '{{ end_dt }}'
    {% endif %}
    {% if is_incremental() %}
    AND dt > {{ max_partition_dt(this) }}
    AND dt <= {{ max_partition_dt_offset(this, var('sales_incremental_days', 10)) }}
    {% else %}
    AND dt < '{{ seed_end_dt }}'
    {% endif %}
{% endmacro %}
```

- [ ] **Step 2: Verify existing callers compile unchanged**

Run:
```bash
cd /Users/tachone/proyectos/Decentraland/dbt
../.venv/bin/dbt compile --select sales --full-refresh 2>&1 | tail -3
grep -c "dt < '2019-01-01'" target/compiled/decentraland/models/silver/sales.sql
```
Expected: `Completed successfully` and grep prints `5` (one seed cap per marketplace branch — the default `seed_end_dt` preserves silver.sales byte-for-byte).

- [ ] **Step 3: Commit**

```bash
cd /Users/tachone/proyectos/Decentraland
git add dbt/macros/sales_dt_window.sql
git commit -m "refactor: parameterize seed cap in sales_dt_window (seed_end_dt)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: `fct_sales` model, schema tests, grain test, seeded build

**Files:**
- Create: `dbt/models/gold/fct_sales.sql`
- Create: `dbt/tests/assert_fct_sales_unique_key.sql`
- Modify: `dbt/models/gold/schema.yml` (add `fct_sales` entry above `dim_currency`)

**Interfaces:**
- Consumes: `sales_dt_window(start_dt, end_dt=none, seed_end_dt)` from Task 1; `ref('sales')` (silver, grain `(chain_id, transaction_hash, log_index, nft_contract_address, token_id)`); `ref('dim_currency')` (`chain_id, contract_address, currency_symbol, decimals`); `ref('token_prices')` (`currency_symbol, price_usd, dt`); `ref('dim_nft_contracts')` (`sk_contract, chain_id, contract_address`); macro `max_partition_dt_offset(relation, days)`.
- Produces: table `gold.fct_sales` partitioned by `dt`, unique on `(chain_id, transaction_hash, log_index, sk_contract, token_id)` — the columns exactly as in the spec's Schema section.

- [ ] **Step 1: Write the tests first (dbt-style TDD — declared expectations before the model)**

Create `dbt/tests/assert_fct_sales_unique_key.sql`:

```sql
-- The declared grain must be unique. Bounded to the window the current
-- run just loaded (max dt minus the incremental cap): an unbounded scan
-- of the full table would blow the workgroup cap; older partitions were
-- validated when loaded (nft_transfers lesson).
SELECT chain_id,
    transaction_hash,
    log_index,
    sk_contract,
    token_id
FROM {{ ref('fct_sales') }}
WHERE dt > {{ max_partition_dt_offset(ref('fct_sales'), -1 * var('sales_incremental_days', 10)) }}
GROUP BY chain_id,
    transaction_hash,
    log_index,
    sk_contract,
    token_id
HAVING COUNT(*) > 1
```

In `dbt/models/gold/schema.yml`, insert this block directly above the `- name: dim_currency` entry (inside `models:`):

```yaml
  - name: fct_sales
    description: >
      Sales fact: one row per token sold across the Ethereum marketplaces,
      priced in USD with the sale-day quote. Grain: (chain_id,
      transaction_hash, log_index, sk_contract, token_id). INNER JOINs to
      dim_currency, token_prices and dim_nft_contracts are intentional:
      only sales with a curated payment token and a price for the day are
      published. Starts 2019-01-01, the first day token_prices covers;
      2018 sales stay silver-only. Incremental, partitioned by dt; runs
      advance at most sales_incremental_days (default 10) past max dt.
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
      - name: buyer
        data_tests:
          - not_null
      - name: seller
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
      - name: currency_symbol
        data_tests:
          - not_null
      - name: total_amount
        data_tests:
          - not_null
      - name: royalty_amount
        data_tests:
          - not_null
      - name: usd_total_amount
        data_tests:
          - not_null
      - name: usd_royalty_amount
        data_tests:
          - not_null
      - name: chain_id
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: [1]
                quote: false
      - name: marketplace
        data_tests:
          - not_null
      - name: dt
        data_tests:
          - not_null
```

- [ ] **Step 2: Verify the tests fail to compile (model missing)**

Run:
```bash
cd /Users/tachone/proyectos/Decentraland/dbt
../.venv/bin/dbt parse 2>&1 | tail -5
```
Expected: FAIL — `Compilation Error ... depends on a node named 'fct_sales' which was not found`.

- [ ] **Step 3: Write the model**

Create `dbt/models/gold/fct_sales.sql`:

```sql
-- Sales fact for the gold star schema. One row per token sold, keyed by
-- (chain_id, transaction_hash, log_index, sk_contract, token_id).
-- INNER JOINs are intentional: only sales whose payment token is curated
-- in dim_currency AND has a USD quote for the sale day (token_prices is
-- gap-filled up to price_fill_end_dt) make it into gold. Addresses travel
-- via sk_contract / currency_symbol; join the dims to recover them.
--
-- No dedup here: silver.sales already keeps one copy per grain (latest
-- decoded_at + bronze_extracted_at per staging scan, insert_overwrite
-- partitions) and its unique-key test guards it. Loading recipe mirrors
-- silver.sales, but starting at 2019-01-01 — the first day token_prices
-- covers (DefiLlama backfill grid); 2018 sales stay silver-only. Initial
-- build seeds January 2019; incremental runs advance at most
-- sales_incremental_days (default 10) past max dt, e.g.
-- --vars '{sales_incremental_days: 180}' to catch up.
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['dt']
) }}

-- The window goes in a CTE: the macro emits an unqualified dt, ambiguous
-- once token_prices (also carrying dt) joins in.
WITH sales_window AS (
    SELECT *
    FROM {{ ref('sales') }}
    WHERE {{ sales_dt_window('2019-01-01', seed_end_dt='2019-02-01') }}
)

SELECT s.transaction_hash,
    s.log_index,
    s.block_timestamp,
    s.buyer,
    s.seller,
    n.sk_contract,
    s.token_id,
    s.quantity,
    c.currency_symbol,
    ROUND(s.total_amount_raw / POW(10, c.decimals), 6) AS total_amount,
    ROUND(COALESCE(s.royalty_amount_raw, 0) / POW(10, c.decimals), 6)
        AS royalty_amount,
    ROUND(s.total_amount_raw / POW(10, c.decimals) * p.price_usd, 6)
        AS usd_total_amount,
    ROUND(COALESCE(s.royalty_amount_raw, 0) / POW(10, c.decimals) * p.price_usd, 6)
        AS usd_royalty_amount,
    s.chain_id,
    s.marketplace,
    s.silver_processed_at,
    CAST(current_timestamp AS timestamp) AS gold_processed_at,
    s.dt
FROM sales_window AS s
    INNER JOIN {{ ref('dim_currency') }} AS c
        ON s.chain_id = c.chain_id
        AND s.currency = c.contract_address
    INNER JOIN {{ ref('token_prices') }} AS p
        ON c.currency_symbol = p.currency_symbol
        AND s.dt = p.dt
    INNER JOIN {{ ref('dim_nft_contracts') }} AS n
        ON s.chain_id = n.chain_id
        AND s.nft_contract_address = n.contract_address
```

- [ ] **Step 4: Verify parse passes**

Run: `../.venv/bin/dbt parse 2>&1 | tail -3`
Expected: no errors (ends with the perf-info line).

- [ ] **Step 5: Seeded build (January 2019) with tests**

Run:
```bash
../.venv/bin/dbt build --select fct_sales --full-refresh 2>&1 | tail -6
```
Expected: `Completed successfully`, `PASS=19 ... ERROR=0` (1 model + 18 tests). `--full-refresh` forces the non-incremental seed branch and replaces any leftover exploration table.

- [ ] **Step 6: Verify seed coverage against silver**

Run:
```bash
../.venv/bin/dbt show --inline "SELECT (SELECT COUNT(*) FROM \"gold\".\"fct_sales\") AS fct_rows, (SELECT COUNT(*) FROM \"silver\".\"sales\" WHERE dt >= '2019-01-01' AND dt < '2019-02-01') AS silver_rows, (SELECT MIN(dt) FROM \"gold\".\"fct_sales\") AS min_dt, (SELECT MAX(dt) FROM \"gold\".\"fct_sales\") AS max_dt, (SELECT ROUND(SUM(usd_total_amount)) FROM \"gold\".\"fct_sales\") AS usd_volume" 2>&1 | tail -6
```
Expected: `fct_rows = silver_rows = 336`, `min_dt = 2019-01-01`, `max_dt = 2019-01-31`, `usd_volume ≈ 255,969` (spec's Testing section: January 2019 sales are all MANA/ETH, priced from day one, so gold matches silver 1:1 in this window).

- [ ] **Step 7: Commit**

```bash
cd /Users/tachone/proyectos/Decentraland
git add dbt/models/gold/fct_sales.sql dbt/models/gold/schema.yml dbt/tests/assert_fct_sales_unique_key.sql
git commit -m "feat: gold.fct_sales — USD-priced sales fact

One row per token sold, keyed by (chain_id, transaction_hash,
log_index, sk_contract, token_id). INNER JOINs to dim_currency,
token_prices and dim_nft_contracts publish only sales with a curated
payment token and a sale-day USD quote. Starts 2019-01-01 (first day
token_prices covers); 2018 sales stay silver-only. Incremental
insert_overwrite partitioned by dt; initial build seeds January 2019.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Backfill 2019-02-01 → 2026-08-20

**Files:** none (repeated dbt runs; no code changes).

**Interfaces:**
- Consumes: the incremental branch of `sales_dt_window` via Task 2's model; var `sales_incremental_days`.
- Produces: `gold.fct_sales` loaded through `dt = 2026-08-20` (the token_prices frontier, `price_fill_end_dt`).

- [ ] **Step 1: Advance in windows until reaching the frontier**

From 2019-02-01 to 2026-08-20 there are ~2,758 days. Run repeatedly with 90-day windows (user directive): each run advances up to 90 days past the table's max dt and validates the loaded window. If a run fails (e.g. Athena scan/workgroup limits), retry the same stretch with a smaller window — degrade 90 → 30 → 10 → 5 → 1 — and continue at the largest size that works.

```bash
cd /Users/tachone/proyectos/Decentraland/dbt
../.venv/bin/dbt build --select fct_sales --vars '{sales_incremental_days: 90}' 2>&1 | tail -2
# repeat (~31 runs at 90 days) until max dt = 2026-08-20
```
Expected: each iteration `Completed successfully`, `ERROR=0`. Runs past the frontier load only days token_prices covers (the INNER JOIN caps at 2026-08-20); once max dt hits 2026-08-20 further runs write zero new partitions and are harmless.

- [ ] **Step 2: Verify the frontier and total volume**

Run:
```bash
../.venv/bin/dbt show --inline "SELECT (SELECT MAX(dt) FROM \"gold\".\"fct_sales\") AS max_dt, (SELECT COUNT(*) FROM \"gold\".\"fct_sales\") AS fct_rows, (SELECT COUNT(*) FROM \"silver\".\"sales\" AS s INNER JOIN \"gold\".\"dim_currency\" AS c ON s.chain_id = c.chain_id AND s.currency = c.contract_address INNER JOIN \"silver\".\"token_prices\" AS p ON c.currency_symbol = p.currency_symbol AND s.dt = p.dt WHERE s.dt >= '2019-01-01') AS expected_rows" 2>&1 | tail -6
```
Expected: `max_dt = 2026-08-20` and `fct_rows = expected_rows` (silver rows dropped only by the intentional INNER joins).

- [ ] **Step 3: Nothing to commit** — data-only task; note the outcome in the session summary.

---

### Task 4: Finish the branch

- [ ] **Step 1: Invoke `superpowers:finishing-a-development-branch`** — PR to `main` with squash merge (repo is squash-only), body ending with the standard generated-with footer. Do NOT push without explicit user confirmation (standing user rule).
