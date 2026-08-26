# Silver Currency Transfers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode ERC-20 Transfer events from bronze into per-chain silver fact tables (`ethereum_currency_transfers`, `polygon_currency_transfers`), tested, and backfilled through 2017 on Ethereum.

**Architecture:** dbt-athena incremental models (`append` strategy, partitioned by `dt`) that decode Transfer logs in SQL. Each run loads a 20-day window after the model's own `max(dt)`, resolved zero-scan via the `max_partition_dt` macro over `$partitions`. No decode Lambda, no join to `dim_contracts` (untracked payment tokens kept for future sales parsing).

**Tech Stack:** dbt-athena (venv at `.venv/bin/dbt`, run from `dbt/`), Athena engine v3, AWS CLI (workgroup `decentraland-data-platform`).

**Spec:** `docs/superpowers/specs/2026-08-26-currency-transfers-silver-design.md`

## Global Constraints

- Athena workgroup cap: `bytes_scanned_cutoff_per_query = 1 GB` — never widen a query beyond a 20-day bronze window.
- SQL style: keywords UPPERCASE; explicit `INNER JOIN`.
- All artifacts in English; commit messages in English.
- Partition column is `dt` (string, `YYYY-MM-DD`); never `date`.
- `max(dt)`/`min(dt)` of partition columns ALWAYS via `"<table>$partitions"`.
- Commit locally; never `git push` without the user's explicit OK.

## Current state (already implemented and verified, NOT yet committed)

- `dbt/macros/max_partition_dt.sql` — macro returning a scalar subquery `(SELECT MAX(dt) FROM "<schema>"."<table>$partitions")`.
- `dbt/models/silver/ethereum_currency_transfers.sql` — incremental/append, 20-day window, bootstrap `dt <= '2017-09-06'`.
- `dbt/models/silver/sources.yml` — `ethereum_logs` and `polygon_logs` sources added.
- Table `silver.ethereum_currency_transfers` exists with data through `dt=2017-09-16` (partition pruning verified: ~0.7 MB scanned per incremental run).

---

### Task 1: Commit the implemented model, macro and sources

**Files:**
- Commit (already on disk): `dbt/macros/max_partition_dt.sql`, `dbt/models/silver/ethereum_currency_transfers.sql`, `dbt/models/silver/sources.yml`

**Interfaces:**
- Produces: macro `max_partition_dt(relation)` — takes a dbt Relation (`this`, `ref()`, `source()`), returns a parenthesized scalar SQL subquery. Model `ethereum_currency_transfers` with columns `transaction_hash` (string), `log_index` (bigint), `block_timestamp` (timestamp), `token_address` (string), `from_address` (string), `to_address` (string), `amount_raw` (decimal(38,0)), `bronze_extracted_at` (timestamp), `processed_at` (timestamp), `dt` (string, partition).

- [ ] **Step 1: Verify the project parses**

Run: `cd dbt && ../.venv/bin/dbt parse`
Expected: `Completed successfully` (warnings about unused configs are acceptable; errors are not).

- [ ] **Step 2: Commit**

```bash
git add dbt/macros/max_partition_dt.sql dbt/models/silver/ethereum_currency_transfers.sql dbt/models/silver/sources.yml
git commit -m "feat: silver.ethereum_currency_transfers (SQL-decoded ERC-20 transfers, incremental 20-day window)"
```

---

### Task 2: Tests — schema not_null, unique grain, overflow guard

**Files:**
- Modify: `dbt/models/silver/schema.yml` (append a new model block)
- Create: `dbt/tests/assert_ethereum_currency_transfers_unique_key.sql`
- Create: `dbt/tests/assert_ethereum_currency_transfers_no_overflow.sql`

**Interfaces:**
- Consumes: model `ethereum_currency_transfers` and macro `max_partition_dt` from Task 1.
- Produces: dbt tests that run on every `dbt build` and stop the pipeline on failure.

- [ ] **Step 1: Add the model block to `dbt/models/silver/schema.yml`**

Append (keep existing content untouched):

```yaml
  - name: ethereum_currency_transfers
    description: >
      ERC-20 Transfer events decoded in SQL from bronze.ethereum_logs.
      Includes untracked payment tokens on purpose (future sales parsing).
      Grain: one row per (transaction_hash, log_index).
    columns:
      - name: transaction_hash
        description: Transaction hash.
        tests: [not_null]
      - name: log_index
        description: Log position within the block.
        tests: [not_null]
      - name: block_timestamp
        description: Block timestamp (UTC).
        tests: [not_null]
      - name: token_address
        description: ERC-20 contract that emitted the Transfer, lowercase.
        tests: [not_null]
      - name: from_address
        description: Sender address (decoded from topics[2]), lowercase.
        tests: [not_null]
      - name: to_address
        description: Recipient address (decoded from topics[3]), lowercase.
        tests: [not_null]
      - name: amount_raw
        description: Transfer amount in the token's smallest unit (uint256, valid below 2^96).
        tests: [not_null]
      - name: dt
        description: Block date partition (YYYY-MM-DD).
        tests: [not_null]
```

- [ ] **Step 2: Create the unique-grain singular test**

`dbt/tests/assert_ethereum_currency_transfers_unique_key.sql`:

```sql
-- Grain guard: one row per (transaction_hash, log_index). Duplicates mean
-- either a decode bug or a bronze day re-extracted after being loaded.
SELECT
    transaction_hash,
    log_index,
    COUNT(*) AS n
FROM {{ ref('ethereum_currency_transfers') }}
GROUP BY transaction_hash, log_index
HAVING COUNT(*) > 1
```

- [ ] **Step 3: Create the overflow-guard singular test**

`dbt/tests/assert_ethereum_currency_transfers_no_overflow.sql`:

```sql
-- The model decodes only the low 96 bits of the uint256 amount; a transfer
-- with any of the high 160 bits set would be silently truncated. Scan is
-- bounded to the 20-day window just loaded so it stays under the 1 GB cap.
SELECT
    transaction_hash,
    log_index,
    dt,
    data
FROM {{ source('bronze', 'ethereum_logs') }}
WHERE topics[1] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND cardinality(topics) = 3
  AND dt <= {{ max_partition_dt(ref('ethereum_currency_transfers')) }}
  AND dt >  CAST(CAST({{ max_partition_dt(ref('ethereum_currency_transfers')) }} AS date) - INTERVAL '20' DAY AS varchar)
  AND substr(data, 3, 40) != repeat('0', 40)
```

- [ ] **Step 4: Build model + tests together**

Run: `cd dbt && ../.venv/bin/dbt build --select ethereum_currency_transfers`
Expected: 1 incremental model PASS (loads the next 20-day window) and 10 tests PASS (8 not_null + 2 singular). If the overflow test fails, STOP and report the offending rows to the user — do not "fix" by deleting data.

- [ ] **Step 5: Commit**

```bash
git add dbt/models/silver/schema.yml dbt/tests/assert_ethereum_currency_transfers_unique_key.sql dbt/tests/assert_ethereum_currency_transfers_no_overflow.sql
git commit -m "test: not_null, unique grain and 2^96 overflow guard for ethereum_currency_transfers"
```

---

### Task 3: Backfill Ethereum through 2017

**Files:** none (repeated `dbt build` runs; no code changes)

**Interfaces:**
- Consumes: model + tests from Tasks 1–2. Table currently at `max(dt) = 2017-09-16`; Task 2's build advanced it to `2017-10-06`.

- [ ] **Step 1: Run the loop until the window passes 2017-12-31**

Each build advances 20 days (10-06 → 10-26 → 11-15 → 12-05 → 12-25 → 2018-01-14): 5 runs. Stop immediately if any run errors.

```bash
cd dbt
for i in 1 2 3 4 5; do
  ../.venv/bin/dbt build --select ethereum_currency_transfers || break
done
```

Expected: 5 successful builds, tests PASS on each.

- [ ] **Step 2: Verify coverage and volume in Athena**

```bash
QID=$(aws athena start-query-execution --work-group decentraland-data-platform \
  --query-string "SELECT MIN(dt), MAX(dt), COUNT(DISTINCT dt), COUNT(*) FROM silver.ethereum_currency_transfers WHERE dt <= '2017-12-31'" \
  --query QueryExecutionId --output text)
sleep 5
aws athena get-query-results --query-execution-id $QID --output text
```

Expected: `MIN(dt) = 2017-09-06`, `MAX(dt) <= 2017-12-31` range covered, non-trivial row count (thousands; 2017-09-15 alone has ~5,096). Report the numbers to the user.

- [ ] **Step 3: Verify the scan stayed small**

```bash
aws athena list-query-executions --work-group decentraland-data-platform --max-results 20 --output json | python3 -c "
import json, subprocess, sys
ids = json.load(sys.stdin)['QueryExecutionIds']
for qid in ids:
    q = json.loads(subprocess.check_output(['aws','athena','get-query-execution','--query-execution-id',qid]))['QueryExecution']
    print(q['Status']['State'], q.get('Statistics',{}).get('DataScannedInBytes'))"
```

Expected: every dbt query well under 1 GB (tens of MB at most). If any run approached the cap, tell the user before continuing.

---

### Task 4: `polygon_currency_transfers`

**Files:**
- Create: `dbt/models/silver/polygon_currency_transfers.sql`
- Modify: `dbt/models/silver/schema.yml` (append model block)
- Create: `dbt/tests/assert_polygon_currency_transfers_unique_key.sql`
- Create: `dbt/tests/assert_polygon_currency_transfers_no_overflow.sql`

**Interfaces:**
- Consumes: macro `max_partition_dt`, source `bronze.polygon_logs`.
- Produces: `polygon_currency_transfers`, same columns as `ethereum_currency_transfers`.

- [ ] **Step 1: Find the Polygon bootstrap date (zero-scan)**

```bash
QID=$(aws athena start-query-execution --work-group decentraland-data-platform \
  --query-string 'SELECT MIN(dt) FROM "bronze"."polygon_logs$partitions"' \
  --query QueryExecutionId --output text)
sleep 4
aws athena get-query-results --query-execution-id $QID --output text
```

If the result is empty/NULL, `bronze.polygon_logs` has no partitions yet: SKIP the rest of this task, note it in the plan checkboxes, and tell the user Polygon is blocked on bronze backfill. Otherwise call the result `<POLYGON_MIN_DT>` and use it verbatim below.

- [ ] **Step 2: Create the model**

`dbt/models/silver/polygon_currency_transfers.sql` — identical to the Ethereum model except source and bootstrap (replace `<POLYGON_MIN_DT>`):

```sql
-- ERC-20 Transfer events decoded straight from bronze in SQL (no decode
-- Lambda for this entity). No join to dim_contracts: untracked payment
-- tokens are kept on purpose for future sales parsing.
-- Grain: one row per (transaction_hash, log_index).
--
-- Incremental append: each run loads the next 20-day window after the
-- table's own max(dt), keeping every build under the workgroup's
-- bytes-scanned cap. Windows never overlap, so append is safe.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partitioned_by=['dt']
) }}

WITH dedup AS (

    SELECT *
    FROM {{ source('bronze', 'polygon_logs') }}
    WHERE topics[1] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
      AND cardinality(topics) = 3   -- ERC-721 Transfer has 4 topics
{% if is_incremental() %}
      AND dt >  {{ max_partition_dt(this) }}
      AND dt <= CAST(CAST({{ max_partition_dt(this) }} AS date) + INTERVAL '20' DAY AS varchar)
{% else %}
      -- bootstrap: the table does not exist yet, so $partitions is not
      -- available; start at the first bronze partition
      AND dt <= '<POLYGON_MIN_DT>'
{% endif %}

)

SELECT
    l.transaction_hash,
    l.log_index,
    l.block_timestamp,
    l.address                              AS token_address,
    concat('0x', substr(l.topics[2], 27))  AS from_address,
    concat('0x', substr(l.topics[3], 27))  AS to_address,
    -- uint256 split into two 48-bit halves to stay inside decimal(38,0);
    -- covers amounts < 2^96 (the overflow test guards the rest)
    CAST(from_base(substr(l.data, 43, 12), 16) AS decimal(38, 0)) * DECIMAL '281474976710656'
        + CAST(from_base(substr(l.data, 55, 12), 16) AS decimal(38, 0)) AS amount_raw,
    l.extracted_at                         AS bronze_extracted_at,
    CAST(current_timestamp AS timestamp)   AS processed_at,
    l.dt
FROM dedup l
```

- [ ] **Step 3: Add tests**

Append to `dbt/models/silver/schema.yml` the same block as Task 2 Step 1 with `name: polygon_currency_transfers` and description "ERC-20 Transfer events decoded in SQL from bronze.polygon_logs." (columns and tests identical).

`dbt/tests/assert_polygon_currency_transfers_unique_key.sql`:

```sql
-- Grain guard: one row per (transaction_hash, log_index).
SELECT
    transaction_hash,
    log_index,
    COUNT(*) AS n
FROM {{ ref('polygon_currency_transfers') }}
GROUP BY transaction_hash, log_index
HAVING COUNT(*) > 1
```

`dbt/tests/assert_polygon_currency_transfers_no_overflow.sql`:

```sql
-- Amounts >= 2^96 would be silently truncated; bounded to the last window.
SELECT
    transaction_hash,
    log_index,
    dt,
    data
FROM {{ source('bronze', 'polygon_logs') }}
WHERE topics[1] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND cardinality(topics) = 3
  AND dt <= {{ max_partition_dt(ref('polygon_currency_transfers')) }}
  AND dt >  CAST(CAST({{ max_partition_dt(ref('polygon_currency_transfers')) }} AS date) - INTERVAL '20' DAY AS varchar)
  AND substr(data, 3, 40) != repeat('0', 40)
```

- [ ] **Step 4: First build (bootstrap) and verify**

Run: `cd dbt && ../.venv/bin/dbt build --select polygon_currency_transfers`
Expected: model PASS + 10 tests PASS. Then confirm partitions exist:

```bash
QID=$(aws athena start-query-execution --work-group decentraland-data-platform \
  --query-string 'SELECT MAX(dt) FROM "silver"."polygon_currency_transfers$partitions"' \
  --query QueryExecutionId --output text)
sleep 4
aws athena get-query-results --query-execution-id $QID --output text
```

Expected: `<POLYGON_MIN_DT>` (or NULL if that day had zero transfers — if NULL, flag the empty-window stall limitation from the spec to the user before looping further).

- [ ] **Step 5: Commit**

```bash
git add dbt/models/silver/polygon_currency_transfers.sql dbt/models/silver/schema.yml dbt/tests/assert_polygon_currency_transfers_unique_key.sql dbt/tests/assert_polygon_currency_transfers_no_overflow.sql
git commit -m "feat: silver.polygon_currency_transfers with grain and overflow tests"
```

---

### Task 5: Refactor `dim_contracts` to use `max_partition_dt`

**Files:**
- Modify: `dbt/models/silver/dim_contracts.sql:11-18` (the `latest` CTE)

**Interfaces:**
- Consumes: macro `max_partition_dt` (Task 1); existing model `dim_contracts` (unchanged output).

- [ ] **Step 1: Replace the inline `$partitions` CTE**

In `dbt/models/silver/dim_contracts.sql`, replace:

```sql
with latest as (

    -- $partitions reads Glue partition metadata only: no data scanned,
    -- unlike max(dt) over the table itself.
    select max(dt) as dt
    from "bronze"."contracts$partitions"

)
```

and its join `inner join latest on c.dt = latest.dt` with a direct filter, leaving the model as:

```sql
select
    c.chain_id,
    c.contract_address,
    c.contract_name,
    c.dcl_contract,
    c.erc_type,
    c.first_mint_dt,
    c.extract_from_dt,
    c.dt as snapshot_dt
from {{ source('bronze', 'contracts') }} c
where c.erc_type != -1
  and c.dt = {{ max_partition_dt(source('bronze', 'contracts')) }}
```

Keep the model's header comment; the macro itself documents the zero-scan rationale. Note this file uses lowercase keywords — leave its existing style as is.

- [ ] **Step 2: Rebuild and verify identical output**

Run: `cd dbt && ../.venv/bin/dbt build --select dim_contracts`
Expected: model + its schema tests + the two `assert_dim_contracts_*` singular tests all PASS.

- [ ] **Step 3: Commit**

```bash
git add dbt/models/silver/dim_contracts.sql
git commit -m "refactor: dim_contracts reads latest partition via max_partition_dt macro"
```
