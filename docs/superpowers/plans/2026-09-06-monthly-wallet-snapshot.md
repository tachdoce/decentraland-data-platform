# gold.fct_monthly_wallet_snapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Monthly wallet-state snapshot in gold: first-touch timestamps, MANA balance and NFT holdings/value for every wallet that ever received a DCL NFT or MANA before the start of the month.

**Architecture:** One dbt-athena incremental model partitioned by `month` with `insert_overwrite`, driven by a mandatory `month` var and tagged `manual` (excluded from every automatic run), plus bounded schema tests — the exact `fct_monthly_nft_prices` pattern. Each run scans full history up to `dt < '<month>-01'` (cumulative snapshot).

**Tech Stack:** dbt-athena (SQL only), Athena/Trino SQL, existing gold facts (`fct_nft_transfers`, `fct_mana_transfers`, `fct_monthly_nft_prices`, `dim_nft_contracts`).

**Spec:** `docs/superpowers/specs/2026-09-06-monthly-wallet-snapshot-design.md`

## Global Constraints

- SQL keywords UPPERCASE; `INNER JOIN` always explicit, never bare `JOIN`.
- `month` semantics: partition `YYYY-MM` = wallet state at the **start** of that month; cutoff is `dt < '<month>-01'`; valuation joins `fct_monthly_nft_prices` at `month - 1`.
- Special contracts by `contract_name` (`'LANDProxy'`, `'DCLRegistrar'`, `'DCLLaunchCollection'`), never hardcoded `sk_contract`.
- Zero address excluded ONLY in the final SELECT: `wallet_address <> '0x0000000000000000000000000000000000000000'`.
- Backfill floor: `2020-01`. Last valid month today: `2026-08` (facts are backfilled through 2026-08-20, so a `2026-09` snapshot would be built on incomplete data — do not load it).
- All schema tests bounded to `month = var('month')` (no unbounded scans).
- Work on branch `feat/fct-monthly-wallet-snapshot`; PR with squash merge; never `git push` without explicit user confirmation.

---

### Task 1: Model SQL + schema tests

**Files:**
- Create: `dbt/models/gold/fct_monthly_wallet_snapshot.sql`
- Modify: `dbt/models/gold/schema.yml` (append model entry at the end)

**Interfaces:**
- Consumes: `ref('fct_nft_transfers')` (`to_address`, `from_address`, `sk_contract`, `token_id`, `quantity`, `block_timestamp`, `log_index`, `dt`), `ref('fct_mana_transfers')` (`to_address`, `from_address`, `amount`, `block_timestamp`, `dt`), `ref('dim_nft_contracts')` (`sk_contract`, `contract_name`, `dcl_contract`, `erc_type`), `ref('fct_monthly_nft_prices')` (`month`, `sk_contract`, `usd_median_unit_price`).
- Produces: table `gold.fct_monthly_wallet_snapshot` partitioned by `month`, one row per `(month, wallet_address)` with the 13 columns from the spec.

- [ ] **Step 1: Write the model**

Create `dbt/models/gold/fct_monthly_wallet_snapshot.sql` with exactly:

```sql
-- Monthly wallet-state snapshot. Grain: one row per (month,
-- wallet_address) for every wallet that received a DCL NFT or MANA at
-- least once before the month started. month = 'YYYY-MM' means the
-- state at the START of that month (cutoff dt < '<month>-01');
-- valuations use fct_monthly_nft_prices of the previous month, the
-- last closed one at that point.
--
-- ERC-721 ownership resolves the latest transfer per (sk_contract,
-- token_id) by (block_timestamp, log_index) — log_index breaks
-- same-block ties. ERC-1155 is a net quantity sum. mana_balance is
-- NULL (not 0) below the 0.001 dust threshold, distinguishable from
-- an exact 0 holding. The zero address is excluded once, in the final
-- SELECT.
--
-- On-demand only: the mandatory 'month' var (YYYY-MM) names the single
-- partition each run replaces. tag:manual keeps it out of every
-- automatic run-dbt selection (the Lambda always passes
-- --exclude tag:manual), so it runs only via local dbt:
--   dbt build --select fct_monthly_wallet_snapshot --vars '{"month": "2020-01"}'
-- Backfill = loop over months from 2020-01.
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['month'],
    tags=['manual']
) }}

{# The guard runs only at execution: every dbt invocation renders every
   model at parse time, and raising there would break the pipelines that
   never pass the var. The none default keeps parse-time rendering
   harmless; a real run never sees it because the guard fires first. #}
{% set month = var('month', none) %}
{% if execute and (month is none or not modules.re.match('^\\d{4}-(0[1-9]|1[0-2])$', month)) %}
    {{ exceptions.raise_compiler_error(
        "fct_monthly_wallet_snapshot requires --vars '{\"month\": \"YYYY-MM\"}'; got: "
        ~ var('month', '<missing>')
    ) }}
{% endif %}

WITH nft_first_time AS (
    SELECT t.to_address AS wallet_address,
        MIN(CASE WHEN c.contract_name = 'LANDProxy'
            THEN t.block_timestamp END) AS land_owner_first_time,
        MIN(CASE WHEN c.contract_name = 'DCLRegistrar'
            THEN t.block_timestamp END) AS dclregistrar_first_time,
        MIN(CASE WHEN c.contract_name = 'DCLLaunchCollection'
            THEN t.block_timestamp END) AS dcllaunchcollection_first_time,
        MIN(CASE WHEN c.contract_name NOT IN
                ('LANDProxy', 'DCLRegistrar', 'DCLLaunchCollection')
            THEN t.block_timestamp END) AS dcl_others_first_time
    FROM {{ ref('fct_nft_transfers') }} AS t
        INNER JOIN {{ ref('dim_nft_contracts') }} AS c
            ON t.sk_contract = c.sk_contract
    WHERE c.dcl_contract
        AND t.dt < '{{ month }}-01'
    GROUP BY t.to_address
),

mana_first_time AS (
    SELECT to_address AS wallet_address,
        MIN(block_timestamp) AS mana_first_time
    FROM {{ ref('fct_mana_transfers') }}
    WHERE dt < '{{ month }}-01'
    GROUP BY to_address
),

first_time AS (
    SELECT COALESCE(n.wallet_address, m.wallet_address) AS wallet_address,
        n.land_owner_first_time,
        n.dclregistrar_first_time,
        n.dcllaunchcollection_first_time,
        n.dcl_others_first_time,
        m.mana_first_time
    FROM nft_first_time AS n
        FULL OUTER JOIN mana_first_time AS m
            ON n.wallet_address = m.wallet_address
),

mana_movements AS (
    SELECT to_address AS wallet_address,
        amount
    FROM {{ ref('fct_mana_transfers') }}
    WHERE dt < '{{ month }}-01'
    UNION ALL
    SELECT from_address AS wallet_address,
        -amount AS amount
    FROM {{ ref('fct_mana_transfers') }}
    WHERE dt < '{{ month }}-01'
),

mana_balance AS (
    SELECT wallet_address,
        ROUND(SUM(amount), 2) AS mana_balance
    FROM mana_movements
    GROUP BY wallet_address
    HAVING SUM(amount) > 0.001
),

erc721_latest AS (
    SELECT t.sk_contract,
        t.token_id,
        t.to_address AS wallet_address,
        ROW_NUMBER() OVER (
            PARTITION BY t.sk_contract, t.token_id
            ORDER BY t.block_timestamp DESC, t.log_index DESC
        ) AS rn
    FROM {{ ref('fct_nft_transfers') }} AS t
        INNER JOIN {{ ref('dim_nft_contracts') }} AS c
            ON t.sk_contract = c.sk_contract
            AND c.erc_type = 721
    WHERE t.dt < '{{ month }}-01'
),

erc721_holdings AS (
    SELECT wallet_address,
        sk_contract,
        COUNT(*) AS quantity
    FROM erc721_latest
    WHERE rn = 1
    GROUP BY wallet_address, sk_contract
),

erc1155_movements AS (
    SELECT t.to_address AS wallet_address,
        t.sk_contract,
        t.quantity
    FROM {{ ref('fct_nft_transfers') }} AS t
        INNER JOIN {{ ref('dim_nft_contracts') }} AS c
            ON t.sk_contract = c.sk_contract
            AND c.erc_type = 1155
    WHERE t.dt < '{{ month }}-01'
    UNION ALL
    SELECT t.from_address AS wallet_address,
        t.sk_contract,
        -t.quantity AS quantity
    FROM {{ ref('fct_nft_transfers') }} AS t
        INNER JOIN {{ ref('dim_nft_contracts') }} AS c
            ON t.sk_contract = c.sk_contract
            AND c.erc_type = 1155
    WHERE t.dt < '{{ month }}-01'
),

erc1155_holdings AS (
    SELECT wallet_address,
        sk_contract,
        SUM(quantity) AS quantity
    FROM erc1155_movements
    GROUP BY wallet_address, sk_contract
    HAVING SUM(quantity) > 0
),

holdings AS (
    SELECT wallet_address, sk_contract, quantity FROM erc721_holdings
    UNION ALL
    SELECT wallet_address, sk_contract, quantity FROM erc1155_holdings
),

holdings_value AS (
    SELECT h.wallet_address,
        SUM(CASE WHEN c.dcl_contract THEN 1 ELSE 0 END) >= 1
            AS own_dcl_nfts,
        SUM(CASE WHEN NOT c.dcl_contract THEN 1 ELSE 0 END) >= 1
            AS own_other_nfts,
        ROUND(SUM(CASE WHEN c.dcl_contract
            THEN COALESCE(p.usd_median_unit_price * h.quantity, 0)
            ELSE 0 END), 2) AS dcl_nft_value,
        ROUND(SUM(CASE WHEN NOT c.dcl_contract
            THEN COALESCE(p.usd_median_unit_price * h.quantity, 0)
            ELSE 0 END), 2) AS other_nft_value
    FROM holdings AS h
        INNER JOIN {{ ref('dim_nft_contracts') }} AS c
            ON h.sk_contract = c.sk_contract
        LEFT JOIN {{ ref('fct_monthly_nft_prices') }} AS p
            ON h.sk_contract = p.sk_contract
            AND p.month = DATE_FORMAT(
                DATE '{{ month }}-01' - INTERVAL '1' MONTH, '%Y-%m')
    GROUP BY h.wallet_address
)

SELECT f.wallet_address,
    f.land_owner_first_time,
    f.dclregistrar_first_time,
    f.dcllaunchcollection_first_time,
    f.dcl_others_first_time,
    f.mana_first_time,
    b.mana_balance,
    COALESCE(h.own_dcl_nfts, FALSE) AS own_dcl_nfts,
    COALESCE(h.own_other_nfts, FALSE) AS own_other_nfts,
    COALESCE(h.dcl_nft_value, 0) AS dcl_nft_value,
    COALESCE(h.other_nft_value, 0) AS other_nft_value,
    CAST(current_timestamp AS timestamp) AS gold_processed_at,
    '{{ month }}' AS month
FROM first_time AS f
    LEFT JOIN mana_balance AS b
        ON f.wallet_address = b.wallet_address
    LEFT JOIN holdings_value AS h
        ON f.wallet_address = h.wallet_address
WHERE f.wallet_address <> '0x0000000000000000000000000000000000000000'
```

- [ ] **Step 2: Append the schema.yml entry**

Append to `dbt/models/gold/schema.yml` (same file, after `dim_currency`):

```yaml
  - name: fct_monthly_wallet_snapshot
    description: >
      Monthly wallet-state snapshot: one row per (month, wallet_address)
      for wallets that received a DCL NFT or MANA at least once before
      the month started. month = state at the START of that month
      (cutoff dt < month-01); NFT valuations use the previous month's
      fct_monthly_nft_prices. mana_balance is NULL below the 0.001 dust
      threshold. On-demand only: mandatory 'month' var names the single
      partition each run replaces; tag:manual keeps it out of automatic
      runs. Tests are bounded to the partition just loaded.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns:
              - month
              - wallet_address
          config:
            where: "month = '{{ var(\"month\", \"\") }}'"
      - dbt_utils.expression_is_true:
          name: fct_monthly_wallet_snapshot_balance_non_negative
          arguments:
            # HAVING keeps SUM(amount) > 0.001, but ROUND(_, 2) can
            # legally leave 0.00 for sums in (0.001, 0.005) — so the
            # test only asserts non-negativity.
            expression: "mana_balance >= 0"
          config:
            where: "month = '{{ var(\"month\", \"\") }}'"
    columns:
      - name: wallet_address
        data_tests:
          - not_null:
              config:
                where: "month = '{{ var(\"month\", \"\") }}'"
      - name: month
        data_tests:
          - not_null:
              config:
                where: "month = '{{ var(\"month\", \"\") }}'"
```

- [ ] **Step 3: Verify the project parses and the guard fires**

```bash
cd dbt && dbt parse
```
Expected: success (parse-time rendering must be harmless without the var).

```bash
cd dbt && dbt compile --select fct_monthly_wallet_snapshot
```
Expected: FAIL with `fct_monthly_wallet_snapshot requires --vars '{"month": "YYYY-MM"}'; got: <missing>`.

```bash
cd dbt && dbt compile --select fct_monthly_wallet_snapshot --vars '{"month": "2020-13"}'
```
Expected: FAIL with the same message showing `got: 2020-13`.

- [ ] **Step 4: Compile with a valid month and eyeball the SQL**

```bash
cd dbt && dbt compile --select fct_monthly_wallet_snapshot --vars '{"month": "2020-01"}'
```
Expected: success. Open `target/compiled/.../fct_monthly_wallet_snapshot.sql` and confirm the cutoffs render as `dt < '2020-01-01'` and the price join as `p.month = DATE_FORMAT(DATE '2020-01-01' - INTERVAL '1' MONTH, '%Y-%m')`.

- [ ] **Step 5: Commit**

```bash
git add dbt/models/gold/fct_monthly_wallet_snapshot.sql dbt/models/gold/schema.yml
git commit -m "feat: gold.fct_monthly_wallet_snapshot — monthly wallet state snapshot"
```

---

### Task 2: First real build (2020-01) + validation against draft numbers

**Files:**
- No file changes; Athena/dbt runs only.

**Interfaces:**
- Consumes: the model from Task 1.
- Produces: partition `month=2020-01` in `gold.fct_monthly_wallet_snapshot`, validated.

- [ ] **Step 1: Build the first month with tests**

```bash
cd dbt && dbt build --select fct_monthly_wallet_snapshot --vars '{"month": "2020-01"}'
```
Expected: model + 4 tests PASS.

- [ ] **Step 2: Sanity-check the partition in Athena**

Run via the tagged workgroup (same as usual ad-hoc checks):

```sql
SELECT COUNT(*) AS wallets,
    COUNT(mana_first_time) AS with_mana,
    COUNT(land_owner_first_time) AS with_land,
    SUM(CASE WHEN own_dcl_nfts THEN 1 ELSE 0 END) AS holders_dcl,
    SUM(CASE WHEN mana_balance IS NOT NULL THEN 1 ELSE 0 END) AS with_balance
FROM "gold"."fct_monthly_wallet_snapshot"
WHERE month = '2020-01';
```

Expected: `wallets` in the same order of magnitude as the user's draft query for `dt < '2020-01'` (tens of thousands); `with_mana ≥ with_land`; no row with `wallet_address = '0x0000000000000000000000000000000000000000'`:

```sql
SELECT COUNT(*)
FROM "gold"."fct_monthly_wallet_snapshot"
WHERE month = '2020-01'
    AND wallet_address = '0x0000000000000000000000000000000000000000';
```
Expected: 0.

- [ ] **Step 3: Re-run the same month to prove idempotency**

```bash
cd dbt && dbt build --select fct_monthly_wallet_snapshot --vars '{"month": "2020-01"}'
```
Then re-run the COUNT query from Step 2. Expected: identical `wallets` count (insert_overwrite replaced, not appended).

---

### Task 3: Backfill 2020-01 → 2026-08

**Files:**
- No file changes; sequential dbt runs.

**Interfaces:**
- Consumes: the validated model.
- Produces: 80 partitions, `2020-01` through `2026-08`.

- [ ] **Step 1: Run the backfill loop (sequential — each run overwrites one partition)**

```bash
cd dbt
for month in $(python3 -c "
from datetime import date
d = date(2020, 1, 1)
while d <= date(2026, 8, 1):
    print(d.strftime('%Y-%m'))
    m = d.month % 12 + 1
    d = date(d.year + (d.month == 12), m, 1)
"); do
    dbt build --select fct_monthly_wallet_snapshot --vars "{\"month\": \"$month\"}" || break
done
```
Expected: 80 successful builds (2020-01 already loaded in Task 2; re-running it is idempotent). If one month fails, the loop stops there — fix and resume from that month.

- [ ] **Step 2: Verify all partitions landed**

```sql
SELECT MIN(month), MAX(month), COUNT(*) AS partitions
FROM "gold"."fct_monthly_wallet_snapshot$partitions";
```
Expected: `2020-01`, `2026-08`, 80.

- [ ] **Step 3: Spot-check monotonic population growth**

```sql
SELECT month, COUNT(*) AS wallets
FROM "gold"."fct_monthly_wallet_snapshot"
GROUP BY month
ORDER BY month;
```
Expected: `wallets` never decreases month over month (the population is cumulative — a decrease means a data bug).

---

### Task 4: Final review + PR

**Files:**
- No new files.

- [ ] **Step 1: Self-review the diff**

```bash
git log --oneline main..HEAD && git diff main --stat
```
Confirm only the spec, plan, model and schema.yml changed.

- [ ] **Step 2: Ask the user for confirmation to push, then open the PR**

Never push without explicit confirmation. After the user confirms:

```bash
git push -u origin feat/fct-monthly-wallet-snapshot
gh pr create --title "feat: gold.fct_monthly_wallet_snapshot — monthly wallet state snapshot" --body "$(cat <<'EOF'
Monthly wallet-state snapshot: one row per (month, wallet_address) for
wallets that ever received a DCL NFT or MANA before the month started.
Mandatory month var + tag:manual (fct_monthly_nft_prices pattern);
backfilled 2020-01 → 2026-08.

Spec: docs/superpowers/specs/2026-09-06-monthly-wallet-snapshot-design.md

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
Merge with squash (repo is squash-only).
