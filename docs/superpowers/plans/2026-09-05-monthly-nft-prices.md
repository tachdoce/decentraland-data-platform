# gold.fct_monthly_nft_prices Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Monthly aggregate of NFT sales per contract (USD median unit price + volume), built only on demand with a mandatory `month` var, and excluded from every automatic dbt selection via `tag:manual`.

**Architecture:** One new dbt gold model over `gold.fct_sales`, incremental `insert_overwrite` partitioned by `month` (`YYYY-MM`, taken verbatim from the var). The `run-dbt` Lambda gains a constant `--exclude tag:manual` so downstream selectors (`source:bronze.token_prices+`, `source:bronze.erc20_tokens+`) can never pull the model in; it runs only via local dbt.

**Tech Stack:** dbt-athena, dbt_utils, pytest, Docker (Lambda image), Terraform.

**Spec:** `docs/superpowers/specs/2026-09-05-monthly-nft-prices-design.md`

## Global Constraints

- Chat in Spanish; ALL artifacts in English (code, columns, commits, docs).
- SQL style: keywords UPPERCASE; explicit `INNER JOIN` (no bare `JOIN`); no `GROUP BY 1`.
- Partition column named `month` (`YYYY-MM`), listed last in the SELECT. Never `date`.
- Docker images for Lambda: `docker build --provenance=false`.
- Account `683569194224`, region `us-east-1`, CLI profile user `terraform-admin`, Athena workgroup `decentraland-data-platform`.
- Branch + PR with squash merge (repo is squash-only). Commit locally freely; **git push only with explicit user confirmation**.
- The var guard must never fire at parse time: every dbt invocation renders every model, and an unconditional raise breaks the daily pipelines. Guard under `{% if execute %}` and give every `var('month', ...)` lookup a default.

---

### Task 1: `run-dbt` Lambda always excludes `tag:manual`

**Files:**
- Modify: `dbt_runner/handler.py`
- Test: `tests/test_dbt_runner.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: every `dbt build` issued by the Lambda carries `--exclude tag:manual`. Task 2's model relies on this to stay out of `source:bronze.token_prices+` and `source:bronze.erc20_tokens+` runs. Constant name: `DEFAULT_EXCLUDE = "tag:manual"`.

- [ ] **Step 1: Create the feature branch**

```bash
git checkout -b feat/monthly-nft-prices
```

- [ ] **Step 2: Write the failing tests**

In `tests/test_dbt_runner.py`, extend `test_default_selector` and `test_selector_override` with the exclude assertion, and add one test making the intent explicit:

```python
# inside test_default_selector, after the existing asserts:
    assert "--exclude" in args
    assert args[args.index("--exclude") + 1] == "tag:manual"

# inside test_selector_override, after the existing asserts:
    args = FakeDbtRunner.calls[0]
    assert "--exclude" in args


def test_manual_tag_always_excluded(env, monkeypatch):
    # Models tagged 'manual' (e.g. fct_monthly_nft_prices) need a mandatory
    # var: any automatic selection reaching them would fail. The Lambda must
    # exclude the tag no matter which selector the event carries.
    _wire(monkeypatch)

    h.handler({"select": "source:bronze.token_prices+"}, None)

    args = FakeDbtRunner.calls[0]
    assert args[args.index("--exclude") + 1] == "tag:manual"
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/test_dbt_runner.py -v`
Expected: FAIL — `'--exclude' in args` assertions (3 tests).

- [ ] **Step 4: Implement the exclude in the handler**

In `dbt_runner/handler.py`:

```python
DEFAULT_SELECT = "source:bronze.contracts+"
# Models tagged 'manual' (mandatory-var, on-demand builds) must never run
# from an automatic selector; --exclude beats even direct selection.
DEFAULT_EXCLUDE = "tag:manual"
```

and in `run_dbt`, extend the invoke args:

```python
        [
            "build",
            "--select", select,
            "--exclude", DEFAULT_EXCLUDE,
            "--project-dir", project_dir,
            "--profiles-dir", project_dir,
            "--target-path", "/tmp/dbt-target",
            "--log-path", "/tmp/dbt-logs",
        ]
```

Also update the module docstring's event description to mention the fixed exclusion.

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_dbt_runner.py -v`
Expected: PASS (all).

- [ ] **Step 6: Full test suite + lint**

Run: `pytest tests/ -q && ruff check .`
Expected: all pass, no lint errors.

- [ ] **Step 7: Commit**

```bash
git add dbt_runner/handler.py tests/test_dbt_runner.py
git commit -m "feat: run-dbt Lambda always excludes tag:manual"
```

### Task 2: model `fct_monthly_nft_prices`

**Files:**
- Create: `dbt/models/gold/fct_monthly_nft_prices.sql`

**Interfaces:**
- Consumes: `{{ ref('fct_sales') }}` (columns `sk_contract`, `quantity`, `usd_total_amount`, `dt`).
- Produces: table `gold.fct_monthly_nft_prices` with columns `sk_contract`, `sales_count`, `nft_quantity`, `usd_total_amount`, `usd_median_unit_price`, `month` (partition). Mandatory var `month` (`YYYY-MM`). Tag `manual`. Task 3's tests target exactly these names.

- [ ] **Step 1: Write the model**

Create `dbt/models/gold/fct_monthly_nft_prices.sql`:

```sql
-- Monthly NFT price/volume aggregate per contract. One row per
-- (month, sk_contract); the median is over the UNIT price
-- (usd_total_amount / quantity) so multi-token bundles don't read as
-- one expensive sale.
--
-- On-demand only: the mandatory 'month' var (YYYY-MM) names the single
-- partition each run replaces. tag:manual keeps it out of every
-- automatic run-dbt selection (the Lambda always passes
-- --exclude tag:manual), so it runs only via local dbt:
--   dbt build --select fct_monthly_nft_prices --vars '{"month": "2020-01"}'
-- Backfill = loop over months from 2019-01 (fct_sales floor).
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['month'],
    tags=['manual']
) }}

{# The guard runs only at execution: every dbt invocation renders every
   model at parse time, and raising there would break the pipelines that
   never pass the var. The '1900-01' default keeps parse-time rendering
   harmless; a real run never sees it because the guard fires first. #}
{% set month = var('month', '1900-01') %}
{% if execute and not modules.re.match('^\\d{4}-(0[1-9]|1[0-2])$', month) %}
    {{ exceptions.raise_compiler_error(
        "fct_monthly_nft_prices requires --vars '{\"month\": \"YYYY-MM\"}'; got: "
        ~ var('month', '<missing>')
    ) }}
{% endif %}

SELECT sk_contract,
    COUNT(*) AS sales_count,
    SUM(quantity) AS nft_quantity,
    ROUND(SUM(usd_total_amount), 2) AS usd_total_amount,
    ROUND(approx_percentile(usd_total_amount / quantity, 0.5), 2)
        AS usd_median_unit_price,
    '{{ month }}' AS month
FROM {{ ref('fct_sales') }}
WHERE dt >= '{{ month }}-01'
    AND dt < CAST(date '{{ month }}-01' + INTERVAL '1' MONTH AS varchar)
GROUP BY sk_contract
```

- [ ] **Step 2: Verify parse safety (no var, whole project)**

Run: `cd dbt && dbt parse`
Expected: succeeds — the missing var must NOT break parse-time rendering.

- [ ] **Step 3: Verify the guard fires without the var**

Run: `cd dbt && dbt build --select fct_monthly_nft_prices`
Expected: FAIL with `fct_monthly_nft_prices requires --vars` (compilation error at execution).

- [ ] **Step 4: Verify the guard rejects malformed input**

Run: `cd dbt && dbt build --select fct_monthly_nft_prices --vars '{"month": "2020-1"}'`
Expected: same FAIL. Also try `{"month": "2020-01-01"}` → same FAIL.

- [ ] **Step 5: Verify automatic selectors exclude the model**

Run: `cd dbt && dbt ls --select "source:bronze.token_prices+" --exclude tag:manual | grep fct_monthly_nft_prices`
Expected: no output (grep exits 1). Repeat with `source:bronze.erc20_tokens+` and `source:bronze.contracts+`.

- [ ] **Step 6: First real build (2020-01)**

Run: `cd dbt && dbt build --select fct_monthly_nft_prices --vars '{"month": "2020-01"}'`
Expected: PASS, table created with the single partition `month=2020-01`. (Tests arrive in Task 3; `dbt build` runs just the model here.)

- [ ] **Step 7: Validate against the ad-hoc query**

Run through Athena (workgroup `decentraland-data-platform`):

```bash
aws athena start-query-execution \
  --work-group decentraland-data-platform \
  --query-string "SELECT COUNT(*) AS rows_model,
       SUM(sales_count) AS sales_model,
       ROUND(SUM(usd_total_amount), 2) AS usd_model
FROM gold.fct_monthly_nft_prices WHERE month = '2020-01'"
```

then compare with the same aggregates computed directly on the fact:

```bash
aws athena start-query-execution \
  --work-group decentraland-data-platform \
  --query-string "SELECT COUNT(DISTINCT sk_contract) AS rows_fact,
       COUNT(*) AS sales_fact,
       ROUND(SUM(usd_total_amount), 2) AS usd_fact
FROM gold.fct_sales WHERE dt >= '2020-01-01' AND dt < '2020-02-01'"
```

Fetch both with `aws athena get-query-results --query-execution-id <id>`.
Expected: `rows_model = rows_fact`, `sales_model = sales_fact`, `usd_model = usd_fact` (same ROUND on both sides).

- [ ] **Step 8: Verify idempotency (re-run replaces, not duplicates)**

Re-run Step 6, then re-run the first Step 7 query.
Expected: identical `rows_model` / `sales_model` — the partition was overwritten, not appended.

- [ ] **Step 9: Commit**

```bash
git add dbt/models/gold/fct_monthly_nft_prices.sql
git commit -m "feat: gold.fct_monthly_nft_prices — monthly median NFT prices"
```

### Task 3: schema tests, bounded to the run's partition

**Files:**
- Modify: `dbt/models/gold/schema.yml`

**Interfaces:**
- Consumes: Task 2's model and column names, var `month`.
- Produces: generic tests that run with the model (they inherit `tag:manual`, so automatic selections skip them too).

- [ ] **Step 1: Add the model entry to `dbt/models/gold/schema.yml`**

Append under `models:`. Every relational test is bounded to the partition just loaded (`where: month = var`) — the nft_transfers lesson: unbounded scans blow the workgroup cap, and older partitions were validated when loaded. The `var('month', '')` default keeps parse-time YAML rendering safe.

```yaml
  - name: fct_monthly_nft_prices
    description: >
      Monthly NFT aggregate per contract: USD median unit price
      (usd_total_amount / quantity) plus volume. Grain: (month,
      sk_contract). On-demand only: mandatory 'month' var (YYYY-MM)
      names the single partition each run replaces; tag:manual keeps it
      out of every automatic run-dbt selection, so it runs only via
      local dbt. Tests are bounded to the partition just loaded.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns:
              - month
              - sk_contract
          config:
            where: "month = '{{ var(\"month\", \"\") }}'"
      - dbt_utils.expression_is_true:
          name: fct_monthly_nft_prices_median_non_negative
          arguments:
            expression: "usd_median_unit_price >= 0"
          config:
            where: "month = '{{ var(\"month\", \"\") }}'"
      - dbt_utils.expression_is_true:
          name: fct_monthly_nft_prices_quantity_positive
          arguments:
            expression: "nft_quantity > 0"
          config:
            where: "month = '{{ var(\"month\", \"\") }}'"
    columns:
      - name: sk_contract
        data_tests:
          - not_null:
              config:
                where: "month = '{{ var(\"month\", \"\") }}'"
      - name: month
        data_tests:
          - not_null:
              config:
                where: "month = '{{ var(\"month\", \"\") }}'"
      - name: usd_median_unit_price
        data_tests:
          - not_null:
              config:
                where: "month = '{{ var(\"month\", \"\") }}'"
```

- [ ] **Step 2: Parse check**

Run: `cd dbt && dbt parse`
Expected: succeeds without the var.

- [ ] **Step 3: Build a second month with tests**

Run: `cd dbt && dbt build --select fct_monthly_nft_prices --vars '{"month": "2020-02"}'`
Expected: model PASS + 6 tests PASS. Table now holds partitions 2020-01 and 2020-02.

- [ ] **Step 4: Confirm tests inherit the manual tag**

Run: `cd dbt && dbt ls --select "source:bronze.token_prices+" --exclude tag:manual --resource-type test | grep fct_monthly_nft_prices`
Expected: no output (grep exits 1) — the bounded tests can never run varless from a pipeline.

- [ ] **Step 5: Commit**

```bash
git add dbt/models/gold/schema.yml
git commit -m "test: bounded schema tests for fct_monthly_nft_prices"
```

### Task 4: deploy the Lambda image and verify the exclusion in AWS

The image bakes both the handler and the dbt project (`COPY dbt/`), so one push ships Task 1 + the new model together. No CI yet: build/push is manual, then `terraform apply` rolls the digest.

**Files:**
- None modified (deploy only).

**Interfaces:**
- Consumes: Tasks 1-3 committed; ECR repo `decentraland-run-dbt`; `data.aws_ecr_image.run_dbt` resolves `:latest`.
- Produces: the deployed `run-dbt` Lambda excluding `tag:manual` on every automatic run.

- [ ] **Step 1: Build and push the image**

From the repo root:

```bash
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 683569194224.dkr.ecr.us-east-1.amazonaws.com
docker build --provenance=false -f dbt_runner/Dockerfile \
  -t 683569194224.dkr.ecr.us-east-1.amazonaws.com/decentraland-run-dbt:latest .
docker push 683569194224.dkr.ecr.us-east-1.amazonaws.com/decentraland-run-dbt:latest
```

- [ ] **Step 2: Terraform plan (read it) and apply**

```bash
cd terraform && terraform plan
```

Expected: only `aws_lambda_function.run_dbt` changes (`image_uri` digest). ALWAYS read the plan; if anything else changes, stop and report.

```bash
terraform apply
```

- [ ] **Step 3: Verify a downstream selector skips the model in AWS**

```bash
aws lambda invoke --function-name run-dbt \
  --payload '{"select": "dim_currency"}' \
  --cli-binary-format raw-in-base64-out /dev/stdout
```

Expected: success (`{"select": "dim_currency"}` in the response, no error). Then check CloudWatch logs for the invocation and confirm the run built `dim_currency` and nothing tagged manual. This exercises the deployed `--exclude` path with a cheap selector.

- [ ] **Step 4: Commit nothing — confirm clean tree**

Run: `git status`
Expected: clean (deploy produced no repo changes).

### Task 5: backfill 2019-01 → last closed month

**Files:**
- None (invocations only).

**Interfaces:**
- Consumes: Tasks 2-3 merged behavior locally (branch is fine — dbt runs from the working tree).
- Produces: `gold.fct_monthly_nft_prices` loaded 2019-01 through 2026-08.

- [ ] **Step 1: Run the month loop**

`fct_sales` starts 2019-01-01 and is loaded through 2026-08-20; last closed month for the fact is 2026-07, but 2026-08 exists partially — load it too and note it's partial (re-running the month later refreshes it, insert_overwrite). Sequential loop (each run is one cheap Athena query; no concurrency limits in play):

```bash
cd dbt
for month in $(python3 -c "
from datetime import date
d = date(2019, 1, 1)
while d <= date(2026, 8, 1):
    print(d.strftime('%Y-%m'))
    d = date(d.year + d.month // 12, d.month % 12 + 1, 1)
"); do
  dbt build --select fct_monthly_nft_prices --vars "{\"month\": \"$month\"}" || break
done
```

Expected: 92 runs, all PASS (model + 6 tests each). 2020-01 and 2020-02 are re-runs — harmless by idempotency.

- [ ] **Step 2: Verify coverage via partition metadata (zero bytes scanned)**

```bash
aws athena start-query-execution \
  --work-group decentraland-data-platform \
  --query-string "SELECT MIN(month), MAX(month), COUNT(*) FROM gold.\"fct_monthly_nft_prices\$partitions\""
```

Expected: `2019-01`, `2026-08`, `92`.

- [ ] **Step 3: Sanity-check one well-known month**

Re-run the Task 2 Step 7 comparison for a different month (e.g. `2021-11`, peak market) and eyeball the top medians: `SELECT * FROM gold.fct_monthly_nft_prices WHERE month = '2021-11' ORDER BY usd_median_unit_price DESC LIMIT 10`.
Expected: aggregates match the fact; medians plausible (no negatives, no absurd values).

### Task 6: finish the branch

- [ ] **Step 1: Full verification**

Run: `pytest tests/ -q && ruff check . && cd dbt && dbt parse`
Expected: all pass.

- [ ] **Step 2: Use superpowers:finishing-a-development-branch**

PR to `main` with squash merge (repo is squash-only). **Do not push without explicit user confirmation.** PR body summarizes: new gold model, mandatory var, tag:manual exclusion in run-dbt, backfill done through 2026-08 (partial month).

---

## Self-review notes

- Spec coverage: grain/schema (Task 2), mandatory validated var (Task 2 Steps 3-4), insert_overwrite by month (Task 2 Steps 6/8), tag:manual + Lambda exclude (Tasks 1, 2 Step 5, 3 Step 4, 4), bounded tests (Task 3), backfill 2019-01→last month (Task 5), no triggers (nothing added anywhere).
- `modules.re` is available in dbt Jinja (stdlib `re`); `match` anchors at the start, and the `$` in the pattern anchors the end — `2020-01-01` fails it.
- Athena's `approx_percentile(x, 0.5)` over `usd_total_amount / quantity` divides double by decimal — valid in Trino, returns double.
