# dcl_contracts Explicit Partitions + EventBridge Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align `bronze.dcl_contracts` with the `nft_contracts` pattern — explicit `ALTER TABLE ADD PARTITION` instead of partition projection — and remove the EventBridge daily cron (Step Functions will orchestrate the pipeline in phase 8).

**Architecture:** The partition-registration logic currently living in `ingestion/nft_contracts/handler.py` is extracted to a shared `ingestion/common/partitions.py` parameterized by table name; both Lambdas call it after writing their snapshot. The `dcl_contracts` Glue table loses its `projection.*` parameters; existing S3 snapshots are registered once with `MSCK REPAIR TABLE`. The EventBridge rule/target/permission are destroyed, leaving `extract-dcl-contracts` invocable only manually until Step Functions arrives.

**Tech Stack:** Python 3.13 Lambda (boto3 `athena` client), Terraform (aws_glue_catalog_table, aws_iam_role_policy, archive_file, aws_lambda_function), pytest with fake boto3 clients.

**Spec:** `docs/superpowers/specs/2026-08-21-dcl-contracts-extraction.md` (this plan amends its partition-discovery and trigger mechanisms; Task 5 updates the spec text). Design validated in chat 2026-08-22.

## Global Constraints

- Chat in Spanish; ALL artifacts in English (code, comments, commits, docs).
- Region `us-east-1`; bucket `decentraland-data-platform-${account_id}` — never hardcode the account id in Terraform (tests/CLI may use the literal `decentraland-data-platform-683569194224`).
- Partition key is `dt=YYYY-MM-DD`; never named `date`.
- Idempotency: re-running the Lambda the same day overwrites the parquet and leaves the partition registered exactly once (`IF NOT EXISTS`).
- Backfill contract preserved: `{"date": "YYYY-MM-DD"}` writes that dt; with projection gone there is no longer a minimum date.
- Cost tags unchanged; DDL runs in workgroup `decentraland-data-platform` so its spend stays tagged.
- Run all tests with the project venv: `.venv/bin/pytest` (Python 3.13, matching the Lambda runtime).
- Terraform: always read `terraform plan` output before `apply`.

---

### Task 1: Extract shared `register_partition` to `ingestion/common/partitions.py`

**Files:**
- Create: `ingestion/common/__init__.py` (empty), `ingestion/common/partitions.py`
- Modify: `ingestion/nft_contracts/handler.py`
- Test: create `tests/test_partitions.py`; modify `tests/test_nft_contracts.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `register_partition(table: str, run_date: datetime.date, bucket: str) -> None` in `ingestion/common/partitions.py` — runs `ALTER TABLE bronze.<table> ADD IF NOT EXISTS PARTITION` for `s3://<bucket>/bronze/<table>/dt=<date>/`, raises `RuntimeError` on `FAILED`/`CANCELLED`; module constant `ATHENA_WORKGROUP` from env var `ATHENA_WORKGROUP` (default `"decentraland-data-platform"`). Tasks 2-3 rely on this exact signature.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_partitions.py`:

```python
import datetime

import pytest

from ingestion.common.partitions import register_partition


class FakeAthena:
    """Records DDL statements and reports a fixed terminal state."""

    def __init__(self, queries, state="SUCCEEDED"):
        self.queries = queries
        self.state = state

    def start_query_execution(self, QueryString, WorkGroup):
        self.queries.append({"ddl": QueryString, "workgroup": WorkGroup})
        return {"QueryExecutionId": "fake-query-id"}

    def get_query_execution(self, QueryExecutionId):
        return {
            "QueryExecution": {
                "Status": {"State": self.state, "StateChangeReason": "fake reason"}
            }
        }


def test_register_partition_builds_ddl_for_the_given_table(monkeypatch):
    import ingestion.common.partitions as p

    queries = []
    monkeypatch.setattr(p.boto3, "client", lambda service: FakeAthena(queries))
    register_partition("dcl_contracts", datetime.date(2026, 8, 22), "test-bucket")

    assert len(queries) == 1
    ddl = queries[0]["ddl"]
    assert "ALTER TABLE bronze.dcl_contracts" in ddl
    assert "ADD IF NOT EXISTS PARTITION (dt = '2026-08-22')" in ddl
    assert "LOCATION 's3://test-bucket/bronze/dcl_contracts/dt=2026-08-22/'" in ddl
    assert queries[0]["workgroup"] == "decentraland-data-platform"


def test_register_partition_raises_on_failed_ddl(monkeypatch):
    import ingestion.common.partitions as p

    monkeypatch.setattr(
        p.boto3, "client", lambda service: FakeAthena([], state="FAILED")
    )
    with pytest.raises(RuntimeError, match="FAILED.*fake reason"):
        register_partition("nft_contracts", datetime.date(2026, 8, 21), "test-bucket")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_partitions.py -v`
Expected: both FAIL at import time with `ModuleNotFoundError: No module named 'ingestion.common'`.

- [ ] **Step 3: Create the shared module**

Create empty `ingestion/common/__init__.py`. Create `ingestion/common/partitions.py`:

```python
"""Explicit partition registration for bronze tables (no partition projection)."""

import datetime
import os
import time

import boto3

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")


def register_partition(table: str, run_date: datetime.date, bucket: str) -> None:
    """Register a snapshot partition in the Glue catalog via Athena DDL.

    The bronze tables have no partition projection, so each dt must be
    added explicitly. IF NOT EXISTS keeps same-day re-runs idempotent.
    """
    athena = boto3.client("athena")
    dt = run_date.isoformat()
    ddl = (
        f"ALTER TABLE bronze.{table} "
        f"ADD IF NOT EXISTS PARTITION (dt = '{dt}') "
        f"LOCATION 's3://{bucket}/bronze/{table}/dt={dt}/'"
    )
    query_id = athena.start_query_execution(
        QueryString=ddl, WorkGroup=ATHENA_WORKGROUP
    )["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)[
            "QueryExecution"
        ]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "no reason given")
            raise RuntimeError(f"partition DDL {state}: {reason}")
        time.sleep(1)  # DDL completes in ~1-2s; the Lambda timeout is the backstop
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `.venv/bin/pytest tests/test_partitions.py -v`
Expected: 2 PASS.

- [ ] **Step 5: Refactor nft_contracts handler to use the shared module**

In `ingestion/nft_contracts/handler.py`:
- Delete the whole local `register_partition` function, the `ATHENA_WORKGROUP` constant, and the now-unused `import os`, `import time` lines.
- Add to the imports: `from ingestion.common.partitions import register_partition`.
- Change the call site inside `handler` from `register_partition(run_date, bucket)` to `register_partition("nft_contracts", run_date, bucket)`.

- [ ] **Step 6: Update nft tests for the new patch target**

In `tests/test_nft_contracts.py`:
- Delete `test_register_partition_raises_on_failed_ddl` (now covered by `tests/test_partitions.py`).
- In `test_handler_reads_event_and_writes_partition`, the Athena client is now created inside `ingestion.common.partitions`, so patch both modules. Replace the single `monkeypatch.setattr(h.boto3, "client", ...)` with:

```python
    import ingestion.common.partitions as p

    monkeypatch.setattr(h.boto3, "client", lambda service: FakeS3())
    monkeypatch.setattr(
        p.boto3, "client", lambda service: FakeAthena(queries)
    )
```

(The local `FakeAthena` class definition in this file stays; it is still used by this test.)

- [ ] **Step 7: Run the full suite**

Run: `.venv/bin/pytest tests/ -v`
Expected: all PASS (46 previous − 1 deleted + 2 new = 47), no warnings.

- [ ] **Step 8: Commit**

```bash
git add ingestion/common/ ingestion/nft_contracts/handler.py tests/test_partitions.py tests/test_nft_contracts.py
git commit -m "refactor: extract shared register_partition to ingestion/common"
```

---

### Task 2: dcl_contracts handler registers its partition, drops the projection date floor

**Files:**
- Modify: `ingestion/dcl_contracts/handler.py`
- Test: `tests/test_dcl_contracts.py`

**Interfaces:**
- Consumes: `register_partition(table, run_date, bucket)` from Task 1; `flatten`, `partition_key`, `validate_payload` from `ingestion/dcl_contracts/transform.py` (unchanged).
- Produces: handler behavior Task 3 deploys — after `put_object`, calls `register_partition("dcl_contracts", run_date, bucket)`; any `{"date": "YYYY-MM-DD"}` is accepted (no minimum).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_dcl_contracts.py` (FIXTURE is already defined at the top of the file):

```python
def test_handler_writes_snapshot_and_registers_partition(monkeypatch):
    import ingestion.common.partitions as p
    import ingestion.dcl_contracts.handler as h
    from tests.test_partitions import FakeAthena

    written = {}
    queries = []

    class FakeS3:
        def put_object(self, Bucket, Key, Body):
            written["bucket"], written["key"], written["body"] = Bucket, Key, Body

    monkeypatch.setenv("LAKE_BUCKET", "test-bucket")
    monkeypatch.setattr(h, "fetch", lambda: FIXTURE)
    monkeypatch.setattr(h.boto3, "client", lambda service: FakeS3())
    monkeypatch.setattr(p.boto3, "client", lambda service: FakeAthena(queries))

    # A date before 2026-08-01 must now be accepted: the projection range
    # floor is gone along with the projection itself.
    result = h.handler({"date": "2025-01-15"}, None)

    assert written["key"] == "bronze/dcl_contracts/dt=2025-01-15/contracts.parquet"
    assert result["s3_key"] == written["key"]
    assert len(queries) == 1
    ddl = queries[0]["ddl"]
    assert "ALTER TABLE bronze.dcl_contracts" in ddl
    assert "ADD IF NOT EXISTS PARTITION (dt = '2025-01-15')" in ddl
    assert "LOCATION 's3://test-bucket/bronze/dcl_contracts/dt=2025-01-15/'" in ddl
```

Note: `from tests.test_partitions import FakeAthena` requires pytest's rootdir on `sys.path`; the existing suite already imports `ingestion.*` the same way, so this works. If the import fails in your environment, copy the `FakeAthena` class into this file instead.

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/bin/pytest tests/test_dcl_contracts.py::test_handler_writes_snapshot_and_registers_partition -v`
Expected: FAIL with `ValueError: event 'date' 2025-01-15 predates the table's partition projection range (2026-08-01)`.

- [ ] **Step 3: Modify the handler**

In `ingestion/dcl_contracts/handler.py`:

Add to the imports:

```python
from ingestion.common.partitions import register_partition
```

Replace the date-validation block inside `handler` (the whole `if event.get("date"): ... else: ...`) with:

```python
    if event.get("date"):
        try:
            run_date = datetime.date.fromisoformat(event["date"])
        except ValueError:
            raise ValueError(f"event 'date' must be YYYY-MM-DD, got: {event['date']!r}")
    else:
        run_date = datetime.datetime.now(datetime.timezone.utc).date()
```

Replace the write block at the end with:

```python
    key = partition_key(run_date)
    bucket = os.environ["LAKE_BUCKET"]
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=rows_to_parquet(rows))
    register_partition("dcl_contracts", run_date, bucket)
    print(f"wrote {len(rows)} rows to s3://{bucket}/{key}")
    return {"rows": len(rows), "s3_key": key}
```

- [ ] **Step 4: Run the full suite**

Run: `.venv/bin/pytest tests/ -v`
Expected: 48 PASS (47 from Task 1 + 1 new).

- [ ] **Step 5: Commit**

```bash
git add ingestion/dcl_contracts/handler.py tests/test_dcl_contracts.py
git commit -m "feat: dcl_contracts registers partitions explicitly, any backfill date allowed"
```

---

### Task 3: Terraform — drop projection, extend IAM, remove EventBridge, package shared module

**Files:**
- Modify: `terraform/table_dcl_contracts.tf`
- Modify: `terraform/lambda_dcl_contracts.tf`
- Modify: `terraform/lambda_nft_contracts.tf`

**Interfaces:**
- Consumes: `aws_athena_workgroup.main`, `aws_glue_catalog_database.bronze` (in `terraform/glue_athena.tf`), `aws_s3_bucket.lake`, `data.aws_caller_identity.current`.
- Produces: deployable state for Task 4. Both Lambda zips now contain `ingestion/common/partitions.py`.

- [ ] **Step 1: Remove projection parameters from the Glue table**

In `terraform/table_dcl_contracts.tf`, replace the whole `parameters` block with:

```hcl
  # No partition projection here: the Lambda registers each partition
  # explicitly (ALTER TABLE ADD PARTITION) and the catalog lists exactly
  # the partitions that really exist — same pattern as nft_contracts.
  parameters = {
    "classification" = "parquet"
  }
```

(Keep `partition_keys` and the `storage_descriptor` untouched.)

- [ ] **Step 2: Add the shared module to BOTH Lambda zips**

In `terraform/lambda_dcl_contracts.tf`, inside `data "archive_file" "dcl_contracts_zip"`, add before the closing brace:

```hcl
  source {
    content  = file("${path.module}/../ingestion/common/partitions.py")
    filename = "ingestion/common/partitions.py"
  }
  source {
    content  = ""
    filename = "ingestion/common/__init__.py"
  }
```

In `terraform/lambda_nft_contracts.tf`, inside `data "archive_file" "nft_contracts_zip"`, add the same two `source` blocks.

- [ ] **Step 3: Extend the dcl Lambda IAM policy and env**

In `terraform/lambda_dcl_contracts.tf`, replace the single-statement `policy` of `aws_iam_role_policy.dcl_contracts_s3` with:

```hcl
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/dcl_contracts/*"
      },
      # Athena writes DDL query results with the caller's credentials
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${aws_s3_bucket.lake.arn}/athena-results/*"
      },
      # Run ALTER TABLE ADD PARTITION in the tagged workgroup
      {
        Effect   = "Allow"
        Action   = ["athena:StartQueryExecution", "athena:GetQueryExecution"]
        Resource = aws_athena_workgroup.main.arn
      },
      # Athena DDL resolves the table and creates the partition through Glue
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.bronze.arn,
          aws_glue_catalog_table.dcl_contracts.arn,
        ]
      }
    ]
  })
```

In `aws_lambda_function.dcl_contracts`, replace the `environment` block with:

```hcl
  environment {
    variables = {
      LAKE_BUCKET      = aws_s3_bucket.lake.bucket
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }
```

- [ ] **Step 4: Remove the EventBridge trigger**

In `terraform/lambda_dcl_contracts.tf`, delete these three whole resources:
- `aws_cloudwatch_event_rule.dcl_contracts_daily`
- `aws_cloudwatch_event_target.dcl_contracts`
- `aws_lambda_permission.dcl_contracts_events`

- [ ] **Step 5: Validate and read the plan**

Run: `cd terraform && terraform validate && terraform plan`
Expected: valid; plan shows exactly 4 changes and 3 destroys —
`aws_glue_catalog_table.dcl_contracts` update (parameters shrink),
`aws_iam_role_policy.dcl_contracts_s3` update,
`aws_lambda_function.dcl_contracts` update (env var + new zip hash),
`aws_lambda_function.nft_contracts` update (new zip hash only),
and the three EventBridge resources destroyed. Nothing added.

- [ ] **Step 6: Commit**

```bash
git add terraform/table_dcl_contracts.tf terraform/lambda_dcl_contracts.tf terraform/lambda_nft_contracts.tf
git commit -m "feat: dcl_contracts explicit partitions, drop EventBridge cron"
```

---

### Task 4: Deploy, migrate existing partitions, verify E2E

**Files:**
- None modified (operational task).

**Interfaces:**
- Consumes: everything from Tasks 1-3.
- Produces: `bronze.dcl_contracts` queryable with catalog-registered partitions only; Lambda with no trigger, invocable manually.

- [ ] **Step 1: Apply**

Run: `cd terraform && terraform apply -auto-approve`
Expected: `Apply complete! Resources: 0 added, 4 changed, 3 destroyed.`

- [ ] **Step 2: Register the snapshots already in S3 (one-time migration)**

Projection made existing snapshots queryable without catalog entries; removing it orphans them until registered. `MSCK REPAIR TABLE` discovers every `dt=` prefix in one shot:

```bash
aws athena start-query-execution \
  --work-group decentraland-data-platform \
  --query-string "MSCK REPAIR TABLE bronze.dcl_contracts"
```

Poll until SUCCEEDED: `aws athena get-query-execution --query-execution-id <id> --query 'QueryExecution.Status.State'`

- [ ] **Step 3: Verify the catalog lists every S3 snapshot**

Compare S3 reality vs catalog:

```bash
aws s3 ls s3://decentraland-data-platform-683569194224/bronze/dcl_contracts/
aws athena start-query-execution \
  --work-group decentraland-data-platform \
  --query-string 'SELECT * FROM bronze."dcl_contracts$partitions" ORDER BY dt'
```

Fetch with `aws athena get-query-results --query-execution-id <id>`.
Expected: one catalog row per `dt=` prefix in S3, no more, no less.

- [ ] **Step 4: E2E — the Lambda registers a partition on its own**

```bash
aws lambda invoke --function-name extract-dcl-contracts \
  --payload '{}' --cli-binary-format raw-in-base64-out /dev/stdout
```

Expected: `{"rows": <n>, "s3_key": "bronze/dcl_contracts/dt=<today>/contracts.parquet"}`, no function error. Re-run Step 3's `$partitions` query: today's dt appears exactly once (IF NOT EXISTS keeps the re-registration idempotent). Then confirm data reads through the catalog:

```bash
aws athena start-query-execution \
  --work-group decentraland-data-platform \
  --query-string "SELECT dt, count(*) AS rows FROM bronze.dcl_contracts GROUP BY dt ORDER BY dt DESC LIMIT 3"
```

Expected: non-zero counts including today.

- [ ] **Step 5: Confirm the trigger is gone**

```bash
aws events list-rules --name-prefix extract-dcl-contracts
```

Expected: empty `Rules` list. From today the snapshot only lands when invoked manually (or by Step Functions once phase 8 ships) — note this for daily freshness expectations.

---

### Task 5: Update docs to match

**Files:**
- Modify: `docs/superpowers/specs/2026-08-21-dcl-contracts-extraction.md` (lines 22, 70, 81)
- Modify: `CLAUDE.md` (line 69)

**Interfaces:**
- Consumes: nothing.
- Produces: docs consistent with deployed reality.

- [ ] **Step 1: Rewrite the three spec mentions**

Line 22: change `extract_dcl_contracts` Lambda, triggered daily by EventBridge:` to
`` `extract_dcl_contracts` Lambda, invoked manually (Step Functions will schedule it from phase 8):``

Line 70: change the sentence about projection to:
`partitions registered explicitly by the Lambda via ALTER TABLE ADD IF NOT EXISTS PARTITION in workgroup decentraland-data-platform (same pattern as nft_contracts; no crawler/MSCK needed in steady state).`

Line 81: change `- EventBridge rule: daily 06:00 UTC.` to
`- No EventBridge trigger: orchestration arrives with Step Functions (phase 8); until then, manual invokes.`

- [ ] **Step 2: Update CLAUDE.md workflow line**

Line 69: change `` `extract_dcl_contracts` (DONE — deployed, daily 06:00 UTC), 2) event-driven`` to
`` `extract_dcl_contracts` (DONE — deployed; manual invoke until Step Functions), 2) event-driven``

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-08-21-dcl-contracts-extraction.md CLAUDE.md
git commit -m "docs: dcl_contracts explicit partitions and no EventBridge trigger"
```

---

## Post-plan note

Per the project convention, these task commits are squashed into a single commit on `main` before pushing (push only with the user's explicit confirmation).
