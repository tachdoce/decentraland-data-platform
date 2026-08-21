# nft_contracts Explicit Partition Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Athena partition projection on `bronze.nft_contracts` with explicit `ALTER TABLE ADD PARTITION` executed by the `load-nft-contracts` Lambda after each snapshot write.

**Architecture:** The Glue table loses its `projection.*` parameters; the catalog becomes the source of truth for which partitions exist. After writing the parquet, the Lambda runs `ALTER TABLE ADD IF NOT EXISTS PARTITION` through the tagged Athena workgroup and polls until the DDL succeeds, failing loudly otherwise. IAM gains the minimal Athena + Glue + results-bucket permissions this requires. Existing S3 partitions written under projection are registered once as a migration step.

**Tech Stack:** Python 3.13 Lambda (boto3 `athena` client), Terraform (aws_glue_catalog_table, aws_iam_role_policy, aws_lambda_function environment), pytest with fake boto3 clients.

**Spec:** `docs/superpowers/specs/2026-08-21-nft-contracts-ingestion.md` (this plan amends its partition-discovery mechanism; Task 4 updates the spec text).

## Global Constraints

- Chat in Spanish; ALL artifacts in English (code, comments, commits, docs).
- Region `us-east-1`; bucket `decentraland-data-platform-${account_id}` — never hardcode the account id in Terraform (tests/CLI may use the literal `decentraland-data-platform-683569194224`).
- Partition key is `dt=YYYY-MM-DD`; never named `date`.
- Whole-file validation semantics unchanged: any CSV defect → ValueError → nothing written, no partition registered.
- Idempotency: re-uploading the CSV the same day must overwrite the parquet and leave the partition registered exactly once (`IF NOT EXISTS`).
- Cost tags unchanged: `local.nft_contracts_tags = { component = "ingestion-nft-contracts", layer = "bronze" }`; DDL runs in workgroup `decentraland-data-platform` so its (negligible) spend stays tagged.
- Run all tests with the project venv: `.venv/bin/pytest` (Python 3.13.13, matching the Lambda runtime).
- Terraform: always read `terraform plan` output before `apply`.

---

### Task 1: Lambda registers the partition via Athena DDL

**Files:**
- Modify: `ingestion/nft_contracts/handler.py`
- Test: `tests/test_nft_contracts.py`

**Interfaces:**
- Consumes: `parse_and_validate(csv_bytes) -> list[dict]` and `partition_key(run_date) -> str` from `ingestion/nft_contracts/validate.py` (unchanged).
- Produces: `register_partition(run_date: datetime.date, bucket: str) -> None` in `handler.py` — raises `RuntimeError` if the DDL ends `FAILED`/`CANCELLED`; module constant `ATHENA_WORKGROUP` read from env var `ATHENA_WORKGROUP` (default `"decentraland-data-platform"`). Task 2 wires that env var; Task 3 relies on the handler calling it after `put_object`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_nft_contracts.py`, replace the existing `test_handler_reads_event_and_writes_partition` with the block below (a shared `FakeAthena`, the updated handler test asserting the DDL, and a failure-path test):

```python
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


def test_handler_reads_event_and_writes_partition(monkeypatch, tmp_path):
    import ingestion.nft_contracts.handler as h

    written = {}
    queries = []

    class FakeS3:
        def get_object(self, Bucket, Key):
            import io as _io

            return {"Body": _io.BytesIO(FIXTURE_BYTES)}

        def put_object(self, Bucket, Key, Body):
            written["bucket"], written["key"], written["body"] = Bucket, Key, Body

    monkeypatch.setattr(
        h.boto3,
        "client",
        lambda service: FakeAthena(queries) if service == "athena" else FakeS3(),
    )
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": "landing/nft_contracts/nft_contracts.csv"},
                }
            }
        ]
    }
    result = h.handler(event, None)
    assert result["rows"] == 830
    assert written["bucket"] == "test-bucket"
    assert written["key"].startswith("bronze/nft_contracts/dt=")
    assert written["key"].endswith("/contracts.parquet")
    assert result["s3_key"] == written["key"]
    assert result["source_key"] == "landing/nft_contracts/nft_contracts.csv"
    assert len(written["body"]) > 1000  # real parquet bytes

    # The partition DDL ran, on the right table/path, in the tagged workgroup
    assert len(queries) == 1
    ddl = queries[0]["ddl"]
    assert "ALTER TABLE bronze.nft_contracts" in ddl
    assert "ADD IF NOT EXISTS PARTITION" in ddl
    dt = written["key"].split("dt=")[1].split("/")[0]
    assert f"(dt = '{dt}')" in ddl
    assert f"LOCATION 's3://test-bucket/bronze/nft_contracts/dt={dt}/'" in ddl
    assert queries[0]["workgroup"] == "decentraland-data-platform"


def test_register_partition_raises_on_failed_ddl(monkeypatch):
    import ingestion.nft_contracts.handler as h

    monkeypatch.setattr(
        h.boto3, "client", lambda service: FakeAthena([], state="FAILED")
    )
    with pytest.raises(RuntimeError, match="FAILED.*fake reason"):
        h.register_partition(datetime.date(2026, 8, 21), "test-bucket")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_nft_contracts.py -v -k "handler or register"`
Expected: `test_handler_reads_event_and_writes_partition` FAILS (FakeAthena recorded no query — `len(queries) == 1` assertion, or earlier `AttributeError` if the S3 fake receives athena calls) and `test_register_partition_raises_on_failed_ddl` FAILS with `AttributeError: ... has no attribute 'register_partition'`.

- [ ] **Step 3: Implement register_partition in the handler**

In `ingestion/nft_contracts/handler.py`, extend the imports and add the function:

```python
import datetime
import io
import os
import time
import urllib.parse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.nft_contracts.validate import parse_and_validate, partition_key

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")
```

Above `handler(...)`:

```python
def register_partition(run_date: datetime.date, bucket: str) -> None:
    """Register the snapshot partition in the Glue catalog via Athena DDL.

    The table has no partition projection, so each dt must be added
    explicitly. IF NOT EXISTS keeps same-day re-uploads idempotent.
    """
    athena = boto3.client("athena")
    dt = run_date.isoformat()
    ddl = (
        "ALTER TABLE bronze.nft_contracts "
        f"ADD IF NOT EXISTS PARTITION (dt = '{dt}') "
        f"LOCATION 's3://{bucket}/bronze/nft_contracts/dt={dt}/'"
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

Inside `handler`, right after the `s3.put_object(...)` line, add:

```python
        register_partition(run_date, bucket)
```

- [ ] **Step 4: Run the full suite to verify everything passes**

Run: `.venv/bin/pytest tests/ -v`
Expected: 33 tests PASS (32 previous + 1 new; the handler test was replaced in place). Note: with `state="SUCCEEDED"` the fake never sleeps, so the suite stays fast.

- [ ] **Step 5: Commit**

```bash
git add ingestion/nft_contracts/handler.py tests/test_nft_contracts.py
git commit -m "feat: register nft_contracts partition via ALTER TABLE ADD PARTITION"
```

---

### Task 2: Terraform — drop projection, grant Athena/Glue IAM, set env var

**Files:**
- Modify: `terraform/table_nft_contracts.tf`
- Modify: `terraform/lambda_nft_contracts.tf`

**Interfaces:**
- Consumes: `aws_athena_workgroup.main` and `aws_glue_catalog_database.bronze` (defined in `terraform/glue_athena.tf`), `aws_s3_bucket.lake`, `data.aws_caller_identity.current` (defined in `terraform/main.tf`).
- Produces: Lambda env var `ATHENA_WORKGROUP` (consumed by Task 1's code); IAM allowing the DDL. Task 3 applies this.

- [ ] **Step 1: Remove projection parameters from the Glue table**

In `terraform/table_nft_contracts.tf`, replace the whole `parameters` block with:

```hcl
  # No partition projection here: snapshots are infrequent, so the Lambda
  # registers each partition explicitly (ALTER TABLE ADD PARTITION) and the
  # catalog lists exactly the partitions that really exist.
  parameters = {
    "classification" = "parquet"
  }
```

(Keep `partition_keys` and the `storage_descriptor` untouched.)

- [ ] **Step 2: Extend the Lambda IAM policy**

In `terraform/lambda_nft_contracts.tf`, inside `aws_iam_role_policy.nft_contracts_s3`, append after the existing `s3:PutObject` statement (note the comma joining statements):

```hcl
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
          aws_glue_catalog_table.nft_contracts.arn,
        ]
      }
```

- [ ] **Step 3: Add the environment variable to the Lambda**

In `aws_lambda_function.nft_contracts`, after the `layers` line, add:

```hcl
  environment {
    variables = {
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }
```

- [ ] **Step 4: Validate and read the plan**

Run: `cd terraform && terraform validate && terraform plan`
Expected: valid; plan shows exactly 3 changes — `aws_glue_catalog_table.nft_contracts` update in-place (parameters shrink), `aws_iam_role_policy.nft_contracts_s3` update in-place, `aws_lambda_function.nft_contracts` update in-place (env var + new source hash from Task 1's code). Nothing destroyed.

- [ ] **Step 5: Commit**

```bash
git add terraform/table_nft_contracts.tf terraform/lambda_nft_contracts.tf
git commit -m "feat: explicit partition IAM and env for load-nft-contracts; drop projection"
```

---

### Task 3: Deploy, migrate existing partitions, verify E2E

**Files:**
- None modified (operational task).

**Interfaces:**
- Consumes: everything from Tasks 1-2 deployed.
- Produces: `bronze.nft_contracts` queryable in Athena with catalog-registered partitions only.

- [ ] **Step 1: Apply**

Run: `cd terraform && terraform apply -auto-approve`
Expected: `Apply complete! Resources: 0 added, 3 changed, 0 destroyed.`

- [ ] **Step 2: Register the partitions already in S3 (one-time migration)**

Projection made existing snapshots queryable without catalog entries; removing it orphans them until registered. List what exists:

```bash
aws s3 ls s3://decentraland-data-platform-683569194224/bronze/nft_contracts/
```

For EACH `dt=YYYY-MM-DD/` prefix listed, run (substitute the date, twice per command):

```bash
aws athena start-query-execution \
  --work-group decentraland-data-platform \
  --query-string "ALTER TABLE bronze.nft_contracts ADD IF NOT EXISTS PARTITION (dt = 'YYYY-MM-DD') LOCATION 's3://decentraland-data-platform-683569194224/bronze/nft_contracts/dt=YYYY-MM-DD/'"
```

- [ ] **Step 3: Verify the catalog now lists the partitions**

```bash
aws athena start-query-execution \
  --work-group decentraland-data-platform \
  --query-string 'SELECT * FROM bronze."nft_contracts$partitions" ORDER BY dt'
```

Then fetch results with `aws athena get-query-results --query-execution-id <id>`.
Expected: one row per `dt=` prefix found in Step 2.

- [ ] **Step 4: E2E — the Lambda registers a partition on its own**

Re-invoke the deployed Lambda with a synthetic S3 event pointing at the CSV already sitting in landing:

```bash
aws lambda invoke --function-name load-nft-contracts \
  --payload '{"Records":[{"s3":{"bucket":{"name":"decentraland-data-platform-683569194224"},"object":{"key":"landing/nft_contracts/nft_contracts.csv"}}}]}' \
  --cli-binary-format raw-in-base64-out /dev/stdout
```

Expected: `{"rows": 830, "s3_key": "bronze/nft_contracts/dt=<today>/contracts.parquet", ...}` and no function error. Then repeat Step 3's `$partitions` query: today's dt appears exactly once (idempotent even though Step 2 may have already added it). Finally confirm data reads through the catalog path:

```bash
aws athena start-query-execution \
  --work-group decentraland-data-platform \
  --query-string "SELECT dt, count(*) AS rows FROM bronze.nft_contracts GROUP BY dt ORDER BY dt"
```

Expected: 830 rows for each registered dt.

---

### Task 4: Update the spec to match the new mechanism

**Files:**
- Modify: `docs/superpowers/specs/2026-08-21-nft-contracts-ingestion.md` (lines mentioning partition projection, currently 34 and 81)

**Interfaces:**
- Consumes: nothing.
- Produces: spec consistent with deployed reality.

- [ ] **Step 1: Rewrite the two projection mentions**

Line 34: change
`Glue table bronze.nft_contracts (partition projection on dt, like dcl_contracts)` to
`Glue table bronze.nft_contracts (no partition projection: the Lambda runs ALTER TABLE ADD IF NOT EXISTS PARTITION after each snapshot, since updates are infrequent and the catalog should list only real partitions)`.

Line 81: change
`- Glue table `bronze.nft_contracts` with dt partition projection` to
`- Glue table `bronze.nft_contracts`; partitions registered explicitly by the Lambda via Athena DDL in workgroup decentraland-data-platform`.

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/specs/2026-08-21-nft-contracts-ingestion.md
git commit -m "docs: nft_contracts spec reflects explicit partition registration"
```

---

## Post-plan note

Per the project convention, these task commits are squashed into a single commit on `main` before pushing (same as Phases 1-2).
