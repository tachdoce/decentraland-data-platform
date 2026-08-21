# dcl_contracts Extraction (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy the base data-platform infrastructure (S3 bucket, Glue catalog, Athena workgroup) plus the `extract-dcl-contracts` Lambda that snapshots Decentraland's contract registry into `bronze.dcl_contracts` daily.

**Architecture:** A zip-packaged Python 3.13 Lambda fetches `addresses.json` with stdlib `urllib`, validates and flattens it with pure functions (unit-tested without mocks), and writes one parquet per day to `bronze/dcl_contracts/dt=YYYY-MM-DD/` using pyarrow from the public AWS SDK Pandas layer. A Glue table with partition projection makes it instantly queryable from Athena — no crawlers.

**Tech Stack:** Terraform (~> 6.0 AWS provider), Python 3.13, pyarrow (via AWSSDKPandas Lambda layer), boto3, pytest, Athena/Glue.

**Spec:** `docs/superpowers/specs/2026-08-21-dcl-contracts-extraction.md` (component) and `docs/superpowers/specs/2026-08-20-platform-architecture.md` (conventions §3, layers §4, ops §8, FinOps §9).

## Global Constraints

- Region: `us-east-1`. All resources via Terraform; never the console.
- Bucket name: `decentraland-data-platform-${account_id}` — account id interpolated via `data.aws_caller_identity`, never hardcoded.
- Partition convention: `dt=YYYY-MM-DD`. Never name a column/partition `date` or `timestamp`.
- Chain ids: mainnet=1, matic=137. Addresses lowercased, names trimmed, at write time.
- Tags: provider `default_tags` `{project="decentraland-data-platform", managed_by="terraform"}`; this component adds `{component="ingestion-dcl-contracts", layer="bronze"}`. Shared/base resources use `{component="platform", layer="infra"}`.
- IAM: the Lambda role may write ONLY `bronze/dcl_contracts/*` in the bucket.
- CloudWatch log retention: 7 days.
- All repo artifacts (code, commits, docs) in English.

---

### Task 1: Terraform base — provider, bucket, Glue database, Athena workgroup

**Files:**
- Create: `terraform/main.tf`
- Create: `terraform/s3.tf`
- Create: `terraform/glue_athena.tf`
- Create: `terraform/outputs.tf`

**Interfaces:**
- Produces: `aws_s3_bucket.lake` (referenced by every later component), `local.bucket_name`, Glue database `bronze`, Athena workgroup `decentraland-data-platform`, output `lake_bucket_name`.

- [ ] **Step 1: Write `terraform/main.tf`**

```hcl
terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
  default_tags {
    tags = {
      project    = "decentraland-data-platform"
      managed_by = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  bucket_name = "decentraland-data-platform-${data.aws_caller_identity.current.account_id}"
  base_tags   = { component = "platform", layer = "infra" }
}
```

- [ ] **Step 2: Write `terraform/s3.tf`**

```hcl
resource "aws_s3_bucket" "lake" {
  bucket = local.bucket_name
  tags   = local.base_tags
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
```

- [ ] **Step 3: Write `terraform/glue_athena.tf`**

```hcl
resource "aws_glue_catalog_database" "bronze" {
  name = "bronze"
  tags = local.base_tags
}

resource "aws_athena_workgroup" "main" {
  name = "decentraland-data-platform"
  tags = local.base_tags

  configuration {
    bytes_scanned_cutoff_per_query = 1073741824 # 1 GB cost safety net
    result_configuration {
      output_location = "s3://${local.bucket_name}/athena-results/"
    }
  }
}
```

- [ ] **Step 4: Write `terraform/outputs.tf`**

```hcl
output "lake_bucket_name" {
  value = aws_s3_bucket.lake.bucket
}
```

- [ ] **Step 5: Validate and review the plan**

Run: `cd terraform && terraform init && terraform validate && terraform plan`
Expected: valid; plan shows 4 resources to add (bucket, public access block, glue database, workgroup), 0 to change/destroy.

- [ ] **Step 6: Apply**

Run: `terraform apply -auto-approve`
Expected: `Apply complete! Resources: 4 added`.

- [ ] **Step 7: Verify**

Run: `aws s3 ls | grep decentraland && aws glue get-database --name bronze --query Database.Name && aws athena get-work-group --work-group decentraland-data-platform --query WorkGroup.Name`
Expected: bucket listed; `"bronze"`; `"decentraland-data-platform"`.

- [ ] **Step 8: Commit**

```bash
git add terraform/
git commit -m "feat: base infra — lake bucket, bronze Glue database, Athena workgroup"
```

---

### Task 2: Glue table `bronze.dcl_contracts` with partition projection

**Files:**
- Create: `terraform/table_dcl_contracts.tf`

**Interfaces:**
- Consumes: `aws_glue_catalog_database.bronze`, `local.bucket_name` (Task 1).
- Produces: Athena-queryable table `bronze.dcl_contracts` over `s3://<bucket>/bronze/dcl_contracts/` partitioned by `dt`.

- [ ] **Step 1: Write `terraform/table_dcl_contracts.tf`**

```hcl
resource "aws_glue_catalog_table" "dcl_contracts" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "dcl_contracts"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "projection.enabled"  = "true"
    "projection.dt.type"  = "date"
    "projection.dt.format" = "yyyy-MM-dd"
    "projection.dt.range" = "2026-08-01,NOW"
    "storage.location.template" = "s3://${local.bucket_name}/bronze/dcl_contracts/dt=$${dt}/"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/bronze/dcl_contracts/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "chain_id"
      type = "int"
    }
    columns {
      name = "contract_address"
      type = "string"
    }
    columns {
      name = "contract_name"
      type = "string"
    }
  }
}
```

Note: `$${dt}` is Terraform escaping for the literal `${dt}` Athena expects.

- [ ] **Step 2: Plan and apply**

Run: `terraform plan` then `terraform apply -auto-approve`
Expected: 1 to add.

- [ ] **Step 3: Verify the table answers (empty) queries**

Run:
```bash
qid=$(aws athena start-query-execution \
  --work-group decentraland-data-platform \
  --query-string "SELECT count(*) FROM bronze.dcl_contracts" \
  --query QueryExecutionId --output text)
sleep 5
aws athena get-query-results --query-execution-id "$qid" \
  --query 'ResultSet.Rows[1].Data[0].VarCharValue'
```
Expected: `"0"` (table exists, no data yet).

- [ ] **Step 4: Commit**

```bash
git add terraform/table_dcl_contracts.tf
git commit -m "feat: bronze.dcl_contracts Glue table with dt partition projection"
```

---

### Task 3: Transform module (pure functions, TDD)

**Files:**
- Create: `tests/fixtures/addresses.json`
- Create: `tests/test_dcl_contracts.py`
- Create: `ingestion/dcl_contracts/transform.py`
- Create: `requirements-dev.txt`

**Interfaces:**
- Produces (used by Task 4's handler):
  - `transform.CHAIN_IDS: dict[str, int]` — `{"mainnet": 1, "matic": 137}`
  - `transform.validate_payload(data: dict) -> None` — raises `ValueError` on empty/malformed payloads
  - `transform.flatten(data: dict) -> list[dict]` — rows with keys `chain_id`, `contract_address`, `contract_name`
  - `transform.partition_key(run_date: datetime.date) -> str` — `"bronze/dcl_contracts/dt=YYYY-MM-DD/contracts.parquet"`

- [ ] **Step 1: Capture the fixture and dev requirements**

```bash
mkdir -p tests/fixtures ingestion/dcl_contracts
curl -s https://contracts.decentraland.org/addresses.json -o tests/fixtures/addresses.json
grep -c 'CollectionManager ' tests/fixtures/addresses.json
printf 'pytest==8.*\npyarrow==17.*\nboto3==1.*\nruff==0.*\n' > requirements-dev.txt
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
```
Expected: grep prints `1` (the trailing-space quirk is present; if the source fixed it, note it and drop that assertion in Step 2).

- [ ] **Step 2: Write the failing tests**

`tests/test_dcl_contracts.py`:

```python
import datetime
import json
from pathlib import Path

import pytest

from ingestion.dcl_contracts.transform import (
    CHAIN_IDS,
    flatten,
    partition_key,
    validate_payload,
)

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "addresses.json").read_text()
)


def test_flatten_keeps_only_mainnet_and_matic():
    rows = flatten(FIXTURE)
    assert set(r["chain_id"] for r in rows) == {1, 137}
    assert len(rows) == len(FIXTURE["mainnet"]) + len(FIXTURE["matic"])


def test_flatten_lowercases_addresses():
    rows = flatten(FIXTURE)
    assert all(r["contract_address"] == r["contract_address"].lower() for r in rows)
    # EstateRegistry comes checksum-cased from the source
    estate = next(r for r in rows if r["contract_name"] == "EstateRegistry")
    assert estate["contract_address"] == "0x52bf3100f4a9337685301614275c85afe28401fc"


def test_flatten_trims_contract_names():
    rows = flatten(FIXTURE)
    assert all(r["contract_name"] == r["contract_name"].strip() for r in rows)
    # The source has "CollectionManager " with a trailing space on matic
    assert any(
        r["contract_name"] == "CollectionManager" and r["chain_id"] == 137
        for r in rows
    )


def test_cross_chain_duplicate_addresses_survive():
    rows = flatten(FIXTURE)
    dup = "0x480a0f4e360e8964e68858dd231c2922f1df45ef"
    matches = [r for r in rows if r["contract_address"] == dup]
    assert {(r["chain_id"], r["contract_name"]) for r in matches} == {
        (1, "TechTribalMarc0matic"),
        (137, "MarketplaceV2"),
    }


@pytest.mark.parametrize(
    "payload",
    [
        {},                                      # empty
        {"mainnet": FIXTURE["mainnet"]},         # missing matic
        {"mainnet": {}, "matic": FIXTURE["matic"]},  # empty network
    ],
)
def test_validate_payload_raises(payload):
    with pytest.raises(ValueError):
        validate_payload(payload)


def test_validate_payload_accepts_fixture():
    validate_payload(FIXTURE)  # must not raise


def test_partition_key_format():
    key = partition_key(datetime.date(2026, 8, 21))
    assert key == "bronze/dcl_contracts/dt=2026-08-21/contracts.parquet"


def test_chain_ids_mapping():
    assert CHAIN_IDS == {"mainnet": 1, "matic": 137}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_dcl_contracts.py -v`
Expected: collection error — `ModuleNotFoundError: ingestion.dcl_contracts.transform`. Add empty `__init__.py` files are NOT needed (pytest rootdir import works with `ingestion/dcl_contracts/__init__.py` absent only if using src layout); create `ingestion/__init__.py` and `ingestion/dcl_contracts/__init__.py` as empty files so the import path resolves:

```bash
touch ingestion/__init__.py ingestion/dcl_contracts/__init__.py
```

- [ ] **Step 4: Write the implementation**

`ingestion/dcl_contracts/transform.py`:

```python
"""Pure transformation logic for the Decentraland contract registry snapshot.

No AWS or network calls here — everything is unit-testable without mocks.
"""

import datetime

CHAIN_IDS = {"mainnet": 1, "matic": 137}


def validate_payload(data: dict) -> None:
    """Reject payloads that must never become a snapshot."""
    if not isinstance(data, dict) or not data:
        raise ValueError("addresses.json payload is empty or not an object")
    for network in CHAIN_IDS:
        contracts = data.get(network)
        if not isinstance(contracts, dict) or not contracts:
            raise ValueError(f"network '{network}' is missing or has no contracts")
        bad = [n for n, a in contracts.items() if not str(a).startswith("0x")]
        if bad:
            raise ValueError(f"network '{network}' has non-address values: {bad[:3]}")


def flatten(data: dict) -> list[dict]:
    """{network: {name: address}} -> rows keyed by (chain_id, contract_address)."""
    rows = []
    for network, chain_id in CHAIN_IDS.items():
        for name, address in data[network].items():
            rows.append(
                {
                    "chain_id": chain_id,
                    "contract_address": address.lower(),
                    "contract_name": name.strip(),
                }
            )
    return rows


def partition_key(run_date: datetime.date) -> str:
    return f"bronze/dcl_contracts/dt={run_date.isoformat()}/contracts.parquet"
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_dcl_contracts.py -v`
Expected: 9 passed.

- [ ] **Step 6: Update .gitignore and commit**

Ensure `.venv/` is covered by the existing `.gitignore` (it lists `venv/` and `.venv/`; verify with `git status` — no venv files staged).

```bash
git add tests/ ingestion/ requirements-dev.txt
git commit -m "feat: dcl_contracts transform module with fixture-based tests"
```

---

### Task 4: Lambda handler

**Files:**
- Create: `ingestion/dcl_contracts/handler.py`
- Create: `ingestion/dcl_contracts/requirements.txt`
- Test: `tests/test_dcl_contracts.py` (append one test)

**Interfaces:**
- Consumes: `transform.validate_payload`, `transform.flatten`, `transform.partition_key` (Task 3).
- Produces: `handler.handler(event, context) -> dict` with keys `rows`, `s3_key`; reads env var `LAKE_BUCKET`; optional event field `date` (`"YYYY-MM-DD"`) for re-runs. Terraform (Task 5) points at `handler.handler`.

- [ ] **Step 1: Write the failing test for parquet serialization**

Append to `tests/test_dcl_contracts.py`:

```python
def test_rows_to_parquet_roundtrip(tmp_path):
    import pyarrow.parquet as pq

    from ingestion.dcl_contracts.handler import rows_to_parquet

    rows = flatten(FIXTURE)
    buf = rows_to_parquet(rows)
    out = tmp_path / "contracts.parquet"
    out.write_bytes(buf)
    table = pq.read_table(out)
    assert table.column_names == ["chain_id", "contract_name", "contract_address"] or \
        set(table.column_names) == {"chain_id", "contract_address", "contract_name"}
    assert table.num_rows == len(rows)
    assert table.schema.field("chain_id").type == "int32"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_dcl_contracts.py::test_rows_to_parquet_roundtrip -v`
Expected: FAIL — `ImportError: cannot import name 'rows_to_parquet'`.

- [ ] **Step 3: Write the handler**

`ingestion/dcl_contracts/handler.py`:

```python
"""extract-dcl-contracts Lambda: addresses.json -> bronze/dcl_contracts snapshot."""

import datetime
import io
import json
import os
import urllib.request

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.dcl_contracts.transform import flatten, partition_key, validate_payload

ADDRESSES_URL = "https://contracts.decentraland.org/addresses.json"

SCHEMA = pa.schema(
    [
        ("chain_id", pa.int32()),
        ("contract_address", pa.string()),
        ("contract_name", pa.string()),
    ]
)


def fetch(url: str = ADDRESSES_URL) -> dict:
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read())


def rows_to_parquet(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def handler(event, context):
    event = event or {}
    run_date = (
        datetime.date.fromisoformat(event["date"])
        if event.get("date")
        else datetime.datetime.now(datetime.timezone.utc).date()
    )

    data = fetch()
    validate_payload(data)
    rows = flatten(data)

    key = partition_key(run_date)
    boto3.client("s3").put_object(
        Bucket=os.environ["LAKE_BUCKET"], Key=key, Body=rows_to_parquet(rows)
    )
    print(f"wrote {len(rows)} rows to s3://{os.environ['LAKE_BUCKET']}/{key}")
    return {"rows": len(rows), "s3_key": key}
```

`ingestion/dcl_contracts/requirements.txt` (documentation of runtime deps — pyarrow and boto3 come from the Lambda layer/runtime, so the deploy zip carries no third-party packages):

```
# provided by AWSSDKPandas Lambda layer: pyarrow
# provided by Lambda runtime: boto3
```

- [ ] **Step 4: Run all tests**

Run: `.venv/bin/pytest tests/test_dcl_contracts.py -v`
Expected: 10 passed.

- [ ] **Step 5: Commit**

```bash
git add ingestion/dcl_contracts/ tests/test_dcl_contracts.py
git commit -m "feat: dcl_contracts Lambda handler with parquet serialization"
```

---

### Task 5: Terraform for the Lambda (role, function, layer, schedule, logs)

**Files:**
- Create: `terraform/lambda_dcl_contracts.tf`

**Interfaces:**
- Consumes: `aws_s3_bucket.lake`, `local.bucket_name` (Task 1); `ingestion/dcl_contracts/*.py` (Tasks 3-4).
- Produces: Lambda `extract-dcl-contracts` with env `LAKE_BUCKET`, daily EventBridge rule.

- [ ] **Step 1: Resolve the current AWSSDKPandas layer version**

Run: `aws lambda list-layer-versions --layer-name AWSSDKPandas-Python313 --region us-east-1 --query 'LayerVersions[0].LayerVersionArn' --output text`
Expected: an ARN like `arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python313:<N>`. Use it in Step 2. If the Python313 layer does not exist yet, fall back to `AWSSDKPandas-Python312` AND change the function runtime to `python3.12` (the handler code is version-agnostic).

- [ ] **Step 2: Write `terraform/lambda_dcl_contracts.tf`**

```hcl
locals {
  dcl_contracts_tags = { component = "ingestion-dcl-contracts", layer = "bronze" }
  # From Task 5 Step 1:
  sdk_pandas_layer_arn = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python313:REPLACE_ME"
}

data "archive_file" "dcl_contracts_zip" {
  type        = "zip"
  output_path = "${path.module}/build/dcl_contracts.zip"

  source {
    content  = file("${path.module}/../ingestion/dcl_contracts/handler.py")
    filename = "ingestion/dcl_contracts/handler.py"
  }
  source {
    content  = file("${path.module}/../ingestion/dcl_contracts/transform.py")
    filename = "ingestion/dcl_contracts/transform.py"
  }
  source {
    content  = ""
    filename = "ingestion/__init__.py"
  }
  source {
    content  = ""
    filename = "ingestion/dcl_contracts/__init__.py"
  }
}

resource "aws_iam_role" "dcl_contracts" {
  name = "extract-dcl-contracts-role"
  tags = local.dcl_contracts_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "dcl_contracts_s3" {
  name = "write-bronze-dcl-contracts"
  role = aws_iam_role.dcl_contracts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "s3:PutObject"
      Resource = "${aws_s3_bucket.lake.arn}/bronze/dcl_contracts/*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "dcl_contracts_logs" {
  role       = aws_iam_role.dcl_contracts.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "dcl_contracts" {
  name              = "/aws/lambda/extract-dcl-contracts"
  retention_in_days = 7
  tags              = local.dcl_contracts_tags
}

resource "aws_lambda_function" "dcl_contracts" {
  function_name = "extract-dcl-contracts"
  role          = aws_iam_role.dcl_contracts.arn
  tags          = local.dcl_contracts_tags

  filename         = data.archive_file.dcl_contracts_zip.output_path
  source_code_hash = data.archive_file.dcl_contracts_zip.output_base64sha256

  handler = "ingestion.dcl_contracts.handler.handler"
  runtime = "python3.13"
  timeout = 60
  memory_size = 512
  layers  = [local.sdk_pandas_layer_arn]

  environment {
    variables = { LAKE_BUCKET = aws_s3_bucket.lake.bucket }
  }

  depends_on = [aws_cloudwatch_log_group.dcl_contracts]
}

resource "aws_cloudwatch_event_rule" "dcl_contracts_daily" {
  name                = "extract-dcl-contracts-daily"
  schedule_expression = "cron(0 6 * * ? *)"
  tags                = local.dcl_contracts_tags
}

resource "aws_cloudwatch_event_target" "dcl_contracts" {
  rule = aws_cloudwatch_event_rule.dcl_contracts_daily.name
  arn  = aws_lambda_function.dcl_contracts.arn
}

resource "aws_lambda_permission" "dcl_contracts_events" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.dcl_contracts.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.dcl_contracts_daily.arn
}
```

Replace `REPLACE_ME` with the version number from Step 1.

- [ ] **Step 3: Plan, review, apply**

Run: `cd terraform && terraform plan`
Expected: 8 to add (role, 2 policies/attachments, log group, function, rule, target, permission). Review that the IAM policy resource is scoped to `bronze/dcl_contracts/*`.
Run: `terraform apply -auto-approve`
Expected: `Apply complete!`

- [ ] **Step 4: Commit**

```bash
git add terraform/lambda_dcl_contracts.tf
git commit -m "feat: extract-dcl-contracts Lambda, scoped IAM, daily EventBridge schedule"
```

---

### Task 6: End-to-end verification (acceptance criteria from the spec)

**Files:** none (verification only; fixes loop back to earlier tasks).

- [ ] **Step 1: Invoke manually**

```bash
aws lambda invoke --function-name extract-dcl-contracts \
  --cli-binary-format raw-in-base64-out --payload '{}' /tmp/out.json
cat /tmp/out.json
```
Expected: `{"rows": 142, "s3_key": "bronze/dcl_contracts/dt=<today>/contracts.parquet"}` (row count ± source changes; StatusCode 200, no `FunctionError`).

- [ ] **Step 2: Verify idempotency**

Invoke again with the same command; then:
```bash
aws s3 ls s3://$(cd terraform && terraform output -raw lake_bucket_name)/bronze/dcl_contracts/ --recursive
```
Expected: exactly ONE object for today's `dt=` partition (overwritten, not duplicated).

- [ ] **Step 3: Verify from Athena (spec acceptance query)**

```bash
qid=$(aws athena start-query-execution \
  --work-group decentraland-data-platform \
  --query-string "SELECT chain_id, count(*) AS contracts FROM bronze.dcl_contracts WHERE dt = '$(date -u +%F)' GROUP BY chain_id ORDER BY chain_id" \
  --query QueryExecutionId --output text)
sleep 5
aws athena get-query-results --query-execution-id "$qid" \
  --query 'ResultSet.Rows[*].Data[*].VarCharValue'
```
Expected: rows `1 → 108` and `137 → 34` (± source changes).

- [ ] **Step 4: Verify logs and re-run with explicit date**

```bash
aws logs tail /aws/lambda/extract-dcl-contracts --since 10m | head -5
aws lambda invoke --function-name extract-dcl-contracts \
  --cli-binary-format raw-in-base64-out --payload '{"date": "2026-08-20"}' /tmp/out2.json
cat /tmp/out2.json
```
Expected: log line `wrote 142 rows to s3://...`; second invoke writes `dt=2026-08-20` partition (backfill path works).

- [ ] **Step 5: Activate cost allocation tags (one-time, console)**

Manual step for the user: Billing console → Cost allocation tags → activate `project`, `component`, `layer`, `managed_by`. Takes up to 24 h to appear in Cost Explorer. (This cannot be done by Terraform for user-defined tags on first activation.)

- [ ] **Step 6: Push**

```bash
git push
```

---

## Self-Review

- **Spec coverage**: behavior 1-4 → Tasks 3-4; table schema/projection → Task 2; base infra + tagging → Task 1; component infra/IAM/schedule/retention → Task 5; all 6 spec tests → Task 3 Step 2 (tests 1-5) and Task 3 `test_partition_key_format` (test 6); acceptance criteria → Task 6. Downstream silver models are explicitly out of this phase.
- **Placeholders**: `REPLACE_ME` in Task 5 is intentional and resolved by its Step 1 command; no other placeholders.
- **Type consistency**: `flatten` returns `list[dict]` consumed by `rows_to_parquet`; `partition_key` returns the S3 key consumed in `handler`; Terraform handler string `ingestion.dcl_contracts.handler.handler` matches the zip layout in `archive_file`.
