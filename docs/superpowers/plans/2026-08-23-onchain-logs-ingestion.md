# onchain-logs Ingestion (Phase 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One multi-chain container-image Lambda `extract-onchain-logs` that, per invocation, pulls one day of Decentraland-related logs for one chain from the BigQuery public blockchain datasets into `bronze.ethereum_logs` / `bronze.polygon_logs`.

**Architecture:** The Lambda validates `{chain_id, date?}`, fetches the chain's contract addresses from `silver.dim_contracts` via Athena, reads the GCP key from SSM, runs a two-step parameterized BigQuery query (transactions touching those contracts → all logs of those transactions), writes ONE timestamped parquet (append-only, never deletes) to the chain's bronze table partition, and registers the `dt` partition via the shared `register_partition` helper. Pure logic lives in `extract.py` (TDD, no clients); orchestration in `handler.py`.

**Tech Stack:** Python 3.13 container image (`google-cloud-bigquery`, `pyarrow`, boto3 from base), Terraform (ECR, Glue tables, IAM, Lambda), pytest with fake boto3 clients (repo pattern from `tests/test_partitions.py`).

**Spec:** `docs/superpowers/specs/2026-08-23-onchain-logs-ingestion.md`

## Global Constraints

- Chat in Spanish; ALL artifacts in English (code, comments, commits, docs).
- Region `us-east-1`. Bucket `decentraland-data-platform-${account_id}` via `local.bucket_name` in Terraform — never hardcode the account id in HCL (tests/CLI may use the literal `decentraland-data-platform-683569194224`).
- Partition key `dt=YYYY-MM-DD`; never named `date`.
- Datasets: `1 → goog_blockchain_ethereum_mainnet_us`, `137 → goog_blockchain_polygon_mainnet_us`; bronze tables `1 → ethereum_logs`, `137 → polygon_logs`.
- `chain_id` REQUIRED (only 1 or 137); `date` optional, default UTC today − 2 days.
- Append-only: parquet filename `YYYY-MM-DD_HH-MM-SS.parquet` (dashes, from `extracted_at`); the Lambda has NO `s3:DeleteObject` anywhere.
- `extracted_at` (UTC timestamp) is both the filename stamp and a column on every row.
- BigQuery: query parameters only (never string-interpolate the date or addresses into SQL); `maximum_bytes_billed = 50 GB`; log the rendered SQL and `total_bytes_processed` every run.
- Empty contract list from Athena → raise. Zero BigQuery rows → valid, write empty parquet with full schema.
- Sizing: timeout 900 s, memory 2048 MB, arm64. Tags `{component = "ingestion-onchain", layer = "bronze"}`.
- GCP key: SSM SecureString `/decentraland/gcp/bq-service-account-key` (exists; created manually, NOT managed by Terraform — Terraform only references the ARN).
- Explicit partition registration through workgroup `decentraland-data-platform`; Glue tables have NO partition projection.
- Run tests with the project venv from repo root: `.venv/bin/pytest` (Python 3.13, matches the Lambda runtime).
- Terraform: `terraform fmt`/`validate` locally; the USER runs `terraform plan`/`apply` at the marked checkpoints (plan/apply are blocked for the implementer). Always read the plan before apply.
- Git: commit locally at each task end; NEVER `git push` without explicit user confirmation.

---

### Task 1: Pure extraction logic (`extract.py`, TDD)

**Files:**
- Create: `ingestion/onchain/__init__.py` (empty)
- Create: `ingestion/onchain/extract.py`
- Test: `tests/test_onchain.py`

**Interfaces:**
- Consumes: nothing (stdlib + pyarrow only — no boto3, no google-cloud).
- Produces (used by Task 2's handler):
  - `CHAINS: dict[int, dict]` — `{1: {"dataset": "goog_blockchain_ethereum_mainnet_us", "table": "ethereum_logs"}, 137: {"dataset": "goog_blockchain_polygon_mainnet_us", "table": "polygon_logs"}}`
  - `MAX_BYTES_BILLED: int` — `50 * 1024**3`
  - `parse_event(event: dict, today: datetime.date | None = None) -> tuple[int, datetime.date]` — raises `ValueError` on bad input
  - `build_query(chain_id: int) -> str` — the rendered SQL with `@dt` / `@addresses` placeholders
  - `object_key(chain_id: int, run_date: datetime.date, extracted_at: datetime.datetime) -> str`
  - `rows_to_parquet(rows: list[dict]) -> bytes` — rows already carry `extracted_at`
  - `SCHEMA: pyarrow.Schema`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_onchain.py`:

```python
import datetime
import io

import pyarrow.parquet as pq
import pytest

from ingestion.onchain.extract import (
    CHAINS,
    build_query,
    object_key,
    parse_event,
    rows_to_parquet,
)

TODAY = datetime.date(2026, 8, 23)


class TestParseEvent:
    def test_explicit_date_and_chain(self):
        chain_id, run_date = parse_event({"chain_id": 137, "date": "2026-08-19"})
        assert chain_id == 137
        assert run_date == datetime.date(2026, 8, 19)

    def test_date_defaults_to_two_days_before_today(self):
        _, run_date = parse_event({"chain_id": 1}, today=TODAY)
        assert run_date == datetime.date(2026, 8, 21)

    def test_missing_chain_id_raises(self):
        with pytest.raises(ValueError, match="chain_id is required"):
            parse_event({"date": "2026-08-19"})

    def test_unknown_chain_id_raises(self):
        with pytest.raises(ValueError, match="chain_id must be one of"):
            parse_event({"chain_id": 56})

    def test_malformed_date_raises(self):
        with pytest.raises(ValueError):
            parse_event({"chain_id": 1, "date": "19/08/2026"})


class TestBuildQuery:
    def test_ethereum_dataset_and_two_step_shape(self):
        sql = build_query(1)
        assert "goog_blockchain_ethereum_mainnet_us.logs" in sql
        assert "@dt" in sql and "UNNEST(@addresses)" in sql
        assert "ARRAY_LENGTH(topics) >= 1" in sql
        assert "ORDER BY block_timestamp, log_index" in sql

    def test_polygon_dataset(self):
        assert "goog_blockchain_polygon_mainnet_us.logs" in build_query(137)

    def test_never_interpolates_values(self):
        # parameters only: no quoted dates or addresses in the SQL text
        assert "2026" not in build_query(1)


class TestObjectKey:
    def test_key_shape_dashes_in_time(self):
        extracted_at = datetime.datetime(
            2026, 8, 20, 20, 34, 59, tzinfo=datetime.timezone.utc
        )
        key = object_key(1, datetime.date(2026, 8, 19), extracted_at)
        assert key == "bronze/ethereum_logs/dt=2026-08-19/2026-08-20_20-34-59.parquet"

    def test_polygon_table_prefix(self):
        extracted_at = datetime.datetime(
            2026, 8, 20, 1, 2, 3, tzinfo=datetime.timezone.utc
        )
        key = object_key(137, datetime.date(2026, 8, 19), extracted_at)
        assert key.startswith("bronze/polygon_logs/dt=2026-08-19/")


class TestRowsToParquet:
    EXTRACTED_AT = datetime.datetime(
        2026, 8, 20, 20, 34, 59, tzinfo=datetime.timezone.utc
    )

    def _row(self):
        return {
            "transaction_hash": "0xabc",
            "log_index": 7,
            "block_timestamp": datetime.datetime(
                2026, 8, 19, 12, 0, 0, tzinfo=datetime.timezone.utc
            ),
            "address": "0x0f5d2fb29fb7d3cfee444a200298f468908cc942",
            "topics": ["0xddf2", "0x0001"],
            "data": "0x00",
            "extracted_at": self.EXTRACTED_AT,
        }

    def test_roundtrip_preserves_columns(self):
        table = pq.read_table(io.BytesIO(rows_to_parquet([self._row()])))
        assert table.num_rows == 1
        assert table.column("topics").to_pylist() == [["0xddf2", "0x0001"]]
        assert table.column("extracted_at").to_pylist() == [self.EXTRACTED_AT]

    def test_empty_rows_keep_full_schema(self):
        table = pq.read_table(io.BytesIO(rows_to_parquet([])))
        assert table.num_rows == 0
        assert table.schema.names == [
            "transaction_hash",
            "log_index",
            "block_timestamp",
            "address",
            "topics",
            "data",
            "extracted_at",
        ]


def test_chains_config():
    assert set(CHAINS) == {1, 137}
    assert CHAINS[1]["table"] == "ethereum_logs"
    assert CHAINS[137]["table"] == "polygon_logs"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_onchain.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ingestion.onchain'`

- [ ] **Step 3: Write the implementation**

Create empty `ingestion/onchain/__init__.py` and `ingestion/onchain/extract.py`:

```python
"""Pure logic for the onchain-logs extraction: no AWS/GCP clients here."""

import datetime
import io

import pyarrow as pa
import pyarrow.parquet as pq

CHAINS = {
    1: {"dataset": "goog_blockchain_ethereum_mainnet_us", "table": "ethereum_logs"},
    137: {"dataset": "goog_blockchain_polygon_mainnet_us", "table": "polygon_logs"},
}

DEFAULT_LAG_DAYS = 2  # BigQuery public partitions may lag; today-2 is safe
MAX_BYTES_BILLED = 50 * 1024**3  # hard per-job ceiling (free tier guard)

SCHEMA = pa.schema(
    [
        ("transaction_hash", pa.string()),
        ("log_index", pa.int64()),
        ("block_timestamp", pa.timestamp("us", tz="UTC")),
        ("address", pa.string()),
        ("topics", pa.list_(pa.string())),
        ("data", pa.string()),
        ("extracted_at", pa.timestamp("us", tz="UTC")),
    ]
)


def parse_event(
    event: dict, today: datetime.date | None = None
) -> tuple[int, datetime.date]:
    chain_id = event.get("chain_id")
    if chain_id is None:
        raise ValueError("chain_id is required")
    if chain_id not in CHAINS:
        raise ValueError(f"chain_id must be one of {sorted(CHAINS)}, got {chain_id!r}")

    raw_date = event.get("date")
    if raw_date:
        run_date = datetime.date.fromisoformat(raw_date)
    else:
        today = today or datetime.datetime.now(datetime.timezone.utc).date()
        run_date = today - datetime.timedelta(days=DEFAULT_LAG_DAYS)
    return chain_id, run_date


def build_query(chain_id: int) -> str:
    """Two-step query: whole transactions that touched Decentraland contracts.

    A sale emits events from the marketplace, the NFT, and MANA in one
    transaction; fetching every log of those transactions preserves the
    context decoding needs. @dt and @addresses are BigQuery query
    parameters — values are never interpolated into the SQL text.
    """
    dataset = CHAINS[chain_id]["dataset"]
    return f"""\
WITH tx AS (
  SELECT DISTINCT transaction_hash
  FROM `bigquery-public-data.{dataset}.logs`
  WHERE DATE(block_timestamp) = @dt
    AND address IN UNNEST(@addresses)
)
SELECT transaction_hash, log_index, block_timestamp, address, topics, data
FROM `bigquery-public-data.{dataset}.logs`
WHERE DATE(block_timestamp) = @dt
  AND ARRAY_LENGTH(topics) >= 1
  AND transaction_hash IN (SELECT transaction_hash FROM tx)
ORDER BY block_timestamp, log_index"""


def object_key(
    chain_id: int, run_date: datetime.date, extracted_at: datetime.datetime
) -> str:
    # Dashes in the time part: colons in S3 keys break URL handling.
    table = CHAINS[chain_id]["table"]
    stamp = extracted_at.strftime("%Y-%m-%d_%H-%M-%S")
    return f"bronze/{table}/dt={run_date.isoformat()}/{stamp}.parquet"


def rows_to_parquet(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_onchain.py -v`
Expected: all PASS

- [ ] **Step 5: Run the full suite and ruff**

Run: `.venv/bin/pytest && .venv/bin/ruff check .` (use plain `ruff check .` if ruff is not in the venv)
Expected: PASS, no lint errors

- [ ] **Step 6: Commit**

```bash
git add ingestion/onchain/__init__.py ingestion/onchain/extract.py tests/test_onchain.py
git commit -m "feat: pure extraction logic for onchain logs (phase 3)"
```

---

### Task 2: Handler (`handler.py`) — Athena, SSM, BigQuery, S3, partition

**Files:**
- Create: `ingestion/onchain/handler.py`
- Test: `tests/test_onchain.py` (append)

**Interfaces:**
- Consumes: everything Task 1 produces; `register_partition(table, run_date, bucket)` from `ingestion/common/partitions.py` (existing).
- Produces: `handler(event, context) -> dict` returning `{"chain_id", "dt", "rows", "bytes_processed", "s3_key"}`; helper `fetch_contract_addresses(athena, chain_id: int) -> list[str]` (unit-tested with a fake client). Env vars consumed: `LAKE_BUCKET` (required), `ATHENA_WORKGROUP` (default `decentraland-data-platform`), `GCP_KEY_PARAM` (default `/decentraland/gcp/bq-service-account-key`).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_onchain.py`:

```python
class FakeAthenaResults:
    """start/poll/paginate contract like the real Athena client."""

    def __init__(self, pages, state="SUCCEEDED"):
        self.pages = pages  # list of lists of address strings (per page)
        self.state = state
        self.queries = []

    def start_query_execution(self, QueryString, WorkGroup):
        self.queries.append({"sql": QueryString, "workgroup": WorkGroup})
        return {"QueryExecutionId": "fake-id"}

    def get_query_execution(self, QueryExecutionId):
        return {
            "QueryExecution": {
                "Status": {"State": self.state, "StateChangeReason": "fake reason"}
            }
        }

    def get_query_results(self, QueryExecutionId, NextToken=None):
        index = 0 if NextToken is None else int(NextToken)
        rows = []
        if index == 0:  # Athena's first page starts with the header row
            rows.append({"Data": [{"VarCharValue": "contract_address"}]})
        rows += [
            {"Data": [{"VarCharValue": a}]} for a in self.pages[index]
        ]
        result = {"ResultSet": {"Rows": rows}}
        if index + 1 < len(self.pages):
            result["NextToken"] = str(index + 1)
        return result


class TestFetchContractAddresses:
    def test_reads_addresses_across_pages_skipping_header(self):
        from ingestion.onchain.handler import fetch_contract_addresses

        fake = FakeAthenaResults(pages=[["0xaaa", "0xbbb"], ["0xccc"]])
        addresses = fetch_contract_addresses(fake, 137)
        assert addresses == ["0xaaa", "0xbbb", "0xccc"]
        assert "chain_id = 137" in fake.queries[0]["sql"]
        assert fake.queries[0]["workgroup"] == "decentraland-data-platform"

    def test_raises_on_failed_query(self):
        from ingestion.onchain.handler import fetch_contract_addresses

        fake = FakeAthenaResults(pages=[[]], state="FAILED")
        with pytest.raises(RuntimeError, match="FAILED.*fake reason"):
            fetch_contract_addresses(fake, 1)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_onchain.py -v -k FetchContract`
Expected: FAIL — no module `ingestion.onchain.handler`

- [ ] **Step 3: Write the handler**

Create `ingestion/onchain/handler.py`:

```python
"""extract-onchain-logs Lambda: BigQuery public logs -> bronze, one day+chain.

Payload: {"chain_id": 1 | 137, "date": "YYYY-MM-DD"} (date optional,
default UTC today-2). Append-only: each run writes a new timestamped
parquet; downstream dedups by latest extracted_at per dt partition.
"""

import datetime
import json
import os
import time

import boto3
from google.cloud import bigquery
from google.oauth2 import service_account

from ingestion.common.partitions import register_partition
from ingestion.onchain.extract import (
    CHAINS,
    MAX_BYTES_BILLED,
    build_query,
    object_key,
    parse_event,
    rows_to_parquet,
)

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")
GCP_KEY_PARAM = os.environ.get(
    "GCP_KEY_PARAM", "/decentraland/gcp/bq-service-account-key"
)


def fetch_contract_addresses(athena, chain_id: int) -> list[str]:
    # chain_id is validated against CHAINS before this runs; safe to inline.
    sql = (
        "SELECT contract_address FROM silver.dim_contracts "
        f"WHERE chain_id = {chain_id}"
    )
    query_id = athena.start_query_execution(
        QueryString=sql, WorkGroup=ATHENA_WORKGROUP
    )["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)[
            "QueryExecution"
        ]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "no reason given")
            raise RuntimeError(f"contract-list query {state}: {reason}")
        time.sleep(1)

    addresses, token, first_page = [], None, True
    while True:
        kwargs = {"QueryExecutionId": query_id}
        if token:
            kwargs["NextToken"] = token
        page = athena.get_query_results(**kwargs)
        rows = page["ResultSet"]["Rows"]
        if first_page:
            rows = rows[1:]  # header row
            first_page = False
        addresses += [r["Data"][0]["VarCharValue"] for r in rows]
        token = page.get("NextToken")
        if not token:
            return addresses


def _bigquery_client(ssm) -> bigquery.Client:
    key = json.loads(
        ssm.get_parameter(Name=GCP_KEY_PARAM, WithDecryption=True)["Parameter"][
            "Value"
        ]
    )
    creds = service_account.Credentials.from_service_account_info(key)
    return bigquery.Client(credentials=creds, project=key["project_id"])


def handler(event, context):
    chain_id, run_date = parse_event(event or {})
    bucket = os.environ["LAKE_BUCKET"]
    table = CHAINS[chain_id]["table"]

    addresses = fetch_contract_addresses(boto3.client("athena"), chain_id)
    if not addresses:
        raise RuntimeError(
            f"silver.dim_contracts returned no addresses for chain_id={chain_id}; "
            "refusing to extract against an empty contract list"
        )

    client = _bigquery_client(boto3.client("ssm"))
    extracted_at = datetime.datetime.now(datetime.timezone.utc)
    sql = build_query(chain_id)
    # Logged for debugging: the exact SQL plus the parameter values.
    print(
        f"bigquery query (chain_id={chain_id}, dt={run_date}, "
        f"{len(addresses)} addresses):\n{sql}\naddresses={addresses}"
    )
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dt", "DATE", run_date),
                bigquery.ArrayQueryParameter("addresses", "STRING", addresses),
            ],
            maximum_bytes_billed=MAX_BYTES_BILLED,
        ),
    )
    rows = [dict(row) | {"extracted_at": extracted_at} for row in job.result()]

    out_key = object_key(chain_id, run_date, extracted_at)
    boto3.client("s3").put_object(
        Bucket=bucket, Key=out_key, Body=rows_to_parquet(rows)
    )
    register_partition(table, run_date, bucket)

    result = {
        "chain_id": chain_id,
        "dt": run_date.isoformat(),
        "rows": len(rows),
        "bytes_processed": job.total_bytes_processed,
        "s3_key": out_key,
    }
    print(result)
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_onchain.py -v`
Expected: all PASS. Note: `google-cloud-bigquery` must be importable for the
handler tests to collect — if the venv lacks it, `.venv/bin/pip install
google-cloud-bigquery` first (dev dependency only; the image pins its own).

- [ ] **Step 5: Full suite + ruff**

Run: `.venv/bin/pytest && ruff check .`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add ingestion/onchain/handler.py tests/test_onchain.py
git commit -m "feat: extract-onchain-logs handler (Athena contracts -> BigQuery -> bronze)"
```

---

### Task 3: Container image

**Files:**
- Create: `ingestion/onchain/requirements.txt`
- Create: `ingestion/onchain/Dockerfile`

**Interfaces:**
- Consumes: `ingestion/onchain/handler.py` (Task 2), `ingestion/common/partitions.py` (existing).
- Produces: an image whose CMD is `ingestion.onchain.handler.handler`; Task 4's `data "aws_ecr_image"` resolves the pushed `:latest` digest.

- [ ] **Step 1: Write requirements.txt**

`ingestion/onchain/requirements.txt` (boto3 ships with the Lambda base image):

```
google-cloud-bigquery==3.27.0
pyarrow==18.1.0
```

- [ ] **Step 2: Write the Dockerfile**

`ingestion/onchain/Dockerfile` (build context = repo root, same flow as `dbt_runner`):

```dockerfile
FROM public.ecr.aws/lambda/python:3.13

COPY ingestion/onchain/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Code only — no reference data baked into images (repo convention).
COPY ingestion/__init__.py /var/task/ingestion/__init__.py
COPY ingestion/common/__init__.py ingestion/common/partitions.py /var/task/ingestion/common/
COPY ingestion/onchain/__init__.py ingestion/onchain/extract.py ingestion/onchain/handler.py /var/task/ingestion/onchain/

CMD ["ingestion.onchain.handler.handler"]
```

- [ ] **Step 3: Build locally to prove it assembles**

```bash
docker build -f ingestion/onchain/Dockerfile -t extract-onchain-logs:dev .
docker run --rm --entrypoint python extract-onchain-logs:dev -c "import ingestion.onchain.handler as h; print(h.handler.__name__)"
```

Expected: prints `handler` (imports resolve inside the image; push happens in Task 5).

- [ ] **Step 4: Commit**

```bash
git add ingestion/onchain/requirements.txt ingestion/onchain/Dockerfile
git commit -m "build: container image for extract-onchain-logs"
```

---

### Task 4: Terraform — Glue tables, ECR, IAM, Lambda

**Files:**
- Create: `terraform/table_onchain_logs.tf`
- Create: `terraform/lambda_onchain_logs.tf`
- Modify: `terraform/alerts.tf` (add to `monitored_lambdas`)

**Interfaces:**
- Consumes: existing `aws_glue_catalog_database.bronze` / `.silver`, `aws_athena_workgroup.main`, `aws_s3_bucket.lake`, `local.bucket_name`, `data.aws_caller_identity.current`.
- Produces: `aws_lambda_function.onchain_logs` (name `extract-onchain-logs`), `aws_ecr_repository.onchain_logs` (`decentraland-extract-onchain-logs`), Glue tables `bronze.ethereum_logs` / `bronze.polygon_logs`.

- [ ] **Step 1: Write `terraform/table_onchain_logs.tf`**

```hcl
# Bronze log tables, one per chain. No partition projection: the Lambda
# registers each dt explicitly, which keeps the "$partitions" metadata
# convention working (projection tables expose no real partitions).
resource "aws_glue_catalog_table" "onchain_logs" {
  for_each = toset(["ethereum_logs", "polygon_logs"])

  database_name = aws_glue_catalog_database.bronze.name
  name          = each.value
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/bronze/${each.value}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "transaction_hash"
      type = "string"
    }
    columns {
      name = "log_index"
      type = "bigint"
    }
    columns {
      name = "block_timestamp"
      type = "timestamp"
    }
    columns {
      name = "address"
      type = "string"
    }
    columns {
      name = "topics"
      type = "array<string>"
    }
    columns {
      name = "data"
      type = "string"
    }
    columns {
      name = "extracted_at"
      type = "timestamp"
    }
  }
}
```

- [ ] **Step 2: Write `terraform/lambda_onchain_logs.tf`**

```hcl
locals {
  onchain_logs_tags = { component = "ingestion-onchain", layer = "bronze" }
  # Created manually (aws ssm put-parameter) so the secret never enters
  # tfstate; Terraform only references the ARN for IAM.
  gcp_key_param_arn = "arn:aws:ssm:us-east-1:${data.aws_caller_identity.current.account_id}:parameter/decentraland/gcp/bq-service-account-key"
}

resource "aws_ecr_repository" "onchain_logs" {
  name         = "decentraland-extract-onchain-logs"
  force_delete = true
  tags         = local.onchain_logs_tags
}

resource "aws_ecr_lifecycle_policy" "onchain_logs" {
  repository = aws_ecr_repository.onchain_logs.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 3 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 3
      }
      action = { type = "expire" }
    }]
  })
}

data "aws_ecr_image" "onchain_logs" {
  repository_name = aws_ecr_repository.onchain_logs.name
  image_tag       = "latest"
}

resource "aws_iam_role" "onchain_logs" {
  name = "extract-onchain-logs-role"
  tags = local.onchain_logs_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "onchain_logs" {
  name = "athena-glue-s3-ssm"
  role = aws_iam_role.onchain_logs.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup",
        ]
        Resource = aws_athena_workgroup.main.arn
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartition",
          "glue:GetPartitions",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.bronze.arn,
          aws_glue_catalog_database.silver.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/*",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/silver/*",
        ]
      },
      # Explicit partition registration (ALTER TABLE ADD PARTITION)
      {
        Effect = "Allow"
        Action = ["glue:CreatePartition", "glue:BatchCreatePartition"]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.bronze.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/ethereum_logs",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/polygon_logs",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      },
      # Append-only by design: PutObject only, no DeleteObject anywhere.
      {
        Effect = "Allow"
        Action = "s3:PutObject"
        Resource = [
          "${aws_s3_bucket.lake.arn}/bronze/ethereum_logs/*",
          "${aws_s3_bucket.lake.arn}/bronze/polygon_logs/*",
        ]
      },
      # Athena result staging; also where dbt materializes dim_contracts.
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${aws_s3_bucket.lake.arn}/athena-results/*"
      },
      {
        Effect   = "Allow"
        Action   = "ssm:GetParameter"
        Resource = local.gcp_key_param_arn
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "onchain_logs_logs" {
  role       = aws_iam_role.onchain_logs.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "onchain_logs" {
  name              = "/aws/lambda/extract-onchain-logs"
  retention_in_days = 7
  tags              = local.onchain_logs_tags
}

resource "aws_lambda_function" "onchain_logs" {
  function_name = "extract-onchain-logs"
  description   = "One day + one chain of Decentraland logs: BigQuery -> bronze"
  role          = aws_iam_role.onchain_logs.arn
  tags          = local.onchain_logs_tags

  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.onchain_logs.repository_url}@${data.aws_ecr_image.onchain_logs.image_digest}"
  architectures = ["arm64"]

  timeout     = 900
  memory_size = 2048

  environment {
    variables = {
      LAKE_BUCKET      = local.bucket_name
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
      GCP_KEY_PARAM    = "/decentraland/gcp/bq-service-account-key"
    }
  }

  depends_on = [aws_cloudwatch_log_group.onchain_logs]
}
```

- [ ] **Step 3: Add the Lambda to `monitored_lambdas` in `terraform/alerts.tf`**

```hcl
  monitored_lambdas = [
    aws_lambda_function.dcl_contracts.function_name,
    aws_lambda_function.dcl_contracts_diff.function_name,
    aws_lambda_function.contracts.function_name,
    aws_lambda_function.run_dbt.function_name,
    aws_lambda_function.onchain_logs.function_name,
  ]
```

- [ ] **Step 4: Format and validate**

Run: `cd terraform && terraform fmt && terraform validate`
Expected: `Success! The configuration is valid.` (the `data.aws_ecr_image` lookup only fails at plan/apply time if no image is pushed — that ordering is handled in Task 5).

- [ ] **Step 5: Commit**

```bash
git add terraform/table_onchain_logs.tf terraform/lambda_onchain_logs.tf terraform/alerts.tf
git commit -m "infra: extract-onchain-logs Lambda, bronze log tables, least-privilege IAM"
```

---

### Task 5: Deploy + E2E verification — USER CHECKPOINTS

**Files:**
- Modify: `CLAUDE.md` (workflow section: mark phase 3 status after verification)

**Interfaces:**
- Consumes: everything above, deployed.
- Produces: `bronze.ethereum_logs` partition `dt=2026-08-19` queryable in Athena.

- [ ] **Step 1 (USER): Create the ECR repo and tables first**

```bash
cd terraform
terraform plan -target=aws_ecr_repository.onchain_logs -target=aws_glue_catalog_table.onchain_logs
terraform apply -target=aws_ecr_repository.onchain_logs -target=aws_glue_catalog_table.onchain_logs
```

Read the plan before applying (targeted, because `data.aws_ecr_image` needs an image in the repo before the full plan can resolve).

- [ ] **Step 2: Build and push the image**

```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 683569194224.dkr.ecr.us-east-1.amazonaws.com
# --provenance=false: Docker otherwise pushes an OCI index with an
# attestation manifest, which Lambda rejects (needs single-platform).
docker build --provenance=false -f ingestion/onchain/Dockerfile -t 683569194224.dkr.ecr.us-east-1.amazonaws.com/decentraland-extract-onchain-logs:latest .
docker push 683569194224.dkr.ecr.us-east-1.amazonaws.com/decentraland-extract-onchain-logs:latest
```

- [ ] **Step 3 (USER): Full plan + apply**

```bash
cd terraform && terraform plan
terraform apply
```

Expected new resources: IAM role+policy, log group, Lambda, lifecycle policy, alarm `lambda-errors-extract-onchain-logs`.

- [ ] **Step 4: The single agreed E2E invocation**

```bash
aws lambda invoke --function-name extract-onchain-logs \
  --payload '{"chain_id": 1, "date": "2026-08-19"}' \
  --cli-binary-format raw-in-base64-out /dev/stdout
```

Expected: JSON response with `"dt": "2026-08-19"`, a positive `rows`, `bytes_processed`, and an `s3_key` like `bronze/ethereum_logs/dt=2026-08-19/2026-08-2X_HH-MM-SS.parquet`.

- [ ] **Step 5: Verify logs and data**

- CloudWatch (`/aws/lambda/extract-onchain-logs`): the rendered SQL, the address list, and the result dict are all present.
- Athena:

```sql
SELECT count(*) FROM bronze.ethereum_logs WHERE dt = '2026-08-19';
SELECT * FROM bronze.ethereum_logs WHERE dt = '2026-08-19' ORDER BY block_timestamp LIMIT 5;
```

Expected: count matches the invocation's `rows`; columns populated, `extracted_at` constant across rows.
- Spot-check one `transaction_hash` on etherscan.io: the transaction exists on 2026-08-19 and involves a Decentraland contract.

- [ ] **Step 6: Update CLAUDE.md workflow status and commit**

In `CLAUDE.md`'s Workflow section, mark step 3 as DONE with a one-line summary (single Lambda `extract-onchain-logs`, payload `{chain_id, date?}`, append-only bronze `ethereum_logs`/`polygon_logs`).

```bash
git add CLAUDE.md
git commit -m "docs: phase 3 (onchain logs ingestion) deployed and verified"
```
