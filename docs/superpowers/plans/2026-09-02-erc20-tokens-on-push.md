# erc20-tokens-on-push Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publishing `reference/erc20_tokens.csv` to `landing/erc20_tokens/` validates the file, snapshots it into `bronze.erc20_tokens`, and rebuilds `silver.dim_erc20_tokens` with dbt tests, alerting Slack on failure.

**Architecture:** Clone of the contracts-on-push pattern: dedicated zip Lambda `load-erc20-tokens` (validate CSV → parquet snapshot → register partition), Glue table in Terraform, EventBridge rule → Step Function `erc20-tokens-on-push` (`LoadErc20Tokens` → `RunDbt` with explicit selector), dbt model over the latest snapshot via `$partitions`.

**Tech Stack:** Python 3.13 (pyarrow via AWSSDKPandas layer), Terraform, Step Functions, Glue/Athena, dbt-athena, pytest.

**Spec:** `docs/superpowers/specs/2026-09-02-erc20-tokens-on-push-design.md`

## Global Constraints

- All artifacts in English; SQL keywords UPPERCASE; explicit `INNER JOIN` (no bare `JOIN`).
- Partition column is `dt` (never `date`); addresses lowercase; key is `(chain_id, contract_address)`.
- Bucket always `local.bucket_name` / `${aws_s3_bucket.lake.arn}` — never hardcoded.
- Region us-east-1; IaC only through Terraform; ALWAYS read `terraform plan` output before `terraform apply`.
- Max/min of a partition column reads `"<table>$partitions"` (macro `max_partition_dt`); models using it are `materialized='table'`.
- Tags: Lambda group `component=ingestion-erc20-tokens, layer=bronze`; orchestration `component=orchestration, layer=ops`.
- No surrogate key in this feature (user decision): natural key only.
- Work on branch `erc20-tokens-on-push`; commit locally, push only with explicit user confirmation.

---

### Task 1: CSV validator (`validate.py`) with TDD

**Files:**
- Create: `ingestion/erc20_tokens/__init__.py` (empty)
- Create: `ingestion/erc20_tokens/validate.py`
- Test: `tests/test_erc20_tokens.py`

**Interfaces:**
- Produces: `parse_and_validate(csv_bytes: bytes) -> list[dict]` (keys: `chain_id int, contract_address str, name str, fsym str, decimals int`; raises `ValueError` on any defect), `partition_key(run_date: datetime.date) -> str`, `EXPECTED_HEADER: list[str]`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_erc20_tokens.py`:

```python
import datetime
from pathlib import Path

import pytest

from ingestion.erc20_tokens.validate import (
    EXPECTED_HEADER,
    parse_and_validate,
    partition_key,
)

FIXTURE_BYTES = (
    Path(__file__).parent.parent / "reference" / "erc20_tokens.csv"
).read_bytes()


def test_real_reference_file_parses():
    rows = parse_and_validate(FIXTURE_BYTES)
    assert len(rows) == 22
    assert {r["chain_id"] for r in rows} == {1}
    first = rows[0]
    assert isinstance(first["chain_id"], int)
    assert isinstance(first["contract_address"], str)
    assert isinstance(first["name"], str)
    assert isinstance(first["fsym"], str)
    assert isinstance(first["decimals"], int)


def test_reference_file_known_facts():
    rows = parse_and_validate(FIXTURE_BYTES)
    by_addr = {r["contract_address"]: r for r in rows}
    # Native-ETH placeholder row is a regular row
    assert by_addr["0x0000000000000000000000000000000000000000"]["fsym"] == "ETH"
    # fsym is NOT unique: GALA v1 and v2 share the ticker
    assert sum(1 for r in rows if r["fsym"] == "GALA") == 2
    # decimals exceptions
    assert by_addr["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"]["decimals"] == 6
    assert by_addr["0xdac17f958d2ee523a2206206994597c13d831ec7"]["decimals"] == 6
    assert by_addr["0xe3c408bd53c31c085a1746af401a4042954ff740"]["decimals"] == 8


HEADER = "chain_id,contract_address,name,fsym,decimals"
VALID_LINE = "1,0x0f5d2fb29fb7d3cfee444a200298f468908cc942,Decentraland MANA,MANA,18"


def _csv(*lines):
    return ("\n".join(lines) + "\n").encode()


def _line(**overrides):
    fields = {
        "chain_id": "1",
        "contract_address": "0x0f5d2fb29fb7d3cfee444a200298f468908cc942",
        "name": "Decentraland MANA",
        "fsym": "MANA",
        "decimals": "18",
    }
    fields.update(overrides)
    return ",".join(fields.values())


@pytest.mark.parametrize(
    "bad_csv,match",
    [
        (_csv("chain_id,address,name,fsym,decimals", VALID_LINE), "bad header"),
        (_csv(HEADER.rsplit(",", 1)[0], _line()), "bad header"),
        (_csv(HEADER, _line(chain_id="99")), "unknown chain_id"),
        (_csv(HEADER, _line(chain_id="x")), "chain_id is not an integer"),
        (_csv(HEADER, _line(contract_address="0x0F5D2FB29FB7D3CFEE444A200298F468908CC942")), "invalid address"),
        (_csv(HEADER, _line(contract_address="0x1234")), "invalid address"),
        (_csv(HEADER, _line(name="")), "name must not be empty"),
        (_csv(HEADER, _line(fsym="mana")), "invalid fsym"),
        (_csv(HEADER, _line(fsym="")), "invalid fsym"),
        (_csv(HEADER, _line(fsym="VERYLONGTICKER")), "invalid fsym"),
        (_csv(HEADER, _line(decimals="-1")), "decimals out of range"),
        (_csv(HEADER, _line(decimals="37")), "decimals out of range"),
        (_csv(HEADER, _line(decimals="abc")), "decimals is not an integer"),
        (_csv(HEADER, _line(decimals="")), "decimals is not an integer"),
        (_csv(HEADER, VALID_LINE, VALID_LINE), "duplicate"),
        (_csv(HEADER), "no data rows"),
        (b"", "bad header|CSV is empty"),
    ],
)
def test_rejects_bad_input(bad_csv, match):
    with pytest.raises(ValueError, match=match):
        parse_and_validate(bad_csv)


def test_bom_prefixed_csv_is_accepted():
    bom_csv = b"\xef\xbb\xbf" + _csv(HEADER, VALID_LINE)
    assert len(parse_and_validate(bom_csv)) == 1


def test_crlf_line_endings_are_accepted():
    crlf_csv = f"{HEADER}\r\n{VALID_LINE}\r\n".encode()
    rows = parse_and_validate(crlf_csv)
    assert len(rows) == 1
    assert rows[0]["decimals"] == 18


def test_duplicate_fsym_on_different_addresses_is_allowed():
    line_v1 = "1,0x15d4c048f83bd7e37d49ea4c83a07267ec4203da,Gala,GALA,8"
    line_v2 = "1,0xd1d2eb1b1e90b638588728b4130137d262c87cae,Gala,GALA,8"
    rows = parse_and_validate(_csv(HEADER, line_v1, line_v2))
    assert len(rows) == 2


def test_same_address_on_two_chains_is_not_a_duplicate():
    line137 = VALID_LINE.replace("1,0x", "137,0x", 1)
    rows = parse_and_validate(_csv(HEADER, VALID_LINE, line137))
    assert len(rows) == 2


def test_partition_key_format():
    key = partition_key(datetime.date(2026, 9, 2))
    assert key == "bronze/erc20_tokens/dt=2026-09-02/erc20_tokens.parquet"


def test_expected_header():
    assert EXPECTED_HEADER == [
        "chain_id",
        "contract_address",
        "name",
        "fsym",
        "decimals",
    ]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_erc20_tokens.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ingestion.erc20_tokens'`

- [ ] **Step 3: Write the validator**

Create empty `ingestion/erc20_tokens/__init__.py` and `ingestion/erc20_tokens/validate.py`:

```python
"""Pure validation/parsing for the curated ERC-20 tokens CSV.

Whole-file semantics: any defect raises ValueError and nothing is written.
No AWS or network calls here.
"""

import csv
import datetime
import io
import re

EXPECTED_HEADER = [
    "chain_id",
    "contract_address",
    "name",
    "fsym",
    "decimals",
]

KNOWN_CHAIN_IDS = {1, 137}
ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
# Price-source ticker (informative, NOT unique: GALA v1/v2 share it)
FSYM_RE = re.compile(r"^[A-Z0-9]{1,10}$")


def parse_and_validate(csv_bytes: bytes) -> list[dict]:
    # utf-8-sig: tolerate a BOM from curators re-saving in Excel
    text = csv_bytes.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError("CSV is empty")
    if header != EXPECTED_HEADER:
        raise ValueError(f"bad header: expected {EXPECTED_HEADER}, got {header}")

    rows, seen = [], set()
    for lineno, raw in enumerate(reader, start=2):
        if not raw:
            continue  # trailing blank line
        if len(raw) != len(EXPECTED_HEADER):
            raise ValueError(
                f"line {lineno}: expected {len(EXPECTED_HEADER)} fields, got {len(raw)}"
            )
        chain_raw, address, name, fsym, decimals_raw = raw

        try:
            chain_id = int(chain_raw)
        except ValueError:
            raise ValueError(f"line {lineno}: chain_id is not an integer: {chain_raw!r}")
        if chain_id not in KNOWN_CHAIN_IDS:
            raise ValueError(
                f"line {lineno}: unknown chain_id {chain_id} (known: {sorted(KNOWN_CHAIN_IDS)})"
            )

        if not ADDRESS_RE.match(address):
            raise ValueError(
                f"line {lineno}: invalid address (must be 0x + 40 lowercase hex): {address!r}"
            )

        if not name:
            raise ValueError(f"line {lineno}: name must not be empty")

        if not FSYM_RE.match(fsym):
            raise ValueError(
                f"line {lineno}: invalid fsym (must be 1-10 uppercase A-Z/0-9): {fsym!r}"
            )

        try:
            decimals = int(decimals_raw)
        except ValueError:
            raise ValueError(
                f"line {lineno}: decimals is not an integer: {decimals_raw!r}"
            )
        if not 0 <= decimals <= 36:
            raise ValueError(f"line {lineno}: decimals out of range [0, 36]: {decimals}")

        key = (chain_id, address)
        if key in seen:
            raise ValueError(
                f"line {lineno}: duplicate (chain_id, contract_address): {key}"
            )
        seen.add(key)

        rows.append(
            {
                "chain_id": chain_id,
                "contract_address": address,
                "name": name,
                "fsym": fsym,
                "decimals": decimals,
            }
        )

    if not rows:
        raise ValueError("CSV has a header but no data rows")
    return rows


def partition_key(run_date: datetime.date) -> str:
    return f"bronze/erc20_tokens/dt={run_date.isoformat()}/erc20_tokens.parquet"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_erc20_tokens.py -v`
Expected: all PASS

- [ ] **Step 5: Run the full suite and ruff**

Run: `pytest tests/ && ruff check ingestion/erc20_tokens tests/test_erc20_tokens.py`
Expected: all PASS, no lint errors

- [ ] **Step 6: Commit**

```bash
git add ingestion/erc20_tokens/ tests/test_erc20_tokens.py
git commit -m "feat: validator for erc20_tokens reference CSV"
```

---

### Task 2: Lambda handler (`handler.py`) with TDD

**Files:**
- Create: `ingestion/erc20_tokens/handler.py`
- Modify: `tests/test_erc20_tokens.py` (append tests)

**Interfaces:**
- Consumes: `parse_and_validate`, `partition_key` from Task 1; `register_partition(table, run_date, bucket)` from `ingestion/common/partitions.py` (existing, unchanged).
- Produces: `handler(event, context)` accepting the S3 `Records` event shape; `rows_to_parquet(rows: list[dict]) -> bytes`; `SCHEMA` (pyarrow schema).

- [ ] **Step 1: Append the failing tests**

Append to `tests/test_erc20_tokens.py`:

```python
def test_rows_to_parquet_roundtrip(tmp_path):
    import pyarrow.parquet as pq

    from ingestion.erc20_tokens.handler import rows_to_parquet

    rows = parse_and_validate(FIXTURE_BYTES)
    out = tmp_path / "erc20_tokens.parquet"
    out.write_bytes(rows_to_parquet(rows))
    table = pq.read_table(out)
    assert table.column_names == [
        "chain_id",
        "contract_address",
        "name",
        "fsym",
        "decimals",
    ]
    assert table.num_rows == 22
    assert str(table.schema.field("chain_id").type) == "int32"
    assert str(table.schema.field("contract_address").type) == "string"
    assert str(table.schema.field("name").type) == "string"
    assert str(table.schema.field("fsym").type) == "string"
    assert str(table.schema.field("decimals").type) == "int32"


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


def test_handler_reads_event_and_writes_partition(monkeypatch):
    import ingestion.erc20_tokens.handler as h

    written = {}
    queries = []

    class FakeS3:
        def get_object(self, Bucket, Key):
            import io as _io

            return {"Body": _io.BytesIO(FIXTURE_BYTES)}

        def put_object(self, Bucket, Key, Body):
            written["bucket"], written["key"], written["body"] = Bucket, Key, Body

    def fake_client(service):
        if service == "s3":
            return FakeS3()
        else:
            return FakeAthena(queries)

    monkeypatch.setattr(h.boto3, "client", fake_client)
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": "landing/erc20_tokens/erc20_tokens.csv"},
                }
            }
        ]
    }
    result = h.handler(event, None)
    assert result["rows"] == 22
    assert written["bucket"] == "test-bucket"
    assert written["key"].startswith("bronze/erc20_tokens/dt=")
    assert written["key"].endswith("/erc20_tokens.parquet")
    assert result["s3_key"] == written["key"]
    assert result["source_key"] == "landing/erc20_tokens/erc20_tokens.csv"
    assert len(written["body"]) > 500  # real parquet bytes

    # The partition DDL ran, on the right table/path, in the tagged workgroup
    assert len(queries) == 1
    ddl = queries[0]["ddl"]
    assert "ALTER TABLE bronze.erc20_tokens" in ddl
    assert "ADD IF NOT EXISTS PARTITION" in ddl
    dt = written["key"].split("dt=")[1].split("/")[0]
    assert f"(dt = '{dt}')" in ddl
    assert f"LOCATION 's3://test-bucket/bronze/erc20_tokens/dt={dt}/'" in ddl
    assert queries[0]["workgroup"] == "decentraland-data-platform"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_erc20_tokens.py -v -k "parquet or handler"`
Expected: FAIL — `ModuleNotFoundError` / `No module named 'ingestion.erc20_tokens.handler'`

- [ ] **Step 3: Write the handler**

Create `ingestion/erc20_tokens/handler.py`:

```python
"""load-erc20-tokens Lambda: landing/ CSV upload -> bronze/erc20_tokens snapshot.

Triggered by S3 ObjectCreated events (prefix landing/erc20_tokens/, suffix .csv).
"""

import datetime
import io
import urllib.parse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common.partitions import register_partition
from ingestion.erc20_tokens.validate import parse_and_validate, partition_key

SCHEMA = pa.schema(
    [
        ("chain_id", pa.int32()),
        ("contract_address", pa.string()),
        ("name", pa.string()),
        ("fsym", pa.string()),
        ("decimals", pa.int32()),
    ]
)


def rows_to_parquet(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def handler(event, context):
    # S3 sends one record per direct notification, but the contract is a
    # list — process every record rather than silently dropping extras.
    s3 = boto3.client("s3")
    results = []
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        # S3 URL-encodes keys in event payloads (spaces become '+')
        source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        body = s3.get_object(Bucket=bucket, Key=source_key)["Body"].read()
        rows = parse_and_validate(body)

        run_date = datetime.datetime.now(datetime.timezone.utc).date()
        out_key = partition_key(run_date)
        s3.put_object(Bucket=bucket, Key=out_key, Body=rows_to_parquet(rows))
        register_partition("erc20_tokens", run_date, bucket)

        print(
            f"validated {len(rows)} rows from s3://{bucket}/{source_key}; "
            f"wrote s3://{bucket}/{out_key}"
        )
        results.append({"rows": len(rows), "s3_key": out_key, "source_key": source_key})
    return results[0] if len(results) == 1 else {"processed": results}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_erc20_tokens.py -v`
Expected: all PASS

- [ ] **Step 5: Run the full suite and ruff**

Run: `pytest tests/ && ruff check ingestion/erc20_tokens`
Expected: all PASS, no lint errors

- [ ] **Step 6: Commit**

```bash
git add ingestion/erc20_tokens/handler.py tests/test_erc20_tokens.py
git commit -m "feat: load-erc20-tokens Lambda handler"
```

---

### Task 3: Terraform — Glue table + Lambda + alarm + run-dbt read

**Files:**
- Create: `terraform/table_erc20_tokens.tf`
- Create: `terraform/lambda_erc20_tokens.tf`
- Modify: `terraform/alerts.tf` (add to `local.monitored_lambdas`)
- Modify: `terraform/lambda_run_dbt.tf` (add `bronze/erc20_tokens/*` to the `s3:GetObject` statement)

**Interfaces:**
- Consumes: existing `aws_s3_bucket.lake`, `aws_athena_workgroup.main`, `aws_glue_catalog_database.bronze`, `local.sdk_pandas_layer_arn`, `local.bucket_name`.
- Produces: `aws_lambda_function.erc20_tokens` (name `load-erc20-tokens`) and `aws_glue_catalog_table.erc20_tokens` — referenced by Task 4.

- [ ] **Step 1: Write `terraform/table_erc20_tokens.tf`**

```hcl
resource "aws_glue_catalog_table" "erc20_tokens" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "erc20_tokens"
  table_type    = "EXTERNAL_TABLE"

  # No partition projection here: snapshots are infrequent, so the Lambda
  # registers each partition explicitly (ALTER TABLE ADD PARTITION) and the
  # catalog lists exactly the partitions that really exist.
  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/bronze/erc20_tokens/"
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
      name = "name"
      type = "string"
    }
    columns {
      name = "fsym"
      type = "string"
    }
    columns {
      name = "decimals"
      type = "int"
    }
  }
}
```

- [ ] **Step 2: Write `terraform/lambda_erc20_tokens.tf`**

Note: the shared `aws_s3_bucket_notification "lake"` already exists in
`lambda_contracts.tf` — do NOT add another one.

```hcl
locals {
  erc20_tokens_tags = { component = "ingestion-erc20-tokens", layer = "bronze" }
}

data "archive_file" "erc20_tokens_zip" {
  type        = "zip"
  output_path = "${path.module}/build/erc20_tokens.zip"

  source {
    content  = file("${path.module}/../ingestion/erc20_tokens/handler.py")
    filename = "ingestion/erc20_tokens/handler.py"
  }
  source {
    content  = file("${path.module}/../ingestion/erc20_tokens/validate.py")
    filename = "ingestion/erc20_tokens/validate.py"
  }
  source {
    content  = ""
    filename = "ingestion/__init__.py"
  }
  source {
    content  = ""
    filename = "ingestion/erc20_tokens/__init__.py"
  }
  source {
    content  = file("${path.module}/../ingestion/common/partitions.py")
    filename = "ingestion/common/partitions.py"
  }
  source {
    content  = ""
    filename = "ingestion/common/__init__.py"
  }
}

resource "aws_iam_role" "erc20_tokens" {
  name = "load-erc20-tokens-role"
  tags = local.erc20_tokens_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "erc20_tokens_s3" {
  name = "landing-read-bronze-write"
  role = aws_iam_role.erc20_tokens.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/landing/erc20_tokens/*"
      },
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/erc20_tokens/*"
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
          aws_glue_catalog_table.erc20_tokens.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "erc20_tokens_logs" {
  role       = aws_iam_role.erc20_tokens.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "erc20_tokens" {
  name              = "/aws/lambda/load-erc20-tokens"
  retention_in_days = 7
  tags              = local.erc20_tokens_tags
}

resource "aws_lambda_function" "erc20_tokens" {
  function_name = "load-erc20-tokens"
  role          = aws_iam_role.erc20_tokens.arn
  tags          = local.erc20_tokens_tags

  filename         = data.archive_file.erc20_tokens_zip.output_path
  source_code_hash = data.archive_file.erc20_tokens_zip.output_base64sha256

  handler     = "ingestion.erc20_tokens.handler.handler"
  runtime     = "python3.13"
  timeout     = 60
  memory_size = 512
  layers      = [local.sdk_pandas_layer_arn] # defined in lambda_dcl_contracts.tf

  environment {
    variables = {
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.erc20_tokens]
}
```

- [ ] **Step 3: Add the function to `local.monitored_lambdas` in `terraform/alerts.tf`**

Append one line inside the existing list:

```hcl
  monitored_lambdas = [
    aws_lambda_function.dcl_contracts.function_name,
    aws_lambda_function.dcl_contracts_diff.function_name,
    aws_lambda_function.contracts.function_name,
    aws_lambda_function.run_dbt.function_name,
    aws_lambda_function.onchain_logs.function_name,
    aws_lambda_function.onchain_logs_chunk.function_name,
    aws_lambda_function.erc20_tokens.function_name,
  ]
```

- [ ] **Step 4: Widen the run-dbt read policy in `terraform/lambda_run_dbt.tf`**

Find the statement whose `Resource` is `"${aws_s3_bucket.lake.arn}/bronze/contracts/*"` and turn `Resource` into a list:

```hcl
      {
        Effect = "Allow"
        Action = "s3:GetObject"
        Resource = [
          "${aws_s3_bucket.lake.arn}/bronze/contracts/*",
          "${aws_s3_bucket.lake.arn}/bronze/erc20_tokens/*",
        ]
      },
```

- [ ] **Step 5: Validate and plan**

Run: `cd terraform && terraform validate && terraform plan`
Expected: validate OK. Plan shows ONLY adds (glue table, IAM role + policy + attachment, log group, lambda) plus in-place updates to the run-dbt inline policy and the alarms `for_each` (one new `lambda-errors-load-erc20-tokens`). Read the whole plan; nothing may be destroyed.

- [ ] **Step 6: Apply**

Run: `cd terraform && terraform apply` (approve after re-reading the summary)
Expected: `Apply complete`. Verify: `aws lambda get-function --function-name load-erc20-tokens --query 'Configuration.State'` → `"Active"`.

- [ ] **Step 7: Commit**

```bash
git add terraform/table_erc20_tokens.tf terraform/lambda_erc20_tokens.tf terraform/alerts.tf terraform/lambda_run_dbt.tf
git commit -m "feat: terraform for load-erc20-tokens lambda and bronze.erc20_tokens table"
```

---

### Task 4: Terraform — Step Function + EventBridge rule

**Files:**
- Create: `terraform/step_functions_erc20_tokens.tf`

**Interfaces:**
- Consumes: `aws_lambda_function.erc20_tokens` (Task 3), existing `aws_lambda_function.run_dbt`, `aws_sns_topic.alerts`, `aws_s3_bucket.lake`.
- Produces: state machine `erc20-tokens-on-push` triggered by `landing/erc20_tokens/*.csv`.

- [ ] **Step 1: Write `terraform/step_functions_erc20_tokens.tf`**

```hcl
# erc20-tokens-on-push: CSV lands in landing/erc20_tokens/ -> ingest -> dbt
# build. Event-driven only (no schedule); failures use the custom
# notify-slack contract, same skeleton as contracts-on-push.
locals {
  sfn_erc20_tokens_tags = { component = "orchestration", layer = "ops" }
}

resource "aws_iam_role" "sfn_erc20_tokens" {
  name = "erc20-tokens-on-push-sfn-role"
  tags = local.sfn_erc20_tokens_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_erc20_tokens" {
  name = "invoke-lambdas-publish-alerts"
  role = aws_iam_role.sfn_erc20_tokens.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = [
          aws_lambda_function.erc20_tokens.arn,
          aws_lambda_function.run_dbt.arn,
        ]
      },
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.alerts.arn
      }
    ]
  })
}

resource "aws_sfn_state_machine" "erc20_tokens_on_push" {
  name     = "erc20-tokens-on-push"
  role_arn = aws_iam_role.sfn_erc20_tokens.arn
  tags     = local.sfn_erc20_tokens_tags

  definition = jsonencode({
    Comment = "landing/erc20_tokens CSV push -> load-erc20-tokens -> dbt build"
    StartAt = "LoadErc20Tokens"
    States = {
      # Input is the raw EventBridge S3 event; Parameters rebuilds the S3
      # Records shape the Lambda already understands (no handler change).
      LoadErc20Tokens = {
        Type     = "Task"
        Resource = aws_lambda_function.erc20_tokens.arn
        Parameters = {
          Records = [{
            s3 = {
              bucket = { "name.$" = "$.detail.bucket.name" }
              object = { "key.$" = "$.detail.object.key" }
            }
          }]
        }
        ResultPath = "$.load"
        Retry = [{
          ErrorEquals = [
            "Lambda.ServiceException",
            "Lambda.TooManyRequestsException",
          ]
          IntervalSeconds = 5
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "RunDbt"
      }
      RunDbt = {
        Type     = "Task"
        Resource = aws_lambda_function.run_dbt.arn
        Parameters = {
          select = "source:bronze.erc20_tokens+"
        }
        ResultPath = "$.dbt"
        Retry = [{
          ErrorEquals = [
            "Lambda.ServiceException",
            "Lambda.TooManyRequestsException",
          ]
          IntervalSeconds = 5
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        End = true
      }
      # Custom notify-slack contract; Step Functions serializes Message to JSON.
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.alerts.arn
          Message = {
            source            = "step-functions"
            component         = "erc20-tokens"
            status            = "FAILED"
            "detail.$"        = "States.Format('{}: {}', $.error.Error, $.error.Cause)"
            "execution_url.$" = "States.Format('https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1#/v2/executions/details/{}', $$.Execution.Id)"
          }
        }
        Next = "FailExecution"
      }
      FailExecution = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "A step failed; details were sent to the alerts topic"
      }
    }
  })
}

resource "aws_cloudwatch_event_rule" "erc20_tokens_on_push" {
  name        = "erc20-tokens-on-push"
  description = "Start erc20-tokens-on-push when a CSV lands in landing/erc20_tokens/"
  tags        = local.sfn_erc20_tokens_tags

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.lake.id] }
      object = { key = [{ wildcard = "landing/erc20_tokens/*.csv" }] }
    }
  })
}

resource "aws_iam_role" "eventbridge_sfn_erc20_tokens" {
  name = "erc20-tokens-on-push-events-role"
  tags = local.sfn_erc20_tokens_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_sfn_erc20_tokens" {
  name = "start-erc20-tokens-on-push"
  role = aws_iam_role.eventbridge_sfn_erc20_tokens.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.erc20_tokens_on_push.arn
    }]
  })
}

resource "aws_cloudwatch_event_target" "erc20_tokens_on_push" {
  rule     = aws_cloudwatch_event_rule.erc20_tokens_on_push.name
  arn      = aws_sfn_state_machine.erc20_tokens_on_push.arn
  role_arn = aws_iam_role.eventbridge_sfn_erc20_tokens.arn
  # Full event passes through: the state machine reads $.detail.bucket/object.
}
```

- [ ] **Step 2: Validate and plan**

Run: `cd terraform && terraform validate && terraform plan`
Expected: validate OK; plan shows ONLY adds (2 IAM roles, 2 inline policies, state machine, event rule, event target). Read the whole plan; nothing may be destroyed.

- [ ] **Step 3: Apply**

Run: `cd terraform && terraform apply` (approve after re-reading the summary)
Expected: `Apply complete`. Verify: `aws stepfunctions list-state-machines --query "stateMachines[?name=='erc20-tokens-on-push']"` returns the machine.

- [ ] **Step 4: Commit**

```bash
git add terraform/step_functions_erc20_tokens.tf
git commit -m "feat: erc20-tokens-on-push state machine and eventbridge rule"
```

---

### Task 5: dbt — source, model and tests

**Files:**
- Modify: `dbt/models/silver/sources.yml` (add `erc20_tokens` under the `bronze` source)
- Create: `dbt/models/silver/dim_erc20_tokens.sql`
- Modify: `dbt/models/silver/schema.yml` (add `dim_erc20_tokens` entry)
- Create: `dbt/tests/assert_dim_erc20_tokens_unique_key.sql`
- Create: `dbt/tests/assert_dim_erc20_tokens_not_empty.sql`

**Interfaces:**
- Consumes: Glue table `bronze.erc20_tokens` (Task 3), existing macro `max_partition_dt(source)`.
- Produces: model `dim_erc20_tokens` (schema `silver`), selected by `source:bronze.erc20_tokens+`.

- [ ] **Step 1: Add the source to `dbt/models/silver/sources.yml`**

Append under the `bronze` source's `tables:` list (after the `contracts` entry):

```yaml
      - name: erc20_tokens
        description: >
          Hand-curated ERC-20 payment-token list ingested from reference/
          via landing/ -> Lambda -> bronze snapshot. Partitions registered
          explicitly by the Lambda.
        columns:
          - name: chain_id
            description: EIP-155 numeric chain id (1=ethereum, 137=polygon).
          - name: contract_address
            description: >
              Token contract address, lowercase. The zero address is the
              native-ETH placeholder used by marketplace trades paid in ETH.
          - name: name
            description: Human-readable token name.
          - name: fsym
            description: >
              Price-source ticker. Informative only, NOT unique — GALA v1
              and v2 share it; joins always use (chain_id, contract_address).
          - name: decimals
            description: >
              ERC-20 decimals for raw-amount conversion
              (amount = raw / 10^decimals). Not always 18: USDC/USDT use 6;
              GALA, CUBE and GMT use 8.
          - name: dt
            description: Snapshot date partition (YYYY-MM-DD).
```

- [ ] **Step 2: Write `dbt/models/silver/dim_erc20_tokens.sql`**

```sql
-- Curated payment-token dimension. Single source of truth:
-- bronze.erc20_tokens (the hand-curated CSV). Grain: one row per
-- (chain_id, contract_address). fsym is informative and NOT unique
-- (GALA v1/v2 share it); joins use the natural key.
--
-- Table, not view: Athena views cannot reference $partitions, and a table
-- pins the latest snapshot at build time (the build runs after every
-- ingestion, so it never goes stale).
{{ config(materialized='table') }}

SELECT
    t.chain_id,
    t.contract_address,
    t.name,
    t.fsym,
    t.decimals,
    t.dt AS snapshot_dt
FROM {{ source('bronze', 'erc20_tokens') }} t
WHERE t.dt = {{ max_partition_dt(source('bronze', 'erc20_tokens')) }}
```

- [ ] **Step 3: Add the model to `dbt/models/silver/schema.yml`**

Append to the `models:` list:

```yaml
  - name: dim_erc20_tokens
    description: >
      Curated ERC-20 payment-token dimension from the latest
      bronze.erc20_tokens snapshot. One row per
      (chain_id, contract_address); fsym is NOT unique (GALA v1/v2).
    columns:
      - name: chain_id
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: [1, 137]
                quote: false
      - name: contract_address
        data_tests:
          - not_null
      - name: name
        data_tests:
          - not_null
      - name: fsym
        data_tests:
          - not_null
      - name: decimals
        data_tests:
          - not_null
      - name: snapshot_dt
        data_tests:
          - not_null
```

- [ ] **Step 4: Write the singular tests**

`dbt/tests/assert_dim_erc20_tokens_unique_key.sql`:

```sql
-- Fails when (chain_id, contract_address) is not unique in the dimension.
SELECT
    chain_id,
    contract_address,
    COUNT(*) AS n
FROM {{ ref('dim_erc20_tokens') }}
GROUP BY chain_id, contract_address
HAVING COUNT(*) > 1
```

`dbt/tests/assert_dim_erc20_tokens_not_empty.sql`:

```sql
-- Fails when the dimension is empty: guards against max(dt) over a table
-- with no partitions silently producing zero rows.
SELECT n
FROM (
    SELECT COUNT(*) AS n
    FROM {{ ref('dim_erc20_tokens') }}
)
WHERE n = 0
```

- [ ] **Step 5: Parse the project**

Run: `cd dbt && dbt parse`
Expected: no errors; `dbt ls --select source:bronze.erc20_tokens+` lists `dim_erc20_tokens` and its tests.

- [ ] **Step 6: Rebuild and push the run-dbt image, redeploy**

The run-dbt Lambda ships `dbt/` inside its container image — new models require a rebuild:

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $ACCOUNT.dkr.ecr.us-east-1.amazonaws.com
docker build --platform linux/arm64 -f dbt_runner/Dockerfile -t $ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/decentraland-run-dbt:latest .
docker push $ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/decentraland-run-dbt:latest
cd terraform && terraform plan   # image digest change -> run-dbt redeploy; READ IT
terraform apply
```

Expected: plan shows the `run-dbt` Lambda `image_uri` update only; apply completes.

- [ ] **Step 7: Commit**

```bash
git add dbt/models/silver/sources.yml dbt/models/silver/dim_erc20_tokens.sql dbt/models/silver/schema.yml dbt/tests/assert_dim_erc20_tokens_unique_key.sql dbt/tests/assert_dim_erc20_tokens_not_empty.sql
git commit -m "feat: silver.dim_erc20_tokens model and tests"
```

---

### Task 6: Documentation, publish and end-to-end verification

**Files:**
- Modify: `reference/README.md` (new section)

**Interfaces:**
- Consumes: everything deployed in Tasks 3-5.
- Produces: the live `bronze.erc20_tokens` partition and `silver.dim_erc20_tokens` table.

- [ ] **Step 1: Add the README section**

Append to `reference/README.md`:

````markdown
## erc20_tokens.csv

ERC-20 tokens (plus the native-ETH placeholder) seen as payment currency in
Decentraland marketplace trades. Symbols and names were resolved from each
contract address via CoinGecko's contract lookup (Ethplorer for delisted
tokens); decimals were verified against DefiLlama responses.

- `chain_id`: EIP-155 numeric chain id. All rows are 1 (mainnet) today.
- `contract_address`: token contract, lowercase.
  `0x0000000000000000000000000000000000000000` is the native-ETH
  placeholder used when trades are paid in ETH.
- `name`: human-readable token name.
- `fsym`: price-source ticker. Informative only and NOT unique — GALA v1
  (`0x15d4…`) and GALA v2 (`0xd1d2…`) share `GALA`. Joins always use
  `(chain_id, contract_address)`.
- `decimals`: ERC-20 decimals for raw-amount conversion
  (`amount = raw / 10^decimals`). Not always 18: USDC/USDT use 6; GALA,
  CUBE and STEPN GMT use 8.

Publish:

```bash
aws s3 cp reference/erc20_tokens.csv \
  s3://decentraland-data-platform-<account_id>/landing/erc20_tokens/
```
````

- [ ] **Step 2: Publish the CSV**

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
aws s3 cp reference/erc20_tokens.csv s3://decentraland-data-platform-$ACCOUNT/landing/erc20_tokens/
```

- [ ] **Step 3: Watch the execution succeed**

```bash
SM_ARN=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='erc20-tokens-on-push'].stateMachineArn" --output text)
aws stepfunctions list-executions --state-machine-arn $SM_ARN --max-items 1
```

Expected: one execution, `status: SUCCEEDED` (poll until it leaves RUNNING).
If FAILED: read the execution history and the `load-erc20-tokens` /
`run-dbt` CloudWatch logs; a Slack alert should also have fired.

- [ ] **Step 4: Verify the data in Athena**

```bash
aws athena start-query-execution \
  --work-group decentraland-data-platform \
  --query-string "SELECT COUNT(*) AS n, COUNT(DISTINCT fsym) AS tickers FROM silver.dim_erc20_tokens" \
  --query 'QueryExecutionId' --output text
# then fetch results with aws athena get-query-results --query-execution-id <id>
```

Expected: `n = 22`, `tickers = 21` (GALA counted once).

- [ ] **Step 5: Run the full local suite one last time**

Run: `pytest tests/ && cd dbt && dbt parse && cd ../terraform && terraform validate`
Expected: everything green.

- [ ] **Step 6: Commit**

```bash
git add reference/README.md
git commit -m "docs: reference README section for erc20_tokens.csv"
```

- [ ] **Step 7: Final review checkpoint**

Stop here for the user's final review before any push/PR (repo flow:
feature branch + squash-merge PR; push only with explicit confirmation).
