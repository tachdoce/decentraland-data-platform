# nft_contracts Ingestion (Phase 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Event-driven ingestion of the curated contract dictionary: uploading `reference/nft_contracts.csv` to `landing/nft_contracts/` triggers a Lambda that validates it and writes a dated parquet snapshot to `bronze.nft_contracts`.

**Architecture:** An S3 Event Notification (prefix `landing/nft_contracts/`, suffix `.csv`) invokes a zip-packaged Python 3.13 Lambda. Pure validation/parsing lives in `validate.py` (tested against the real CSV, no mocks); the handler reads the uploaded object, validates it whole-file (reject on any defect, no partial writes), and writes one parquet per day. A Glue table with dt partition projection makes it queryable instantly.

**Tech Stack:** Terraform (AWS provider ~> 6.0, archive provider already configured), Python 3.13, pyarrow via AWSSDKPandas-Python313:9 layer (same as Phase 1), boto3, pytest.

**Spec:** `docs/superpowers/specs/2026-08-21-nft-contracts-ingestion.md` (component) and `docs/superpowers/specs/2026-08-20-platform-architecture.md` (conventions §3, ops §8, FinOps §9).

## Global Constraints

- Region us-east-1; everything via Terraform. NOTE: `terraform plan/apply` are blocked by the permission classifier in this environment — the implementer writes and validates HCL (`terraform validate` and `fmt` are allowed; if validate is also blocked, report it and skip); the controller/user runs plan+apply at the checkpoints marked below.
- CSV header must be exactly: `chain_id,contract_address,contract_name,first_mint_dt,extract_from_dt`.
- Valid chain_ids: `{1, 137}`. Addresses: regex `^0x[0-9a-f]{40}$` (lowercase enforced, not coerced — the reference file is already clean; a non-lowercase address is a validation error). Dates: ISO `YYYY-MM-DD`, parseable. Duplicate `(chain_id, contract_address)` pairs are an error. Empty `contract_name` is allowed.
- Output: ONE parquet at `bronze/nft_contracts/dt=<UTC date>/contracts.parquet`; rerun same day overwrites (idempotent).
- Parquet schema: `chain_id` int32, `contract_address` string, `contract_name` string, `first_mint_dt` date32, `extract_from_dt` date32.
- Tags: component resources `{component="ingestion-nft-contracts", layer="bronze"}` (provider default_tags add project/managed_by).
- IAM: `s3:GetObject` on `landing/nft_contracts/*` + `s3:PutObject` on `bronze/nft_contracts/*` + basic logging. Nothing else.
- Log retention 7 days. No EventBridge schedule — the S3 event is the trigger.
- All artifacts in English. Tests run with `PYTHONPATH=. .venv/bin/pytest` from repo root (venv exists from Phase 1 with pytest+pyarrow+boto3).

---

### Task 1: Validation module (pure functions, TDD)

**Files:**
- Create: `ingestion/nft_contracts/__init__.py` (empty)
- Create: `ingestion/nft_contracts/validate.py`
- Create: `tests/test_nft_contracts.py`

**Interfaces:**
- Produces (used by Task 2's handler):
  - `validate.EXPECTED_HEADER: list[str]` — the 5 column names in order
  - `validate.parse_and_validate(csv_bytes: bytes) -> list[dict]` — rows with keys `chain_id` (int), `contract_address` (str), `contract_name` (str), `first_mint_dt` (datetime.date), `extract_from_dt` (datetime.date); raises `ValueError` with a descriptive message on any defect
  - `validate.partition_key(run_date: datetime.date) -> str` — `"bronze/nft_contracts/dt=YYYY-MM-DD/contracts.parquet"`

- [ ] **Step 1: Write the failing tests**

`tests/test_nft_contracts.py`:

```python
import datetime
from pathlib import Path

import pytest

from ingestion.nft_contracts.validate import (
    EXPECTED_HEADER,
    parse_and_validate,
    partition_key,
)

FIXTURE_BYTES = (
    Path(__file__).parent.parent / "reference" / "nft_contracts.csv"
).read_bytes()


def test_real_reference_file_parses():
    rows = parse_and_validate(FIXTURE_BYTES)
    assert len(rows) == 830
    assert set(r["chain_id"] for r in rows) == {1, 137}
    first = rows[0]
    assert isinstance(first["chain_id"], int)
    assert isinstance(first["first_mint_dt"], datetime.date)
    assert isinstance(first["extract_from_dt"], datetime.date)


def test_default_sentinels_present():
    rows = parse_and_validate(FIXTURE_BYTES)
    assert all(r["first_mint_dt"] == datetime.date(2001, 1, 1) for r in rows)
    assert all(r["extract_from_dt"] == datetime.date(2026, 1, 1) for r in rows)


def test_empty_contract_name_is_allowed():
    rows = parse_and_validate(FIXTURE_BYTES)
    assert any(r["contract_name"] == "" for r in rows)  # 12 pending-curation rows


VALID_LINE = "1,0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,Bored Ape Yacht Club,2001-01-01,2026-01-01"
HEADER = "chain_id,contract_address,contract_name,first_mint_dt,extract_from_dt"


def _csv(*lines):
    return ("\n".join(lines) + "\n").encode()


@pytest.mark.parametrize(
    "bad_csv,reason",
    [
        (_csv("chain_id,address,name,a,b", VALID_LINE), "renamed columns"),
        (_csv(HEADER.rsplit(",", 1)[0], "1,0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,X,2001-01-01"), "missing column"),
        (_csv(HEADER, "99,0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,X,2001-01-01,2026-01-01"), "unknown chain_id"),
        (_csv(HEADER, "1,0xBC4CA0EDA7647A8AB7C2061C2E118A18A936F13D,X,2001-01-01,2026-01-01"), "uppercase address"),
        (_csv(HEADER, "1,0x1234,X,2001-01-01,2026-01-01"), "short address"),
        (_csv(HEADER, "1,0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,X,20010101,2026-01-01"), "bad date format"),
        (_csv(HEADER, VALID_LINE, VALID_LINE), "duplicate (chain_id, address)"),
        (_csv(HEADER), "no data rows"),
        (b"", "empty file"),
    ],
)
def test_rejects_bad_input(bad_csv, reason):
    with pytest.raises(ValueError):
        parse_and_validate(bad_csv)


def test_same_address_on_two_chains_is_not_a_duplicate():
    line137 = VALID_LINE.replace("1,0x", "137,0x", 1)
    rows = parse_and_validate(_csv(HEADER, VALID_LINE, line137))
    assert len(rows) == 2


def test_partition_key_format():
    key = partition_key(datetime.date(2026, 8, 21))
    assert key == "bronze/nft_contracts/dt=2026-08-21/contracts.parquet"


def test_expected_header():
    assert EXPECTED_HEADER == [
        "chain_id",
        "contract_address",
        "contract_name",
        "first_mint_dt",
        "extract_from_dt",
    ]
```

- [ ] **Step 2: Create the package files and run tests to verify they fail**

```bash
touch ingestion/nft_contracts/__init__.py
PYTHONPATH=. .venv/bin/pytest tests/test_nft_contracts.py -v
```
Expected: collection error — `ModuleNotFoundError` / `ImportError` on `ingestion.nft_contracts.validate`.

- [ ] **Step 3: Write the implementation**

`ingestion/nft_contracts/validate.py`:

```python
"""Pure validation/parsing for the curated nft_contracts CSV.

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
    "contract_name",
    "first_mint_dt",
    "extract_from_dt",
]

KNOWN_CHAIN_IDS = {1, 137}
ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")


def parse_and_validate(csv_bytes: bytes) -> list[dict]:
    text = csv_bytes.decode("utf-8")
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
            raise ValueError(f"line {lineno}: expected {len(EXPECTED_HEADER)} fields, got {len(raw)}")
        chain_raw, address, name, first_mint_raw, extract_from_raw = raw

        try:
            chain_id = int(chain_raw)
        except ValueError:
            raise ValueError(f"line {lineno}: chain_id is not an integer: {chain_raw!r}")
        if chain_id not in KNOWN_CHAIN_IDS:
            raise ValueError(f"line {lineno}: unknown chain_id {chain_id} (known: {sorted(KNOWN_CHAIN_IDS)})")

        if not ADDRESS_RE.match(address):
            raise ValueError(f"line {lineno}: invalid address (must be 0x + 40 lowercase hex): {address!r}")

        try:
            first_mint = datetime.date.fromisoformat(first_mint_raw)
            extract_from = datetime.date.fromisoformat(extract_from_raw)
        except ValueError:
            raise ValueError(f"line {lineno}: dates must be YYYY-MM-DD, got {first_mint_raw!r} / {extract_from_raw!r}")

        key = (chain_id, address)
        if key in seen:
            raise ValueError(f"line {lineno}: duplicate (chain_id, contract_address): {key}")
        seen.add(key)

        rows.append(
            {
                "chain_id": chain_id,
                "contract_address": address,
                "contract_name": name,
                "first_mint_dt": first_mint,
                "extract_from_dt": extract_from,
            }
        )

    if not rows:
        raise ValueError("CSV has a header but no data rows")
    return rows


def partition_key(run_date: datetime.date) -> str:
    return f"bronze/nft_contracts/dt={run_date.isoformat()}/contracts.parquet"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=. .venv/bin/pytest tests/test_nft_contracts.py -v`
Expected: 14 passed (5 named tests + 9 parametrized rejections).

- [ ] **Step 5: Commit**

```bash
git add ingestion/nft_contracts/ tests/test_nft_contracts.py
git commit -m "feat: nft_contracts validation module with whole-file semantics"
```

---

### Task 2: Lambda handler (S3-event driven)

**Files:**
- Create: `ingestion/nft_contracts/handler.py`
- Create: `ingestion/nft_contracts/requirements.txt`
- Test: `tests/test_nft_contracts.py` (append two tests)

**Interfaces:**
- Consumes: `validate.parse_and_validate`, `validate.partition_key` (Task 1).
- Produces: `handler.handler(event, context) -> dict` with keys `rows`, `s3_key`, `source_key`; `handler.rows_to_parquet(rows) -> bytes`. Terraform (Task 3) points at `ingestion.nft_contracts.handler.handler`. Reads bucket/key from the S3 event record — no env vars needed (bucket comes from the event; output goes to the same bucket).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_nft_contracts.py`:

```python
def test_rows_to_parquet_roundtrip(tmp_path):
    import pyarrow.parquet as pq

    from ingestion.nft_contracts.handler import rows_to_parquet

    rows = parse_and_validate(FIXTURE_BYTES)
    out = tmp_path / "contracts.parquet"
    out.write_bytes(rows_to_parquet(rows))
    table = pq.read_table(out)
    assert table.column_names == [
        "chain_id",
        "contract_address",
        "contract_name",
        "first_mint_dt",
        "extract_from_dt",
    ]
    assert table.num_rows == 830
    assert str(table.schema.field("chain_id").type) == "int32"
    assert str(table.schema.field("first_mint_dt").type) == "date32[day]"
    assert str(table.schema.field("extract_from_dt").type) == "date32[day]"


def test_handler_reads_event_and_writes_partition(monkeypatch, tmp_path):
    import ingestion.nft_contracts.handler as h

    written = {}

    class FakeS3:
        def get_object(self, Bucket, Key):
            import io as _io
            return {"Body": _io.BytesIO(FIXTURE_BYTES)}

        def put_object(self, Bucket, Key, Body):
            written["bucket"], written["key"], written["body"] = Bucket, Key, Body

    monkeypatch.setattr(h.boto3, "client", lambda service: FakeS3())
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=. .venv/bin/pytest tests/test_nft_contracts.py -v -k "parquet or handler"`
Expected: FAIL — `ModuleNotFoundError`/`ImportError` on `ingestion.nft_contracts.handler`.

- [ ] **Step 3: Write the handler**

`ingestion/nft_contracts/handler.py`:

```python
"""load-nft-contracts Lambda: landing/ CSV upload -> bronze/nft_contracts snapshot.

Triggered by S3 ObjectCreated events (prefix landing/nft_contracts/, suffix .csv).
"""

import datetime
import io
import urllib.parse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.nft_contracts.validate import parse_and_validate, partition_key

SCHEMA = pa.schema(
    [
        ("chain_id", pa.int32()),
        ("contract_address", pa.string()),
        ("contract_name", pa.string()),
        ("first_mint_dt", pa.date32()),
        ("extract_from_dt", pa.date32()),
    ]
)


def rows_to_parquet(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def handler(event, context):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    # S3 URL-encodes keys in event payloads (spaces become '+')
    source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    s3 = boto3.client("s3")
    body = s3.get_object(Bucket=bucket, Key=source_key)["Body"].read()
    rows = parse_and_validate(body)

    run_date = datetime.datetime.now(datetime.timezone.utc).date()
    out_key = partition_key(run_date)
    s3.put_object(Bucket=bucket, Key=out_key, Body=rows_to_parquet(rows))

    print(f"validated {len(rows)} rows from s3://{bucket}/{source_key}; wrote s3://{bucket}/{out_key}")
    return {"rows": len(rows), "s3_key": out_key, "source_key": source_key}
```

`ingestion/nft_contracts/requirements.txt`:

```
# provided by AWSSDKPandas Lambda layer: pyarrow
# provided by Lambda runtime: boto3
```

- [ ] **Step 4: Run the full test file**

Run: `PYTHONPATH=. .venv/bin/pytest tests/test_nft_contracts.py -v`
Expected: 16 passed.

- [ ] **Step 5: Commit**

```bash
git add ingestion/nft_contracts/ tests/test_nft_contracts.py
git commit -m "feat: nft_contracts S3-event Lambda handler with parquet snapshot"
```

---

### Task 3: Terraform — Lambda, S3 trigger, Glue table

**Files:**
- Create: `terraform/lambda_nft_contracts.tf`
- Create: `terraform/table_nft_contracts.tf`

**Interfaces:**
- Consumes: `aws_s3_bucket.lake`, `local.bucket_name`, `aws_glue_catalog_database.bronze` (Phase 1); handler `ingestion.nft_contracts.handler.handler` (Task 2).
- Produces: Lambda `load-nft-contracts`, S3 notification on the lake bucket, Glue table `bronze.nft_contracts`.

- [ ] **Step 1: Write `terraform/lambda_nft_contracts.tf`**

```hcl
locals {
  nft_contracts_tags = { component = "ingestion-nft-contracts", layer = "bronze" }
}

data "archive_file" "nft_contracts_zip" {
  type        = "zip"
  output_path = "${path.module}/build/nft_contracts.zip"

  source {
    content  = file("${path.module}/../ingestion/nft_contracts/handler.py")
    filename = "ingestion/nft_contracts/handler.py"
  }
  source {
    content  = file("${path.module}/../ingestion/nft_contracts/validate.py")
    filename = "ingestion/nft_contracts/validate.py"
  }
  source {
    content  = ""
    filename = "ingestion/__init__.py"
  }
  source {
    content  = ""
    filename = "ingestion/nft_contracts/__init__.py"
  }
}

resource "aws_iam_role" "nft_contracts" {
  name = "load-nft-contracts-role"
  tags = local.nft_contracts_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "nft_contracts_s3" {
  name = "landing-read-bronze-write"
  role = aws_iam_role.nft_contracts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/landing/nft_contracts/*"
      },
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/nft_contracts/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "nft_contracts_logs" {
  role       = aws_iam_role.nft_contracts.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "nft_contracts" {
  name              = "/aws/lambda/load-nft-contracts"
  retention_in_days = 7
  tags              = local.nft_contracts_tags
}

resource "aws_lambda_function" "nft_contracts" {
  function_name = "load-nft-contracts"
  role          = aws_iam_role.nft_contracts.arn
  tags          = local.nft_contracts_tags

  filename         = data.archive_file.nft_contracts_zip.output_path
  source_code_hash = data.archive_file.nft_contracts_zip.output_base64sha256

  handler     = "ingestion.nft_contracts.handler.handler"
  runtime     = "python3.13"
  timeout     = 60
  memory_size = 512
  layers      = [local.sdk_pandas_layer_arn] # defined in lambda_dcl_contracts.tf

  depends_on = [aws_cloudwatch_log_group.nft_contracts]
}

resource "aws_lambda_permission" "nft_contracts_s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.nft_contracts.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.lake.arn
}

# NOTE: a bucket supports ONE aws_s3_bucket_notification resource. Future
# landing/ triggers are added as additional lambda_function blocks HERE.
resource "aws_s3_bucket_notification" "lake" {
  bucket = aws_s3_bucket.lake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.nft_contracts.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "landing/nft_contracts/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.nft_contracts_s3]
}
```

- [ ] **Step 2: Write `terraform/table_nft_contracts.tf`**

```hcl
resource "aws_glue_catalog_table" "nft_contracts" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "nft_contracts"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"             = "parquet"
    "projection.enabled"         = "true"
    "projection.dt.type"         = "date"
    "projection.dt.format"       = "yyyy-MM-dd"
    "projection.dt.range"        = "2026-08-01,NOW"
    "storage.location.template"  = "s3://${local.bucket_name}/bronze/nft_contracts/dt=$${dt}/"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/bronze/nft_contracts/"
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
    columns {
      name = "first_mint_dt"
      type = "date"
    }
    columns {
      name = "extract_from_dt"
      type = "date"
    }
  }
}
```

- [ ] **Step 3: Format and validate (plan/apply are the controller's checkpoint)**

Run: `cd terraform && terraform fmt && terraform validate`
Expected: `Success! The configuration is valid.` (If `terraform validate` is blocked by the classifier, note it in the report and rely on fmt + review.)

- [ ] **Step 4: Commit**

```bash
git add terraform/lambda_nft_contracts.tf terraform/table_nft_contracts.tf
git commit -m "feat: load-nft-contracts Lambda, S3 landing trigger, bronze.nft_contracts table"
```

**CONTROLLER CHECKPOINT after this task:** the user (or controller if permitted) runs `terraform plan` (expect ~9 to add: role, policy, attachment, log group, function, permission, bucket notification, glue table — plus the zip data source) and `terraform apply` before Task 4 can run.

---

### Task 4: End-to-end verification (acceptance criteria from the spec)

**Files:** none (verification only). Requires Task 3 applied.

- [ ] **Step 1: Publish the CSV and watch the trigger fire**

```bash
BUCKET=$(cd terraform && terraform output -raw lake_bucket_name 2>/dev/null || echo "decentraland-data-platform-683569194224")
aws s3 cp reference/nft_contracts.csv "s3://$BUCKET/landing/nft_contracts/"
sleep 10
aws logs tail /aws/lambda/load-nft-contracts --since 2m
```
Expected: log line `validated 830 rows from s3://.../landing/nft_contracts/nft_contracts.csv; wrote s3://.../bronze/nft_contracts/dt=<today>/contracts.parquet`. No FunctionError, no manual invocation anywhere.

- [ ] **Step 2: Verify the parquet landed and re-upload is idempotent**

```bash
aws s3 ls "s3://$BUCKET/bronze/nft_contracts/" --recursive
aws s3 cp reference/nft_contracts.csv "s3://$BUCKET/landing/nft_contracts/"
sleep 10
aws s3 ls "s3://$BUCKET/bronze/nft_contracts/" --recursive
```
Expected: exactly ONE object for today's partition, both times.

- [ ] **Step 3: Verify from Athena (spec acceptance queries)**

```bash
run_query() {
  qid=$(aws athena start-query-execution --work-group decentraland-data-platform \
    --query-string "$1" --query QueryExecutionId --output text)
  until [ "$(aws athena get-query-execution --query-execution-id "$qid" --query 'QueryExecution.Status.State' --output text)" != "RUNNING" ]; do sleep 2; done
  aws athena get-query-results --query-execution-id "$qid" --query 'ResultSet.Rows[*].Data[*].VarCharValue'
}
run_query "SELECT count(*) FROM bronze.nft_contracts WHERE dt = '$(date -u +%F)'"
run_query "SELECT count(*) FROM bronze.nft_contracts WHERE dt = '$(date -u +%F)' AND first_mint_dt > DATE '2001-01-01'"
run_query "SELECT chain_id, count(*) FROM bronze.nft_contracts WHERE dt = '$(date -u +%F)' GROUP BY chain_id ORDER BY chain_id"
```
Expected: `830`; `0` (all sentinels); `1 → 815`, `137 → 15`.

- [ ] **Step 4: Verify validation rejects a broken upload**

```bash
printf 'chain_id,address,name\n1,0xdead,X\n' > /tmp/broken.csv
aws s3 cp /tmp/broken.csv "s3://$BUCKET/landing/nft_contracts/broken.csv"
sleep 10
aws logs tail /aws/lambda/load-nft-contracts --since 2m | grep -i "bad header" && echo "REJECTED OK"
aws s3 ls "s3://$BUCKET/bronze/nft_contracts/" --recursive | wc -l
aws s3 rm "s3://$BUCKET/landing/nft_contracts/broken.csv"
```
Expected: the error log mentions the bad header; bronze still has the same object count as Step 2 (nothing new written); cleanup removes the broken file.

- [ ] **Step 5: Commit nothing — report results** (verification-only task; findings loop back to earlier tasks).

---

## Self-Review

- **Spec coverage**: flow steps 1-3 → Tasks 1-2; whole-file validation rules → Task 1 (each rule has a rejection test); table schema + projection → Task 3 Step 2; S3 notification + permission + scoped IAM + tags + retention → Task 3 Step 1; all 5 spec tests → Tasks 1-2; all 4 acceptance criteria → Task 4 Steps 1-4. Downstream silver models and the unified view are explicitly out of this phase.
- **Placeholders**: none; the terraform-apply block is a named controller checkpoint, not a TBD.
- **Type consistency**: `parse_and_validate` returns `list[dict]` with `datetime.date` values consumed by `rows_to_parquet` whose SCHEMA declares `pa.date32()` (pyarrow converts `datetime.date` natively); handler string `ingestion.nft_contracts.handler.handler` matches the zip layout; `local.sdk_pandas_layer_arn` reuses Phase 1's definition in `lambda_dcl_contracts.tf`.
