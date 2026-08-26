# decode-ethereum-nft-transfers Lambda Implementation Plan (v2 — awswrangler)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and deploy `decode-ethereum-nft-transfers` as a **zip Lambda + managed AWSSDKPandas layer**: `wr.athena.read_sql_query` (UNLOAD approach) → pandas hex→decimal conversion (`token_id` up to 78 digits) → `wr.s3.to_parquet` with `overwrite_partitions` into `staging/ethereum_nft_transfers/dt=…/`.

**Architecture:** Athena does the set-based extraction (three event shapes flattened, TransferBatch exploded one row per token id); Python does only the record-level bignum conversion; awswrangler handles Athena polling, parquet IO and Glue partition registration. Idempotent per dt partition.

**Tech Stack:** Python zip Lambda (arm64) + managed layer `AWSSDKPandas-Python3xx-Arm64`, awswrangler 3.x, Terraform (`archive_file`), Athena workgroup `decentraland-data-platform`.

**Spec:** `docs/superpowers/specs/2026-08-26-decode-ethereum-nft-transfers-design.md`

## Global Constraints

- All artifacts in English. SQL keywords UPPERCASE, explicit `INNER JOIN`.
- Partition column `dt` (string `YYYY-MM-DD`), never `date`.
- Bucket never hardcoded in Terraform (`local.bucket_name`); Lambda reads `LAKE_BUCKET` env var.
- Cost tags: `component = "decode"`, `layer = "staging"`.
- Scan cap 1 GB/query: the query is always bounded by a constant `dt BETWEEN` range.
- Staging parquet filenames start with the run timestamp `YYYY-MM-DD_HH-MM-SS` (bronze convention; wrangler appends its own suffix).
- Commit locally; never `git push` without explicit user OK.
- The silver dbt model is NOT part of this plan.

---

### Task 1: SQL builder (`decode/query.py`) with unit tests

**Files:**
- Create: `decode/__init__.py` (empty)
- Create: `decode/query.py`
- Test: `tests/test_decode_nft_query.py`

**Interfaces:**
- Produces: `build_query(start_date: str, end_date: str) -> str` — plain SELECT (no UNLOAD wrapper: wrangler adds its own). Output columns, in order: `transaction_hash`, `log_index`, `block_timestamp`, `contract_address`, `erc_type`, `token_id_hex` (64-char hex, no 0x), `quantity_hex` (64-char hex, no 0x), `from_address`, `to_address`, `bronze_extracted_at`, `dt`.

- [x] **Step 1: Write failing tests**

```python
# tests/test_decode_nft_query.py
import pytest

from decode.query import build_query

TOPIC_721 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TOPIC_1155_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TOPIC_1155_BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"


def test_query_contains_all_topics_and_range():
    q = build_query("2018-06-01", "2018-06-05")
    assert TOPIC_721 in q and TOPIC_1155_SINGLE in q and TOPIC_1155_BATCH in q
    assert "BETWEEN '2018-06-01' AND '2018-06-05'" in q
    assert q.lstrip().startswith("WITH logs AS")


def test_query_rejects_bad_dates():
    for bad in ("2018-6-1", "20180601", "2018-06-01'; DROP", ""):
        with pytest.raises(ValueError):
            build_query(bad, "2018-06-05")
        with pytest.raises(ValueError):
            build_query("2018-06-01", bad)
```

- [x] **Step 2: Run tests, expect import failure**

Run: `.venv/bin/pytest tests/test_decode_nft_query.py -v` → FAIL (module not found).

- [x] **Step 3: Implement `decode/query.py`**

Same SQL as validated in chat (WITH logs / transfers / single_transfers / pre_batch_transfers / pre2_batch_transfers / split_limiters / batch_transfers, UNION ALL), returned as a bare SELECT:

```python
"""Builds the Athena query that extracts and flattens ERC-721 and
ERC-1155 Transfer events for curated contracts.

token_id / quantity stay as raw 64-char hex: all numeric decoding happens
in pandas (Python ints are arbitrary precision; Athena tops out at 64-bit
integers / 38-digit decimals, while LAND token ids use the high 128 bits).
"""

import re

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

TOPIC_721_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TOPIC_1155_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TOPIC_1155_BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"

# One TransferBatch log can carry up to this many token ids.
MAX_BATCH_IDS = 4000


def build_query(start_date: str, end_date: str) -> str:
    for d in (start_date, end_date):
        if not _DT_RE.match(d):
            raise ValueError(f"invalid date {d!r}, expected YYYY-MM-DD")

    return f"""WITH logs AS (
    SELECT el.*, c.erc_type
    FROM "bronze"."ethereum_logs" AS el
    INNER JOIN "silver"."dim_contracts" AS c ON c.chain_id = 1
        AND c.contract_address = el.address
        AND (
            (c.erc_type = 721  AND el.topics[1] = '{TOPIC_721_TRANSFER}') OR
            (c.erc_type = 1155 AND el.topics[1] IN ('{TOPIC_1155_BATCH}', '{TOPIC_1155_SINGLE}'))
        )
    WHERE el.dt BETWEEN '{start_date}' AND '{end_date}'
),
transfers AS (
    SELECT transaction_hash,
        log_index,
        block_timestamp,
        address AS contract_address,
        erc_type,
        substr(topics[4], 3) AS token_id_hex,
        lpad('1', 64, '0') AS quantity_hex,
        concat('0x', substr(topics[2], 27)) AS from_address,
        concat('0x', substr(topics[3], 27)) AS to_address,
        extracted_at AS bronze_extracted_at,
        dt
    FROM logs
    WHERE topics[1] = '{TOPIC_721_TRANSFER}'
        AND cardinality(topics) = 4
),
single_transfers AS (
    SELECT transaction_hash,
        log_index,
        block_timestamp,
        address AS contract_address,
        erc_type,
        substr(data, 3, 64) AS token_id_hex,
        substr(data, 67, 64) AS quantity_hex,
        concat('0x', substr(topics[3], 27)) AS from_address,
        concat('0x', substr(topics[4], 27)) AS to_address,
        extracted_at AS bronze_extracted_at,
        dt
    FROM logs
    WHERE topics[1] = '{TOPIC_1155_SINGLE}'
        AND cardinality(topics) = 4
),
pre_batch_transfers AS (
    -- strip '0x' + the two offset words; layout left: [N][ids...][M][values...]
    SELECT transaction_hash,
        log_index,
        block_timestamp,
        address AS contract_address,
        erc_type,
        substr(data, 131, length(data) - 130) AS data,
        concat('0x', substr(topics[3], 27)) AS from_address,
        concat('0x', substr(topics[4], 27)) AS to_address,
        extracted_at AS bronze_extracted_at,
        dt
    FROM logs
    WHERE topics[1] = '{TOPIC_1155_BATCH}'
        AND cardinality(topics) = 4
        AND length(data) >= 386
        AND substr(data, 3, 64) = lpad('40', 64, '0')
),
pre2_batch_transfers AS (
    -- ids and values arrays always have equal length, so each half is
    -- [count word][elements]; drop the count word from each half
    SELECT transaction_hash,
        log_index,
        block_timestamp,
        contract_address,
        erc_type,
        substr(data, 65, length(data) / 2 - 64) AS token_ids,
        substr(data, 65 + length(data) / 2, length(data) / 2 - 64) AS quantities,
        from_address,
        to_address,
        bronze_extracted_at,
        dt
    FROM pre_batch_transfers
),
split_limiters AS (
    SELECT CAST(_limit AS integer) AS _limit
    FROM UNNEST(sequence(1, 64 * {MAX_BATCH_IDS}, 64)) AS t(_limit)
),
batch_transfers AS (
    SELECT p.transaction_hash,
        p.log_index,
        p.block_timestamp,
        p.contract_address,
        p.erc_type,
        substr(p.token_ids, s._limit, 64) AS token_id_hex,
        substr(p.quantities, s._limit, 64) AS quantity_hex,
        p.from_address,
        p.to_address,
        p.bronze_extracted_at,
        p.dt
    FROM pre2_batch_transfers AS p
    INNER JOIN split_limiters AS s ON length(p.token_ids) > s._limit - 1
)
SELECT * FROM transfers
UNION ALL
SELECT * FROM single_transfers
UNION ALL
SELECT * FROM batch_transfers"""
```

- [x] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_decode_nft_query.py -v` → PASS.

- [x] **Step 5: Commit**

```bash
git add decode/__init__.py decode/query.py tests/test_decode_nft_query.py
git commit -m "feat: extraction query builder for decode-ethereum-nft-transfers"
```

---

### Task 2: handler (`decode/handler.py`) with unit tests

**Files:**
- Create: `decode/handler.py`
- Modify: `requirements-dev.txt` (add `pandas==2.3.*` if absent, `awswrangler==3.*`)
- Test: `tests/test_decode_nft_handler.py`

**Interfaces:**
- Consumes: `build_query` from Task 1.
- Produces: `parse_event(event) -> (start, end)`, `postprocess(df) -> df` (pure), `handler(event, context)`. Staging columns: `transaction_hash, log_index, block_timestamp, contract_address, erc_type, token_id, quantity, from_address, to_address, bronze_extracted_at, decoded_at, dt`.

- [x] **Step 1: Write failing tests**

```python
# tests/test_decode_nft_handler.py
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd
import pytest

from decode.handler import parse_event, postprocess

# LAND-style id: x=10 in the high 128 bits, y=20 low -> needs bignum
LAND_HEX = format((10 << 128) | 20, "064x")


def _df(**overrides):
    base = {
        "transaction_hash": ["0xaa"],
        "log_index": [1],
        "block_timestamp": [pd.Timestamp("2018-06-01 10:00:00")],
        "contract_address": ["0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d"],
        "erc_type": [721],
        "token_id_hex": [LAND_HEX],
        "quantity_hex": ["1".zfill(64)],
        "from_address": ["0x" + "1" * 40],
        "to_address": ["0x" + "2" * 40],
        "bronze_extracted_at": [pd.Timestamp("2026-08-25 18:00:00")],
        "dt": ["2018-06-01"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def test_postprocess_token_id_exact_decimal_string():
    out = postprocess(_df())
    assert out.loc[0, "token_id"] == str((10 << 128) | 20)  # 39-digit exact
    assert out.loc[0, "quantity"] == Decimal(1)
    assert "token_id_hex" not in out.columns and "quantity_hex" not in out.columns
    assert "decoded_at" in out.columns


def test_postprocess_max_uint256_is_78_digits():
    out = postprocess(_df(token_id_hex=["f" * 64]))
    assert len(out.loc[0, "token_id"]) == 78


def test_postprocess_rejects_quantity_over_decimal38():
    with pytest.raises(ValueError, match="quantity"):
        postprocess(_df(quantity_hex=[format(10**38, "064x")]))


def test_parse_event_default_is_utc_today_minus_2():
    start, end = parse_event({})
    expected = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
    assert start == end == expected


def test_parse_event_range():
    assert parse_event({"start_date": "2018-06-01", "end_date": "2018-06-05"}) == (
        "2018-06-01",
        "2018-06-05",
    )
    with pytest.raises(ValueError):
        parse_event({"start_date": "2018-06-05", "end_date": "2018-06-01"})
```

- [x] **Step 2: Install dev deps, run tests, expect import failure**

Run: `.venv/bin/pip install "awswrangler==3.*" "pandas==2.3.*"` then `.venv/bin/pytest tests/test_decode_nft_handler.py -v` → FAIL (no `decode.handler`).

- [x] **Step 3: Implement `decode/handler.py`**

```python
"""decode-ethereum-nft-transfers Lambda: bronze -> staging.

wr.athena.read_sql_query runs the set-based extraction (see query.py);
this handler converts token_id/quantity from hex with Python's
arbitrary-precision ints and hands the result to wr.s3.to_parquet, which
overwrites only the touched dt partitions and registers them in Glue.

Event: {"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"} (inclusive).
Default: single day, UTC today-2. Raises on any failure so the caller
(manual invoke today, Step Functions later) surfaces it.
"""

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import awswrangler as wr
import pandas as pd

from decode.query import build_query

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")
GLUE_DATABASE = "staging"
GLUE_TABLE = "ethereum_nft_transfers"

# Athena decimal(38,0) ceiling for the quantity column
_MAX_DECIMAL38 = 10**38

_FINAL_COLUMNS = [
    "transaction_hash",
    "log_index",
    "block_timestamp",
    "contract_address",
    "erc_type",
    "token_id",
    "quantity",
    "from_address",
    "to_address",
    "bronze_extracted_at",
    "decoded_at",
    "dt",
]


def parse_event(event: dict) -> tuple[str, str]:
    event = event or {}
    default = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
    start = event.get("start_date") or default
    end = event.get("end_date") or start
    if start > end:
        raise ValueError(f"start_date {start} after end_date {end}")
    return start, end


def postprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["token_id"] = df["token_id_hex"].map(lambda h: str(int(h, 16)))
    quantities = df["quantity_hex"].map(lambda h: int(h, 16))
    too_big = quantities >= _MAX_DECIMAL38
    if too_big.any():
        rows = df.loc[too_big, ["transaction_hash", "log_index"]].to_dict("records")
        raise ValueError(f"quantity exceeds decimal(38,0) in rows: {rows[:5]}")
    # Decimal, not int: that is what pyarrow maps onto decimal128(38,0)
    df["quantity"] = quantities.map(Decimal)
    # floor to ms: the staging schema stores timestamp(ms) and pyarrow
    # refuses lossy casts from microseconds
    df["decoded_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("ms")
    return df[_FINAL_COLUMNS]


def handler(event, context):
    start, end = parse_event(event)
    bucket = os.environ["LAKE_BUCKET"]
    # Same filename convention as bronze: timestamp with dashes in the
    # time part (colons in S3 keys break URL handling).
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")

    df = wr.athena.read_sql_query(
        sql=build_query(start, end),
        database="bronze",
        workgroup=ATHENA_WORKGROUP,
        ctas_approach=False,
        unload_approach=True,
        s3_output=f"s3://{bucket}/athena-results/unload/nft_transfers/",
    )
    if df.empty:
        return {"start_date": start, "end_date": end, "rows_by_dt": {}}

    df = postprocess(df)
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/staging/ethereum_nft_transfers/",
        dataset=True,
        partition_cols=["dt"],
        mode="overwrite_partitions",
        database=GLUE_DATABASE,
        table=GLUE_TABLE,
        filename_prefix=f"{stamp}_",
        compression="snappy",
        dtype={"quantity": "decimal(38,0)"},
    )
    return {
        "start_date": start,
        "end_date": end,
        "rows_by_dt": df.groupby("dt").size().to_dict(),
    }
```

- [x] **Step 4: Run tests + ruff**

Run: `.venv/bin/pytest tests/test_decode_nft_query.py tests/test_decode_nft_handler.py -v` → PASS; `.venv/bin/ruff check decode/ tests/` → clean; `.venv/bin/pytest tests/ -q` → whole suite green.

- [x] **Step 5: Commit**

```bash
git add decode/handler.py tests/test_decode_nft_handler.py requirements-dev.txt
git commit -m "feat: decode-ethereum-nft-transfers handler (awswrangler + pandas bignum decode)"
```

---

### Task 3: Terraform — staging Glue DB + table, zip Lambda + managed layer

**Files:**
- Modify: `terraform/glue_athena.tf` (add `staging` database)
- Create: `terraform/table_staging_nft_transfers.tf`
- Create: `terraform/lambda_decode_nft.tf`

**Interfaces:**
- Consumes: `aws_s3_bucket.lake`, `aws_athena_workgroup.main`, glue databases, `local.bucket_name`, `local.base_tags`, `data.aws_caller_identity.current`.
- Produces: Lambda `decode-ethereum-nft-transfers` (zip, arm64, AWSSDKPandas layer), `staging` DB, `staging.ethereum_nft_transfers` table.

- [x] **Step 1: Resolve the newest managed layer for arm64/us-east-1**

The AWSSDKPandas layers are published by account `336392948345`. Find the newest Python runtime with an arm64 layer (try 3.13 first, fall back to 3.12):

```bash
aws lambda list-layer-versions --layer-name arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python313-Arm64 --query 'LayerVersions[0].LayerVersionArn' --output text
```

Use the returned ARN verbatim as `LAYER_ARN` below, and match `runtime` to its Python version (`python3.13` or `python3.12`).

- [x] **Step 2: Add the staging database to `terraform/glue_athena.tf`**

```hcl
resource "aws_glue_catalog_database" "staging" {
  name = "staging"
  tags = local.base_tags
}
```

- [x] **Step 3: Create `terraform/table_staging_nft_transfers.tf`**

```hcl
# Staging table written by the decode-ethereum-nft-transfers Lambda (awswrangler
# registers the dt partitions on each write). No partition projection, so
# the "$partitions" metadata convention keeps working.
resource "aws_glue_catalog_table" "staging_nft_transfers" {
  database_name = aws_glue_catalog_database.staging.name
  name          = "ethereum_nft_transfers"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/staging/ethereum_nft_transfers/"
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
      name = "contract_address"
      type = "string"
    }
    columns {
      name = "erc_type"
      type = "int"
    }
    columns {
      name = "token_id"
      type = "string"
    }
    columns {
      name = "quantity"
      type = "decimal(38,0)"
    }
    columns {
      name = "from_address"
      type = "string"
    }
    columns {
      name = "to_address"
      type = "string"
    }
    columns {
      name = "bronze_extracted_at"
      type = "timestamp"
    }
    columns {
      name = "decoded_at"
      type = "timestamp"
    }
  }
}
```

- [x] **Step 4: Create `terraform/lambda_decode_nft.tf`** (zip + layer; replace `LAYER_ARN` and `runtime` with Step 1's result)

```hcl
locals {
  decode_nft_tags = { component = "decode", layer = "staging" }
}

data "archive_file" "decode_nft" {
  type        = "zip"
  source_dir  = "${path.module}/../decode"
  output_path = "${path.module}/build/decode_nft.zip"
  excludes    = ["__pycache__"]
}

resource "aws_iam_role" "decode_nft" {
  name = "decode-ethereum-nft-transfers-role"
  tags = local.decode_nft_tags
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "decode_nft" {
  name = "athena-glue-s3"
  role = aws_iam_role.decode_nft.id
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
          aws_glue_catalog_database.staging.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/*",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/silver/*",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/staging/*",
        ]
      },
      {
        # overwrite_partitions deletes and re-registers dt partitions
        Effect = "Allow"
        Action = [
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.staging.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/staging/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      },
      {
        # read bronze + dim_contracts data for the extraction query
        Effect = "Allow"
        Action = "s3:GetObject"
        Resource = [
          "${aws_s3_bucket.lake.arn}/bronze/*",
          "${aws_s3_bucket.lake.arn}/silver/*",
        ]
      },
      {
        # wrangler UNLOAD scratch + staging output (overwrite needs delete)
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "${aws_s3_bucket.lake.arn}/athena-results/*",
          "${aws_s3_bucket.lake.arn}/staging/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "decode_nft_logs" {
  role       = aws_iam_role.decode_nft.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "decode_nft" {
  name              = "/aws/lambda/decode-ethereum-nft-transfers"
  retention_in_days = 7
  tags              = local.decode_nft_tags
}

resource "aws_lambda_function" "decode_nft" {
  function_name    = "decode-ethereum-nft-transfers"
  role             = aws_iam_role.decode_nft.arn
  filename         = data.archive_file.decode_nft.output_path
  source_code_hash = data.archive_file.decode_nft.output_base64sha256
  handler          = "decode.handler.handler"
  runtime          = "python3.13" # match the layer's Python version
  architectures    = ["arm64"]
  timeout          = 300
  memory_size      = 2048
  layers           = ["LAYER_ARN"] # AWSSDKPandas managed layer from Step 1
  tags             = local.decode_nft_tags

  environment {
    variables = {
      LAKE_BUCKET      = aws_s3_bucket.lake.id
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }
}
```

- [x] **Step 5: Plan and apply**

```bash
cd terraform
terraform plan   # READ the plan: expect ~6 to add, 0 destroy
terraform apply
```

- [x] **Step 6: Commit**

```bash
git add terraform/glue_athena.tf terraform/table_staging_nft_transfers.tf terraform/lambda_decode_nft.tf
git commit -m "infra: decode-ethereum-nft-transfers zip Lambda (AWSSDKPandas layer), staging Glue DB and table"
```

---

### Task 4: Smoke run and verification

**Files:** none.

**Interfaces:**
- Consumes: deployed Lambda. Known-good day: `2026-08-20` (verified in v1: 5,566 transfers — 5,070 ERC-721 + 496 ERC-1155). Note 2018 days have zero standard-721 events (legacy LAND signatures, see spec).

- [x] **Step 1: Invoke for 2026-08-20**

```bash
aws lambda invoke --function-name decode-ethereum-nft-transfers \
  --payload '{"start_date": "2026-08-20", "end_date": "2026-08-20"}' \
  --cli-binary-format raw-in-base64-out --cli-read-timeout 300 /dev/stdout
```

Expected: `rows_by_dt = {"2026-08-20": 5566}` (same count as v1).

- [x] **Step 2: Verify in Athena**

```sql
SELECT erc_type, COUNT(*) AS transfers, MAX(length(token_id)) AS max_digits
FROM staging.ethereum_nft_transfers WHERE dt = '2026-08-20' GROUP BY erc_type
```

Expected: 721 → 5070, 1155 → 496, max_digits 78.

- [x] **Step 3: Idempotency — re-invoke, then check grain**

Re-run Step 1, then:

```sql
SELECT COUNT(*) AS total,
       COUNT(DISTINCT concat(transaction_hash, CAST(log_index AS varchar), token_id)) AS uniq
FROM staging.ethereum_nft_transfers WHERE dt = '2026-08-20'
```

Expected: total = uniq = 5566, and `aws s3 ls` of the dt prefix shows only the newest timestamped file set.

- [x] **Step 4: Cross-check one token id against bronze**

Join staging with bronze on (transaction_hash, log_index) for one 721 row and confirm in Python that `token_id == str(int(topics[4][2:], 16))`.

- [x] **Step 5: Commit docs**

```bash
git add docs/superpowers/plans/2026-08-26-decode-ethereum-nft-transfers.md
git commit -m "docs: decode-ethereum-nft-transfers plan (v2 awswrangler) executed and verified"
```
