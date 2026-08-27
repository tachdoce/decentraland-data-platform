# Currency Transfers Decode Lambda Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode ERC-20 Transfer events from `bronze.ethereum_logs` into `staging.ethereum_currency_transfers` via a new zip Lambda, then delete the dbt silver model that did this in SQL.

**Architecture:** Same silhouette as `decode-ethereum-nft-transfers`: an Athena UNLOAD query does the set-based decode (one static data word), pandas converts `amount_hex` → decimal(38,0), and `wr.s3.to_parquet(mode="overwrite_partitions")` writes idempotently to staging. Terraform mirrors `lambda_decode_nft.tf` + `table_staging_nft_transfers.tf`.

**Tech Stack:** Python 3.13 ARM64 zip Lambda + AWSSDKPandas layer, awswrangler/Athena, Terraform, pytest.

**Spec:** `docs/superpowers/specs/2026-08-27-currency-transfers-decode-lambda-design.md`

## Global Constraints

- All artifacts in English; SQL keywords UPPERCASE; explicit `INNER JOIN` (no bare `JOIN`).
- Addresses lowercase; chains by numeric `chain_id`; partitions `dt=YYYY-MM-DD`.
- Bucket name never hardcoded: `decentraland-data-platform-${account_id}` via Terraform interpolation / `LAKE_BUCKET` env var.
- Always read `terraform plan` before `apply`. Never create resources via console.
- Overflow filter: drop amounts ≥ 2^96 in SQL (`substr(data, 3, 40) = '0…0'`); Python guard raises at ≥ 10^38.
- Run tests with the project venv: `.venv/bin/pytest`.
- Commit after each task; NEVER `git push` without explicit user confirmation.

**Note:** Task 1–2 test files and the fixture were already drafted in the working tree during design (`tests/fixtures/erc20_transfer_2021-08-15.json`, `tests/test_decode_currency_query.py`, `tests/test_decode_currency_handler.py`). If present, verify their content matches the plan instead of rewriting.

---

### Task 1: SQL builder `decode/currency_query.py`

**Files:**
- Create: `decode/currency_query.py`
- Test: `tests/test_decode_currency_query.py` (uses fixture `tests/fixtures/erc20_transfer_2021-08-15.json`, real bronze logs)

**Interfaces:**
- Produces: `build_query(start_date: str, end_date: str) -> str` (raises `ValueError` on non-`YYYY-MM-DD` input) and constant `TRANSFER_TOPIC`. The query returns columns: `transaction_hash, log_index, block_timestamp, token_address, from_address, to_address, amount_hex, bronze_extracted_at, dt`.

- [ ] **Step 1: Write the failing test**

`tests/test_decode_currency_query.py`:

```python
import json
from pathlib import Path

import pytest
from eth_abi import decode as abi_decode

from decode.currency_query import TRANSFER_TOPIC, build_query

FIXTURE = Path(__file__).parent / "fixtures" / "erc20_transfer_2021-08-15.json"


def test_query_filters_topic_cardinality_and_range():
    q = build_query("2021-08-01", "2021-08-05")
    assert TRANSFER_TOPIC in q
    assert "BETWEEN '2021-08-01' AND '2021-08-05'" in q
    assert '"bronze"."ethereum_logs"' in q
    assert "cardinality(topics) = 3" in q
    assert "length(data) = 66" in q
    # overflow-era filter: high 160 bits of the amount word must be zero
    assert f"substr(data, 3, 40) = '{'0' * 40}'" in q


def test_query_rejects_bad_dates():
    with pytest.raises(ValueError):
        build_query("2021/08/01", "2021-08-05")
    with pytest.raises(ValueError):
        build_query("2021-08-01", "not-a-date")


def test_sql_substr_offsets_match_eth_abi_on_real_logs():
    # Reproduce the query's substr arithmetic in Python (1-based, same
    # positions) and compare against eth_abi on real bronze logs.
    for record in json.loads(FIXTURE.read_text()):
        data, topics = record["data"], record["topics"]
        (amount,) = abi_decode(["uint256"], bytes.fromhex(data[2:]))
        # amount_hex: substr(data, 3, 64)
        assert int(data[2:66], 16) == amount
        # overflow filter: substr(data, 3, 40)
        assert data[2:42] == "0" * 40
        # from/to: concat('0x', substr(topics[n], 27))
        assert len(topics[1][26:]) == 40
        assert len(topics[2][26:]) == 40
```

The fixture holds 6 real ERC-20 Transfer logs pulled from
`bronze.ethereum_logs` `dt='2021-08-15'` (same record shape as
`wyvern_orders_matched_2021-08-15.json`: `transaction_hash`,
`log_index`, `address`, `topics`, `data`).

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_decode_currency_query.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'decode.currency_query'`

- [ ] **Step 3: Write the implementation**

`decode/currency_query.py`:

```python
"""Builds the Athena query that extracts ERC-20 Transfer events.

The whole decode fits in SQL: one static data word (value) and
from/to as indexed topics. Python only converts value hex ->
decimal (uint256 exceeds bigint). No join to dim_contracts:
untracked payment tokens are kept on purpose for sales parsing.
"""

import re

TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def build_query(start_date: str, end_date: str) -> str:
    for d in (start_date, end_date):
        if not _DT_RE.match(d):
            raise ValueError(f"invalid date {d!r}, expected YYYY-MM-DD")

    # cardinality = 3: ERC-721 Transfer shares topic0 but has 4 topics.
    # substr(data, 3, 40) = zeros drops amounts >= 2^96: bogus events
    # from the 2018 overflow-exploit era, and it guarantees the value
    # fits decimal(38,0).
    return f"""SELECT transaction_hash,
    log_index,
    block_timestamp,
    address AS token_address,
    concat('0x', substr(topics[2], 27)) AS from_address,
    concat('0x', substr(topics[3], 27)) AS to_address,
    substr(data, 3, 64) AS amount_hex,
    extracted_at AS bronze_extracted_at,
    dt
FROM "bronze"."ethereum_logs"
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
    AND topics[1] = '{TRANSFER_TOPIC}'
    AND cardinality(topics) = 3
    AND length(data) = 66
    AND substr(data, 3, 40) = '{'0' * 40}'"""
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_decode_currency_query.py -q`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add decode/currency_query.py tests/test_decode_currency_query.py tests/fixtures/erc20_transfer_2021-08-15.json
git commit -m "feat: SQL builder for ERC-20 transfer decode"
```

### Task 2: Handler `decode/currency_handler.py`

**Files:**
- Create: `decode/currency_handler.py`
- Test: `tests/test_decode_currency_handler.py`

**Interfaces:**
- Consumes: `decode.currency_query.build_query`, `decode.common.parse_event`.
- Produces: `handler(event, context) -> {"start_date", "end_date", "rows_by_dt"}`; `postprocess(df) -> df` with columns `_FINAL_COLUMNS`; writes `staging.ethereum_currency_transfers` (`amount_raw decimal(38,0)`).

- [ ] **Step 1: Write the failing test**

`tests/test_decode_currency_handler.py`:

```python
from decimal import Decimal

import pandas as pd

from decode.currency_handler import _FINAL_COLUMNS, postprocess


def _df(**overrides):
    base = {
        "transaction_hash": ["0xb849"],
        "log_index": [302],
        "block_timestamp": [pd.Timestamp("2021-08-15 10:00:00")],
        "token_address": ["0x0f5d2fb29fb7d3cfee444a200298f468908cc942"],
        "from_address": ["0x" + "1" * 40],
        "to_address": ["0x" + "2" * 40],
        "amount_hex": [format(25 * 10**18, "064x")],  # 25 MANA in wei
        "bronze_extracted_at": [pd.Timestamp("2026-08-25 18:00:00")],
        "dt": ["2021-08-15"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def test_postprocess_amount_decimal_and_columns():
    out = postprocess(_df())
    assert list(out.columns) == _FINAL_COLUMNS
    assert out.loc[0, "amount_raw"] == Decimal(25 * 10**18)
    assert isinstance(out.loc[0, "amount_raw"], Decimal)
    assert "amount_hex" not in out.columns
    assert pd.notna(out.loc[0, "decoded_at"])


def test_handler_returns_empty_on_zero_row_unload(monkeypatch):
    # a zero-row UNLOAD makes awswrangler raise EmptyDataFrame instead of
    # returning an empty frame; the handler must treat it as "no data"
    import awswrangler as wr

    from decode import currency_handler

    def _raise(**kwargs):
        raise wr.exceptions.EmptyDataFrame("Query would return untyped, empty dataframe.")

    monkeypatch.setenv("LAKE_BUCKET", "test-bucket")
    monkeypatch.setattr(wr.athena, "read_sql_query", _raise)
    result = currency_handler.handler(
        {"start_date": "2017-09-06", "end_date": "2017-09-10"}, None
    )
    assert result == {
        "start_date": "2017-09-06",
        "end_date": "2017-09-10",
        "rows_by_dt": {},
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_decode_currency_handler.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'decode.currency_handler'`

- [ ] **Step 3: Write the implementation**

`decode/currency_handler.py`:

```python
"""decode-ethereum-currency-transfers Lambda: bronze -> staging.

The Athena query does the whole decode in SQL (one static data word);
this handler only converts the amount from hex with Python's
arbitrary-precision ints and hands the result to wr.s3.to_parquet,
which overwrites only the touched dt partitions and registers them in
Glue. Untracked payment tokens are kept on purpose (sales parsing).

Event: {"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"} (inclusive).
Default: single day, UTC today-2. Raises on any failure so the caller
(manual invoke today, Step Functions later) surfaces it.
"""

import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import awswrangler as wr
import pandas as pd

from decode.common import parse_event
from decode.currency_query import build_query

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")
GLUE_DATABASE = "staging"
GLUE_TABLE = "ethereum_currency_transfers"

# Athena decimal(38,0) ceiling for the amount column; the query's 2^96
# filter already guarantees this, kept as defense in depth
_MAX_DECIMAL38 = 10**38

_FINAL_COLUMNS = [
    "transaction_hash",
    "log_index",
    "block_timestamp",
    "token_address",
    "from_address",
    "to_address",
    "amount_raw",
    "bronze_extracted_at",
    "decoded_at",
    "dt",
]


def postprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    amounts = df["amount_hex"].map(lambda h: int(h, 16))
    too_big = amounts >= _MAX_DECIMAL38
    if too_big.any():
        rows = df.loc[too_big, ["transaction_hash", "log_index"]].to_dict("records")
        raise ValueError(f"amount exceeds decimal(38,0) in rows: {rows[:5]}")
    # Decimal, not int: that is what pyarrow maps onto decimal128(38,0)
    df["amount_raw"] = amounts.map(Decimal)
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

    sql = build_query(start, end)
    logger.info("extraction query for %s..%s:\n%s", start, end, sql)
    try:
        df = wr.athena.read_sql_query(
            sql=sql,
            database="bronze",
            workgroup=ATHENA_WORKGROUP,
            ctas_approach=False,
            unload_approach=True,
            # unique per run: UNLOAD refuses an existing target directory
            s3_output=f"s3://{bucket}/athena-results/unload/currency_transfers/{uuid.uuid4()}/",
            keep_files=False,
        )
    except wr.exceptions.EmptyDataFrame:
        # a zero-row UNLOAD raises instead of returning an empty frame
        return {"start_date": start, "end_date": end, "rows_by_dt": {}}
    if df.empty:
        return {"start_date": start, "end_date": end, "rows_by_dt": {}}

    df = postprocess(df)
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/staging/ethereum_currency_transfers/",
        dataset=True,
        partition_cols=["dt"],
        mode="overwrite_partitions",
        database=GLUE_DATABASE,
        table=GLUE_TABLE,
        filename_prefix=f"{stamp}_",
        compression="snappy",
        dtype={"amount_raw": "decimal(38,0)"},
    )
    return {
        "start_date": start,
        "end_date": end,
        "rows_by_dt": df.groupby("dt").size().to_dict(),
    }
```

- [ ] **Step 4: Run the full suite and lint**

Run: `.venv/bin/pytest tests/ -q && .venv/bin/ruff check decode/ tests/`
Expected: all tests pass, no lint errors

- [ ] **Step 5: Commit**

```bash
git add decode/currency_handler.py tests/test_decode_currency_handler.py
git commit -m "feat: decode ERC-20 transfers into staging.ethereum_currency_transfers"
```

### Task 3: Terraform — Lambda + Glue table

**Files:**
- Create: `terraform/lambda_decode_currency.tf`
- Create: `terraform/table_currency_transfers.tf`

**Interfaces:**
- Consumes: `local.awssdkpandas_layer_arn` (defined in `terraform/lambda_decode_nft.tf`), `aws_athena_workgroup.main`, `aws_glue_catalog_database.{bronze,staging}`, `aws_s3_bucket.lake`, `local.bucket_name`.
- Produces: Lambda `decode-ethereum-currency-transfers`, Glue table `staging.ethereum_currency_transfers`.

- [ ] **Step 1: Write `terraform/lambda_decode_currency.tf`**

Mirror of `lambda_decode_nft.tf` minus the silver grants (this query
reads only bronze; IAM keeps the partition-delete actions
`overwrite_partitions` needs):

```hcl
locals {
  decode_currency_tags = { component = "decode", layer = "staging" }
}

# source blocks (not source_dir) so the zip keeps the decode/ package
# directory and the handler resolves as decode.currency_handler.handler.
# Only this Lambda's modules ship.
data "archive_file" "decode_currency" {
  type        = "zip"
  output_path = "${path.module}/build/decode_currency.zip"

  source {
    content  = file("${path.module}/../decode/__init__.py")
    filename = "decode/__init__.py"
  }
  source {
    content  = file("${path.module}/../decode/common.py")
    filename = "decode/common.py"
  }
  source {
    content  = file("${path.module}/../decode/currency_query.py")
    filename = "decode/currency_query.py"
  }
  source {
    content  = file("${path.module}/../decode/currency_handler.py")
    filename = "decode/currency_handler.py"
  }
}

resource "aws_iam_role" "decode_currency" {
  name = "decode-ethereum-currency-transfers-role"
  tags = local.decode_currency_tags
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "decode_currency" {
  name = "athena-glue-s3"
  role = aws_iam_role.decode_currency.id
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
          aws_glue_catalog_database.staging.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/*",
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
        # read bronze data for the extraction query
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/*"
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

resource "aws_iam_role_policy_attachment" "decode_currency_logs" {
  role       = aws_iam_role.decode_currency.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "decode_currency" {
  name              = "/aws/lambda/decode-ethereum-currency-transfers"
  retention_in_days = 7
  tags              = local.decode_currency_tags
}

resource "aws_lambda_function" "decode_currency" {
  function_name    = "decode-ethereum-currency-transfers"
  role             = aws_iam_role.decode_currency.arn
  filename         = data.archive_file.decode_currency.output_path
  source_code_hash = data.archive_file.decode_currency.output_base64sha256
  handler          = "decode.currency_handler.handler"
  runtime          = "python3.13"
  architectures    = ["arm64"]
  timeout          = 300
  memory_size      = 2048
  layers           = [local.awssdkpandas_layer_arn]
  tags             = local.decode_currency_tags

  environment {
    variables = {
      LAKE_BUCKET      = aws_s3_bucket.lake.id
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }
}
```

- [ ] **Step 2: Write `terraform/table_currency_transfers.tf`**

```hcl
# Staging table written by the decode-ethereum-currency-transfers Lambda
# (awswrangler registers the dt partitions on each write). No partition
# projection, so the "$partitions" metadata convention keeps working.
resource "aws_glue_catalog_table" "staging_currency_transfers" {
  database_name = aws_glue_catalog_database.staging.name
  name          = "ethereum_currency_transfers"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/staging/ethereum_currency_transfers/"
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
      name = "token_address"
      type = "string"
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
      name = "amount_raw"
      type = "decimal(38,0)"
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

- [ ] **Step 3: Validate and plan**

Run: `cd terraform && terraform validate && terraform plan`
Expected: valid; plan shows exactly 6 resources to add (role, policy,
attachment, log group, lambda, glue table), 0 to change/destroy. Read
the whole plan before continuing.

- [ ] **Step 4: Apply**

Run: `cd terraform && terraform apply` (approve after re-reading)
Expected: `Apply complete! Resources: 6 added`

- [ ] **Step 5: Commit**

```bash
git add terraform/lambda_decode_currency.tf terraform/table_currency_transfers.tf
git commit -m "feat: terraform for decode-ethereum-currency-transfers lambda and staging table"
```

### Task 4: Smoke test + backfill 2017-09-06 → today

**Files:** none (AWS invokes only)

**Interfaces:**
- Consumes: deployed Lambda `decode-ethereum-currency-transfers`.
- Produces: `staging.ethereum_currency_transfers` populated for every dt with data since 2017-09-06.

- [ ] **Step 1: Smoke invoke one known day**

```bash
aws lambda invoke --function-name decode-ethereum-currency-transfers \
  --payload '{"start_date": "2021-08-15", "end_date": "2021-08-15"}' \
  --cli-binary-format raw-in-base64-out /dev/stdout
```

Expected: `rows_by_dt` with a non-zero count for `2021-08-15`, no errors.

- [ ] **Step 2: Cross-check against the silver table**

Athena (workgroup `decentraland-data-platform`): compare a day both
tables cover:

```sql
SELECT 'staging' AS src, count(*) AS n
FROM "staging"."ethereum_currency_transfers" WHERE dt = '2018-06-15'
UNION ALL
SELECT 'silver', count(*)
FROM "silver"."ethereum_currency_transfers" WHERE dt = '2018-06-15'
```

(Requires first invoking the Lambda for that day, e.g.
`{"start_date": "2018-06-15", "end_date": "2018-06-15"}`.)
Expected: identical counts.

- [ ] **Step 3: Backfill in ~20-day windows**

Sequential invokes (each waits for the previous; Lambda timeout is
300s, one window is one Athena UNLOAD):

```bash
python3 - <<'EOF'
import json, subprocess
from datetime import date, timedelta

start, today = date(2017, 9, 6), date.today() - timedelta(days=2)
step = timedelta(days=20)
d = start
while d <= today:
    end = min(d + step - timedelta(days=1), today)
    payload = json.dumps({"start_date": d.isoformat(), "end_date": end.isoformat()})
    print(payload, flush=True)
    out = subprocess.run(
        ["aws", "lambda", "invoke", "--function-name",
         "decode-ethereum-currency-transfers", "--payload", payload,
         "--cli-binary-format", "raw-in-base64-out", "/dev/stdout"],
        capture_output=True, text=True, check=True)
    print(out.stdout[:200], flush=True)
    assert '"errorMessage"' not in out.stdout
    d = end + timedelta(days=1)
EOF
```

Expected: every window prints `rows_by_dt` (empty for pre-activity
gaps is fine), no `errorMessage`.

- [ ] **Step 4: Verify coverage via $partitions**

Athena:

```sql
SELECT min(dt), max(dt), count(*)
FROM "staging"."ethereum_currency_transfers$partitions"
```

Expected: `min = 2017-09-06` (or first day with data), `max` = UTC
today−2, and a partition count in the low thousands (only days with
matching logs get a partition).

- [ ] **Step 5: Update project docs**

Update the marketplace-sales status note in the memory index entry and
`CLAUDE.md` workflow line if applicable (currency transfers now decoded
in staging via Lambda). Commit any doc change:

```bash
git add -A docs CLAUDE.md
git commit -m "docs: currency transfers decoded in staging via lambda" || true
```

### Task 5: Silver teardown (no separate plan — simple deletion)

**Files:**
- Delete: `dbt/models/silver/ethereum_currency_transfers.sql`
- Modify: `dbt/models/silver/schema.yml` (remove the `ethereum_currency_transfers` model block)
- Delete: `dbt/tests/assert_ethereum_currency_transfers_unique_key.sql`
- Delete (if present in git): `dbt/tests/assert_ethereum_currency_transfers_no_overflow.sql`

**Interfaces:**
- Consumes: confirmation from the user before the destructive AWS steps (DROP TABLE + S3 delete).
- Produces: dbt project with no reference to `ethereum_currency_transfers`; silver database/S3 clean.

- [ ] **Step 1: Delete the dbt files**

```bash
git rm dbt/models/silver/ethereum_currency_transfers.sql \
       dbt/tests/assert_ethereum_currency_transfers_unique_key.sql
```

Then edit `dbt/models/silver/schema.yml` removing the whole
`- name: ethereum_currency_transfers` block (lines 32–69 at time of
writing). Check for a stray `assert_ethereum_currency_transfers_no_overflow.sql`
and `git rm` it too if tracked.

- [ ] **Step 2: Verify dbt still parses and nothing references it**

Run: `grep -rn "currency_transfers" dbt/models dbt/tests dbt/macros` → no hits.
Run: `cd dbt && ../.venv/bin/dbt parse`
Expected: parse OK.

- [ ] **Step 3: CONFIRM WITH USER, then drop the Athena table**

Destructive — ask the user before running:

```sql
DROP TABLE `silver`.`ethereum_currency_transfers`
```

(workgroup `decentraland-data-platform`).

- [ ] **Step 4: Delete the silver S3 data**

Destructive — same confirmation covers it. Look first, then delete:

```bash
aws s3 ls s3://decentraland-data-platform-<account_id>/silver/ethereum_currency_transfers/ | head
aws s3 rm s3://decentraland-data-platform-<account_id>/silver/ethereum_currency_transfers/ --recursive
```

(resolve `<account_id>` via `aws sts get-caller-identity --query Account --output text`).

- [ ] **Step 5: Full test suite + commit**

```bash
.venv/bin/pytest tests/ -q
git add dbt/models/silver/schema.yml
git commit -m "refactor: drop silver ethereum_currency_transfers (moved to staging decode lambda)"
```
