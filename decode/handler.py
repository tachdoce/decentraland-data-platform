"""decode-nft-transfers Lambda: bronze -> staging.

wr.athena.read_sql_query runs the set-based extraction (see query.py);
this handler converts token_id/quantity from hex with Python's
arbitrary-precision ints and hands the result to wr.s3.to_parquet, which
overwrites only the touched dt partitions and registers them in Glue.

Event: {"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"} (inclusive).
Default: single day, UTC today-2. Raises on any failure so the caller
(manual invoke today, Step Functions later) surfaces it.
"""

import os
import uuid
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
        # unique per run: UNLOAD refuses an existing target directory
        s3_output=f"s3://{bucket}/athena-results/unload/nft_transfers/{uuid.uuid4()}/",
        keep_files=False,
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
