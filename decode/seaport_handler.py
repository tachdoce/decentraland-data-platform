"""decode-ethereum-seaport-sales Lambda: bronze -> staging.

The Athena query only filters (topic0) and fetches raw logs; all ABI
decoding happens in seaport_parser. Only sales reach staging: orders
with order_side 'unknown' (NFT swaps, matchOrders counterlegs) or with
no payments are dropped here. wr.s3.to_parquet appends timestamped
parquets (bronze-style): re-runs add rows rather than replace them, so
downstream dbt dedups by (transaction_hash, log_index) keeping the
latest decoded_at.

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
from decode.seaport_parser import parse_order_fulfilled
from decode.seaport_query import build_query

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")
GLUE_DATABASE = "staging"
GLUE_TABLE = "ethereum_seaport_sales"

# Athena decimal(38,0) ceiling for amounts/quantities
_MAX_DECIMAL38 = 10**38

_FINAL_COLUMNS = [
    "transaction_hash",
    "log_index",
    "block_timestamp",
    "seaport_address",
    "order_hash",
    "offerer",
    "recipient",
    "order_side",
    "buyer",
    "seller",
    "nft_contract_addresses",
    "nft_token_ids",
    "nft_quantities",
    "nft_froms",
    "nft_tos",
    "payment_currencies",
    "payment_amounts",
    "payment_recipients",
    "bronze_extracted_at",
    "decoded_at",
    "dt",
]


def _to_decimal_list(values, column, tx, log_index):
    for v in values:
        if v >= _MAX_DECIMAL38:
            raise ValueError(
                f"{column} exceeds decimal(38,0) in {tx} log_index {log_index}"
            )
    return [Decimal(v) for v in values]


def postprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    parsed = pd.DataFrame(
        [parse_order_fulfilled(list(row.topics), row.data) for row in df.itertuples()],
        index=df.index,
    )
    out = pd.concat([df.drop(columns=["topics", "data"]), parsed], axis=1)

    is_sale = (out["order_side"] != "unknown") & (
        out["payment_amounts"].map(len) > 0
    )
    skipped = int((~is_sale).sum())
    if skipped:
        logger.info("skipping %d non-sale orders (swaps or zero-payment)", skipped)
    out = out[is_sale].copy()

    out["seaport_address"] = out["address"].str.lower()
    out["nft_quantities"] = out.apply(
        lambda r: _to_decimal_list(
            r["nft_quantities"], "nft_quantities", r["transaction_hash"], r["log_index"]
        ),
        axis=1,
    )
    out["payment_amounts"] = out.apply(
        lambda r: _to_decimal_list(
            r["payment_amounts"],
            "payment_amounts",
            r["transaction_hash"],
            r["log_index"],
        ),
        axis=1,
    )
    out = out.rename(columns={"extracted_at": "bronze_extracted_at"})
    # floor to ms: the staging schema stores timestamp(ms) and pyarrow
    # refuses lossy casts from microseconds
    out["decoded_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("ms")
    return out[_FINAL_COLUMNS]


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
            s3_output=f"s3://{bucket}/athena-results/unload/seaport_sales/{uuid.uuid4()}/",
            keep_files=False,
        )
    except wr.exceptions.EmptyDataFrame:
        # a zero-row UNLOAD raises instead of returning an empty frame
        return {"start_date": start, "end_date": end, "rows_by_dt": {}}
    if df.empty:
        return {"start_date": start, "end_date": end, "rows_by_dt": {}}

    df = postprocess(df)
    if df.empty:
        return {"start_date": start, "end_date": end, "rows_by_dt": {}}
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/staging/ethereum_seaport_sales/",
        dataset=True,
        partition_cols=["dt"],
        mode="append",
        database=GLUE_DATABASE,
        table=GLUE_TABLE,
        filename_prefix=f"{stamp}_",
        compression="snappy",
        dtype={
            "nft_quantities": "array<decimal(38,0)>",
            "payment_amounts": "array<decimal(38,0)>",
        },
    )
    return {
        "start_date": start,
        "end_date": end,
        "rows_by_dt": df.groupby("dt").size().to_dict(),
    }
