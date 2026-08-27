"""decode-ethereum-legacy-marketplace-auctions Lambda: bronze -> staging.

The Athena query does the whole decode in SQL (static per-event word
layout); this handler only converts hex with Python's
arbitrary-precision ints and appends timestamped parquets
(bronze-style): re-runs add rows rather than replace them, so
downstream dbt dedups by (transaction_hash, log_index) keeping the
latest decoded_at. The event carries no NFT contract address — silver
attributes each asset_id to a contract later.

expiresAt in AuctionCreated is unix MILLISECONDS (the legacy dApp sent
JavaScript timestamps); pd.to_datetime(unit="ms") converts it and
raises OutOfBoundsDatetime on garbage values — deliberately loud.

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
from decode.ethereum_legacy_marketplace_query import build_query

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")
GLUE_DATABASE = "staging"
GLUE_TABLE = "ethereum_legacy_marketplace_auctions"

# Athena decimal(38,0) ceiling for the total_price column
_MAX_DECIMAL38 = 10**38

_FINAL_COLUMNS = [
    "transaction_hash",
    "log_index",
    "block_timestamp",
    "event_name",
    "auction_id",
    "asset_id",
    "seller",
    "winner",
    "total_price",
    "expires_at",
    "bronze_extracted_at",
    "decoded_at",
    "dt",
]


def postprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # unsigned decimal string, same convention as nft_transfers.token_id
    df["asset_id"] = df["asset_id_hex"].map(lambda h: str(int(h, 16)))
    prices = df["total_price_hex"].map(lambda h: None if pd.isna(h) else int(h, 16))
    too_big = prices.map(lambda p: p is not None and p >= _MAX_DECIMAL38)
    if too_big.any():
        rows = df.loc[too_big, ["transaction_hash", "log_index"]].to_dict("records")
        raise ValueError(f"total_price exceeds decimal(38,0) in rows: {rows[:5]}")
    # Decimal, not int: that is what pyarrow maps onto decimal128(38,0)
    df["total_price"] = prices.map(lambda p: None if p is None else Decimal(p))
    df["expires_at"] = pd.to_datetime(
        df["expires_at_hex"].map(lambda h: None if pd.isna(h) else int(h, 16)),
        unit="ms",
    )
    for col in ("seller", "winner"):
        df[col] = df[col].map(lambda a: None if pd.isna(a) else a.lower())
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
            s3_output=(
                f"s3://{bucket}/athena-results/unload/"
                f"legacy_marketplace_auctions/{uuid.uuid4()}/"
            ),
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
        path=f"s3://{bucket}/staging/ethereum_legacy_marketplace_auctions/",
        dataset=True,
        partition_cols=["dt"],
        mode="append",
        database=GLUE_DATABASE,
        table=GLUE_TABLE,
        filename_prefix=f"{stamp}_",
        compression="snappy",
        dtype={"total_price": "decimal(38,0)"},
    )
    return {
        "start_date": start,
        "end_date": end,
        "rows_by_dt": df.groupby("dt").size().to_dict(),
    }
