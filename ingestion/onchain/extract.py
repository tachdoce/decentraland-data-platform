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


def parse_hours(event: dict) -> tuple[int, int] | None:
    """Optional intraday window: {"hour_start": 0, "hour_end": 6} -> (0, 6).

    Used by the chunked fallback when the full-day extraction fails.
    """
    start, end = event.get("hour_start"), event.get("hour_end")
    if start is None and end is None:
        return None
    if start is None or end is None:
        raise ValueError("hour_start and hour_end must be provided together")
    if not (0 <= start <= 24 and 0 <= end <= 24):
        raise ValueError("hours must be between 0 and 24")
    if start >= end:
        raise ValueError("window requires hour_start < hour_end")
    return start, end


def window_bounds(
    run_date: datetime.date, hours: tuple[int, int]
) -> tuple[datetime.datetime, datetime.datetime]:
    """UTC [start, end) timestamps for an intraday hour window; 24 = next midnight."""
    midnight = datetime.datetime.combine(
        run_date, datetime.time(0), tzinfo=datetime.timezone.utc
    )
    return (
        midnight + datetime.timedelta(hours=hours[0]),
        midnight + datetime.timedelta(hours=hours[1]),
    )


def build_query(chain_id: int, windowed: bool = False) -> str:
    """Two-step query: whole transactions that touched Decentraland contracts.

    A sale emits events from the marketplace, the NFT, and MANA in one
    transaction; fetching every log of those transactions preserves the
    context decoding needs. @dt and @addresses are BigQuery query
    parameters — values are never interpolated into the SQL text.

    With windowed=True both steps also bound block_timestamp to
    [@ts_start, @ts_end): a transaction's logs share one block, so no
    transaction ever splits across windows.
    """
    dataset = CHAINS[chain_id]["dataset"]
    window = ""
    if windowed:
        window = (
            "\n    AND block_timestamp >= @ts_start"
            "\n    AND block_timestamp < @ts_end"
        )
    return f"""\
WITH tx AS (
  SELECT DISTINCT transaction_hash
  FROM `bigquery-public-data.{dataset}.logs`
  WHERE DATE(block_timestamp) = @dt
    AND address IN UNNEST(@addresses){window}
)
SELECT transaction_hash, log_index, block_timestamp, address, topics, data
FROM `bigquery-public-data.{dataset}.logs`
WHERE DATE(block_timestamp) = @dt
  AND ARRAY_LENGTH(topics) >= 1
  AND transaction_hash IN (SELECT transaction_hash FROM tx){window}
ORDER BY block_timestamp, log_index"""


def object_key(
    chain_id: int,
    run_date: datetime.date,
    extracted_at: datetime.datetime,
    hours: tuple[int, int] | None = None,
) -> str:
    # Dashes in the time part: colons in S3 keys break URL handling.
    table = CHAINS[chain_id]["table"]
    stamp = extracted_at.strftime("%Y-%m-%d_%H-%M-%S")
    if hours is not None:
        stamp += f"_h{hours[0]:02d}-{hours[1]:02d}"
    return f"bronze/{table}/dt={run_date.isoformat()}/{stamp}.parquet"


def rows_to_parquet(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()
