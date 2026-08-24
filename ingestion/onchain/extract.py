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
