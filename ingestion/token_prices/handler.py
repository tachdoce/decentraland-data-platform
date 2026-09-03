"""extract-token-prices Lambda: DefiLlama daily USD prices -> bronze/token_prices.

Manually triggered (via the token-prices state machine). Payload {} extracts
today (UTC); {"start_date", "end_date"} backfills a range. Append-only:
each run writes a new timestamped parquet per touched month partition;
duplicates are resolved downstream in silver by latest extracted_at.
"""

import datetime
import io
import json
import os
import time
import urllib.error
import urllib.request

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common.partitions import ATHENA_WORKGROUP, register_partition
from ingestion.token_prices.defillama import (
    chart_url,
    chunk_range,
    coin_id,
    month_key,
    parse_chart_response,
)

SCHEMA = pa.schema(
    [
        ("chain_id", pa.int32()),
        ("contract_address", pa.string()),
        ("dt", pa.date32()),
        ("price_usd", pa.float64()),
        ("price_ts", pa.timestamp("us")),
        ("confidence", pa.float64()),
        ("extracted_at", pa.timestamp("us")),
    ]
)

HTTP_ATTEMPTS = 3
HTTP_TIMEOUT = 30
RETRYABLE = {429, 500, 502, 503, 504}


def fetch_tokens(athena) -> list[tuple[int, str]]:
    """Token universe from the curated dimension (same pattern as the
    onchain Lambda reading silver.dim_contracts)."""
    sql = "SELECT chain_id, contract_address FROM silver.dim_erc20_tokens"
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
            raise RuntimeError(f"token-list query {state}: {reason}")
        time.sleep(1)

    tokens, token, first_page = [], None, True
    while True:
        kwargs = {"QueryExecutionId": query_id}
        if token:
            kwargs["NextToken"] = token
        page = athena.get_query_results(**kwargs)
        rows = page["ResultSet"]["Rows"]
        if first_page:
            rows = rows[1:]  # header row
            first_page = False
        for r in rows:
            chain_raw = r["Data"][0].get("VarCharValue")
            address = r["Data"][1].get("VarCharValue")
            if chain_raw is None or address is None:
                raise RuntimeError(
                    "silver.dim_erc20_tokens returned a NULL key; "
                    "fix the dimension upstream"
                )
            tokens.append((int(chain_raw), address))
        token = page.get("NextToken")
        if not token:
            return tokens


def fetch_chart(url: str) -> dict:
    """GET with exponential backoff on 429/5xx (DefiLlama has no SLA)."""
    for attempt in range(1, HTTP_ATTEMPTS + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "curl/8.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as err:
            if err.code not in RETRYABLE or attempt == HTTP_ATTEMPTS:
                raise
            time.sleep(2**attempt)
        except urllib.error.URLError:
            if attempt == HTTP_ATTEMPTS:
                raise
            time.sleep(2**attempt)


def rows_to_parquet(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def handler(event, context):
    event = event or {}
    today = datetime.datetime.now(datetime.timezone.utc).date()
    start = datetime.date.fromisoformat(event.get("start_date", today.isoformat()))
    end = datetime.date.fromisoformat(event.get("end_date", start.isoformat()))
    if end < start:
        raise ValueError(f"end_date {end} is before start_date {start}")

    bucket = os.environ["LAKE_BUCKET"]
    athena = boto3.client("athena")
    s3 = boto3.client("s3")

    tokens = fetch_tokens(athena)
    id_map = {coin_id(c, a): (c, a) for c, a in tokens}
    coin_ids = sorted(id_map)

    extracted_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    rows = []
    for chunk_start, span in chunk_range(start, end):
        payload = fetch_chart(chart_url(coin_ids, chunk_start, span))
        rows.extend(parse_chart_response(payload, chunk_start, span, id_map))
    for row in rows:
        row["extracted_at"] = extracted_at

    by_month: dict[str, list[dict]] = {}
    for row in rows:
        by_month.setdefault(month_key(row["dt"]), []).append(row)

    stamp = extracted_at.strftime("%Y%m%d%H%M%S")
    for month in sorted(by_month):
        key = f"bronze/token_prices/month={month}/token_prices_{stamp}.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=rows_to_parquet(by_month[month]))
        register_partition("token_prices", None, bucket, column="month", value=month)
        print(f"wrote {len(by_month[month])} rows to s3://{bucket}/{key}")

    return {
        "rows": len(rows),
        "tokens": len(tokens),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "months": sorted(by_month),
    }
