"""extract-dcl-contracts Lambda: addresses.json -> bronze/dcl_contracts snapshot."""

import datetime
import io
import json
import os
import urllib.request

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.dcl_contracts.transform import flatten, partition_key, validate_payload

ADDRESSES_URL = "https://contracts.decentraland.org/addresses.json"

SCHEMA = pa.schema(
    [
        ("chain_id", pa.int32()),
        ("contract_address", pa.string()),
        ("contract_name", pa.string()),
    ]
)


def fetch(url: str = ADDRESSES_URL) -> dict:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "decentraland-data-platform/1.0"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def rows_to_parquet(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def handler(event, context):
    event = event or {}
    if event.get("date"):
        try:
            run_date = datetime.date.fromisoformat(event["date"])
        except ValueError:
            raise ValueError(f"event 'date' must be YYYY-MM-DD, got: {event['date']!r}")
        if run_date < datetime.date(2026, 8, 1):
            raise ValueError(
                f"event 'date' {run_date} predates the table's partition projection range (2026-08-01)"
            )
    else:
        run_date = datetime.datetime.now(datetime.timezone.utc).date()

    data = fetch()
    validate_payload(data)
    rows = flatten(data)

    key = partition_key(run_date)
    boto3.client("s3").put_object(
        Bucket=os.environ["LAKE_BUCKET"], Key=key, Body=rows_to_parquet(rows)
    )
    print(f"wrote {len(rows)} rows to s3://{os.environ['LAKE_BUCKET']}/{key}")
    return {"rows": len(rows), "s3_key": key}
