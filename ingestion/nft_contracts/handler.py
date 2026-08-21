"""load-nft-contracts Lambda: landing/ CSV upload -> bronze/nft_contracts snapshot.

Triggered by S3 ObjectCreated events (prefix landing/nft_contracts/, suffix .csv).
"""

import datetime
import io
import os
import time
import urllib.parse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.nft_contracts.validate import parse_and_validate, partition_key

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")

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


def register_partition(run_date: datetime.date, bucket: str) -> None:
    """Register the snapshot partition in the Glue catalog via Athena DDL.

    The table has no partition projection, so each dt must be added
    explicitly. IF NOT EXISTS keeps same-day re-uploads idempotent.
    """
    athena = boto3.client("athena")
    dt = run_date.isoformat()
    ddl = (
        "ALTER TABLE bronze.nft_contracts "
        f"ADD IF NOT EXISTS PARTITION (dt = '{dt}') "
        f"LOCATION 's3://{bucket}/bronze/nft_contracts/dt={dt}/'"
    )
    query_id = athena.start_query_execution(
        QueryString=ddl, WorkGroup=ATHENA_WORKGROUP
    )["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)[
            "QueryExecution"
        ]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "no reason given")
            raise RuntimeError(f"partition DDL {state}: {reason}")
        time.sleep(1)  # DDL completes in ~1-2s; the Lambda timeout is the backstop


def handler(event, context):
    # S3 sends one record per direct notification, but the contract is a
    # list — process every record rather than silently dropping extras.
    s3 = boto3.client("s3")
    results = []
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        # S3 URL-encodes keys in event payloads (spaces become '+')
        source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        body = s3.get_object(Bucket=bucket, Key=source_key)["Body"].read()
        rows = parse_and_validate(body)

        run_date = datetime.datetime.now(datetime.timezone.utc).date()
        out_key = partition_key(run_date)
        s3.put_object(Bucket=bucket, Key=out_key, Body=rows_to_parquet(rows))
        register_partition(run_date, bucket)

        print(
            f"validated {len(rows)} rows from s3://{bucket}/{source_key}; "
            f"wrote s3://{bucket}/{out_key}"
        )
        results.append({"rows": len(rows), "s3_key": out_key, "source_key": source_key})
    return results[0] if len(results) == 1 else {"processed": results}
