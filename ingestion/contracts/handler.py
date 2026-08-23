"""load-contracts Lambda: landing/ CSV upload -> bronze/contracts snapshot.

Triggered by S3 ObjectCreated events (prefix landing/contracts/, suffix .csv).
"""

import datetime
import io
import urllib.parse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common.partitions import register_partition
from ingestion.contracts.validate import parse_and_validate, partition_key

SCHEMA = pa.schema(
    [
        ("chain_id", pa.int32()),
        ("contract_address", pa.string()),
        ("contract_name", pa.string()),
        ("first_mint_dt", pa.date32()),
        ("extract_from_dt", pa.date32()),
        ("dcl_contract", pa.bool_()),
        ("erc_type", pa.int32()),
    ]
)


def rows_to_parquet(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


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
        register_partition("contracts", run_date, bucket)

        print(
            f"validated {len(rows)} rows from s3://{bucket}/{source_key}; "
            f"wrote s3://{bucket}/{out_key}"
        )
        results.append({"rows": len(rows), "s3_key": out_key, "source_key": source_key})
    return results[0] if len(results) == 1 else {"processed": results}
