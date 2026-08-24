"""extract-onchain-logs Lambda: BigQuery public logs -> bronze, one day+chain.

Payload: {"chain_id": 1 | 137, "date": "YYYY-MM-DD"} (date optional,
default UTC today-2). Append-only: each run writes a new timestamped
parquet; downstream dedups by latest extracted_at per dt partition.
"""

import datetime
import json
import os
import time

import boto3
from google.cloud import bigquery
from google.oauth2 import service_account

from ingestion.common.partitions import register_partition
from ingestion.onchain.extract import (
    CHAINS,
    MAX_BYTES_BILLED,
    build_query,
    object_key,
    parse_event,
    rows_to_parquet,
)

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")
GCP_KEY_PARAM = os.environ.get(
    "GCP_KEY_PARAM", "/decentraland/gcp/bq-service-account-key"
)


def fetch_contract_addresses(athena, chain_id: int) -> list[str]:
    # chain_id is validated against CHAINS before this runs; safe to inline.
    sql = (
        "SELECT contract_address FROM silver.dim_contracts "
        f"WHERE chain_id = {chain_id}"
    )
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
            raise RuntimeError(f"contract-list query {state}: {reason}")
        time.sleep(1)

    addresses, token, first_page = [], None, True
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
            # Athena renders NULL as a cell without VarCharValue.
            value = r["Data"][0].get("VarCharValue")
            if value is None:
                raise RuntimeError(
                    "silver.dim_contracts returned a NULL contract_address "
                    f"for chain_id={chain_id}; fix the dimension upstream"
                )
            addresses.append(value)
        token = page.get("NextToken")
        if not token:
            return addresses


def _bigquery_client(ssm) -> bigquery.Client:
    key = json.loads(
        ssm.get_parameter(Name=GCP_KEY_PARAM, WithDecryption=True)["Parameter"][
            "Value"
        ]
    )
    creds = service_account.Credentials.from_service_account_info(key)
    return bigquery.Client(credentials=creds, project=key["project_id"])


def handler(event, context):
    chain_id, run_date = parse_event(event or {})
    bucket = os.environ["LAKE_BUCKET"]
    table = CHAINS[chain_id]["table"]

    addresses = fetch_contract_addresses(boto3.client("athena"), chain_id)
    if not addresses:
        raise RuntimeError(
            f"silver.dim_contracts returned no addresses for chain_id={chain_id}; "
            "refusing to extract against an empty contract list"
        )

    client = _bigquery_client(boto3.client("ssm"))
    extracted_at = datetime.datetime.now(datetime.timezone.utc)
    sql = build_query(chain_id)
    # Logged for debugging: the exact SQL plus the parameter values.
    print(
        f"bigquery query (chain_id={chain_id}, dt={run_date}, "
        f"{len(addresses)} addresses):\n{sql}\naddresses={addresses}"
    )
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dt", "DATE", run_date),
                bigquery.ArrayQueryParameter("addresses", "STRING", addresses),
            ],
            maximum_bytes_billed=MAX_BYTES_BILLED,
        ),
    )
    rows = [dict(row) | {"extracted_at": extracted_at} for row in job.result()]

    out_key = object_key(chain_id, run_date, extracted_at)
    boto3.client("s3").put_object(
        Bucket=bucket, Key=out_key, Body=rows_to_parquet(rows)
    )
    register_partition(table, run_date, bucket)

    result = {
        "chain_id": chain_id,
        "dt": run_date.isoformat(),
        "rows": len(rows),
        "bytes_processed": job.total_bytes_processed,
        "s3_key": out_key,
    }
    print(result)
    return result
