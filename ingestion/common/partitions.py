"""Explicit partition registration for bronze tables (no partition projection)."""

import datetime
import os
import time

import boto3

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")


def register_partition(table: str, run_date: datetime.date, bucket: str) -> None:
    """Register a snapshot partition in the Glue catalog via Athena DDL.

    The bronze tables have no partition projection, so each dt must be
    added explicitly. IF NOT EXISTS keeps same-day re-runs idempotent.
    """
    athena = boto3.client("athena")
    dt = run_date.isoformat()
    ddl = (
        f"ALTER TABLE bronze.{table} "
        f"ADD IF NOT EXISTS PARTITION (dt = '{dt}') "
        f"LOCATION 's3://{bucket}/bronze/{table}/dt={dt}/'"
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
