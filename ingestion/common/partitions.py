"""Explicit partition registration for bronze tables (no partition projection)."""

import datetime
import os
import time

import boto3

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")


def register_partition(
    table: str,
    run_date: datetime.date | None,
    bucket: str,
    column: str = "dt",
    value: str | None = None,
) -> None:
    """Register a snapshot partition in the Glue catalog via Athena DDL.

    The bronze tables have no partition projection, so each partition must
    be added explicitly. IF NOT EXISTS keeps re-runs idempotent. Default
    partitioning is daily (column dt, value from run_date); monthly tables
    pass column="month" and an explicit value.
    """
    athena = boto3.client("athena")
    value = value if value is not None else run_date.isoformat()
    ddl = (
        f"ALTER TABLE bronze.{table} "
        f"ADD IF NOT EXISTS PARTITION ({column} = '{value}') "
        f"LOCATION 's3://{bucket}/bronze/{table}/{column}={value}/'"
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
