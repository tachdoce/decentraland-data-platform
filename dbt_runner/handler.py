"""run-dbt Lambda: builds the dbt models that depend on bronze.contracts.

Invoked by the contracts-on-push state machine after a successful CSV
ingestion. Event: {} for the default selector, {"select": "<selector>"}
for manual runs. Raises on any dbt failure so Step Functions catches it.

After a successful build, queries silver.dim_contracts_changes and posts
an INFO notification to the alerts topic when it has rows (custom
notify-slack contract; the changes model is a skeleton today, so this
never fires until its diff logic lands).
"""

import json
import os
import time

import boto3
from dbt.cli.main import dbtRunner

DEFAULT_SELECT = "source:bronze.contracts+"
CHANGES_QUERY = "select change_type from silver.dim_contracts_changes"


def run_dbt(select: str) -> None:
    project_dir = os.environ["DBT_PROJECT_DIR"]
    # Lambda's filesystem is read-only except /tmp: dbt artifacts and logs
    # must be redirected there.
    result = dbtRunner().invoke(
        [
            "build",
            "--select", select,
            "--project-dir", project_dir,
            "--profiles-dir", project_dir,
            "--target-path", "/tmp/dbt-target",
            "--log-path", "/tmp/dbt-logs",
        ]
    )
    if not result.success:
        raise RuntimeError(
            f"dbt build failed: {result.exception or 'model or test failures'}"
        )


def fetch_change_counts(athena) -> dict[str, int]:
    qid = athena.start_query_execution(
        QueryString=CHANGES_QUERY, WorkGroup=os.environ["ATHENA_WORKGROUP"]
    )["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
            "Status"
        ]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(
            f"changes query {status['State']}: {status.get('StateChangeReason', '')}"
        )
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"][1:]
    counts: dict[str, int] = {}
    for row in rows:
        change_type = row["Data"][0].get("VarCharValue", "unknown")
        counts[change_type] = counts.get(change_type, 0) + 1
    return counts


def handler(event, context):
    select = (event or {}).get("select") or DEFAULT_SELECT
    run_dbt(select)

    counts = fetch_change_counts(boto3.client("athena"))
    if counts:
        detail = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        boto3.client("sns").publish(
            TopicArn=os.environ["ALERTS_TOPIC_ARN"],
            Message=json.dumps(
                {
                    "source": "dbt",
                    "component": "dbt build",
                    "status": "INFO",
                    "detail": f"dim_contracts changes: {detail}",
                }
            ),
        )
    return {"select": select, "changes": counts}
