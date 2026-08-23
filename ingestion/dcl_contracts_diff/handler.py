"""diff-dcl-contracts Lambda: compare today's snapshot vs yesterday's.

Runs as the second step of the dcl-contracts-daily state machine, after
extract-dcl-contracts. On any change it publishes an INFO message to the
alerts SNS topic (notify-slack renders it in Slack); no changes, no message.
"""

import datetime
import io
import json
import os

import boto3
import pyarrow.parquet as pq

from ingestion.dcl_contracts.transform import partition_key
from ingestion.dcl_contracts_diff.transform import diff_snapshots, summarize


def read_snapshot(s3, bucket: str, key: str) -> list[dict] | None:
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None
    return pq.read_table(io.BytesIO(body)).to_pylist()


def handler(event, context):
    event = event or {}
    if event.get("date"):
        try:
            run_date = datetime.date.fromisoformat(event["date"])
        except ValueError:
            raise ValueError(f"event 'date' must be YYYY-MM-DD, got: {event['date']!r}")
    else:
        run_date = datetime.datetime.now(datetime.timezone.utc).date()
    prev_date = run_date - datetime.timedelta(days=1)

    bucket = os.environ["LAKE_BUCKET"]
    s3 = boto3.client("s3")

    today = read_snapshot(s3, bucket, partition_key(run_date))
    if today is None:
        raise RuntimeError(f"no snapshot for {run_date}; extract step should have written it")

    yesterday = read_snapshot(s3, bucket, partition_key(prev_date))
    if yesterday is None:
        print(f"no snapshot for {prev_date}; nothing to compare against")
        return {"compared": False, "reason": f"no snapshot for {prev_date}"}

    changes = diff_snapshots(today, yesterday)
    total = sum(len(v) for v in changes.values())
    if total:
        detail = f"dcl_contracts {run_date} vs {prev_date}:\n{summarize(changes)}"
        boto3.client("sns").publish(
            TopicArn=os.environ["ALERTS_TOPIC_ARN"],
            Message=json.dumps(
                {
                    "source": "diff-dcl-contracts",
                    "component": "dcl_contracts",
                    "status": "INFO",
                    "detail": detail,
                }
            ),
        )
    print(f"{run_date} vs {prev_date}: {total} changes, notified={bool(total)}")
    return {
        "compared": True,
        "added": len(changes["added"]),
        "removed": len(changes["removed"]),
        "renamed": len(changes["renamed"]),
        "notified": bool(total),
    }
