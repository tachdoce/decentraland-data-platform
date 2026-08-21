"""notify-slack Lambda: SNS alerts topic -> Slack Incoming Webhook.

Subscribed to the alerts SNS topic. Understands two shapes:
- CloudWatch Alarm notifications: only failures (state ALARM) are posted.
- Custom pipeline contract {source, component, status, detail,
  execution_url?}: FAILED posts red, INFO posts blue, others skipped.
Anything else is forwarded as raw text so an unexpected payload never
gets lost.
"""

import json
import os
import urllib.parse
import urllib.request

import boto3

WEBHOOK_SSM_PARAM = os.environ.get(
    "WEBHOOK_SSM_PARAM", "/decentraland/slack_webhook_url"
)
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
MAX_REASON_CHARS = 500
RED = "#d62d20"
BLUE = "#439fe0"

_webhook_cache = None


def webhook_url() -> str:
    # Cached across invocations of a warm Lambda container
    global _webhook_cache
    if _webhook_cache is None:
        ssm = boto3.client("ssm")
        _webhook_cache = ssm.get_parameter(
            Name=WEBHOOK_SSM_PARAM, WithDecryption=True
        )["Parameter"]["Value"]
    return _webhook_cache


def alarm_console_url(alarm_name: str) -> str:
    # The alarm name goes URL-encoded twice: once for the query string,
    # once for the console's client-side route after the '#'
    encoded = urllib.parse.quote(urllib.parse.quote(alarm_name, safe=""))
    return (
        f"https://{AWS_REGION}.console.aws.amazon.com/cloudwatch/home"
        f"?region={AWS_REGION}#alarmsV2:alarm/{encoded}"
    )


def _alarm_payload(alarm: dict) -> dict | None:
    if alarm["NewStateValue"] != "ALARM":
        return None

    dimensions = alarm.get("Trigger", {}).get("Dimensions", [])
    function_name = next(
        (d["value"] for d in dimensions if d["name"] == "FunctionName"),
        "unknown",
    )
    reason = alarm.get("NewStateReason", "")[:MAX_REASON_CHARS]
    alarm_name = alarm["AlarmName"]

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🔴 Pipeline failure: {function_name}",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*Source:*\nLambda"},
                {"type": "mrkdwn", "text": f"*Component:*\n`{function_name}`"},
                {"type": "mrkdwn", "text": "*Status:*\nALARM"},
                {
                    "type": "mrkdwn",
                    "text": f"*When (UTC):*\n{alarm['StateChangeTime']}",
                },
            ],
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": reason}},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"<{alarm_console_url(alarm_name)}|View alarm in CloudWatch> · {alarm_name}",
                }
            ],
        },
    ]
    return {"attachments": [{"color": RED, "blocks": blocks}]}


def _custom_payload(message: dict) -> dict | None:
    """Pipeline contract: {source, component, status, detail, execution_url?}.

    FAILED -> red alert, INFO -> blue notification, anything else skipped.
    """
    status = message["status"]
    if status not in ("FAILED", "INFO"):
        return None

    color, icon, kind = (
        (RED, "🔴", "Pipeline failure")
        if status == "FAILED"
        else (BLUE, "ℹ️", "Pipeline notification")
    )
    component = message.get("component", "unknown")
    detail = message.get("detail", "")[:MAX_REASON_CHARS]

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{icon} {kind}: {component}"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Source:*\n{message.get('source', 'unknown')}"},
                {"type": "mrkdwn", "text": f"*Status:*\n{status}"},
            ],
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": detail}},
    ]
    execution_url = message.get("execution_url")
    if execution_url:
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"<{execution_url}|View execution>"}],
            }
        )
    return {"attachments": [{"color": color, "blocks": blocks}]}


def build_payload(message: str) -> dict | None:
    """Turn an SNS message into a Slack payload, or None to skip (non-failure)."""
    try:
        parsed = json.loads(message)
        if isinstance(parsed, dict) and "source" in parsed and "status" in parsed:
            return _custom_payload(parsed)
        assert isinstance(parsed, dict) and "AlarmName" in parsed
    except (ValueError, AssertionError):
        # Unknown payload: never drop an alert on the floor
        return {
            "attachments": [
                {
                    "color": RED,
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"🔴 Unrecognized alert:\n```{message[:MAX_REASON_CHARS]}```",
                            },
                        }
                    ],
                }
            ]
        }
    return _alarm_payload(parsed)


def post_to_slack(payload: dict) -> None:
    request = urllib.request.Request(
        webhook_url(),
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status != 200:
            raise RuntimeError(f"Slack webhook returned {response.status}")


def handler(event, context):
    posted = skipped = 0
    for record in event["Records"]:
        payload = build_payload(record["Sns"]["Message"])
        if payload is None:
            skipped += 1
            continue
        post_to_slack(payload)
        posted += 1
    print(f"posted={posted} skipped={skipped}")
    return {"posted": posted, "skipped": skipped}
