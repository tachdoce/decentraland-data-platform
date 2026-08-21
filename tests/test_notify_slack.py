import json
from unittest.mock import MagicMock, patch

from alerts.notify_slack.handler import build_payload, handler

ALARM_MESSAGE = json.dumps(
    {
        "AlarmName": "lambda-errors-extract-dcl-contracts",
        "AlarmDescription": "Errors >= 1 in extract-dcl-contracts",
        "AWSAccountId": "683569194224",
        "NewStateValue": "ALARM",
        "NewStateReason": (
            "Threshold Crossed: 1 out of the last 1 datapoints [1.0] was "
            "greater than or equal to the threshold (1.0)."
        ),
        "StateChangeTime": "2026-08-21T06:05:12.345+0000",
        "Region": "US East (N. Virginia)",
        "OldStateValue": "OK",
        "Trigger": {
            "MetricName": "Errors",
            "Namespace": "AWS/Lambda",
            "Dimensions": [
                {"value": "extract-dcl-contracts", "name": "FunctionName"}
            ],
        },
    }
)

OK_MESSAGE = json.dumps(
    {
        "AlarmName": "lambda-errors-extract-dcl-contracts",
        "NewStateValue": "OK",
        "NewStateReason": "back to normal",
        "StateChangeTime": "2026-08-21T07:05:12.345+0000",
        "Trigger": {
            "Namespace": "AWS/Lambda",
            "Dimensions": [
                {"value": "extract-dcl-contracts", "name": "FunctionName"}
            ],
        },
    }
)


DBT_FAILED_MESSAGE = json.dumps(
    {
        "source": "dbt",
        "component": "dbt build",
        "status": "FAILED",
        "detail": "Failure in test assert_dim_contracts_unique_key (dbt/tests)",
        "execution_url": "https://us-east-1.console.aws.amazon.com/cloudwatch/logs/dbt",
    }
)

DBT_INFO_MESSAGE = json.dumps(
    {
        "source": "dbt",
        "component": "dim_contracts_changes",
        "status": "INFO",
        "detail": "3 contract changes: 2 added, 1 removed",
        "execution_url": "https://us-east-1.console.aws.amazon.com/athena/dim_contracts_changes",
    }
)


def sns_event(message: str) -> dict:
    return {"Records": [{"Sns": {"Message": message}}]}


def all_text(payload: dict) -> str:
    return json.dumps(payload)


def test_alarm_message_builds_red_block_kit_payload():
    payload = build_payload(ALARM_MESSAGE)
    attachment = payload["attachments"][0]
    assert attachment["color"] == "#d62d20"
    text = all_text(payload)
    assert "extract-dcl-contracts" in text
    assert "Threshold Crossed" in text
    assert "2026-08-21T06:05:12" in text


def test_alarm_message_links_to_cloudwatch_alarm():
    payload = build_payload(ALARM_MESSAGE)
    text = all_text(payload)
    assert "console.aws.amazon.com/cloudwatch" in text
    assert "lambda-errors-extract-dcl-contracts" in text


def test_ok_state_is_skipped():
    assert build_payload(OK_MESSAGE) is None


def test_unknown_payload_falls_back_to_raw_text():
    payload = build_payload("something broke, not JSON")
    assert "something broke, not JSON" in all_text(payload)


def test_long_reason_is_truncated():
    alarm = json.loads(ALARM_MESSAGE)
    alarm["NewStateReason"] = "x" * 2000
    payload = build_payload(json.dumps(alarm))
    assert len(all_text(payload)) < 2000


def test_custom_failed_message_builds_red_payload():
    payload = build_payload(DBT_FAILED_MESSAGE)
    attachment = payload["attachments"][0]
    assert attachment["color"] == "#d62d20"
    text = all_text(payload)
    assert "Unrecognized" not in text
    assert "dbt build" in text
    assert "assert_dim_contracts_unique_key" in text
    assert "console.aws.amazon.com/cloudwatch/logs/dbt" in text


def test_custom_info_message_builds_non_red_payload():
    payload = build_payload(DBT_INFO_MESSAGE)
    attachment = payload["attachments"][0]
    assert attachment["color"] != "#d62d20"
    text = all_text(payload)
    assert "dim_contracts_changes" in text
    assert "2 added, 1 removed" in text
    assert "athena/dim_contracts_changes" in text


def test_custom_message_with_other_status_is_skipped():
    message = json.loads(DBT_INFO_MESSAGE)
    message["status"] = "SUCCEEDED"
    assert build_payload(json.dumps(message)) is None


def test_custom_message_without_execution_url_still_renders():
    message = json.loads(DBT_INFO_MESSAGE)
    del message["execution_url"]
    payload = build_payload(json.dumps(message))
    text = all_text(payload)
    assert "Unrecognized" not in text
    assert "dim_contracts_changes" in text


def test_custom_long_detail_is_truncated():
    message = json.loads(DBT_FAILED_MESSAGE)
    message["detail"] = "x" * 2000
    payload = build_payload(json.dumps(message))
    text = all_text(payload)
    assert "Unrecognized" not in text
    assert len(text) < 2000


@patch("alerts.notify_slack.handler.urllib.request.urlopen")
@patch("alerts.notify_slack.handler.webhook_url", return_value="https://hooks.example/x")
def test_handler_posts_alarm_to_webhook(mock_url, mock_urlopen):
    mock_urlopen.return_value.__enter__.return_value.status = 200
    result = handler(sns_event(ALARM_MESSAGE), None)
    assert result == {"posted": 1, "skipped": 0}
    request = mock_urlopen.call_args[0][0]
    assert request.full_url == "https://hooks.example/x"
    body = json.loads(request.data.decode())
    assert "extract-dcl-contracts" in json.dumps(body)


@patch("alerts.notify_slack.handler.urllib.request.urlopen")
@patch("alerts.notify_slack.handler.webhook_url", return_value="https://hooks.example/x")
def test_handler_skips_ok_without_posting(mock_url, mock_urlopen):
    result = handler(sns_event(OK_MESSAGE), None)
    assert result == {"posted": 0, "skipped": 1}
    mock_urlopen.assert_not_called()


@patch("alerts.notify_slack.handler.boto3")
def test_webhook_url_reads_ssm_with_decryption(mock_boto3):
    from alerts.notify_slack import handler as h

    h._webhook_cache = None
    mock_ssm = MagicMock()
    mock_boto3.client.return_value = mock_ssm
    mock_ssm.get_parameter.return_value = {
        "Parameter": {"Value": "https://hooks.slack.com/services/T/B/x"}
    }
    assert h.webhook_url() == "https://hooks.slack.com/services/T/B/x"
    mock_ssm.get_parameter.assert_called_once_with(
        Name="/decentraland/slack_webhook_url", WithDecryption=True
    )
