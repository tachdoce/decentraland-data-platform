import json
from types import SimpleNamespace
from typing import ClassVar

import pytest

import dbt_runner.handler as h


class FakeDbtRunner:
    """Captures invoke args and returns a canned result."""

    calls: ClassVar[list[list[str]]] = []
    success = True

    def invoke(self, args):
        FakeDbtRunner.calls.append(list(args))
        return SimpleNamespace(success=FakeDbtRunner.success, exception=None)


class FakeAthena:
    """One changes query returning the configured data rows."""

    def __init__(self, data_rows, state="SUCCEEDED"):
        self.data_rows = data_rows
        self.state = state

    def start_query_execution(self, QueryString, WorkGroup):
        assert WorkGroup == "decentraland-data-platform"
        return {"QueryExecutionId": "qid"}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": self.state}}}

    def get_query_results(self, QueryExecutionId):
        header = {"Data": [{"VarCharValue": "change_type"}]}
        rows = [{"Data": [{"VarCharValue": v}]} for v in self.data_rows]
        return {"ResultSet": {"Rows": [header] + rows}}


class FakeSNS:
    def __init__(self):
        self.published = []

    def publish(self, TopicArn, Message):
        self.published.append({"topic": TopicArn, "message": json.loads(Message)})


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("DBT_PROJECT_DIR", "/var/task/dbt")
    monkeypatch.setenv("ATHENA_WORKGROUP", "decentraland-data-platform")
    monkeypatch.setenv("ALERTS_TOPIC_ARN", "arn:aws:sns:us-east-1:1:decentraland-alerts")


def _wire(monkeypatch, athena, sns):
    FakeDbtRunner.calls = []
    monkeypatch.setattr(h, "dbtRunner", FakeDbtRunner)
    monkeypatch.setattr(
        h.boto3, "client", lambda svc: {"athena": athena, "sns": sns}[svc]
    )


def test_default_selector_and_no_changes(env, monkeypatch):
    sns = FakeSNS()
    _wire(monkeypatch, FakeAthena([]), sns)
    FakeDbtRunner.success = True

    result = h.handler({}, None)

    args = FakeDbtRunner.calls[0]
    assert args[:3] == ["build", "--select", "source:bronze.contracts+"]
    assert "--project-dir" in args and "/var/task/dbt" in args
    assert "--target-path" in args and "/tmp/dbt-target" in args
    assert result == {"select": "source:bronze.contracts+", "changes": {}}
    assert sns.published == []


def test_selector_override(env, monkeypatch):
    _wire(monkeypatch, FakeAthena([]), FakeSNS())
    FakeDbtRunner.success = True

    result = h.handler({"select": "dim_contracts"}, None)

    assert FakeDbtRunner.calls[0][:3] == ["build", "--select", "dim_contracts"]
    assert result["select"] == "dim_contracts"


def test_dbt_failure_raises_before_touching_athena(env, monkeypatch):
    def boom(svc):  # any AWS call after a failed build is a bug
        raise AssertionError("no AWS client expected")

    FakeDbtRunner.calls = []
    monkeypatch.setattr(h, "dbtRunner", FakeDbtRunner)
    monkeypatch.setattr(h.boto3, "client", boom)
    FakeDbtRunner.success = False

    with pytest.raises(RuntimeError, match="dbt build failed"):
        h.handler({}, None)


def test_changes_publish_info(env, monkeypatch):
    sns = FakeSNS()
    _wire(monkeypatch, FakeAthena(["added", "added", "renamed"]), sns)
    FakeDbtRunner.success = True

    result = h.handler({}, None)

    assert result["changes"] == {"added": 2, "renamed": 1}
    assert len(sns.published) == 1
    msg = sns.published[0]["message"]
    assert msg["source"] == "dbt"
    assert msg["component"] == "dbt build"
    assert msg["status"] == "INFO"
    assert "added=2" in msg["detail"] and "renamed=1" in msg["detail"]


def test_changes_query_failure_raises(env, monkeypatch):
    _wire(monkeypatch, FakeAthena([], state="FAILED"), FakeSNS())
    FakeDbtRunner.success = True

    with pytest.raises(RuntimeError, match="changes query"):
        h.handler({}, None)
