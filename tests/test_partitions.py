import datetime

import pytest

from ingestion.common.partitions import register_partition


class FakeAthena:
    """Records DDL statements and reports a fixed terminal state."""

    def __init__(self, queries, state="SUCCEEDED"):
        self.queries = queries
        self.state = state

    def start_query_execution(self, QueryString, WorkGroup):
        self.queries.append({"ddl": QueryString, "workgroup": WorkGroup})
        return {"QueryExecutionId": "fake-query-id"}

    def get_query_execution(self, QueryExecutionId):
        return {
            "QueryExecution": {
                "Status": {"State": self.state, "StateChangeReason": "fake reason"}
            }
        }


def test_register_partition_builds_ddl_for_the_given_table(monkeypatch):
    import ingestion.common.partitions as p

    queries = []
    monkeypatch.setattr(p.boto3, "client", lambda service: FakeAthena(queries))
    register_partition("dcl_contracts", datetime.date(2026, 8, 22), "test-bucket")

    assert len(queries) == 1
    ddl = queries[0]["ddl"]
    assert "ALTER TABLE bronze.dcl_contracts" in ddl
    assert "ADD IF NOT EXISTS PARTITION (dt = '2026-08-22')" in ddl
    assert "LOCATION 's3://test-bucket/bronze/dcl_contracts/dt=2026-08-22/'" in ddl
    assert queries[0]["workgroup"] == "decentraland-data-platform"


def test_register_partition_raises_on_failed_ddl(monkeypatch):
    import ingestion.common.partitions as p

    monkeypatch.setattr(
        p.boto3, "client", lambda service: FakeAthena([], state="FAILED")
    )
    with pytest.raises(RuntimeError, match="FAILED.*fake reason"):
        register_partition("contracts", datetime.date(2026, 8, 21), "test-bucket")
