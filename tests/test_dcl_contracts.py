import datetime
import json
from pathlib import Path

import pytest

from ingestion.dcl_contracts.transform import (
    CHAIN_IDS,
    flatten,
    partition_key,
    validate_payload,
)

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "addresses.json").read_text()
)


def test_flatten_keeps_only_mainnet_and_matic():
    rows = flatten(FIXTURE)
    assert set(r["chain_id"] for r in rows) == {1, 137}
    assert len(rows) == len(FIXTURE["mainnet"]) + len(FIXTURE["matic"])


def test_flatten_lowercases_addresses():
    rows = flatten(FIXTURE)
    assert all(r["contract_address"] == r["contract_address"].lower() for r in rows)
    # EstateRegistry comes checksum-cased from the source
    estate = next(r for r in rows if r["contract_name"] == "EstateRegistry")
    assert estate["contract_address"] == "0x52bf3100f4a9337685301614275c85afe28401fc"


def test_flatten_trims_contract_names():
    rows = flatten(FIXTURE)
    assert all(r["contract_name"] == r["contract_name"].strip() for r in rows)
    # The source has "CollectionManager " with a trailing space on matic
    assert any(
        r["contract_name"] == "CollectionManager" and r["chain_id"] == 137
        for r in rows
    )


def test_cross_chain_duplicate_addresses_survive():
    rows = flatten(FIXTURE)
    dup = "0x480a0f4e360e8964e68858dd231c2922f1df45ef"
    matches = [r for r in rows if r["contract_address"] == dup]
    assert {(r["chain_id"], r["contract_name"]) for r in matches} == {
        (1, "TechTribalMarc0matic"),
        (137, "MarketplaceV2"),
    }


@pytest.mark.parametrize(
    "payload",
    [
        {},                                      # empty
        {"mainnet": FIXTURE["mainnet"]},         # missing matic
        {"mainnet": {}, "matic": FIXTURE["matic"]},  # empty network
    ],
)
def test_validate_payload_raises(payload):
    with pytest.raises(ValueError):
        validate_payload(payload)


def test_validate_payload_accepts_fixture():
    validate_payload(FIXTURE)  # must not raise


def test_partition_key_format():
    key = partition_key(datetime.date(2026, 8, 21))
    assert key == "bronze/dcl_contracts/dt=2026-08-21/contracts.parquet"


def test_chain_ids_mapping():
    assert CHAIN_IDS == {"mainnet": 1, "matic": 137}


def test_rows_to_parquet_roundtrip(tmp_path):
    import pyarrow.parquet as pq

    from ingestion.dcl_contracts.handler import rows_to_parquet

    rows = flatten(FIXTURE)
    buf = rows_to_parquet(rows)
    out = tmp_path / "contracts.parquet"
    out.write_bytes(buf)
    table = pq.read_table(out)
    assert table.column_names == ["chain_id", "contract_address", "contract_name"]
    assert table.num_rows == len(rows)
    assert table.schema.field("chain_id").type == "int32"


def test_fetch_sends_explicit_user_agent(monkeypatch):
    import io
    import urllib.request
    from unittest.mock import MagicMock

    from ingestion.dcl_contracts.handler import fetch

    # Mock response object that supports context manager and .read()
    mock_response = MagicMock()
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)
    mock_response.read = MagicMock(return_value=json.dumps(FIXTURE).encode())

    # Capture the Request object passed to urlopen
    captured_request = None

    def fake_urlopen(request, timeout=None):
        nonlocal captured_request
        captured_request = request
        return mock_response

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    # Call fetch
    result = fetch()

    # Verify the Request was created with explicit User-Agent
    assert captured_request is not None
    assert isinstance(captured_request, urllib.request.Request)
    user_agent = captured_request.get_header("User-agent")
    assert user_agent is not None
    assert not user_agent.startswith("Python-urllib")
    assert user_agent == "decentraland-data-platform/1.0"
    assert result == FIXTURE


def test_fetch_raises_on_invalid_json(monkeypatch):
    import io
    import urllib.request

    from ingestion.dcl_contracts.handler import fetch

    class FakeResponse(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(
        urllib.request, "urlopen", lambda req, timeout=None: FakeResponse(b"<html>not json</html>")
    )
    with pytest.raises(ValueError):
        fetch()
