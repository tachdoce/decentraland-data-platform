import datetime
from pathlib import Path

import pytest

from ingestion.erc20_tokens.validate import (
    EXPECTED_HEADER,
    parse_and_validate,
    partition_key,
)

FIXTURE_BYTES = (
    Path(__file__).parent.parent / "reference" / "erc20_tokens.csv"
).read_bytes()


def test_real_reference_file_parses():
    rows = parse_and_validate(FIXTURE_BYTES)
    assert len(rows) == 22
    assert {r["chain_id"] for r in rows} == {1}
    first = rows[0]
    assert isinstance(first["chain_id"], int)
    assert isinstance(first["contract_address"], str)
    assert isinstance(first["name"], str)
    assert isinstance(first["fsym"], str)
    assert isinstance(first["decimals"], int)


def test_reference_file_known_facts():
    rows = parse_and_validate(FIXTURE_BYTES)
    by_addr = {r["contract_address"]: r for r in rows}
    # Native-ETH placeholder row is a regular row
    assert by_addr["0x0000000000000000000000000000000000000000"]["fsym"] == "ETH"
    # fsym is NOT unique: GALA v1 and v2 share the ticker
    assert sum(1 for r in rows if r["fsym"] == "GALA") == 2
    # decimals exceptions
    assert by_addr["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"]["decimals"] == 6
    assert by_addr["0xdac17f958d2ee523a2206206994597c13d831ec7"]["decimals"] == 6
    assert by_addr["0xe3c408bd53c31c085a1746af401a4042954ff740"]["decimals"] == 8


HEADER = "chain_id,contract_address,name,fsym,decimals"
VALID_LINE = "1,0x0f5d2fb29fb7d3cfee444a200298f468908cc942,Decentraland MANA,MANA,18"


def _csv(*lines):
    return ("\n".join(lines) + "\n").encode()


def _line(**overrides):
    fields = {
        "chain_id": "1",
        "contract_address": "0x0f5d2fb29fb7d3cfee444a200298f468908cc942",
        "name": "Decentraland MANA",
        "fsym": "MANA",
        "decimals": "18",
    }
    fields.update(overrides)
    return ",".join(fields.values())


@pytest.mark.parametrize(
    "bad_csv,match",
    [
        (_csv("chain_id,address,name,fsym,decimals", VALID_LINE), "bad header"),
        (_csv(HEADER.rsplit(",", 1)[0], _line()), "bad header"),
        (_csv(HEADER, _line(chain_id="99")), "unknown chain_id"),
        (_csv(HEADER, _line(chain_id="x")), "chain_id is not an integer"),
        (_csv(HEADER, _line(contract_address="0x0F5D2FB29FB7D3CFEE444A200298F468908CC942")), "invalid address"),
        (_csv(HEADER, _line(contract_address="0x1234")), "invalid address"),
        (_csv(HEADER, _line(name="")), "name must not be empty"),
        (_csv(HEADER, _line(fsym="mana")), "invalid fsym"),
        (_csv(HEADER, _line(fsym="")), "invalid fsym"),
        (_csv(HEADER, _line(fsym="VERYLONGTICKER")), "invalid fsym"),
        (_csv(HEADER, _line(decimals="-1")), "decimals out of range"),
        (_csv(HEADER, _line(decimals="37")), "decimals out of range"),
        (_csv(HEADER, _line(decimals="abc")), "decimals is not an integer"),
        (_csv(HEADER, _line(decimals="")), "decimals is not an integer"),
        (_csv(HEADER, VALID_LINE, VALID_LINE), "duplicate"),
        (_csv(HEADER), "no data rows"),
        (b"", "bad header|CSV is empty"),
    ],
)
def test_rejects_bad_input(bad_csv, match):
    with pytest.raises(ValueError, match=match):
        parse_and_validate(bad_csv)


def test_bom_prefixed_csv_is_accepted():
    bom_csv = b"\xef\xbb\xbf" + _csv(HEADER, VALID_LINE)
    assert len(parse_and_validate(bom_csv)) == 1


def test_crlf_line_endings_are_accepted():
    crlf_csv = f"{HEADER}\r\n{VALID_LINE}\r\n".encode()
    rows = parse_and_validate(crlf_csv)
    assert len(rows) == 1
    assert rows[0]["decimals"] == 18


def test_duplicate_fsym_on_different_addresses_is_allowed():
    line_v1 = "1,0x15d4c048f83bd7e37d49ea4c83a07267ec4203da,Gala,GALA,8"
    line_v2 = "1,0xd1d2eb1b1e90b638588728b4130137d262c87cae,Gala,GALA,8"
    rows = parse_and_validate(_csv(HEADER, line_v1, line_v2))
    assert len(rows) == 2


def test_same_address_on_two_chains_is_not_a_duplicate():
    line137 = VALID_LINE.replace("1,0x", "137,0x", 1)
    rows = parse_and_validate(_csv(HEADER, VALID_LINE, line137))
    assert len(rows) == 2


def test_partition_key_format():
    key = partition_key(datetime.date(2026, 9, 2))
    assert key == "bronze/erc20_tokens/dt=2026-09-02/erc20_tokens.parquet"


def test_expected_header():
    assert EXPECTED_HEADER == [
        "chain_id",
        "contract_address",
        "name",
        "fsym",
        "decimals",
    ]


def test_rows_to_parquet_roundtrip(tmp_path):
    import pyarrow.parquet as pq

    from ingestion.erc20_tokens.handler import rows_to_parquet

    rows = parse_and_validate(FIXTURE_BYTES)
    out = tmp_path / "erc20_tokens.parquet"
    out.write_bytes(rows_to_parquet(rows))
    table = pq.read_table(out)
    assert table.column_names == [
        "chain_id",
        "contract_address",
        "name",
        "fsym",
        "decimals",
    ]
    assert table.num_rows == 22
    assert str(table.schema.field("chain_id").type) == "int32"
    assert str(table.schema.field("contract_address").type) == "string"
    assert str(table.schema.field("name").type) == "string"
    assert str(table.schema.field("fsym").type) == "string"
    assert str(table.schema.field("decimals").type) == "int32"


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


def test_handler_reads_event_and_writes_partition(monkeypatch):
    import ingestion.erc20_tokens.handler as h

    written = {}
    queries = []

    class FakeS3:
        def get_object(self, Bucket, Key):
            import io as _io

            return {"Body": _io.BytesIO(FIXTURE_BYTES)}

        def put_object(self, Bucket, Key, Body):
            written["bucket"], written["key"], written["body"] = Bucket, Key, Body

    def fake_client(service):
        if service == "s3":
            return FakeS3()
        else:
            return FakeAthena(queries)

    monkeypatch.setattr(h.boto3, "client", fake_client)
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": "landing/erc20_tokens/erc20_tokens.csv"},
                }
            }
        ]
    }
    result = h.handler(event, None)
    assert result["rows"] == 22
    assert written["bucket"] == "test-bucket"
    assert written["key"].startswith("bronze/erc20_tokens/dt=")
    assert written["key"].endswith("/erc20_tokens.parquet")
    assert result["s3_key"] == written["key"]
    assert result["source_key"] == "landing/erc20_tokens/erc20_tokens.csv"
    assert len(written["body"]) > 500  # real parquet bytes

    # The partition DDL ran, on the right table/path, in the tagged workgroup
    assert len(queries) == 1
    ddl = queries[0]["ddl"]
    assert "ALTER TABLE bronze.erc20_tokens" in ddl
    assert "ADD IF NOT EXISTS PARTITION" in ddl
    dt = written["key"].split("dt=")[1].split("/")[0]
    assert f"(dt = '{dt}')" in ddl
    assert f"LOCATION 's3://test-bucket/bronze/erc20_tokens/dt={dt}/'" in ddl
    assert queries[0]["workgroup"] == "decentraland-data-platform"
