import datetime
from pathlib import Path

import pytest

from ingestion.contracts.validate import (
    EXPECTED_HEADER,
    parse_and_validate,
    partition_key,
)

FIXTURE_BYTES = (
    Path(__file__).parent.parent / "reference" / "contracts.csv"
).read_bytes()


def test_real_reference_file_parses():
    rows = parse_and_validate(FIXTURE_BYTES)
    assert len(rows) == 966
    assert {r["chain_id"] for r in rows} == {1, 137}
    first = rows[0]
    assert isinstance(first["chain_id"], int)
    assert isinstance(first["first_mint_dt"], datetime.date)
    assert isinstance(first["extract_from_dt"], datetime.date)
    assert isinstance(first["dcl_contract"], bool)
    assert isinstance(first["erc_type"], int)


def test_default_sentinels_present():
    rows = parse_and_validate(FIXTURE_BYTES)
    assert all(r["first_mint_dt"] == datetime.date(2001, 1, 1) for r in rows)
    assert all(r["extract_from_dt"] == datetime.date(2026, 1, 1) for r in rows)


def test_empty_contract_name_is_allowed():
    # The reference file is fully curated today; empty names remain legal
    # for future pending-curation rows.
    rows = parse_and_validate(_csv(HEADER, _line(contract_name="")))
    assert rows[0]["contract_name"] == ""


def test_reference_file_erc_type_distribution():
    rows = parse_and_validate(FIXTURE_BYTES)
    assert {r["erc_type"] for r in rows} <= {-1, 0, 20, 721, 1155}
    assert any(r["dcl_contract"] for r in rows)
    assert any(not r["dcl_contract"] for r in rows)


VALID_LINE = "1,0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,Bored Ape Yacht Club,2001-01-01,2026-01-01,FALSE,721"
HEADER = "chain_id,contract_address,contract_name,first_mint_dt,extract_from_dt,dcl_contract,erc_type"


def _csv(*lines):
    return ("\n".join(lines) + "\n").encode()


def _line(**overrides):
    fields = {
        "chain_id": "1",
        "contract_address": "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d",
        "contract_name": "X",
        "first_mint_dt": "2001-01-01",
        "extract_from_dt": "2026-01-01",
        "dcl_contract": "FALSE",
        "erc_type": "721",
    }
    fields.update(overrides)
    return ",".join(fields.values())


@pytest.mark.parametrize(
    "bad_csv,match",
    [
        (_csv("chain_id,address,name,a,b,c,d", VALID_LINE), "bad header"),
        (_csv(HEADER.rsplit(",", 1)[0], _line()), "bad header"),
        (_csv(HEADER, _line(chain_id="99")), "unknown chain_id"),
        (_csv(HEADER, _line(contract_address="0xBC4CA0EDA7647A8AB7C2061C2E118A18A936F13D")), "invalid address"),
        (_csv(HEADER, _line(contract_address="0x1234")), "invalid address"),
        (_csv(HEADER, _line(first_mint_dt="20010101")), "must be YYYY-MM-DD"),
        (_csv(HEADER, _line(extract_from_dt="2026-13-45")), "not a valid date"),
        (_csv(HEADER, _line(dcl_contract="true")), "dcl_contract must be TRUE or FALSE"),
        (_csv(HEADER, _line(dcl_contract="1")), "dcl_contract must be TRUE or FALSE"),
        (_csv(HEADER, _line(erc_type="777")), "unknown erc_type"),
        (_csv(HEADER, _line(erc_type="abc")), "erc_type is not an integer"),
        (_csv(HEADER, _line(erc_type="")), "erc_type is not an integer"),
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
    assert rows[0]["erc_type"] == 721


def test_every_erc_type_is_accepted():
    lines = [
        _line(contract_address=f"0x{i:040x}", erc_type=str(t))
        for i, t in enumerate([-1, 0, 20, 721, 1155])
    ]
    rows = parse_and_validate(_csv(HEADER, *lines))
    assert [r["erc_type"] for r in rows] == [-1, 0, 20, 721, 1155]


def test_same_address_on_two_chains_is_not_a_duplicate():
    line137 = VALID_LINE.replace("1,0x", "137,0x", 1)
    rows = parse_and_validate(_csv(HEADER, VALID_LINE, line137))
    assert len(rows) == 2


def test_partition_key_format():
    key = partition_key(datetime.date(2026, 8, 21))
    assert key == "bronze/contracts/dt=2026-08-21/contracts.parquet"


def test_expected_header():
    assert EXPECTED_HEADER == [
        "chain_id",
        "contract_address",
        "contract_name",
        "first_mint_dt",
        "extract_from_dt",
        "dcl_contract",
        "erc_type",
    ]


def test_rows_to_parquet_roundtrip(tmp_path):
    import pyarrow.parquet as pq

    from ingestion.contracts.handler import rows_to_parquet

    rows = parse_and_validate(FIXTURE_BYTES)
    out = tmp_path / "contracts.parquet"
    out.write_bytes(rows_to_parquet(rows))
    table = pq.read_table(out)
    assert table.column_names == [
        "chain_id",
        "contract_address",
        "contract_name",
        "first_mint_dt",
        "extract_from_dt",
        "dcl_contract",
        "erc_type",
    ]
    assert table.num_rows == 966
    assert str(table.schema.field("chain_id").type) == "int32"
    assert str(table.schema.field("first_mint_dt").type) == "date32[day]"
    assert str(table.schema.field("extract_from_dt").type) == "date32[day]"
    assert str(table.schema.field("dcl_contract").type) == "bool"
    assert str(table.schema.field("erc_type").type) == "int32"


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


def test_handler_reads_event_and_writes_partition(monkeypatch, tmp_path):
    import ingestion.contracts.handler as h

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
                    "object": {"key": "landing/contracts/contracts.csv"},
                }
            }
        ]
    }
    result = h.handler(event, None)
    assert result["rows"] == 966
    assert written["bucket"] == "test-bucket"
    assert written["key"].startswith("bronze/contracts/dt=")
    assert written["key"].endswith("/contracts.parquet")
    assert result["s3_key"] == written["key"]
    assert result["source_key"] == "landing/contracts/contracts.csv"
    assert len(written["body"]) > 1000  # real parquet bytes

    # The partition DDL ran, on the right table/path, in the tagged workgroup
    assert len(queries) == 1
    ddl = queries[0]["ddl"]
    assert "ALTER TABLE bronze.contracts" in ddl
    assert "ADD IF NOT EXISTS PARTITION" in ddl
    dt = written["key"].split("dt=")[1].split("/")[0]
    assert f"(dt = '{dt}')" in ddl
    assert f"LOCATION 's3://test-bucket/bronze/contracts/dt={dt}/'" in ddl
    assert queries[0]["workgroup"] == "decentraland-data-platform"
