import datetime
from pathlib import Path

import pytest

from ingestion.nft_contracts.validate import (
    EXPECTED_HEADER,
    parse_and_validate,
    partition_key,
)

FIXTURE_BYTES = (
    Path(__file__).parent.parent / "reference" / "nft_contracts.csv"
).read_bytes()


def test_real_reference_file_parses():
    rows = parse_and_validate(FIXTURE_BYTES)
    assert len(rows) == 830
    assert {r["chain_id"] for r in rows} == {1, 137}
    first = rows[0]
    assert isinstance(first["chain_id"], int)
    assert isinstance(first["first_mint_dt"], datetime.date)
    assert isinstance(first["extract_from_dt"], datetime.date)


def test_default_sentinels_present():
    rows = parse_and_validate(FIXTURE_BYTES)
    assert all(r["first_mint_dt"] == datetime.date(2001, 1, 1) for r in rows)
    assert all(r["extract_from_dt"] == datetime.date(2026, 1, 1) for r in rows)


def test_empty_contract_name_is_allowed():
    rows = parse_and_validate(FIXTURE_BYTES)
    assert any(r["contract_name"] == "" for r in rows)  # 12 pending-curation rows


VALID_LINE = "1,0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,Bored Ape Yacht Club,2001-01-01,2026-01-01"
HEADER = "chain_id,contract_address,contract_name,first_mint_dt,extract_from_dt"


def _csv(*lines):
    return ("\n".join(lines) + "\n").encode()


@pytest.mark.parametrize(
    "bad_csv,match",
    [
        (_csv("chain_id,address,name,a,b", VALID_LINE), "bad header"),
        (_csv(HEADER.rsplit(",", 1)[0], "1,0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,X,2001-01-01"), "bad header"),
        (_csv(HEADER, "99,0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,X,2001-01-01,2026-01-01"), "unknown chain_id"),
        (_csv(HEADER, "1,0xBC4CA0EDA7647A8AB7C2061C2E118A18A936F13D,X,2001-01-01,2026-01-01"), "invalid address"),
        (_csv(HEADER, "1,0x1234,X,2001-01-01,2026-01-01"), "invalid address"),
        (_csv(HEADER, "1,0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,X,20010101,2026-01-01"), "must be YYYY-MM-DD"),
        (_csv(HEADER, "1,0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,X,2001-01-01,2026-13-45"), "not a valid date"),
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


def test_same_address_on_two_chains_is_not_a_duplicate():
    line137 = VALID_LINE.replace("1,0x", "137,0x", 1)
    rows = parse_and_validate(_csv(HEADER, VALID_LINE, line137))
    assert len(rows) == 2


def test_partition_key_format():
    key = partition_key(datetime.date(2026, 8, 21))
    assert key == "bronze/nft_contracts/dt=2026-08-21/contracts.parquet"


def test_expected_header():
    assert EXPECTED_HEADER == [
        "chain_id",
        "contract_address",
        "contract_name",
        "first_mint_dt",
        "extract_from_dt",
    ]


def test_rows_to_parquet_roundtrip(tmp_path):
    import pyarrow.parquet as pq

    from ingestion.nft_contracts.handler import rows_to_parquet

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
    ]
    assert table.num_rows == 830
    assert str(table.schema.field("chain_id").type) == "int32"
    assert str(table.schema.field("first_mint_dt").type) == "date32[day]"
    assert str(table.schema.field("extract_from_dt").type) == "date32[day]"


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
    import ingestion.nft_contracts.handler as h

    written = {}
    queries = []

    class FakeS3:
        def get_object(self, Bucket, Key):
            import io as _io

            return {"Body": _io.BytesIO(FIXTURE_BYTES)}

        def put_object(self, Bucket, Key, Body):
            written["bucket"], written["key"], written["body"] = Bucket, Key, Body

    monkeypatch.setattr(
        h.boto3,
        "client",
        lambda service: FakeAthena(queries) if service == "athena" else FakeS3(),
    )
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": "landing/nft_contracts/nft_contracts.csv"},
                }
            }
        ]
    }
    result = h.handler(event, None)
    assert result["rows"] == 830
    assert written["bucket"] == "test-bucket"
    assert written["key"].startswith("bronze/nft_contracts/dt=")
    assert written["key"].endswith("/contracts.parquet")
    assert result["s3_key"] == written["key"]
    assert result["source_key"] == "landing/nft_contracts/nft_contracts.csv"
    assert len(written["body"]) > 1000  # real parquet bytes

    # The partition DDL ran, on the right table/path, in the tagged workgroup
    assert len(queries) == 1
    ddl = queries[0]["ddl"]
    assert "ALTER TABLE bronze.nft_contracts" in ddl
    assert "ADD IF NOT EXISTS PARTITION" in ddl
    dt = written["key"].split("dt=")[1].split("/")[0]
    assert f"(dt = '{dt}')" in ddl
    assert f"LOCATION 's3://test-bucket/bronze/nft_contracts/dt={dt}/'" in ddl
    assert queries[0]["workgroup"] == "decentraland-data-platform"


def test_register_partition_raises_on_failed_ddl(monkeypatch):
    import ingestion.nft_contracts.handler as h

    monkeypatch.setattr(
        h.boto3, "client", lambda service: FakeAthena([], state="FAILED")
    )
    with pytest.raises(RuntimeError, match="FAILED.*fake reason"):
        h.register_partition(datetime.date(2026, 8, 21), "test-bucket")
