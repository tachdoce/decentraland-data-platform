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
