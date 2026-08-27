import json
from pathlib import Path

import pytest
from eth_abi import decode as abi_decode

from decode.ethereum_erc20_query import TRANSFER_TOPIC, build_query

FIXTURE = Path(__file__).parent / "fixtures" / "erc20_transfer_2021-08-15.json"


def test_query_filters_topic_cardinality_and_range():
    q = build_query("2021-08-01", "2021-08-05")
    assert TRANSFER_TOPIC in q
    assert "BETWEEN '2021-08-01' AND '2021-08-05'" in q
    assert '"bronze"."ethereum_logs"' in q
    assert "cardinality(topics) = 3" in q
    assert "length(data) = 66" in q
    # overflow-era filter: high 160 bits of the amount word must be zero
    assert f"substr(data, 3, 40) = '{'0' * 40}'" in q


def test_query_rejects_bad_dates():
    with pytest.raises(ValueError):
        build_query("2021/08/01", "2021-08-05")
    with pytest.raises(ValueError):
        build_query("2021-08-01", "not-a-date")


def test_sql_substr_offsets_match_eth_abi_on_real_logs():
    # Reproduce the query's substr arithmetic in Python (1-based, same
    # positions) and compare against eth_abi on real bronze logs.
    for record in json.loads(FIXTURE.read_text()):
        data, topics = record["data"], record["topics"]
        (amount,) = abi_decode(["uint256"], bytes.fromhex(data[2:]))
        # amount_hex: substr(data, 3, 64)
        assert int(data[2:66], 16) == amount
        # overflow filter: substr(data, 3, 40)
        assert data[2:42] == "0" * 40
        # from/to: concat('0x', substr(topics[n], 27))
        assert len(topics[1][26:]) == 40
        assert len(topics[2][26:]) == 40
