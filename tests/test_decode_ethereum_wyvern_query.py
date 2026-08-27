import json
from pathlib import Path

import pytest
from eth_abi import decode as abi_decode

from decode.ethereum_wyvern_query import (
    ORDERS_MATCHED_TOPIC,
    WYVERN_V1,
    WYVERN_V23,
    build_query,
)

FIXTURE = Path(__file__).parent / "fixtures" / "wyvern_orders_matched_2021-08-15.json"


def test_query_filters_addresses_topic_and_range():
    q = build_query("2021-08-01", "2021-08-05")
    assert ORDERS_MATCHED_TOPIC in q
    assert WYVERN_V1 in q and WYVERN_V23 in q
    assert "BETWEEN '2021-08-01' AND '2021-08-05'" in q
    assert '"bronze"."ethereum_logs"' in q
    assert "cardinality(topics) = 4" in q
    assert "length(data) = 194" in q


def test_query_rejects_bad_dates():
    with pytest.raises(ValueError):
        build_query("2021/08/01", "2021-08-05")
    with pytest.raises(ValueError):
        build_query("2021-08-01", "not-a-date")


def test_sql_substr_offsets_match_eth_abi_on_real_log():
    # Reproduce the query's substr arithmetic in Python (1-based, same
    # positions) and compare against eth_abi on a real bronze log.
    for record in json.loads(FIXTURE.read_text()):
        data, topics = record["data"], record["topics"]
        buy_hash, sell_hash, price = abi_decode(
            ["bytes32", "bytes32", "uint256"], bytes.fromhex(data[2:])
        )
        # substr(data, 3, 64) / substr(data, 67, 64) / substr(data, 131, 64)
        assert data[2:66] == buy_hash.hex()
        assert data[66:130] == sell_hash.hex()
        assert int(data[130:194], 16) == price
        # maker/taker: concat('0x', substr(topics[n], 27))
        assert len(topics[1][26:]) == 40
        assert len(topics[2][26:]) == 40
