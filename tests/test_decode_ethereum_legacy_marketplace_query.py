import json
from pathlib import Path

import pytest
from eth_abi import decode as abi_decode

from decode.ethereum_legacy_marketplace_query import (
    AUCTION_CANCELLED_TOPIC,
    AUCTION_CREATED_TOPIC,
    AUCTION_SUCCESSFUL_TOPIC,
    LEGACY_MARKETPLACE,
    build_query,
)

FIXTURE = (
    Path(__file__).parent / "fixtures" / "legacy_marketplace_auctions_2018-03-19.json"
)


def test_query_filters_address_topics_and_range():
    q = build_query("2018-03-01", "2018-03-05")
    assert LEGACY_MARKETPLACE in q
    assert AUCTION_CREATED_TOPIC in q
    assert AUCTION_CANCELLED_TOPIC in q
    assert AUCTION_SUCCESSFUL_TOPIC in q
    assert "BETWEEN '2018-03-01' AND '2018-03-05'" in q
    assert '"bronze"."ethereum_logs"' in q
    # static word layouts: per-event data length guard
    assert "194" in q and "130" in q and "66" in q


def test_query_rejects_bad_dates():
    with pytest.raises(ValueError):
        build_query("2018/03/01", "2018-03-05")
    with pytest.raises(ValueError):
        build_query("2018-03-01", "not-a-date")


def test_sql_substr_offsets_match_eth_abi_on_real_logs():
    # Reproduce the query's substr arithmetic in Python (1-based, same
    # positions) and compare against eth_abi on real bronze logs.
    records = json.loads(FIXTURE.read_text())
    seen = set()
    for record in records:
        data, topics = record["data"], record["topics"]
        topic0 = topics[0]
        seen.add(topic0)
        if topic0 == AUCTION_CREATED_TOPIC:
            auction_id, price, expires = abi_decode(
                ["bytes32", "uint256", "uint256"], bytes.fromhex(data[2:])
            )
            assert len(data) == 194
            # total_price_hex: substr(data, 67, 64)
            assert int(data[66:130], 16) == price
            # expires_at_hex: substr(data, 131, 64); value is unix MILLISECONDS
            assert int(data[130:194], 16) == expires
            assert 10**12 < expires < 10**13  # ms magnitude, not seconds
        elif topic0 == AUCTION_SUCCESSFUL_TOPIC:
            auction_id, price = abi_decode(
                ["bytes32", "uint256"], bytes.fromhex(data[2:])
            )
            assert len(data) == 130
            assert int(data[66:130], 16) == price
            # winner: concat('0x', substr(topics[4], 27))
            assert len(topics[3][26:]) == 40
        else:
            assert topic0 == AUCTION_CANCELLED_TOPIC
            (auction_id,) = abi_decode(["bytes32"], bytes.fromhex(data[2:]))
            assert len(data) == 66
        # auction_id: concat('0x', substr(data, 3, 64))
        assert data[2:66] == auction_id.hex()
        # asset_id: topics[2] hex -> unsigned decimal string, <= 78 chars
        asset_id = str(int(topics[1], 16))
        assert asset_id.isdigit() and len(asset_id) <= 78
        # seller: concat('0x', substr(topics[3], 27))
        assert len(topics[2][26:]) == 40
    # the fixture exercises all three events
    assert seen == {
        AUCTION_CREATED_TOPIC,
        AUCTION_CANCELLED_TOPIC,
        AUCTION_SUCCESSFUL_TOPIC,
    }
