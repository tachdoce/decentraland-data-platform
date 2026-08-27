import pytest

from decode.ethereum_marketplace_trades_query import (
    MARKETPLACE_V3,
    MARKETPLACE_V4,
    TRADED_TOPIC,
    build_query,
)


def test_query_filters_addresses_topic_and_range():
    q = build_query("2025-01-01", "2025-01-05")
    assert TRADED_TOPIC in q
    assert MARKETPLACE_V3 in q and MARKETPLACE_V4 in q
    assert "BETWEEN '2025-01-01' AND '2025-01-05'" in q
    assert '"bronze"."ethereum_logs"' in q
    assert "cardinality(topics) = 3" in q


def test_query_rejects_bad_dates():
    with pytest.raises(ValueError):
        build_query("2025/01/01", "2025-01-05")
    with pytest.raises(ValueError):
        build_query("2025-01-01", "not-a-date")
