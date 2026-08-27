import pytest

from decode.ethereum_seaport_parser import ORDER_FULFILLED_TOPIC
from decode.ethereum_seaport_query import build_query


def test_query_filters_topic_and_range():
    q = build_query("2026-08-01", "2026-08-05")
    assert ORDER_FULFILLED_TOPIC in q
    assert "BETWEEN '2026-08-01' AND '2026-08-05'" in q
    assert '"bronze"."ethereum_logs"' in q
    # no address filter by design: topic0 already identifies the event
    assert "address IN" not in q


def test_query_rejects_bad_dates():
    with pytest.raises(ValueError):
        build_query("2026/08/01", "2026-08-05")
    with pytest.raises(ValueError):
        build_query("2026-08-01", "not-a-date")
