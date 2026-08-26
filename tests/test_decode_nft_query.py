import pytest

from decode.query import build_query

TOPIC_721 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TOPIC_1155_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TOPIC_1155_BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"


def test_query_contains_all_topics_and_range():
    q = build_query("2018-06-01", "2018-06-05")
    assert TOPIC_721 in q and TOPIC_1155_SINGLE in q and TOPIC_1155_BATCH in q
    assert "BETWEEN '2018-06-01' AND '2018-06-05'" in q
    assert q.lstrip().startswith("WITH logs AS")


def test_query_rejects_bad_dates():
    for bad in ("2018-6-1", "20180601", "2018-06-01'; DROP", ""):
        with pytest.raises(ValueError):
            build_query(bad, "2018-06-05")
        with pytest.raises(ValueError):
            build_query("2018-06-01", bad)
