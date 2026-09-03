import datetime
import json
from pathlib import Path

import pytest

from ingestion.token_prices.defillama import (
    chart_url,
    chunk_range,
    coin_id,
    month_key,
    parse_chart_response,
)

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "defillama_chart_2020-12.json").read_text()
)
MANA = "0x0f5d2fb29fb7d3cfee444a200298f468908cc942"
ZERO = "0x0000000000000000000000000000000000000000"
DEC_START = datetime.date(2020, 12, 1)
ID_MAP = {
    f"ethereum:{MANA}": (1, MANA),
    "coingecko:ethereum": (1, ZERO),
}


def test_coin_id_regular_token():
    assert coin_id(1, MANA) == f"ethereum:{MANA}"


def test_coin_id_native_eth_placeholder():
    assert coin_id(1, ZERO) == "coingecko:ethereum"


def test_coin_id_unknown_chain_raises():
    with pytest.raises(ValueError, match="unsupported chain_id"):
        coin_id(137, MANA)


def test_chunk_range_single_short_chunk():
    assert chunk_range(DEC_START, datetime.date(2020, 12, 31)) == [(DEC_START, 31)]


def test_chunk_range_splits_and_covers_every_day_once():
    start, end = datetime.date(2019, 1, 1), datetime.date(2019, 12, 31)
    chunks = chunk_range(start, end)  # 365 days, max 120 per chunk
    assert all(span <= 120 for _, span in chunks)
    days = []
    for chunk_start, span in chunks:
        days += [chunk_start + datetime.timedelta(days=i) for i in range(span)]
    assert days == [start + datetime.timedelta(days=i) for i in range(365)]


def test_chunk_range_single_day():
    d = datetime.date(2026, 9, 2)
    assert chunk_range(d, d) == [(d, 1)]


def test_chart_url_batches_coins():
    url = chart_url(["coingecko:ethereum", f"ethereum:{MANA}"], DEC_START, 31)
    assert url == (
        "https://coins.llama.fi/chart/"
        f"coingecko:ethereum,ethereum:{MANA}"
        "?start=1606780800&span=31&period=1d"
    )


def test_parse_real_fixture_full_grid():
    rows = parse_chart_response(FIXTURE, DEC_START, 31, ID_MAP)
    mana = [r for r in rows if r["contract_address"] == MANA]
    # Every December day exactly once, from the grid — not from timestamps
    assert [r["dt"] for r in mana] == [
        DEC_START + datetime.timedelta(days=i) for i in range(31)
    ]
    assert all(r["chain_id"] == 1 for r in mana)
    assert all(0.05 < r["price_usd"] < 0.2 for r in mana)  # Dec 2020 MANA range
    # price_ts keeps the real drifting sample time and may cross midnight
    drifted = [r for r in mana if r["price_ts"].date() != r["dt"]]
    assert drifted  # the fixture contains at least one drifted sample
    assert all(r["confidence"] == pytest.approx(0.99) for r in mana)


def test_parse_maps_native_eth_to_zero_address():
    rows = parse_chart_response(FIXTURE, DEC_START, 31, ID_MAP)
    eth = [r for r in rows if r["contract_address"] == ZERO]
    assert len(eth) == 31
    assert all(400 < r["price_usd"] < 800 for r in eth)  # Dec 2020 ETH range


def test_parse_missing_coin_yields_no_rows():
    payload = {"coins": {}}
    assert parse_chart_response(payload, DEC_START, 31, ID_MAP) == []


def test_parse_drops_samples_outside_grid():
    ts = int(
        datetime.datetime(2020, 12, 1, tzinfo=datetime.timezone.utc).timestamp()
    )
    payload = {
        "coins": {
            f"ethereum:{MANA}": {
                "symbol": "MANA",
                "confidence": 0.99,
                "prices": [
                    {"timestamp": ts - 90000, "price": 1.0},  # > half a day early
                    {"timestamp": ts + 10, "price": 2.0},
                ],
            }
        }
    }
    rows = parse_chart_response(payload, DEC_START, 1, ID_MAP)
    assert [r["price_usd"] for r in rows] == [2.0]


def test_parse_keeps_nearest_sample_on_grid_collision():
    ts = int(
        datetime.datetime(2020, 12, 1, tzinfo=datetime.timezone.utc).timestamp()
    )
    payload = {
        "coins": {
            f"ethereum:{MANA}": {
                "symbol": "MANA",
                "confidence": 0.99,
                "prices": [
                    {"timestamp": ts + 3600, "price": 1.0},  # 01:00, further
                    {"timestamp": ts + 60, "price": 2.0},  # 00:01, nearest
                ],
            }
        }
    }
    rows = parse_chart_response(payload, DEC_START, 1, ID_MAP)
    assert [r["price_usd"] for r in rows] == [2.0]


def test_month_key():
    assert month_key(datetime.date(2020, 12, 7)) == "2020-12"
