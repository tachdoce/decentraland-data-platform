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

# Real hourly response for 2026-08-01: ETH has all 24 ticks (its first
# sample even drifts to 2026-07-31 23:59:59); MANA is entirely absent —
# DefiLlama's hourly resolution is a short rolling window for most tokens.
FIXTURE = json.loads(
    (
        Path(__file__).parent / "fixtures" / "defillama_chart_hourly_2026-08-01.json"
    ).read_text()
)
MANA = "0x0f5d2fb29fb7d3cfee444a200298f468908cc942"
ZERO = "0x0000000000000000000000000000000000000000"
AUG_1 = datetime.date(2026, 8, 1)
AUG_1_START = datetime.datetime.combine(AUG_1, datetime.time.min)
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


def test_chunk_range_single_day_is_24_hours():
    assert chunk_range(AUG_1, AUG_1) == [(AUG_1_START, 24)]


def test_chunk_range_splits_and_covers_every_hour_once():
    start, end = datetime.date(2026, 8, 1), datetime.date(2026, 8, 31)
    chunks = chunk_range(start, end)  # 744 hours, max 50 per chunk
    # 50-hour cap: /chart rejects coins x span > 500 points (10-coin batches)
    assert all(span <= 50 for _, span in chunks)
    ticks = []
    for chunk_start, span in chunks:
        ticks += [chunk_start + datetime.timedelta(hours=i) for i in range(span)]
    assert ticks == [
        AUG_1_START + datetime.timedelta(hours=i) for i in range(31 * 24)
    ]


def test_chart_url_hourly():
    url = chart_url(["coingecko:ethereum", f"ethereum:{MANA}"], AUG_1_START, 24)
    assert url == (
        "https://coins.llama.fi/chart/"
        f"coingecko:ethereum,ethereum:{MANA}"
        "?start=1785542400&span=24&period=1h"
    )


def test_parse_real_fixture_eth_full_day_mana_absent():
    rows = parse_chart_response(FIXTURE, AUG_1_START, 24, ID_MAP)
    # MANA has no hourly data that far back -> no rows, never NULLs
    assert [r for r in rows if r["contract_address"] == MANA] == []
    eth = [r for r in rows if r["contract_address"] == ZERO]
    # Every hour of the day exactly once, from the grid — not from timestamps
    assert [r["grid_ts"] for r in eth] == [
        AUG_1_START + datetime.timedelta(hours=i) for i in range(24)
    ]
    assert all(r["dt"] == AUG_1 for r in eth)
    assert all(r["chain_id"] == 1 for r in eth)
    assert all(1000 < r["price_usd"] < 20000 for r in eth)
    # The 00:00 tick was answered with a 2026-07-31 23:59:59 sample:
    # price_ts drifts across the day boundary, grid_ts does not
    assert eth[0]["price_ts"].date() == datetime.date(2026, 7, 31)


def test_parse_missing_coin_yields_no_rows():
    assert parse_chart_response({"coins": {}}, AUG_1_START, 24, ID_MAP) == []


def test_parse_drops_samples_outside_grid():
    ts = 1785542400  # 2026-08-01 00:00 UTC
    payload = {
        "coins": {
            f"ethereum:{MANA}": {
                "symbol": "MANA",
                "confidence": 0.99,
                "prices": [
                    {"timestamp": ts - 7200, "price": 1.0},  # 2h early
                    {"timestamp": ts + 10, "price": 2.0},
                ],
            }
        }
    }
    rows = parse_chart_response(payload, AUG_1_START, 1, ID_MAP)
    assert [r["price_usd"] for r in rows] == [2.0]


def test_parse_keeps_nearest_sample_on_grid_collision():
    ts = 1785542400
    payload = {
        "coins": {
            f"ethereum:{MANA}": {
                "symbol": "MANA",
                "confidence": 0.99,
                "prices": [
                    {"timestamp": ts + 1500, "price": 1.0},  # 25 min, further
                    {"timestamp": ts + 60, "price": 2.0},  # 1 min, nearest
                ],
            }
        }
    }
    rows = parse_chart_response(payload, AUG_1_START, 1, ID_MAP)
    assert [r["price_usd"] for r in rows] == [2.0]


def test_month_key():
    assert month_key(datetime.date(2020, 12, 7)) == "2020-12"


def test_batch_coins_splits_preserving_order():
    from ingestion.token_prices.defillama import batch_coins

    ids = [f"ethereum:0x{i:040x}" for i in range(22)]
    batches = batch_coins(ids)
    assert all(len(b) <= 10 for b in batches)
    assert [i for b in batches for i in b] == ids
    assert len(batches) == 3


def test_rows_to_parquet_roundtrip(tmp_path):
    import pyarrow.parquet as pq

    from ingestion.token_prices.handler import rows_to_parquet

    rows = parse_chart_response(FIXTURE, AUG_1_START, 24, ID_MAP)
    extracted_at = datetime.datetime.combine(
        datetime.date(2026, 9, 3), datetime.time(12, 0, 0)
    )
    for r in rows:
        r["extracted_at"] = extracted_at
    out = tmp_path / "token_prices.parquet"
    out.write_bytes(rows_to_parquet(rows))
    table = pq.read_table(out)
    assert table.column_names == [
        "chain_id",
        "contract_address",
        "grid_ts",
        "dt",
        "price_usd",
        "price_ts",
        "confidence",
        "extracted_at",
    ]
    assert table.num_rows == 24  # ETH only; MANA absent in the fixture
    assert str(table.schema.field("chain_id").type) == "int32"
    assert str(table.schema.field("grid_ts").type) == "timestamp[us]"
    assert str(table.schema.field("dt").type) == "date32[day]"
    assert str(table.schema.field("price_usd").type) == "double"
    assert str(table.schema.field("price_ts").type) == "timestamp[us]"
    assert str(table.schema.field("extracted_at").type) == "timestamp[us]"


def test_fetch_chart_retries_on_429(monkeypatch):
    import urllib.error

    import ingestion.token_prices.handler as h

    calls = {"n": 0}

    def fake_urlopen(req, timeout):
        calls["n"] += 1
        if calls["n"] < 3:
            raise urllib.error.HTTPError(
                req.full_url, 429, "Too Many Requests", {}, None
            )
        import io as _io

        return _io.BytesIO(b'{"coins": {}}')

    monkeypatch.setattr(h.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(h.time, "sleep", lambda s: None)
    assert h.fetch_chart("https://coins.llama.fi/chart/x") == {"coins": {}}
    assert calls["n"] == 3


def test_fetch_chart_gives_up_after_max_attempts(monkeypatch):
    import urllib.error

    import ingestion.token_prices.handler as h

    def always_429(req, timeout):
        raise urllib.error.HTTPError(req.full_url, 429, "Too Many Requests", {}, None)

    monkeypatch.setattr(h.urllib.request, "urlopen", always_429)
    monkeypatch.setattr(h.time, "sleep", lambda s: None)
    with pytest.raises(urllib.error.HTTPError):
        h.fetch_chart("https://coins.llama.fi/chart/x")


def _athena_result_rows():
    # Athena GetQueryResults shape: header row + one row per token
    def cell(v):
        return {"VarCharValue": v}

    return [
        {"Data": [cell("chain_id"), cell("contract_address")]},
        {"Data": [cell("1"), cell(MANA)]},
        {"Data": [cell("1"), cell(ZERO)]},
    ]


class FakeAthenaQuery:
    def __init__(self):
        self.queries = []

    def start_query_execution(self, QueryString, WorkGroup):
        self.queries.append({"ddl": QueryString, "workgroup": WorkGroup})
        return {"QueryExecutionId": f"q{len(self.queries)}"}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    def get_query_results(self, QueryExecutionId, **kwargs):
        return {"ResultSet": {"Rows": _athena_result_rows()}}


def test_handler_end_to_end_with_fakes(monkeypatch):
    import ingestion.token_prices.handler as h

    written = []
    athena = FakeAthenaQuery()

    class FakeS3:
        def put_object(self, Bucket, Key, Body):
            written.append({"bucket": Bucket, "key": Key, "size": len(Body)})

    def fake_client(service):
        return FakeS3() if service == "s3" else athena

    monkeypatch.setattr(h.boto3, "client", fake_client)
    monkeypatch.setattr(h, "fetch_chart", lambda url: FIXTURE)
    monkeypatch.setenv("LAKE_BUCKET", "test-bucket")

    result = h.handler({"start_date": "2026-08-01", "end_date": "2026-08-01"}, None)

    assert result["rows"] == 24  # ETH hourly; MANA absent in the fixture
    assert result["months"] == ["2026-08"]
    # One parquet written, into the right month partition, append-only name
    assert len(written) == 1
    assert written[0]["bucket"] == "test-bucket"
    key = written[0]["key"]
    assert key.startswith("bronze/token_prices/month=2026-08/token_prices_")
    assert key.endswith(".parquet")
    assert written[0]["size"] > 500
    # Athena ran: 1 universe query (fetch_price filter) + 1 month DDL
    universe = athena.queries[0]["ddl"]
    assert "FROM silver.dim_erc20_tokens" in universe
    assert "WHERE fetch_price = TRUE" in universe
    ddls = [q["ddl"] for q in athena.queries[1:]]
    assert len(ddls) == 1
    assert "ALTER TABLE bronze.token_prices" in ddls[0]
    assert "(month = '2026-08')" in ddls[0]


def test_handler_default_payload_is_today(monkeypatch):
    import ingestion.token_prices.handler as h

    urls = []
    athena = FakeAthenaQuery()

    class FakeS3:
        def put_object(self, Bucket, Key, Body):
            pass

    monkeypatch.setattr(
        h.boto3, "client", lambda s: FakeS3() if s == "s3" else athena
    )

    def fake_fetch(url):
        urls.append(url)
        return {"coins": {}}

    monkeypatch.setattr(h, "fetch_chart", fake_fetch)
    monkeypatch.setenv("LAKE_BUCKET", "test-bucket")

    result = h.handler({}, None)
    today = datetime.datetime.now(datetime.timezone.utc).date()
    epoch = int(
        datetime.datetime(
            today.year, today.month, today.day, tzinfo=datetime.timezone.utc
        ).timestamp()
    )
    assert result["rows"] == 0
    assert len(urls) == 1
    assert f"start={epoch}" in urls[0]
    assert "span=24" in urls[0]
    assert "period=1h" in urls[0]
