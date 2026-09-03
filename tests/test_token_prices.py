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


def test_rows_to_parquet_roundtrip(tmp_path):
    import pyarrow.parquet as pq

    from ingestion.token_prices.handler import rows_to_parquet

    rows = parse_chart_response(FIXTURE, DEC_START, 31, ID_MAP)
    extracted_at = datetime.datetime(2026, 9, 2, 12, 0, 0)
    for r in rows:
        r["extracted_at"] = extracted_at
    out = tmp_path / "token_prices.parquet"
    out.write_bytes(rows_to_parquet(rows))
    table = pq.read_table(out)
    assert table.column_names == [
        "chain_id",
        "contract_address",
        "dt",
        "price_usd",
        "price_ts",
        "confidence",
        "extracted_at",
    ]
    assert table.num_rows == 62  # 31 MANA + 31 ETH
    assert str(table.schema.field("chain_id").type) == "int32"
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

    result = h.handler({"start_date": "2020-12-01", "end_date": "2020-12-31"}, None)

    assert result["rows"] == 62
    assert result["months"] == ["2020-12"]
    # One parquet written, into the right month partition, append-only name
    assert len(written) == 1
    assert written[0]["bucket"] == "test-bucket"
    key = written[0]["key"]
    assert key.startswith("bronze/token_prices/month=2020-12/token_prices_")
    assert key.endswith(".parquet")
    assert written[0]["size"] > 500
    # Athena ran: 1 universe query + 1 month-partition DDL
    universe = athena.queries[0]["ddl"]
    assert "FROM silver.dim_erc20_tokens" in universe
    ddls = [q["ddl"] for q in athena.queries[1:]]
    assert len(ddls) == 1
    assert "ALTER TABLE bronze.token_prices" in ddls[0]
    assert "(month = '2020-12')" in ddls[0]


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
    assert "span=1" in urls[0]


def test_batch_coins_splits_preserving_order():
    from ingestion.token_prices.defillama import batch_coins

    ids = [f"ethereum:0x{i:040x}" for i in range(22)]
    batches = batch_coins(ids)
    assert all(len(b) <= 10 for b in batches)
    assert [i for b in batches for i in b] == ids
    assert len(batches) == 3
