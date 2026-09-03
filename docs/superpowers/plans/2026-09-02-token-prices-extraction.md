# token-prices-extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract daily USD prices (2019-01-01 → today) for every token in `silver.dim_erc20_tokens` from DefiLlama into append-only `bronze.token_prices`, manually triggered via a `token-prices` state machine with Slack alerting.

**Architecture:** Zip Lambda `extract-token-prices`: token universe from Athena → batched `coins.llama.fi/chart` calls (≤120-day chunks, all tokens per call) → grid-aligned rows → timestamped parquet per touched `month=YYYY-MM` partition → explicit partition registration. State machine wraps it with retries and SNS failure alerts; no cron, no dbt step (bronze-only scope).

**Tech Stack:** Python 3.13 (pyarrow via AWSSDKPandas layer, urllib stdlib for HTTP), Terraform, Step Functions, Glue/Athena, pytest.

**Spec:** `docs/superpowers/specs/2026-09-02-token-prices-extraction-design.md`

## Global Constraints

- All artifacts in English; SQL keywords UPPERCASE.
- Addresses lowercase; key `(chain_id, contract_address)`; bucket via `local.bucket_name` / `${aws_s3_bucket.lake.arn}`, never hardcoded; region us-east-1.
- IaC only through Terraform; ALWAYS read `terraform plan` before `terraform apply`.
- `dt` comes from the requested grid, never from DefiLlama's returned timestamp; returned timestamp is stored as `price_ts`.
- Append-only bronze: never overwrite existing objects; duplicates resolved later in silver by `extracted_at`.
- Missing token/day → no row (never NULL rows).
- Container image builds (if any) use `docker build --provenance=false` — not needed in this plan (zip Lambda, no dbt change).
- Tags: `component=ingestion-token-prices, layer=bronze` (Lambda group); `component=orchestration, layer=ops` (state machine).
- Work on branch `token-prices-extraction`; commit locally; push only with explicit user confirmation.

---

### Task 1: Extend `ingestion/common/partitions.py` for non-`dt` partitions

**Files:**
- Modify: `ingestion/common/partitions.py`
- Test: `tests/test_partitions.py` (append)

**Interfaces:**
- Produces: `register_partition(table, run_date, bucket, column="dt", value=None)` — existing callers (`contracts`, `erc20_tokens`) pass 3 args and behave identically; new callers pass `column="month", value="YYYY-MM"` (with `run_date=None`).

- [ ] **Step 1: Read the existing tests**

Read `tests/test_partitions.py` to mirror its fake-Athena style exactly.

- [ ] **Step 2: Append the failing test**

Append to `tests/test_partitions.py` (reuse the file's existing fake Athena helper; if it defines one locally, follow its pattern):

```python
def test_register_month_partition_builds_month_ddl(monkeypatch):
    import ingestion.common.partitions as p

    queries = []

    class FakeAthena:
        def start_query_execution(self, QueryString, WorkGroup):
            queries.append({"ddl": QueryString, "workgroup": WorkGroup})
            return {"QueryExecutionId": "fake-query-id"}

        def get_query_execution(self, QueryExecutionId):
            return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    monkeypatch.setattr(p.boto3, "client", lambda service: FakeAthena())
    p.register_partition(
        "token_prices", None, "test-bucket", column="month", value="2020-05"
    )
    assert len(queries) == 1
    ddl = queries[0]["ddl"]
    assert "ALTER TABLE bronze.token_prices" in ddl
    assert "ADD IF NOT EXISTS PARTITION (month = '2020-05')" in ddl
    assert "LOCATION 's3://test-bucket/bronze/token_prices/month=2020-05/'" in ddl
```

- [ ] **Step 3: Run it to verify it fails**

Run: `.venv/bin/pytest tests/test_partitions.py -v`
Expected: new test FAILS (`TypeError: unexpected keyword argument 'column'`); existing tests PASS.

- [ ] **Step 4: Extend the function**

In `ingestion/common/partitions.py`, replace the `register_partition` signature and the two lines that build `dt`/DDL:

```python
def register_partition(
    table: str,
    run_date: datetime.date | None,
    bucket: str,
    column: str = "dt",
    value: str | None = None,
) -> None:
    """Register a snapshot partition in the Glue catalog via Athena DDL.

    The bronze tables have no partition projection, so each partition must
    be added explicitly. IF NOT EXISTS keeps re-runs idempotent. Default
    partitioning is daily (column dt, value from run_date); monthly tables
    pass column="month" and an explicit value.
    """
    athena = boto3.client("athena")
    value = value if value is not None else run_date.isoformat()
    ddl = (
        f"ALTER TABLE bronze.{table} "
        f"ADD IF NOT EXISTS PARTITION ({column} = '{value}') "
        f"LOCATION 's3://{bucket}/bronze/{table}/{column}={value}/'"
    )
```

(The polling loop below stays untouched.)

- [ ] **Step 5: Run the full suite**

Run: `.venv/bin/pytest tests/ -q && .venv/bin/ruff check ingestion/common`
Expected: all PASS (existing `dt` callers unaffected).

- [ ] **Step 6: Commit**

```bash
git add ingestion/common/partitions.py tests/test_partitions.py
git commit -m "feat: register_partition supports non-dt partition columns"
```

---

### Task 2: Pure DefiLlama module (`defillama.py`) with TDD

**Files:**
- Create: `ingestion/token_prices/__init__.py` (empty)
- Create: `ingestion/token_prices/defillama.py`
- Create: `tests/fixtures/defillama_chart_2020-12.json` (captured real response)
- Test: `tests/test_token_prices.py`

**Interfaces:**
- Produces (all pure, no AWS/network):
  - `coin_id(chain_id: int, address: str) -> str` — zero address → `"coingecko:ethereum"`, else `"ethereum:<address>"`; raises `ValueError` on chain_id != 1.
  - `chunk_range(start: datetime.date, end: datetime.date, max_days: int = 120) -> list[tuple[datetime.date, int]]` — (chunk_start, span) pairs covering [start, end] exactly once.
  - `parse_chart_response(payload: dict, chunk_start: datetime.date, span: int, id_map: dict[str, tuple[int, str]]) -> list[dict]` — rows with keys `chain_id, contract_address, dt, price_usd, price_ts, confidence`; each sample is assigned to the nearest grid day (`round((ts - chunk_start_epoch) / 86400)`), samples rounding outside `[0, span)` are dropped, and if two samples round to the same day the one closest to the grid point wins.
  - `month_key(dt: datetime.date) -> str` — `"YYYY-MM"`.
  - `chart_url(coin_ids: list[str], chunk_start: datetime.date, span: int) -> str`.

- [ ] **Step 1: Capture the real fixture**

```bash
curl -s "https://coins.llama.fi/chart/ethereum:0x0f5d2fb29fb7d3cfee444a200298f468908cc942,coingecko:ethereum?start=1606780800&span=31&period=1d" \
  -o tests/fixtures/defillama_chart_2020-12.json
python3 -m json.tool tests/fixtures/defillama_chart_2020-12.json | head -20
```

Expected: JSON with `coins` → both ids, each with `symbol`, `decimals` (MANA only), `confidence`, `prices` (31 entries of `{timestamp, price}`). December 2020 is deliberate: its MANA samples drift up to ~40 min around midnight, including one past-midnight sample (the Dec-21 grid tick answered at Dec-22 00:12), so the fixture exercises the grid rule.

- [ ] **Step 2: Write the failing tests**

Create `tests/test_token_prices.py`:

```python
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
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_token_prices.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ingestion.token_prices'`

- [ ] **Step 4: Write the module**

Create empty `ingestion/token_prices/__init__.py` and `ingestion/token_prices/defillama.py`:

```python
"""Pure helpers for the DefiLlama coins API (no AWS or network calls).

Grid rule: /chart returns, for each requested daily tick, the nearest real
sample; its timestamp drifts around midnight and can cross calendar days.
dt therefore ALWAYS comes from the requested grid, never from the returned
timestamp (kept separately as price_ts for drift auditing).
"""

import datetime

BASE_URL = "https://coins.llama.fi"
# Native-ETH placeholder used by marketplace trades paid in ETH
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
CHAIN_SLUGS = {1: "ethereum"}
MAX_CHUNK_DAYS = 120


def coin_id(chain_id: int, address: str) -> str:
    if chain_id not in CHAIN_SLUGS:
        raise ValueError(f"unsupported chain_id {chain_id}")
    if address == ZERO_ADDRESS:
        return "coingecko:ethereum"
    return f"{CHAIN_SLUGS[chain_id]}:{address}"


def chunk_range(
    start: datetime.date, end: datetime.date, max_days: int = MAX_CHUNK_DAYS
) -> list[tuple[datetime.date, int]]:
    """Split [start, end] (inclusive) into (chunk_start, span) covering
    every day exactly once."""
    chunks = []
    cursor = start
    while cursor <= end:
        span = min(max_days, (end - cursor).days + 1)
        chunks.append((cursor, span))
        cursor += datetime.timedelta(days=span)
    return chunks


def _epoch(day: datetime.date) -> int:
    return int(
        datetime.datetime(
            day.year, day.month, day.day, tzinfo=datetime.timezone.utc
        ).timestamp()
    )


def chart_url(coin_ids: list[str], chunk_start: datetime.date, span: int) -> str:
    coins = ",".join(coin_ids)
    return f"{BASE_URL}/chart/{coins}?start={_epoch(chunk_start)}&span={span}&period=1d"


def parse_chart_response(
    payload: dict,
    chunk_start: datetime.date,
    span: int,
    id_map: dict[str, tuple[int, str]],
) -> list[dict]:
    """Flatten a /chart payload into grid-aligned rows.

    Each sample is assigned to the nearest grid day; samples rounding
    outside [0, span) are dropped; on a collision the sample closest to
    its grid point wins. Coins absent from the payload yield no rows.
    """
    start_epoch = _epoch(chunk_start)
    rows = []
    for cid, coin in payload.get("coins", {}).items():
        chain_id, address = id_map[cid]
        best: dict[int, dict] = {}  # grid index -> sample
        for sample in coin.get("prices", []):
            ts = sample["timestamp"]
            index = round((ts - start_epoch) / 86400)
            if not 0 <= index < span:
                continue
            distance = abs(ts - (start_epoch + index * 86400))
            if index in best and best[index]["distance"] <= distance:
                continue
            best[index] = {"sample": sample, "distance": distance}
        for index in sorted(best):
            sample = best[index]["sample"]
            rows.append(
                {
                    "chain_id": chain_id,
                    "contract_address": address,
                    "dt": chunk_start + datetime.timedelta(days=index),
                    "price_usd": float(sample["price"]),
                    "price_ts": datetime.datetime.fromtimestamp(
                        sample["timestamp"], tz=datetime.timezone.utc
                    ).replace(tzinfo=None),
                    "confidence": float(coin.get("confidence", 0.0)),
                }
            )
    return rows


def month_key(day: datetime.date) -> str:
    return f"{day.year:04d}-{day.month:02d}"
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_token_prices.py -v`
Expected: all PASS

- [ ] **Step 6: Full suite + ruff, then commit**

Run: `.venv/bin/pytest tests/ -q && .venv/bin/ruff check ingestion/token_prices tests/test_token_prices.py`

```bash
git add ingestion/token_prices/ tests/test_token_prices.py tests/fixtures/defillama_chart_2020-12.json
git commit -m "feat: pure DefiLlama chart parsing with grid-aligned dt"
```

---

### Task 3: Lambda handler (`handler.py`) with TDD

**Files:**
- Create: `ingestion/token_prices/handler.py`
- Modify: `tests/test_token_prices.py` (append)

**Interfaces:**
- Consumes: everything from Task 2; `register_partition(..., column="month", value=...)` from Task 1; `fetch_tokens` mirrors `ingestion/onchain/handler.py:fetch_contract_addresses`.
- Produces: `handler(event, context)` with payload `{}` (today UTC) or `{"start_date": "...", "end_date": "..."}`; `fetch_tokens(athena) -> list[tuple[int, str]]`; `fetch_chart(url) -> dict` (urllib + retry); `rows_to_parquet(rows) -> bytes`; `SCHEMA`.

- [ ] **Step 1: Append the failing tests**

Append to `tests/test_token_prices.py`:

```python
def test_rows_to_parquet_roundtrip(tmp_path):
    import pyarrow.parquet as pq

    from ingestion.token_prices.handler import rows_to_parquet

    rows = parse_chart_response(FIXTURE, DEC_START, 31, ID_MAP)
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
    assert result["rows"] == 0
    assert len(urls) == 1
    assert f"start={int(datetime.datetime(today.year, today.month, today.day, tzinfo=datetime.timezone.utc).timestamp())}" in urls[0]
    assert "span=1" in urls[0]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_token_prices.py -v -k "handler or fetch or parquet"`
Expected: FAIL — `No module named 'ingestion.token_prices.handler'`

- [ ] **Step 3: Write the handler**

Create `ingestion/token_prices/handler.py`:

```python
"""extract-token-prices Lambda: DefiLlama daily USD prices -> bronze/token_prices.

Manually triggered (via the token-prices state machine). Payload {} extracts
today (UTC); {"start_date", "end_date"} backfills a range. Append-only:
each run writes a new timestamped parquet per touched month partition;
duplicates are resolved downstream in silver by latest extracted_at.
"""

import datetime
import io
import json
import os
import time
import urllib.error
import urllib.request

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common.partitions import ATHENA_WORKGROUP, register_partition
from ingestion.token_prices.defillama import (
    chart_url,
    chunk_range,
    coin_id,
    month_key,
    parse_chart_response,
)

SCHEMA = pa.schema(
    [
        ("chain_id", pa.int32()),
        ("contract_address", pa.string()),
        ("dt", pa.date32()),
        ("price_usd", pa.float64()),
        ("price_ts", pa.timestamp("us")),
        ("confidence", pa.float64()),
        ("extracted_at", pa.timestamp("us")),
    ]
)

HTTP_ATTEMPTS = 3
HTTP_TIMEOUT = 30
RETRYABLE = {429, 500, 502, 503, 504}


def fetch_tokens(athena) -> list[tuple[int, str]]:
    """Token universe from the curated dimension (same pattern as the
    onchain Lambda reading silver.dim_contracts)."""
    sql = "SELECT chain_id, contract_address FROM silver.dim_erc20_tokens"
    query_id = athena.start_query_execution(
        QueryString=sql, WorkGroup=ATHENA_WORKGROUP
    )["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)[
            "QueryExecution"
        ]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "no reason given")
            raise RuntimeError(f"token-list query {state}: {reason}")
        time.sleep(1)

    tokens, token, first_page = [], None, True
    while True:
        kwargs = {"QueryExecutionId": query_id}
        if token:
            kwargs["NextToken"] = token
        page = athena.get_query_results(**kwargs)
        rows = page["ResultSet"]["Rows"]
        if first_page:
            rows = rows[1:]  # header row
            first_page = False
        for r in rows:
            chain_raw = r["Data"][0].get("VarCharValue")
            address = r["Data"][1].get("VarCharValue")
            if chain_raw is None or address is None:
                raise RuntimeError(
                    "silver.dim_erc20_tokens returned a NULL key; "
                    "fix the dimension upstream"
                )
            tokens.append((int(chain_raw), address))
        token = page.get("NextToken")
        if not token:
            return tokens


def fetch_chart(url: str) -> dict:
    """GET with exponential backoff on 429/5xx (DefiLlama has no SLA)."""
    for attempt in range(1, HTTP_ATTEMPTS + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "curl/8.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as err:
            if err.code not in RETRYABLE or attempt == HTTP_ATTEMPTS:
                raise
            time.sleep(2**attempt)
        except urllib.error.URLError:
            if attempt == HTTP_ATTEMPTS:
                raise
            time.sleep(2**attempt)


def rows_to_parquet(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def handler(event, context):
    event = event or {}
    today = datetime.datetime.now(datetime.timezone.utc).date()
    start = datetime.date.fromisoformat(event.get("start_date", today.isoformat()))
    end = datetime.date.fromisoformat(event.get("end_date", start.isoformat()))
    if end < start:
        raise ValueError(f"end_date {end} is before start_date {start}")

    bucket = os.environ["LAKE_BUCKET"]
    athena = boto3.client("athena")
    s3 = boto3.client("s3")

    tokens = fetch_tokens(athena)
    id_map = {coin_id(c, a): (c, a) for c, a in tokens}
    coin_ids = sorted(id_map)

    extracted_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    rows = []
    for chunk_start, span in chunk_range(start, end):
        payload = fetch_chart(chart_url(coin_ids, chunk_start, span))
        rows.extend(parse_chart_response(payload, chunk_start, span, id_map))
    for row in rows:
        row["extracted_at"] = extracted_at

    by_month: dict[str, list[dict]] = {}
    for row in rows:
        by_month.setdefault(month_key(row["dt"]), []).append(row)

    stamp = extracted_at.strftime("%Y%m%d%H%M%S")
    for month in sorted(by_month):
        key = f"bronze/token_prices/month={month}/token_prices_{stamp}.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=rows_to_parquet(by_month[month]))
        register_partition(
            "token_prices", None, bucket, column="month", value=month
        )
        print(f"wrote {len(by_month[month])} rows to s3://{bucket}/{key}")

    return {
        "rows": len(rows),
        "tokens": len(tokens),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "months": sorted(by_month),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_token_prices.py -v`
Expected: all PASS

- [ ] **Step 5: Full suite + ruff, then commit**

Run: `.venv/bin/pytest tests/ -q && .venv/bin/ruff check ingestion/token_prices`

```bash
git add ingestion/token_prices/handler.py tests/test_token_prices.py
git commit -m "feat: extract-token-prices Lambda handler"
```

---

### Task 4: Terraform — Glue table + Lambda + alarm

**Files:**
- Create: `terraform/table_token_prices.tf`
- Create: `terraform/lambda_token_prices.tf`
- Modify: `terraform/alerts.tf` (add to `local.monitored_lambdas`)

**Interfaces:**
- Consumes: existing `aws_s3_bucket.lake`, `aws_athena_workgroup.main`, `aws_glue_catalog_database.{bronze,silver}`, `local.sdk_pandas_layer_arn`, `local.bucket_name`.
- Produces: `aws_lambda_function.token_prices` (name `extract-token-prices`), `aws_glue_catalog_table.token_prices` — referenced by Task 5.

- [ ] **Step 1: Write `terraform/table_token_prices.tf`**

```hcl
resource "aws_glue_catalog_table" "token_prices" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "token_prices"
  table_type    = "EXTERNAL_TABLE"

  # Monthly partitions (prices are tiny: ~22 rows/day; daily partitions
  # would be ~2,800 3-KB files). No partition projection: the Lambda
  # registers each month explicitly.
  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "month"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/bronze/token_prices/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "chain_id"
      type = "int"
    }
    columns {
      name = "contract_address"
      type = "string"
    }
    columns {
      name = "dt"
      type = "date"
    }
    columns {
      name = "price_usd"
      type = "double"
    }
    columns {
      name = "price_ts"
      type = "timestamp"
    }
    columns {
      name = "confidence"
      type = "double"
    }
    columns {
      name = "extracted_at"
      type = "timestamp"
    }
  }
}
```

- [ ] **Step 2: Write `terraform/lambda_token_prices.tf`**

```hcl
locals {
  token_prices_tags = { component = "ingestion-token-prices", layer = "bronze" }
}

data "archive_file" "token_prices_zip" {
  type        = "zip"
  output_path = "${path.module}/build/token_prices.zip"

  source {
    content  = file("${path.module}/../ingestion/token_prices/handler.py")
    filename = "ingestion/token_prices/handler.py"
  }
  source {
    content  = file("${path.module}/../ingestion/token_prices/defillama.py")
    filename = "ingestion/token_prices/defillama.py"
  }
  source {
    content  = ""
    filename = "ingestion/__init__.py"
  }
  source {
    content  = ""
    filename = "ingestion/token_prices/__init__.py"
  }
  source {
    content  = file("${path.module}/../ingestion/common/partitions.py")
    filename = "ingestion/common/partitions.py"
  }
  source {
    content  = ""
    filename = "ingestion/common/__init__.py"
  }
}

resource "aws_iam_role" "token_prices" {
  name = "extract-token-prices-role"
  tags = local.token_prices_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "token_prices_s3" {
  name = "prices-write-dim-read"
  role = aws_iam_role.token_prices.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/token_prices/*"
      },
      # Athena reads dimension data and writes query results with the
      # caller's credentials
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      },
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/silver/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${aws_s3_bucket.lake.arn}/athena-results/*"
      },
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
        ]
        Resource = aws_athena_workgroup.main.arn
      },
      # Read the silver dimension (dbt-created tables have no Terraform
      # ARN, hence the table wildcard) and register month partitions on
      # the new bronze table.
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartition",
          "glue:GetPartitions",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.bronze.arn,
          aws_glue_catalog_database.silver.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/silver/*",
          aws_glue_catalog_table.token_prices.arn,
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.bronze.arn,
          aws_glue_catalog_table.token_prices.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "token_prices_logs" {
  role       = aws_iam_role.token_prices.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "token_prices" {
  name              = "/aws/lambda/extract-token-prices"
  retention_in_days = 7
  tags              = local.token_prices_tags
}

resource "aws_lambda_function" "token_prices" {
  function_name = "extract-token-prices"
  role          = aws_iam_role.token_prices.arn
  tags          = local.token_prices_tags

  filename         = data.archive_file.token_prices_zip.output_path
  source_code_hash = data.archive_file.token_prices_zip.output_base64sha256

  handler     = "ingestion.token_prices.handler.handler"
  runtime     = "python3.13"
  timeout     = 300
  memory_size = 512
  layers      = [local.sdk_pandas_layer_arn] # defined in lambda_dcl_contracts.tf

  environment {
    variables = {
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
      LAKE_BUCKET      = aws_s3_bucket.lake.id
    }
  }

  depends_on = [aws_cloudwatch_log_group.token_prices]
}
```

- [ ] **Step 3: Add the function to `local.monitored_lambdas` in `terraform/alerts.tf`**

Append inside the existing list:

```hcl
    aws_lambda_function.token_prices.function_name,
```

- [ ] **Step 4: Validate and plan**

Run: `cd terraform && terraform validate && terraform plan`
Expected: only adds (glue table, role, policy, attachment, log group, lambda, one new alarm). Read the whole plan; nothing may be destroyed. Note: if `aws_glue_catalog_database.silver` does not exist under that Terraform name, check `terraform/glue_athena.tf` for the actual resource name and use it.

- [ ] **Step 5: Apply and verify**

Run: `cd terraform && terraform apply` (after re-reading the summary)
Then: `aws lambda get-function --function-name extract-token-prices --query 'Configuration.State'` → `"Active"`.

- [ ] **Step 6: Commit**

```bash
git add terraform/table_token_prices.tf terraform/lambda_token_prices.tf terraform/alerts.tf
git commit -m "feat: terraform for extract-token-prices lambda and bronze.token_prices table"
```

---

### Task 5: Terraform — token-prices state machine (manual, no cron)

**Files:**
- Create: `terraform/step_functions_token_prices.tf`

**Interfaces:**
- Consumes: `aws_lambda_function.token_prices` (Task 4), existing `aws_sns_topic.alerts`.
- Produces: state machine `token-prices`, started manually; the execution input is passed straight to the Lambda (so `{"start_date": ..., "end_date": ...}` works).

- [ ] **Step 1: Write `terraform/step_functions_token_prices.tf`**

```hcl
# token-prices: manual extraction of DefiLlama daily prices into bronze.
# No EventBridge trigger (user decision: no cron; phase 8 orchestrates) and
# no dbt step yet (bronze-only scope; the silver model adds it). Failures
# use the custom notify-slack contract.
locals {
  sfn_token_prices_tags = { component = "orchestration", layer = "ops" }
}

resource "aws_iam_role" "sfn_token_prices" {
  name = "token-prices-sfn-role"
  tags = local.sfn_token_prices_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_token_prices" {
  name = "invoke-lambda-publish-alerts"
  role = aws_iam_role.sfn_token_prices.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = aws_lambda_function.token_prices.arn
      },
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.alerts.arn
      }
    ]
  })
}

resource "aws_sfn_state_machine" "token_prices" {
  name     = "token-prices"
  role_arn = aws_iam_role.sfn_token_prices.arn
  tags     = local.sfn_token_prices_tags

  definition = jsonencode({
    Comment = "Manual DefiLlama price extraction -> bronze.token_prices"
    StartAt = "ExtractTokenPrices"
    States = {
      # Execution input passes through untouched, so a manual run can
      # carry {} (today) or {start_date, end_date} (backfill).
      ExtractTokenPrices = {
        Type       = "Task"
        Resource   = aws_lambda_function.token_prices.arn
        ResultPath = "$.extract"
        Retry = [{
          ErrorEquals = [
            "Lambda.ServiceException",
            "Lambda.TooManyRequestsException",
          ]
          IntervalSeconds = 5
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        End = true
      }
      # Custom notify-slack contract; Step Functions serializes Message to JSON.
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.alerts.arn
          Message = {
            source            = "step-functions"
            component         = "token-prices"
            status            = "FAILED"
            "detail.$"        = "States.Format('{}: {}', $.error.Error, $.error.Cause)"
            "execution_url.$" = "States.Format('https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1#/v2/executions/details/{}', $$.Execution.Id)"
          }
        }
        Next = "FailExecution"
      }
      FailExecution = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "A step failed; details were sent to the alerts topic"
      }
    }
  })
}
```

- [ ] **Step 2: Validate, plan, apply**

Run: `cd terraform && terraform validate && terraform plan`
Expected: only adds (role, policy, state machine). Then `terraform apply` and verify:
`aws stepfunctions list-state-machines --query "stateMachines[?name=='token-prices'].name" --output text` → `token-prices`.

- [ ] **Step 3: Commit**

```bash
git add terraform/step_functions_token_prices.tf
git commit -m "feat: token-prices state machine (manual trigger)"
```

---

### Task 6: Backfill 2019→today and verification

**Files:** none (operational task)

**Interfaces:**
- Consumes: everything deployed in Tasks 4-5.
- Produces: `bronze.token_prices` populated from 2019-01-01 to today (~92 month partitions).

- [ ] **Step 1: Smoke-test with one month**

```bash
SM_ARN=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='token-prices'].stateMachineArn" --output text)
aws stepfunctions start-execution --state-machine-arn $SM_ARN \
  --input '{"start_date": "2020-12-01", "end_date": "2020-12-31"}'
# poll list-executions until SUCCEEDED, then:
```

Athena check: `SELECT COUNT(*) FROM bronze.token_prices WHERE month = '2020-12'`
Expected: ~340 rows (Dec 2020: MANA/ETH/majors have all 31 days; late-launch tokens like APE/PRIME/WILD contribute none). Spot-check MANA:
`SELECT dt, price_usd FROM bronze.token_prices WHERE month='2020-12' AND contract_address='0x0f5d2fb29fb7d3cfee444a200298f468908cc942' ORDER BY dt LIMIT 5` — prices ~0.085 USD.

- [ ] **Step 2: Full backfill**

```bash
TODAY=$(date -u +%Y-%m-%d)
aws stepfunctions start-execution --state-machine-arn $SM_ARN \
  --input "{\"start_date\": \"2019-01-01\", \"end_date\": \"$TODAY\"}"
```

Poll until SUCCEEDED (~24 DefiLlama calls; a few minutes). Note: December 2020 will now hold two files (smoke test + backfill) — that is the append-only contract working as designed; silver dedups later.

- [ ] **Step 3: Verify coverage in Athena**

```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT contract_address) AS tokens,
       MIN(dt) AS first_dt,
       MAX(dt) AS last_dt
FROM bronze.token_prices
```

Expected: `first_dt = 2019-01-01`, `last_dt` = today, tokens ≤ 22 (tokens with no DefiLlama history before their launch simply start later), rows in the tens of thousands. Also per-token sanity:

```sql
SELECT contract_address, MIN(dt) AS first_dt, COUNT(*) AS days
FROM bronze.token_prices
GROUP BY contract_address
ORDER BY first_dt
```

Expected: MANA/ETH/DAI/BAT/LINK/USDC/USDT/WETH from 2019-01-01; UNI ~2020-09; APE ~2022-03; PRIME ~2023.

- [ ] **Step 4: Full local suite one last time**

Run: `.venv/bin/pytest tests/ -q && cd terraform && terraform validate && terraform plan`
Expected: tests green; plan shows no changes (no drift).

- [ ] **Step 5: Checkpoint — do NOT open a PR**

Report results to the user. The branch stays open: the silver model gets
its own brainstorming and lands on this same branch before the single PR.
