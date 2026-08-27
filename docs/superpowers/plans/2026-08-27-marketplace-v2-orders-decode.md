# Marketplace V2 Orders Decode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode the Marketplace V2 proxy (`0x8e5660b4ab70168b5a6feea0e0315cb49c8cd539`) order events (`OrderCreated`, `OrderCancelled`, `OrderSuccessful`) from `bronze.ethereum_logs` into `staging.ethereum_marketplace_v2_orders` via a new zip Lambda.

**Architecture:** Mirror of the LegacyMarketplace decoder: the whole decode is SQL (static word layout, conditional per event via `CASE` on topic0); Python only converts hex → Decimal / decimal-string / timestamp. Unlike the legacy events, data word 1 carries the NFT `contract_address`. Append-only parquet writes partitioned by `dt`; dbt dedups downstream by `(transaction_hash, log_index)`.

**Tech Stack:** Python 3.13 ARM64 Lambda (zip + AWSSDKPandas layer), awswrangler (Athena UNLOAD + parquet), Terraform, pytest + eth_abi (dev only).

**Spec:** `docs/superpowers/specs/2026-08-27-marketplace-v2-orders-decode-design.md`

## Global Constraints

- All artifacts in English; SQL keywords UPPERCASE.
- Work happens on branch `feat/marketplace-v2-orders-decode`; merged to `main` via PR with squash merge. Push only with explicit user confirmation.
- Addresses lowercase at write time; `0x`-prefixed.
- Token ids as decimal strings (`str(int(hex, 16))`), up to 78 chars.
- Partition column is `dt` (string, `YYYY-MM-DD`); never `date`.
- Amount guardrail: values ≥ 10^38 raise (decimal(38,0) ceiling).
- `expiresAt` in the event is **unix milliseconds** (verified) → convert with `unit="ms"`.
- Event contract: `{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}` inclusive, default UTC today−2, via existing `decode.common.parse_event`.
- Never hardcode the bucket name; Lambda reads `LAKE_BUCKET` env var.
- Run tests with `.venv/bin/pytest tests/ -v` from the repo root; lint with `.venv/bin/ruff check decode/ tests/`.

---

### Task 1: Extraction query module

**Files:**
- Create: `decode/ethereum_marketplace_v2_query.py`
- Create: `tests/fixtures/marketplace_v2_orders_2018-11-01.json`
- Test: `tests/test_decode_ethereum_marketplace_v2_query.py`

**Interfaces:**
- Consumes: nothing new (stdlib `re` only).
- Produces: `build_query(start_date: str, end_date: str) -> str` and constants `MARKETPLACE_V2_PROXY`, `ORDER_CREATED_TOPIC`, `ORDER_CANCELLED_TOPIC`, `ORDER_SUCCESSFUL_TOPIC` — Task 2's handler imports `build_query`; Task 1's tests import all five names.

- [ ] **Step 1: Save the fixture file with real bronze logs (two per event)**

Create `tests/fixtures/marketplace_v2_orders_2018-11-01.json` (real logs pulled from `bronze.ethereum_logs`, dt=2018-11-01; `nftAddress` covers LANDProxy and EstateProxy):

```json
[
 {
  "transaction_hash": "0x7897a63c7c507a86a8966a3831a1b94d7cb1623b9035b7c9c97315b7d74c0401",
  "log_index": 0,
  "address": "0x8e5660b4ab70168b5a6feea0e0315cb49c8cd539",
  "topics": [
   "0x84c66c3f7ba4b390e20e8e8233e2a516f3ce34a72749e4f12bd010dfba238039",
   "0x0000000000000000000000000000000fffffffffffffffffffffffffffffff6f",
   "0x00000000000000000000000081e4fb0c64bf49f89b57f6648562fc9a791b2e92"
  ],
  "data": "0x888d3f72b185afcf4b398c7f4785b29e800ad97e1d3948545eed08050904cced000000000000000000000000f87e31492faf9a91b02ee0deaad50d51d56d5d4d0000000000000000000000000000000000000000000005665b96cf35acf0000000000000000000000000000000000000000000000000000000000166dd856980",
  "dt": "2018-11-01"
 },
 {
  "transaction_hash": "0x13e39d6ffc448bc88308708a85bf12ae794aa38adc69f8d3c1d1f1743d154d2d",
  "log_index": 0,
  "address": "0x8e5660b4ab70168b5a6feea0e0315cb49c8cd539",
  "topics": [
   "0x84c66c3f7ba4b390e20e8e8233e2a516f3ce34a72749e4f12bd010dfba238039",
   "0x0000000000000000000000000000000000000000000000000000000000000185",
   "0x0000000000000000000000000ceec89bff203fb69d77450da89ed35dd321e85e"
  ],
  "data": "0x243004d2b554879650def62163cf8489ddd6c6d3f599d7c69e4c04af1d53d782000000000000000000000000959e104e1a4db6317fa58f8295f586e1a978c29700000000000000000000000000000000000000000000a968085e53a40c9c0000000000000000000000000000000000000000000000000000000001670500d580",
  "dt": "2018-11-01"
 },
 {
  "transaction_hash": "0x4c2c3f63ca77ba7078137726e1ead5f9aa2de43c85a52c4c1e0741897788000f",
  "log_index": 5,
  "address": "0x8e5660b4ab70168b5a6feea0e0315cb49c8cd539",
  "topics": [
   "0x0325426328de5b91ae4ad8462ad4076de4bcaf4551e81556185cacde5a425c6b",
   "0xffffffffffffffffffffffffffffff8effffffffffffffffffffffffffffffd5",
   "0x000000000000000000000000ccd5089557ae6a2ba063e8720e725a6bf743b3e8"
  ],
  "data": "0x7fa7b0c31b4d1d39eb90734ca772f952e5c9d6c5187678358f07cc1eacf09972000000000000000000000000f87e31492faf9a91b02ee0deaad50d51d56d5d4d",
  "dt": "2018-11-01"
 },
 {
  "transaction_hash": "0x5be9801f0b22b4ad1825a5d1af402fc35c846fb38639bce4467b352c221053f0",
  "log_index": 12,
  "address": "0x8e5660b4ab70168b5a6feea0e0315cb49c8cd539",
  "topics": [
   "0x0325426328de5b91ae4ad8462ad4076de4bcaf4551e81556185cacde5a425c6b",
   "0xffffffffffffffffffffffffffffffc80000000000000000000000000000007b",
   "0x000000000000000000000000d37398e2e24a12abcdb9966c199b5cd562c3b305"
  ],
  "data": "0x11c14f0050e463ad4d5910b03526a06c1b652ec52a0d039de24a419adb7328cb000000000000000000000000f87e31492faf9a91b02ee0deaad50d51d56d5d4d",
  "dt": "2018-11-01"
 },
 {
  "transaction_hash": "0x4ec56b61dab5fd9e9b7f4ce1ce56ca8e9d3468976b0251ce24dba5e6de9ac6bc",
  "log_index": 12,
  "address": "0x8e5660b4ab70168b5a6feea0e0315cb49c8cd539",
  "topics": [
   "0x695ec315e8a642a74d450a4505eeea53df699b47a7378c7d752e97d5b16eb9bb",
   "0x0000000000000000000000000000007affffffffffffffffffffffffffffff75",
   "0x00000000000000000000000084c8a9838e70ba4823ca2df163deecf4f73e6519",
   "0x00000000000000000000000095cb76c2aa96436fb09a4c41e7e3b2f199838aaa"
  ],
  "data": "0xb56ef50c14d2a06d35fa7b92869cdd14bfb0b49993ac600bd269f151453f7b37000000000000000000000000f87e31492faf9a91b02ee0deaad50d51d56d5d4d00000000000000000000000000000000000000000000048d8470181e32700000",
  "dt": "2018-11-01"
 },
 {
  "transaction_hash": "0xbbc32a0d9340b471776b35681ff49183a3a004b189d42e60e9a129e4b363e064",
  "log_index": 14,
  "address": "0x8e5660b4ab70168b5a6feea0e0315cb49c8cd539",
  "topics": [
   "0x695ec315e8a642a74d450a4505eeea53df699b47a7378c7d752e97d5b16eb9bb",
   "0x00000000000000000000000000000069ffffffffffffffffffffffffffffff74",
   "0x000000000000000000000000eb7c97836846f4443bfa3f6af7ee39d59eb66214",
   "0x00000000000000000000000095cb76c2aa96436fb09a4c41e7e3b2f199838aaa"
  ],
  "data": "0x67f65967398de0b1445105312e732e11b6a2dace4abdcada69ede6d8da2d19bd000000000000000000000000f87e31492faf9a91b02ee0deaad50d51d56d5d4d0000000000000000000000000000000000000000000002dbd622a9ef3d700000",
  "dt": "2018-11-01"
 }
]
```

- [ ] **Step 2: Write the failing tests**

Create `tests/test_decode_ethereum_marketplace_v2_query.py`:

```python
import json
from pathlib import Path

import pytest
from eth_abi import decode as abi_decode

from decode.ethereum_marketplace_v2_query import (
    MARKETPLACE_V2_PROXY,
    ORDER_CANCELLED_TOPIC,
    ORDER_CREATED_TOPIC,
    ORDER_SUCCESSFUL_TOPIC,
    build_query,
)

FIXTURE = (
    Path(__file__).parent / "fixtures" / "marketplace_v2_orders_2018-11-01.json"
)


def test_query_filters_address_topics_and_range():
    q = build_query("2018-11-01", "2018-11-05")
    assert MARKETPLACE_V2_PROXY in q
    assert ORDER_CREATED_TOPIC in q
    assert ORDER_CANCELLED_TOPIC in q
    assert ORDER_SUCCESSFUL_TOPIC in q
    assert "BETWEEN '2018-11-01' AND '2018-11-05'" in q
    assert '"bronze"."ethereum_logs"' in q
    # static word layouts: per-event data length guard
    assert "258" in q and "194" in q and "130" in q


def test_query_rejects_bad_dates():
    with pytest.raises(ValueError):
        build_query("2018/11/01", "2018-11-05")
    with pytest.raises(ValueError):
        build_query("2018-11-01", "not-a-date")


def test_sql_substr_offsets_match_eth_abi_on_real_logs():
    # Reproduce the query's substr arithmetic in Python (1-based, same
    # positions) and compare against eth_abi on real bronze logs.
    records = json.loads(FIXTURE.read_text())
    seen = set()
    for record in records:
        data, topics = record["data"], record["topics"]
        topic0 = topics[0]
        seen.add(topic0)
        if topic0 == ORDER_CREATED_TOPIC:
            order_id, nft_address, price, expires = abi_decode(
                ["bytes32", "address", "uint256", "uint256"],
                bytes.fromhex(data[2:]),
            )
            assert len(data) == 258
            # total_price_hex: substr(data, 131, 64)
            assert int(data[130:194], 16) == price
            # expires_at_hex: substr(data, 195, 64); value is unix MILLISECONDS
            assert int(data[194:258], 16) == expires
            assert 10**12 < expires < 10**13  # ms magnitude, not seconds
        elif topic0 == ORDER_SUCCESSFUL_TOPIC:
            order_id, nft_address, price = abi_decode(
                ["bytes32", "address", "uint256"], bytes.fromhex(data[2:])
            )
            assert len(data) == 194
            assert int(data[130:194], 16) == price
            # buyer: concat('0x', substr(topics[4], 27))
            assert len(topics[3][26:]) == 40
        else:
            assert topic0 == ORDER_CANCELLED_TOPIC
            order_id, nft_address = abi_decode(
                ["bytes32", "address"], bytes.fromhex(data[2:])
            )
            assert len(data) == 130
        # order_id: concat('0x', substr(data, 3, 64))
        assert data[2:66] == order_id.hex()
        # contract_address: concat('0x', substr(data, 91, 40))
        assert "0x" + data[90:130] == nft_address.lower()
        # asset_id: topics[2] hex -> unsigned decimal string, <= 78 chars
        asset_id = str(int(topics[1], 16))
        assert asset_id.isdigit() and len(asset_id) <= 78
        # seller: concat('0x', substr(topics[3], 27))
        assert len(topics[2][26:]) == 40
    # the fixture exercises all three events
    assert seen == {
        ORDER_CREATED_TOPIC,
        ORDER_CANCELLED_TOPIC,
        ORDER_SUCCESSFUL_TOPIC,
    }
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_decode_ethereum_marketplace_v2_query.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'decode.ethereum_marketplace_v2_query'`

- [ ] **Step 4: Write the query module**

Create `decode/ethereum_marketplace_v2_query.py`:

```python
"""Builds the Athena query that extracts Marketplace V2 order events.

Same silhouette as the LegacyMarketplace decoder: every event is
static words, so the whole decode fits in SQL. Events are emitted by
the MarketplaceProxy; unlike the legacy contract, data word 1 carries
the NFT contract address (nftAddress), so no downstream attribution is
needed. Python only converts hex -> decimal string / Decimal /
timestamp.
"""

import re

MARKETPLACE_V2_PROXY = "0x8e5660b4ab70168b5a6feea0e0315cb49c8cd539"
ORDER_CREATED_TOPIC = (
    "0x84c66c3f7ba4b390e20e8e8233e2a516f3ce34a72749e4f12bd010dfba238039"
)
ORDER_CANCELLED_TOPIC = (
    "0x0325426328de5b91ae4ad8462ad4076de4bcaf4551e81556185cacde5a425c6b"
)
ORDER_SUCCESSFUL_TOPIC = (
    "0x695ec315e8a642a74d450a4505eeea53df699b47a7378c7d752e97d5b16eb9bb"
)

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def build_query(start_date: str, end_date: str) -> str:
    for d in (start_date, end_date):
        if not _DT_RE.match(d):
            raise ValueError(f"invalid date {d!r}, expected YYYY-MM-DD")

    return f"""SELECT transaction_hash,
    log_index,
    block_timestamp,
    CASE topics[1]
        WHEN '{ORDER_CREATED_TOPIC}' THEN 'OrderCreated'
        WHEN '{ORDER_CANCELLED_TOPIC}' THEN 'OrderCancelled'
        WHEN '{ORDER_SUCCESSFUL_TOPIC}' THEN 'OrderSuccessful'
    END AS event_name,
    concat('0x', substr(data, 3, 64)) AS order_id,
    topics[2] AS asset_id_hex,
    concat('0x', substr(data, 91, 40)) AS contract_address,
    concat('0x', substr(topics[3], 27)) AS seller,
    CASE WHEN topics[1] = '{ORDER_SUCCESSFUL_TOPIC}'
        THEN concat('0x', substr(topics[4], 27)) END AS buyer,
    CASE WHEN topics[1] IN ('{ORDER_CREATED_TOPIC}', '{ORDER_SUCCESSFUL_TOPIC}')
        THEN substr(data, 131, 64) END AS total_price_hex,
    CASE WHEN topics[1] = '{ORDER_CREATED_TOPIC}'
        THEN substr(data, 195, 64) END AS expires_at_hex,
    extracted_at AS bronze_extracted_at,
    dt
FROM "bronze"."ethereum_logs"
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
    AND address = '{MARKETPLACE_V2_PROXY}'
    AND topics[1] IN ('{ORDER_CREATED_TOPIC}',
        '{ORDER_CANCELLED_TOPIC}',
        '{ORDER_SUCCESSFUL_TOPIC}')
    AND length(data) = CASE topics[1]
        WHEN '{ORDER_CREATED_TOPIC}' THEN 258
        WHEN '{ORDER_SUCCESSFUL_TOPIC}' THEN 194
        ELSE 130 END
    AND cardinality(topics) = CASE topics[1]
        WHEN '{ORDER_SUCCESSFUL_TOPIC}' THEN 4
        ELSE 3 END"""
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_decode_ethereum_marketplace_v2_query.py -v`
Expected: 3 PASS

- [ ] **Step 6: Commit**

```bash
git add decode/ethereum_marketplace_v2_query.py \
    tests/test_decode_ethereum_marketplace_v2_query.py \
    tests/fixtures/marketplace_v2_orders_2018-11-01.json
git commit -m "feat: extraction query for marketplace v2 order events"
```

---

### Task 2: Handler module

**Files:**
- Create: `decode/ethereum_marketplace_v2_handler.py`
- Modify: `decode/common.py:1-2` (docstring: add marketplace v2 to the Lambda list)
- Test: `tests/test_decode_ethereum_marketplace_v2_handler.py`

**Interfaces:**
- Consumes: `decode.common.parse_event(event) -> (start, end)`; `decode.ethereum_marketplace_v2_query.build_query(start, end) -> str` (Task 1).
- Produces: `handler(event, context) -> {"start_date", "end_date", "rows_by_dt"}` (Lambda entrypoint `decode.ethereum_marketplace_v2_handler.handler`, used by Task 3's Terraform) and `postprocess(df) -> df` + `_FINAL_COLUMNS` (used by its own tests).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_decode_ethereum_marketplace_v2_handler.py`:

```python
from decimal import Decimal

import pandas as pd
import pytest

from decode.ethereum_marketplace_v2_handler import _FINAL_COLUMNS, postprocess

# expires hex is real (OrderCreated fixture, tx 0x7897...); price is synthetic
_CREATED_PRICE_HEX = format(25000 * 10**18, "064x")
# 1541314800000 ms = 2018-11-04 07:00:00 UTC
_EXPIRES_MS_HEX = format(0x166DD856980, "064x")
_NEG_COORD_ASSET_HEX = (
    "0xffffffffffffffffffffffffffffff8effffffffffffffffffffffffffffffd5"
)


def _df(**overrides):
    base = {
        "transaction_hash": ["0x7897"],
        "log_index": [0],
        "block_timestamp": [pd.Timestamp("2018-11-01 10:00:00")],
        "event_name": ["OrderCreated"],
        "order_id": ["0x" + "888d3f72" * 8],
        "asset_id_hex": [
            "0x0000000000000000000000000000000fffffffffffffffffffffffffffffff6f"
        ],
        "contract_address": ["0xF87E31492Faf9A91B02Ee0dEAAd50d51d56D5d4d"],
        "seller": ["0x81E4fb0C64bf49F89b57F6648562fc9A791b2E92"],
        "buyer": [None],
        "total_price_hex": [_CREATED_PRICE_HEX],
        "expires_at_hex": [_EXPIRES_MS_HEX],
        "bronze_extracted_at": [pd.Timestamp("2026-08-27 06:00:00")],
        "dt": ["2018-11-01"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def test_postprocess_created_row_columns_and_types():
    out = postprocess(_df())
    assert list(out.columns) == _FINAL_COLUMNS
    row = out.iloc[0]
    assert row["asset_id"] == str((0xF << 128) | int("f" * 30 + "6f", 16))
    assert row["contract_address"] == "0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d"
    assert row["total_price"] == Decimal(25000 * 10**18)
    assert isinstance(row["total_price"], Decimal)
    # expiresAt is unix MILLISECONDS -> timestamp
    assert row["expires_at"] == pd.Timestamp("2018-11-04 07:00:00")
    assert row["seller"] == "0x81e4fb0c64bf49f89b57f6648562fc9a791b2e92"
    assert pd.isna(row["buyer"])
    assert pd.notna(row["decoded_at"])


def test_postprocess_cancelled_row_nullables():
    out = postprocess(
        _df(
            event_name=["OrderCancelled"],
            total_price_hex=[None],
            expires_at_hex=[None],
        )
    )
    row = out.iloc[0]
    assert pd.isna(row["total_price"])
    assert pd.isna(row["expires_at"])


def test_postprocess_successful_row_buyer_lowercased():
    out = postprocess(
        _df(
            event_name=["OrderSuccessful"],
            buyer=["0x95cB76C2aA96436fb09a4c41E7E3b2f199838AaA"],
            expires_at_hex=[None],
        )
    )
    assert out.iloc[0]["buyer"] == "0x95cb76c2aa96436fb09a4c41e7e3b2f199838aaa"


def test_postprocess_negative_coord_asset_id_unsigned_decimal():
    out = postprocess(_df(asset_id_hex=[_NEG_COORD_ASSET_HEX]))
    asset_id = out.iloc[0]["asset_id"]
    assert asset_id == str(int(_NEG_COORD_ASSET_HEX, 16))
    assert asset_id.isdigit() and len(asset_id) <= 78


def test_postprocess_price_guardrail_raises():
    with pytest.raises(ValueError, match="decimal"):
        postprocess(_df(total_price_hex=[format(10**38, "064x")]))


def test_handler_returns_empty_on_zero_row_unload(monkeypatch):
    # a zero-row UNLOAD makes awswrangler raise EmptyDataFrame instead of
    # returning an empty frame; the handler must treat it as "no data"
    import awswrangler as wr

    from decode import ethereum_marketplace_v2_handler

    def _raise(**kwargs):
        raise wr.exceptions.EmptyDataFrame("Query would return untyped, empty dataframe.")

    monkeypatch.setenv("LAKE_BUCKET", "test-bucket")
    monkeypatch.setattr(wr.athena, "read_sql_query", _raise)
    result = ethereum_marketplace_v2_handler.handler(
        {"start_date": "2018-11-01", "end_date": "2018-11-02"}, None
    )
    assert result == {
        "start_date": "2018-11-01",
        "end_date": "2018-11-02",
        "rows_by_dt": {},
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_decode_ethereum_marketplace_v2_handler.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'decode.ethereum_marketplace_v2_handler'`

- [ ] **Step 3: Write the handler**

Create `decode/ethereum_marketplace_v2_handler.py`:

```python
"""decode-ethereum-marketplace-v2-orders Lambda: bronze -> staging.

The Athena query does the whole decode in SQL (static per-event word
layout); this handler only converts hex with Python's
arbitrary-precision ints and appends timestamped parquets
(bronze-style): re-runs add rows rather than replace them, so
downstream dbt dedups by (transaction_hash, log_index) keeping the
latest decoded_at. Unlike the LegacyMarketplace events, data word 1
carries the NFT contract address, so no downstream attribution is
needed.

expiresAt in OrderCreated is unix MILLISECONDS (the dApp sent
JavaScript timestamps, same quirk as the legacy marketplace);
pd.to_datetime(unit="ms") converts it and raises OutOfBoundsDatetime
on garbage values — deliberately loud.

Event: {"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"} (inclusive).
Default: single day, UTC today-2. Raises on any failure so the caller
(manual invoke today, Step Functions later) surfaces it.
"""

import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import awswrangler as wr
import pandas as pd

from decode.common import parse_event
from decode.ethereum_marketplace_v2_query import build_query

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")
GLUE_DATABASE = "staging"
GLUE_TABLE = "ethereum_marketplace_v2_orders"

# Athena decimal(38,0) ceiling for the total_price column
_MAX_DECIMAL38 = 10**38

_FINAL_COLUMNS = [
    "transaction_hash",
    "log_index",
    "block_timestamp",
    "event_name",
    "order_id",
    "asset_id",
    "contract_address",
    "seller",
    "buyer",
    "total_price",
    "expires_at",
    "bronze_extracted_at",
    "decoded_at",
    "dt",
]


def postprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # unsigned decimal string, same convention as nft_transfers.token_id
    df["asset_id"] = df["asset_id_hex"].map(lambda h: str(int(h, 16)))
    prices = df["total_price_hex"].map(lambda h: None if pd.isna(h) else int(h, 16))
    too_big = prices.map(lambda p: p is not None and p >= _MAX_DECIMAL38)
    if too_big.any():
        rows = df.loc[too_big, ["transaction_hash", "log_index"]].to_dict("records")
        raise ValueError(f"total_price exceeds decimal(38,0) in rows: {rows[:5]}")
    # Decimal, not int: that is what pyarrow maps onto decimal128(38,0)
    df["total_price"] = prices.map(lambda p: None if p is None else Decimal(p))
    df["expires_at"] = pd.to_datetime(
        df["expires_at_hex"].map(lambda h: None if pd.isna(h) else int(h, 16)),
        unit="ms",
    )
    for col in ("contract_address", "seller", "buyer"):
        df[col] = df[col].map(lambda a: None if pd.isna(a) else a.lower())
    # floor to ms: the staging schema stores timestamp(ms) and pyarrow
    # refuses lossy casts from microseconds
    df["decoded_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("ms")
    return df[_FINAL_COLUMNS]


def handler(event, context):
    start, end = parse_event(event)
    bucket = os.environ["LAKE_BUCKET"]
    # Same filename convention as bronze: timestamp with dashes in the
    # time part (colons in S3 keys break URL handling).
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")

    sql = build_query(start, end)
    logger.info("extraction query for %s..%s:\n%s", start, end, sql)
    try:
        df = wr.athena.read_sql_query(
            sql=sql,
            database="bronze",
            workgroup=ATHENA_WORKGROUP,
            ctas_approach=False,
            unload_approach=True,
            # unique per run: UNLOAD refuses an existing target directory
            s3_output=(
                f"s3://{bucket}/athena-results/unload/"
                f"marketplace_v2_orders/{uuid.uuid4()}/"
            ),
            keep_files=False,
        )
    except wr.exceptions.EmptyDataFrame:
        # a zero-row UNLOAD raises instead of returning an empty frame
        return {"start_date": start, "end_date": end, "rows_by_dt": {}}
    if df.empty:
        return {"start_date": start, "end_date": end, "rows_by_dt": {}}

    df = postprocess(df)
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/staging/ethereum_marketplace_v2_orders/",
        dataset=True,
        partition_cols=["dt"],
        mode="append",
        database=GLUE_DATABASE,
        table=GLUE_TABLE,
        filename_prefix=f"{stamp}_",
        compression="snappy",
        dtype={"total_price": "decimal(38,0)"},
    )
    return {
        "start_date": start,
        "end_date": end,
        "rows_by_dt": df.groupby("dt").size().to_dict(),
    }
```

- [ ] **Step 4: Update the `decode/common.py` docstring**

Change lines 1-2 from:

```python
"""Helpers shared by all the decode Lambdas (nft transfers, seaport,
wyvern, erc20 and legacy marketplace)."""
```

to:

```python
"""Helpers shared by all the decode Lambdas (nft transfers, seaport,
wyvern, erc20, legacy marketplace and marketplace v2)."""
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_decode_ethereum_marketplace_v2_handler.py -v`
Expected: 6 PASS

- [ ] **Step 6: Run the whole suite and lint (regressions)**

Run: `.venv/bin/pytest tests/ && .venv/bin/ruff check decode/ tests/`
Expected: all PASS, no lint errors

- [ ] **Step 7: Commit**

```bash
git add decode/ethereum_marketplace_v2_handler.py decode/common.py \
    tests/test_decode_ethereum_marketplace_v2_handler.py
git commit -m "feat: decode handler for marketplace v2 orders"
```

---

### Task 3: Terraform — Glue table + Lambda

**Files:**
- Create: `terraform/table_marketplace_v2_orders.tf`
- Create: `terraform/lambda_decode_marketplace_v2.tf`

**Interfaces:**
- Consumes: existing Terraform symbols `aws_glue_catalog_database.staging`, `aws_glue_catalog_database.bronze`, `aws_athena_workgroup.main`, `aws_s3_bucket.lake`, `local.bucket_name`, `local.awssdkpandas_layer_arn`, `data.aws_caller_identity.current`; handler entrypoint `decode.ethereum_marketplace_v2_handler.handler` (Task 2).
- Produces: Lambda `decode-ethereum-marketplace-v2-orders` and Glue table `staging.ethereum_marketplace_v2_orders` (used by Task 4).

- [ ] **Step 1: Write the Glue table**

Create `terraform/table_marketplace_v2_orders.tf`:

```hcl
# Staging table for decoded Marketplace V2 order events. No partition
# projection: the Lambda registers each dt explicitly, which keeps the
# "$partitions" metadata convention working.
resource "aws_glue_catalog_table" "marketplace_v2_orders" {
  database_name = aws_glue_catalog_database.staging.name
  name          = "ethereum_marketplace_v2_orders"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/staging/ethereum_marketplace_v2_orders/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "transaction_hash"
      type = "string"
    }
    columns {
      name = "log_index"
      type = "bigint"
    }
    columns {
      name = "block_timestamp"
      type = "timestamp"
    }
    columns {
      name = "event_name"
      type = "string"
    }
    columns {
      name = "order_id"
      type = "string"
    }
    columns {
      name = "asset_id"
      type = "string"
    }
    columns {
      name = "contract_address"
      type = "string"
    }
    columns {
      name = "seller"
      type = "string"
    }
    columns {
      name = "buyer"
      type = "string"
    }
    columns {
      name = "total_price"
      type = "decimal(38,0)"
    }
    columns {
      name = "expires_at"
      type = "timestamp"
    }
    columns {
      name = "bronze_extracted_at"
      type = "timestamp"
    }
    columns {
      name = "decoded_at"
      type = "timestamp"
    }
  }
}
```

- [ ] **Step 2: Write the Lambda + IAM**

Create `terraform/lambda_decode_marketplace_v2.tf` (mirror of `lambda_decode_legacy_marketplace.tf`):

```hcl
locals {
  decode_marketplace_v2_tags = { component = "decode", layer = "staging" }
}

# source blocks (not source_dir) so the zip keeps the decode/ package
# directory and the handler resolves as
# decode.ethereum_marketplace_v2_handler.handler. Only this Lambda's
# modules ship.
data "archive_file" "decode_marketplace_v2" {
  type        = "zip"
  output_path = "${path.module}/build/decode_marketplace_v2.zip"

  source {
    content  = file("${path.module}/../decode/__init__.py")
    filename = "decode/__init__.py"
  }
  source {
    content  = file("${path.module}/../decode/common.py")
    filename = "decode/common.py"
  }
  source {
    content  = file("${path.module}/../decode/ethereum_marketplace_v2_query.py")
    filename = "decode/ethereum_marketplace_v2_query.py"
  }
  source {
    content  = file("${path.module}/../decode/ethereum_marketplace_v2_handler.py")
    filename = "decode/ethereum_marketplace_v2_handler.py"
  }
}

resource "aws_iam_role" "decode_marketplace_v2" {
  name = "decode-ethereum-marketplace-v2-orders-role"
  tags = local.decode_marketplace_v2_tags
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "decode_marketplace_v2" {
  name = "athena-glue-s3"
  role = aws_iam_role.decode_marketplace_v2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup",
        ]
        Resource = aws_athena_workgroup.main.arn
      },
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
          aws_glue_catalog_database.staging.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/*",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/staging/*",
        ]
      },
      {
        # append mode still registers new dt partitions
        Effect = "Allow"
        Action = [
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.staging.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/staging/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      },
      {
        # read bronze data for the extraction query
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/*"
      },
      {
        # wrangler UNLOAD scratch + staging output
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "${aws_s3_bucket.lake.arn}/athena-results/*",
          "${aws_s3_bucket.lake.arn}/staging/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "decode_marketplace_v2_logs" {
  role       = aws_iam_role.decode_marketplace_v2.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "decode_marketplace_v2" {
  name              = "/aws/lambda/decode-ethereum-marketplace-v2-orders"
  retention_in_days = 7
  tags              = local.decode_marketplace_v2_tags
}

resource "aws_lambda_function" "decode_marketplace_v2" {
  function_name    = "decode-ethereum-marketplace-v2-orders"
  role             = aws_iam_role.decode_marketplace_v2.arn
  filename         = data.archive_file.decode_marketplace_v2.output_path
  source_code_hash = data.archive_file.decode_marketplace_v2.output_base64sha256
  handler          = "decode.ethereum_marketplace_v2_handler.handler"
  runtime          = "python3.13"
  architectures    = ["arm64"]
  timeout          = 300
  memory_size      = 2048
  layers           = [local.awssdkpandas_layer_arn]
  tags             = local.decode_marketplace_v2_tags

  environment {
    variables = {
      LAKE_BUCKET      = aws_s3_bucket.lake.id
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }
}
```

- [ ] **Step 3: Validate and plan**

Run: `cd terraform && terraform validate && terraform plan`
Expected: valid; plan shows exactly 6 resources to add (table, role, role policy, policy attachment, log group, function). The other decode Lambdas will also show a benign `source_code_hash` update (the `common.py` docstring ships in their zips). Nothing destroyed. READ THE PLAN before continuing.

- [ ] **Step 4: Commit**

```bash
git add terraform/table_marketplace_v2_orders.tf \
    terraform/lambda_decode_marketplace_v2.tf
git commit -m "feat: terraform for decode-ethereum-marketplace-v2-orders lambda and staging table"
```

---

### Task 4: Deploy and smoke test

**Files:**
- None created; deploy + verification only.

**Interfaces:**
- Consumes: Lambda `decode-ethereum-marketplace-v2-orders` and Glue table `staging.ethereum_marketplace_v2_orders` (Task 3).
- Produces: verified rows in staging for dt=2018-11-01 (the fixture day).

- [ ] **Step 1: Apply**

Run: `cd terraform && terraform plan -out=/tmp/mp_v2.tfplan`, review the output, then `terraform apply /tmp/mp_v2.tfplan`
Expected: 6 resources added (+ benign lambda hash updates).

- [ ] **Step 2: Invoke for the fixture day**

```bash
aws lambda invoke --function-name decode-ethereum-marketplace-v2-orders \
  --payload '{"start_date":"2018-11-01","end_date":"2018-11-01"}' \
  --cli-binary-format raw-in-base64-out /dev/stdout
```

Expected: `{"start_date": "2018-11-01", "end_date": "2018-11-01", "rows_by_dt": {"2018-11-01": N}}` with N > 0 and no function error.

- [ ] **Step 3: Verify in Athena**

Run this query in the `decentraland-data-platform` workgroup:

```sql
SELECT event_name,
    count(*) AS events,
    count(total_price) AS with_price,
    count(expires_at) AS with_expiry,
    count(buyer) AS with_buyer,
    count(DISTINCT contract_address) AS nft_contracts
FROM "staging"."ethereum_marketplace_v2_orders"
WHERE dt = '2018-11-01'
GROUP BY event_name
```

Expected: three rows. `OrderCreated` has `with_price = with_expiry = events` and `with_buyer = 0`; `OrderSuccessful` has `with_price = with_buyer = events` and `with_expiry = 0`; `OrderCancelled` has all three optional counts = 0. `nft_contracts` ≥ 2 for `OrderCreated` (LANDProxy + EstateProxy in the fixtures). Spot-check one `expires_at` lands in 2018 (ms conversion correct).

- [ ] **Step 4: Verify the partition metadata convention**

```sql
SELECT max(dt) FROM "staging"."ethereum_marketplace_v2_orders$partitions"
```

Expected: `2018-11-01` (partition registered by awswrangler).

- [ ] **Step 5: Report**

No files changed in this task. Report invoke output and both query results. Backfill (contract active from ~Nov 2018 to today; monthly invokes, 8 in parallel, per the established recipe) is a follow-up pending user go-ahead, as is the PR + squash merge.
