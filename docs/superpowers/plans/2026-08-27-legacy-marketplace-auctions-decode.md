# Legacy Marketplace Auctions Decode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode the LegacyMarketplace (`0xb3bca6f5052c7e24726b44da7403b56a8a1b98f8`) auction events (`AuctionCreated`, `AuctionCancelled`, `AuctionSuccessful`) from `bronze.ethereum_logs` into `staging.ethereum_legacy_marketplace_auctions` via a new zip Lambda.

**Architecture:** Mirror of the Wyvern decoder: the whole decode is SQL (static word layout, conditional per event via `CASE` on topic0); Python only converts hex → Decimal / decimal-string / timestamp. Append-only parquet writes partitioned by `dt`; dbt dedups downstream by `(transaction_hash, log_index)`.

**Tech Stack:** Python 3.13 ARM64 Lambda (zip + AWSSDKPandas layer), awswrangler (Athena UNLOAD + parquet), Terraform, pytest + eth_abi (dev only).

**Spec:** `docs/superpowers/specs/2026-08-27-legacy-marketplace-auctions-decode-design.md`

## Global Constraints

- All artifacts in English; SQL keywords UPPERCASE.
- Addresses lowercase at write time; `0x`-prefixed.
- Token ids as decimal strings (`str(int(hex, 16))`), up to 78 chars — same convention as `staging.ethereum_nft_transfers.token_id`.
- Partition column is `dt` (string, `YYYY-MM-DD`); never `date`.
- Amount guardrail: values ≥ 10^38 raise (decimal(38,0) ceiling).
- `expiresAt` in the event is **unix milliseconds** (verified against real logs) → convert with `unit="ms"`.
- Event contract: `{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}` inclusive, default UTC today−2, via existing `decode.common.parse_event`.
- Never hardcode the bucket name; Lambda reads `LAKE_BUCKET` env var.
- Run tests with `pytest tests/ -v` from the repo root (pytest is on the repo venv).

---

### Task 1: Extraction query module

**Files:**
- Create: `decode/ethereum_legacy_marketplace_query.py`
- Create: `tests/fixtures/legacy_marketplace_auctions_2018-03-19.json`
- Test: `tests/test_decode_ethereum_legacy_marketplace_query.py`

**Interfaces:**
- Consumes: nothing new (stdlib `re` only).
- Produces: `build_query(start_date: str, end_date: str) -> str` and constants `LEGACY_MARKETPLACE`, `AUCTION_CREATED_TOPIC`, `AUCTION_CANCELLED_TOPIC`, `AUCTION_SUCCESSFUL_TOPIC` — Task 2's handler imports `build_query`; Task 1's tests import all four constants.

- [ ] **Step 1: Save the fixture file with real bronze logs (two per event)**

Create `tests/fixtures/legacy_marketplace_auctions_2018-03-19.json` (real logs pulled from `bronze.ethereum_logs`, dt=2018-03-19):

```json
[
 {
  "transaction_hash": "0xd8fd659fc55e4b325805acc698084cfbe17c22089fd997cd50d2e0b6e29802f4",
  "log_index": 9,
  "address": "0xb3bca6f5052c7e24726b44da7403b56a8a1b98f8",
  "topics": [
   "0x88bd2ba46f3dc2567144331c35bd4c5ced3d547d8828638a152ddd9591c137a6",
   "0xffffffffffffffffffffffffffffffa8ffffffffffffffffffffffffffffff82",
   "0x0000000000000000000000008c0928cf8cddd28bc01d0c9b662d7092253b8574"
  ],
  "data": "0x5b0f44ef5a4ec878625ff777f375a81bbe8f84eb160f66db3091477b4d8438ad",
  "dt": "2018-03-19"
 },
 {
  "transaction_hash": "0xffb227dd2dc1b35c6c01ea34f27c3f6f9bcb3ecea93a0a338867b67152f217dd",
  "log_index": 22,
  "address": "0xb3bca6f5052c7e24726b44da7403b56a8a1b98f8",
  "topics": [
   "0x88bd2ba46f3dc2567144331c35bd4c5ced3d547d8828638a152ddd9591c137a6",
   "0xffffffffffffffffffffffffffffffa7ffffffffffffffffffffffffffffff82",
   "0x0000000000000000000000008c0928cf8cddd28bc01d0c9b662d7092253b8574"
  ],
  "data": "0xa2a9a01fc97672bb1c3f1e952585384a09484ebb46932d0830d494337573883e",
  "dt": "2018-03-19"
 },
 {
  "transaction_hash": "0xb1d48f82ad69cae684e231bdb146dcd56d3df2ba6a133cd8c552e324889c0bd4",
  "log_index": 4,
  "address": "0xb3bca6f5052c7e24726b44da7403b56a8a1b98f8",
  "topics": [
   "0x9493ae82b9872af74473effb9d302efba34e0df360a99cc5e577cd3f28e3cab2",
   "0x0000000000000000000000000000002500000000000000000000000000000026",
   "0x0000000000000000000000008c0928cf8cddd28bc01d0c9b662d7092253b8574"
  ],
  "data": "0x0bba2bd2bf068f93ab2276b051df1e02b2c90f1f69925a86abbfca1f997e559700000000000000000000000000000000000000000000043c33c19375648000000000000000000000000000000000000000000000000000000000016415574c00",
  "dt": "2018-03-19"
 },
 {
  "transaction_hash": "0x25f02725530dedfb88f95df229a07be57c53153ebbaf83ca04dde1f0442f1fce",
  "log_index": 5,
  "address": "0xb3bca6f5052c7e24726b44da7403b56a8a1b98f8",
  "topics": [
   "0x9493ae82b9872af74473effb9d302efba34e0df360a99cc5e577cd3f28e3cab2",
   "0x00000000000000000000000000000004ffffffffffffffffffffffffffffff92",
   "0x0000000000000000000000000727fc3970cca8a5e57145777133dc551c124beb"
  ],
  "data": "0x8526b69ccda3ecc3fab9a1e01e028fa4c4dee830b5248f35cd616b392ef999170000000000000000000000000000000000000000000000a2a15d09519be0000000000000000000000000000000000000000000000000000000000162e059bc00",
  "dt": "2018-03-19"
 },
 {
  "transaction_hash": "0xc2524d3b165580ca86751f43978ec52c522fa3716a6a2d8d976f680abdc51db0",
  "log_index": 4,
  "address": "0xb3bca6f5052c7e24726b44da7403b56a8a1b98f8",
  "topics": [
   "0xedcc7e1c269bc295dc24e74dc46b129c8449e6b0544af73b57c4201b78d119db",
   "0x0000000000000000000000000000000effffffffffffffffffffffffffffffe0",
   "0x0000000000000000000000000727fc3970cca8a5e57145777133dc551c124beb",
   "0x00000000000000000000000000e8c2f1e2a359f7295430ee81a67bbbe46cfe6f"
  ],
  "data": "0x2cca3393b712a39564f934bd43ca9ae06387c89b15dcb0077d49205c5707166e0000000000000000000000000000000000000000000000a2a15d09519be00000",
  "dt": "2018-03-19"
 },
 {
  "transaction_hash": "0x14a66caddeef97a0afeaf612a8964a5aa236ee8687ea7977c50766140163cb3e",
  "log_index": 8,
  "address": "0xb3bca6f5052c7e24726b44da7403b56a8a1b98f8",
  "topics": [
   "0xedcc7e1c269bc295dc24e74dc46b129c8449e6b0544af73b57c4201b78d119db",
   "0x0000000000000000000000000000000effffffffffffffffffffffffffffffe1",
   "0x0000000000000000000000000727fc3970cca8a5e57145777133dc551c124beb",
   "0x00000000000000000000000000e8c2f1e2a359f7295430ee81a67bbbe46cfe6f"
  ],
  "data": "0x89e9fd209ee73be293a874f6016c770a8b9f6ca9d6bff1f1afa8e119cf29756a0000000000000000000000000000000000000000000000a2a15d09519be00000",
  "dt": "2018-03-19"
 }
]
```

- [ ] **Step 2: Write the failing tests**

Create `tests/test_decode_ethereum_legacy_marketplace_query.py`:

```python
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `pytest tests/test_decode_ethereum_legacy_marketplace_query.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'decode.ethereum_legacy_marketplace_query'`

- [ ] **Step 4: Write the query module**

Create `decode/ethereum_legacy_marketplace_query.py`:

```python
"""Builds the Athena query that extracts LegacyMarketplace auction events.

Like Wyvern, the whole decode fits in SQL: every event is static words.
AuctionCreated/Cancelled/Successful share topics (assetId, seller;
Successful adds winner) and data word 0 is the bytes32 auction id — the
NFT token id is the assetId topic, and the NFT contract address is not
in the event at all (attribution is silver work). Python only converts
hex -> decimal string / Decimal / timestamp.
"""

import re

LEGACY_MARKETPLACE = "0xb3bca6f5052c7e24726b44da7403b56a8a1b98f8"
AUCTION_CREATED_TOPIC = (
    "0x9493ae82b9872af74473effb9d302efba34e0df360a99cc5e577cd3f28e3cab2"
)
AUCTION_CANCELLED_TOPIC = (
    "0x88bd2ba46f3dc2567144331c35bd4c5ced3d547d8828638a152ddd9591c137a6"
)
AUCTION_SUCCESSFUL_TOPIC = (
    "0xedcc7e1c269bc295dc24e74dc46b129c8449e6b0544af73b57c4201b78d119db"
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
        WHEN '{AUCTION_CREATED_TOPIC}' THEN 'AuctionCreated'
        WHEN '{AUCTION_CANCELLED_TOPIC}' THEN 'AuctionCancelled'
        WHEN '{AUCTION_SUCCESSFUL_TOPIC}' THEN 'AuctionSuccessful'
    END AS event_name,
    concat('0x', substr(data, 3, 64)) AS auction_id,
    topics[2] AS asset_id_hex,
    concat('0x', substr(topics[3], 27)) AS seller,
    CASE WHEN topics[1] = '{AUCTION_SUCCESSFUL_TOPIC}'
        THEN concat('0x', substr(topics[4], 27)) END AS winner,
    CASE WHEN topics[1] IN ('{AUCTION_CREATED_TOPIC}', '{AUCTION_SUCCESSFUL_TOPIC}')
        THEN substr(data, 67, 64) END AS total_price_hex,
    CASE WHEN topics[1] = '{AUCTION_CREATED_TOPIC}'
        THEN substr(data, 131, 64) END AS expires_at_hex,
    extracted_at AS bronze_extracted_at,
    dt
FROM "bronze"."ethereum_logs"
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
    AND address = '{LEGACY_MARKETPLACE}'
    AND topics[1] IN ('{AUCTION_CREATED_TOPIC}',
        '{AUCTION_CANCELLED_TOPIC}',
        '{AUCTION_SUCCESSFUL_TOPIC}')
    AND length(data) = CASE topics[1]
        WHEN '{AUCTION_CREATED_TOPIC}' THEN 194
        WHEN '{AUCTION_SUCCESSFUL_TOPIC}' THEN 130
        ELSE 66 END
    AND cardinality(topics) = CASE topics[1]
        WHEN '{AUCTION_SUCCESSFUL_TOPIC}' THEN 4
        ELSE 3 END"""
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest tests/test_decode_ethereum_legacy_marketplace_query.py -v`
Expected: 3 PASS

- [ ] **Step 6: Commit**

```bash
git add decode/ethereum_legacy_marketplace_query.py \
    tests/test_decode_ethereum_legacy_marketplace_query.py \
    tests/fixtures/legacy_marketplace_auctions_2018-03-19.json
git commit -m "feat: extraction query for legacy marketplace auction events"
```

---

### Task 2: Handler module

**Files:**
- Create: `decode/ethereum_legacy_marketplace_handler.py`
- Modify: `decode/common.py:1-2` (docstring: add legacy marketplace to the Lambda list)
- Test: `tests/test_decode_ethereum_legacy_marketplace_handler.py`

**Interfaces:**
- Consumes: `decode.common.parse_event(event) -> (start, end)`; `decode.ethereum_legacy_marketplace_query.build_query(start, end) -> str` (Task 1).
- Produces: `handler(event, context) -> {"start_date", "end_date", "rows_by_dt"}` (Lambda entrypoint `decode.ethereum_legacy_marketplace_handler.handler`, used by Task 3's Terraform) and `postprocess(df) -> df` + `_FINAL_COLUMNS` (used by its own tests).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_decode_ethereum_legacy_marketplace_handler.py`:

```python
from decimal import Decimal

import pandas as pd
import pytest

from decode.ethereum_legacy_marketplace_handler import _FINAL_COLUMNS, postprocess

# expires hex is real (AuctionCreated fixture, tx 0x25f0...); price is synthetic
_CREATED_PRICE_HEX = format(20000 * 10**18, "064x")
_EXPIRES_MS_HEX = format(0x162E059BC00, "064x")  # 1524182400000 ms = 2018-04-20 00:00 UTC
_NEG_COORD_ASSET_HEX = (
    "0xffffffffffffffffffffffffffffffa8ffffffffffffffffffffffffffffff82"
)


def _df(**overrides):
    base = {
        "transaction_hash": ["0x25f0"],
        "log_index": [5],
        "block_timestamp": [pd.Timestamp("2018-03-19 10:00:00")],
        "event_name": ["AuctionCreated"],
        "auction_id": ["0x" + "8526b69c" * 8],
        "asset_id_hex": [
            "0x00000000000000000000000000000004ffffffffffffffffffffffffffffff92"
        ],
        "seller": ["0x0727FC3970cca8a5e57145777133dc551c124beb"],
        "winner": [None],
        "total_price_hex": [_CREATED_PRICE_HEX],
        "expires_at_hex": [_EXPIRES_MS_HEX],
        "bronze_extracted_at": [pd.Timestamp("2026-08-27 06:00:00")],
        "dt": ["2018-03-19"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def test_postprocess_created_row_columns_and_types():
    out = postprocess(_df())
    assert list(out.columns) == _FINAL_COLUMNS
    row = out.iloc[0]
    assert row["asset_id"] == str((0x4 << 128) | int("f" * 30 + "92", 16))
    assert row["total_price"] == Decimal(20000 * 10**18)
    assert isinstance(row["total_price"], Decimal)
    # expiresAt is unix MILLISECONDS -> timestamp
    assert row["expires_at"] == pd.Timestamp("2018-04-20 00:00:00")
    assert row["seller"] == "0x0727fc3970cca8a5e57145777133dc551c124beb"
    assert pd.isna(row["winner"])
    assert pd.notna(row["decoded_at"])


def test_postprocess_cancelled_row_nullables():
    out = postprocess(
        _df(
            event_name=["AuctionCancelled"],
            total_price_hex=[None],
            expires_at_hex=[None],
        )
    )
    row = out.iloc[0]
    assert pd.isna(row["total_price"])
    assert pd.isna(row["expires_at"])


def test_postprocess_successful_row_winner_lowercased():
    out = postprocess(
        _df(
            event_name=["AuctionSuccessful"],
            winner=["0x00E8C2F1e2a359F7295430ee81a67BBbe46CFe6F"],
            expires_at_hex=[None],
        )
    )
    assert out.iloc[0]["winner"] == "0x00e8c2f1e2a359f7295430ee81a67bbbe46cfe6f"


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

    from decode import ethereum_legacy_marketplace_handler

    def _raise(**kwargs):
        raise wr.exceptions.EmptyDataFrame("Query would return untyped, empty dataframe.")

    monkeypatch.setenv("LAKE_BUCKET", "test-bucket")
    monkeypatch.setattr(wr.athena, "read_sql_query", _raise)
    result = ethereum_legacy_marketplace_handler.handler(
        {"start_date": "2018-03-19", "end_date": "2018-03-20"}, None
    )
    assert result == {
        "start_date": "2018-03-19",
        "end_date": "2018-03-20",
        "rows_by_dt": {},
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_decode_ethereum_legacy_marketplace_handler.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'decode.ethereum_legacy_marketplace_handler'`

- [ ] **Step 3: Write the handler**

Create `decode/ethereum_legacy_marketplace_handler.py`:

```python
"""decode-ethereum-legacy-marketplace-auctions Lambda: bronze -> staging.

The Athena query does the whole decode in SQL (static per-event word
layout); this handler only converts hex with Python's
arbitrary-precision ints and appends timestamped parquets
(bronze-style): re-runs add rows rather than replace them, so
downstream dbt dedups by (transaction_hash, log_index) keeping the
latest decoded_at. The event carries no NFT contract address — silver
attributes each asset_id to a contract later.

expiresAt in AuctionCreated is unix MILLISECONDS (the legacy dApp sent
JavaScript timestamps); pd.to_datetime(unit="ms") converts it and
raises OutOfBoundsDatetime on garbage values — deliberately loud.

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
from decode.ethereum_legacy_marketplace_query import build_query

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")
GLUE_DATABASE = "staging"
GLUE_TABLE = "ethereum_legacy_marketplace_auctions"

# Athena decimal(38,0) ceiling for the total_price column
_MAX_DECIMAL38 = 10**38

_FINAL_COLUMNS = [
    "transaction_hash",
    "log_index",
    "block_timestamp",
    "event_name",
    "auction_id",
    "asset_id",
    "seller",
    "winner",
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
    prices = df["total_price_hex"].map(
        lambda h: None if pd.isna(h) else int(h, 16)
    )
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
    for col in ("seller", "winner"):
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
                f"legacy_marketplace_auctions/{uuid.uuid4()}/"
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
        path=f"s3://{bucket}/staging/ethereum_legacy_marketplace_auctions/",
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
wyvern and erc20)."""
```

to:

```python
"""Helpers shared by all the decode Lambdas (nft transfers, seaport,
wyvern, erc20 and legacy marketplace)."""
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest tests/test_decode_ethereum_legacy_marketplace_handler.py -v`
Expected: 7 PASS

- [ ] **Step 6: Run the whole suite (regressions)**

Run: `pytest tests/ -v`
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add decode/ethereum_legacy_marketplace_handler.py decode/common.py \
    tests/test_decode_ethereum_legacy_marketplace_handler.py
git commit -m "feat: decode handler for legacy marketplace auctions"
```

---

### Task 3: Terraform — Glue table + Lambda

**Files:**
- Create: `terraform/table_legacy_marketplace_auctions.tf`
- Create: `terraform/lambda_decode_legacy_marketplace.tf`

**Interfaces:**
- Consumes: existing Terraform symbols `aws_glue_catalog_database.staging`, `aws_glue_catalog_database.bronze`, `aws_athena_workgroup.main`, `aws_s3_bucket.lake`, `local.bucket_name`, `local.awssdkpandas_layer_arn`, `data.aws_caller_identity.current`; handler entrypoint `decode.ethereum_legacy_marketplace_handler.handler` (Task 2).
- Produces: Lambda `decode-ethereum-legacy-marketplace-auctions` and Glue table `staging.ethereum_legacy_marketplace_auctions` (used by Task 4).

- [ ] **Step 1: Write the Glue table**

Create `terraform/table_legacy_marketplace_auctions.tf`:

```hcl
# Staging table for decoded LegacyMarketplace auction events. No
# partition projection: the Lambda registers each dt explicitly, which
# keeps the "$partitions" metadata convention working.
resource "aws_glue_catalog_table" "legacy_marketplace_auctions" {
  database_name = aws_glue_catalog_database.staging.name
  name          = "ethereum_legacy_marketplace_auctions"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/staging/ethereum_legacy_marketplace_auctions/"
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
      name = "auction_id"
      type = "string"
    }
    columns {
      name = "asset_id"
      type = "string"
    }
    columns {
      name = "seller"
      type = "string"
    }
    columns {
      name = "winner"
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

Create `terraform/lambda_decode_legacy_marketplace.tf` (mirror of `lambda_decode_wyvern.tf`):

```hcl
locals {
  decode_legacy_marketplace_tags = { component = "decode", layer = "staging" }
}

# source blocks (not source_dir) so the zip keeps the decode/ package
# directory and the handler resolves as
# decode.ethereum_legacy_marketplace_handler.handler. Only this
# Lambda's modules ship.
data "archive_file" "decode_legacy_marketplace" {
  type        = "zip"
  output_path = "${path.module}/build/decode_legacy_marketplace.zip"

  source {
    content  = file("${path.module}/../decode/__init__.py")
    filename = "decode/__init__.py"
  }
  source {
    content  = file("${path.module}/../decode/common.py")
    filename = "decode/common.py"
  }
  source {
    content  = file("${path.module}/../decode/ethereum_legacy_marketplace_query.py")
    filename = "decode/ethereum_legacy_marketplace_query.py"
  }
  source {
    content  = file("${path.module}/../decode/ethereum_legacy_marketplace_handler.py")
    filename = "decode/ethereum_legacy_marketplace_handler.py"
  }
}

resource "aws_iam_role" "decode_legacy_marketplace" {
  name = "decode-ethereum-legacy-marketplace-auctions-role"
  tags = local.decode_legacy_marketplace_tags
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "decode_legacy_marketplace" {
  name = "athena-glue-s3"
  role = aws_iam_role.decode_legacy_marketplace.id
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

resource "aws_iam_role_policy_attachment" "decode_legacy_marketplace_logs" {
  role       = aws_iam_role.decode_legacy_marketplace.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "decode_legacy_marketplace" {
  name              = "/aws/lambda/decode-ethereum-legacy-marketplace-auctions"
  retention_in_days = 7
  tags              = local.decode_legacy_marketplace_tags
}

resource "aws_lambda_function" "decode_legacy_marketplace" {
  function_name    = "decode-ethereum-legacy-marketplace-auctions"
  role             = aws_iam_role.decode_legacy_marketplace.arn
  filename         = data.archive_file.decode_legacy_marketplace.output_path
  source_code_hash = data.archive_file.decode_legacy_marketplace.output_base64sha256
  handler          = "decode.ethereum_legacy_marketplace_handler.handler"
  runtime          = "python3.13"
  architectures    = ["arm64"]
  timeout          = 300
  memory_size      = 2048
  layers           = [local.awssdkpandas_layer_arn]
  tags             = local.decode_legacy_marketplace_tags

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
Expected: valid; plan shows exactly 6 resources to add (table, role, role policy, policy attachment, log group, function) and nothing to change or destroy. READ THE PLAN before continuing.

- [ ] **Step 4: Commit**

```bash
git add terraform/table_legacy_marketplace_auctions.tf \
    terraform/lambda_decode_legacy_marketplace.tf
git commit -m "feat: terraform for decode-ethereum-legacy-marketplace-auctions lambda and staging table"
```

---

### Task 4: Deploy and smoke test

**Files:**
- None created; deploy + verification only.

**Interfaces:**
- Consumes: Lambda `decode-ethereum-legacy-marketplace-auctions` and Glue table `staging.ethereum_legacy_marketplace_auctions` (Task 3).
- Produces: verified rows in staging for dt=2018-03-19 (the fixture day).

- [ ] **Step 1: Apply**

Run: `cd terraform && terraform apply`
Expected: 6 resources added.

- [ ] **Step 2: Invoke for the fixture day**

```bash
aws lambda invoke --function-name decode-ethereum-legacy-marketplace-auctions \
  --payload '{"start_date":"2018-03-19","end_date":"2018-03-19"}' \
  --cli-binary-format raw-in-base64-out /dev/stdout
```

Expected: `{"start_date": "2018-03-19", "end_date": "2018-03-19", "rows_by_dt": {"2018-03-19": N}}` with N > 0 and no function error.

- [ ] **Step 3: Verify in Athena**

Run this query in the `decentraland-data-platform` workgroup:

```sql
SELECT event_name,
    count(*) AS events,
    count(total_price) AS with_price,
    count(expires_at) AS with_expiry,
    count(winner) AS with_winner
FROM "staging"."ethereum_legacy_marketplace_auctions"
WHERE dt = '2018-03-19'
GROUP BY event_name
```

Expected: three rows. `AuctionCreated` has `with_price = with_expiry = events` and `with_winner = 0`; `AuctionSuccessful` has `with_price = with_winner = events` and `with_expiry = 0`; `AuctionCancelled` has all three optional counts = 0. Spot-check one `expires_at` lands in 2018 (ms conversion correct).

- [ ] **Step 4: Verify the partition metadata convention**

```sql
SELECT max(dt) FROM "staging"."ethereum_legacy_marketplace_auctions$partitions"
```

Expected: `2018-03-19` (partition registered by awswrangler).

- [ ] **Step 5: Commit nothing — report**

No files changed in this task. Report invoke output and both query results. Backfill of the full contract history is a follow-up (invoke per day range in batches of 10, per the onchain backfill recipe), pending user go-ahead.
