# Marketplace Trades Decode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode `Traded` events of MarketplaceV3 (`0x2d6b3508f9aca32d2550f92b2addba932e73c1ff`) and MarketplaceV4 (`0x1b67d0e31eeb6b52d8eeed71d3616c2f5b33b8e7`) from `bronze.ethereum_logs` into `staging.ethereum_marketplace_trades` via a new zip Lambda.

**Architecture:** Mirror of the Seaport decoder: the Athena query only filters and fetches raw logs; a pure-Python parser walks the dynamic `Trade` struct (nested arrays, per-asset `bytes extra`) word by word, validated against eth_abi in tests. Only sales reach staging (unknown-side / zero-payment rows dropped). Append-only parquet partitioned by `dt`.

**Tech Stack:** Python 3.13 ARM64 Lambda (zip + AWSSDKPandas layer), awswrangler (Athena UNLOAD + parquet), Terraform, pytest + eth_abi (dev only).

**Spec:** `docs/superpowers/specs/2026-08-27-marketplace-trades-decode-design.md`

## Global Constraints

- All artifacts in English; SQL keywords UPPERCASE.
- Work on branch `feat/marketplace-trades-decode`; merge to `main` via PR with squash merge. Push only with explicit user confirmation.
- Addresses lowercase at write time; `0x`-prefixed. Token ids as decimal strings (≤78 chars).
- Partition column is `dt`; never `date`. Amount guardrail: ≥ 10^38 raises.
- Event contract `{"start_date", "end_date"}` inclusive, default UTC today−2, via `decode.common.parse_event`.
- Never hardcode the bucket; Lambda reads `LAKE_BUCKET`.
- Tests: `.venv/bin/pytest tests/ -v`; lint: `.venv/bin/ruff check decode/ tests/`.
- Locked Trade ABI type string (validated on real logs with eth_abi; `Checks` has 7 static fields, the 7th undocumented):
  `(address,bytes,(uint256,uint256,uint256,bytes32,uint256,uint256,uint256,address[],(address,bytes4,uint256,bool)[]),(uint256,address,uint256,address,bytes)[],(uint256,address,uint256,address,bytes)[])`

---

### Task 1: Extraction query module

**Files:**
- Create: `decode/ethereum_marketplace_trades_query.py`
- Test: `tests/test_decode_ethereum_marketplace_trades_query.py`

**Interfaces:**
- Consumes: nothing new (stdlib `re` only).
- Produces: `build_query(start_date: str, end_date: str) -> str` and constants `TRADED_TOPIC`, `MARKETPLACE_V3`, `MARKETPLACE_V4`. Task 3's handler imports `build_query`; Task 2's parser imports `TRADED_TOPIC` from here (single source of truth for the topic).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_decode_ethereum_marketplace_trades_query.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_decode_ethereum_marketplace_trades_query.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write the query module**

Create `decode/ethereum_marketplace_trades_query.py`:

```python
"""Builds the Athena query that extracts V3/V4 marketplace Traded events.

Like Seaport (and unlike the V1/V2 decoders), the event payload is a
nested dynamic struct — per-asset `bytes extra` makes word positions
variable — so the query only filters and fetches raw topics/data; all
ABI decoding happens in ethereum_marketplace_trades_parser.
"""

import re

TRADED_TOPIC = "0xaaecdfa7e74e704650fcb273f630f42f68974eff42bfffc1732cf30db9e4685b"
MARKETPLACE_V3 = "0x2d6b3508f9aca32d2550f92b2addba932e73c1ff"
MARKETPLACE_V4 = "0x1b67d0e31eeb6b52d8eeed71d3616c2f5b33b8e7"

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def build_query(start_date: str, end_date: str) -> str:
    for d in (start_date, end_date):
        if not _DT_RE.match(d):
            raise ValueError(f"invalid date {d!r}, expected YYYY-MM-DD")

    return f"""SELECT transaction_hash,
    log_index,
    block_timestamp,
    address,
    topics,
    data,
    extracted_at,
    dt
FROM "bronze"."ethereum_logs"
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
    AND address IN ('{MARKETPLACE_V3}', '{MARKETPLACE_V4}')
    AND topics[1] = '{TRADED_TOPIC}'
    AND cardinality(topics) = 3"""
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_decode_ethereum_marketplace_trades_query.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add decode/ethereum_marketplace_trades_query.py \
    tests/test_decode_ethereum_marketplace_trades_query.py
git commit -m "feat: extraction query for marketplace v3/v4 traded events"
```

---

### Task 2: Parser module

**Files:**
- Create: `decode/ethereum_marketplace_trades_parser.py`
- Create: `tests/fixtures/marketplace_trades_2025.json` — the four real `Traded` logs sampled from bronze (dt 2025-05/2025-06). The exact JSON records live in the scratchpad file `v34_samples.json` from the design session; regenerate with this Athena query if missing:

```sql
SELECT transaction_hash, log_index, address, topics, data, dt
FROM "bronze"."ethereum_logs"
WHERE dt BETWEEN '2025-01-01' AND '2025-06-30'
    AND address IN ('0x2d6b3508f9aca32d2550f92b2addba932e73c1ff',
        '0x1b67d0e31eeb6b52d8eeed71d3616c2f5b33b8e7')
    AND topics[1] = '0xaaecdfa7e74e704650fcb273f630f42f68974eff42bfffc1732cf30db9e4685b'
LIMIT 4
```

Store as a JSON list of objects with keys `transaction_hash`, `log_index`, `address`, `topics` (list), `data`, `dt` — same shape as `marketplace_v2_orders_2018-11-01.json`. Must include at least one log whose per-asset `extra` is non-empty (the sampled set has one: 38 data words instead of 37).

- Test: `tests/test_decode_ethereum_marketplace_trades_parser.py`

**Interfaces:**
- Consumes: `TRADED_TOPIC` from `decode.ethereum_marketplace_trades_query` (Task 1).
- Produces: `parse_traded(topics: list[str], data: str) -> dict` with keys `trade_id`, `caller`, `signer`, `order_side`, `buyer`, `seller`, `nft_contract_addresses`, `nft_token_ids`, `nft_asset_types`, `nft_beneficiaries`, `payment_currencies`, `payment_amounts` (ints), `payment_asset_types`, `payment_beneficiaries` — consumed by Task 3's handler.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_decode_ethereum_marketplace_trades_parser.py`:

```python
import json
from pathlib import Path

import pytest
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode

from decode.ethereum_marketplace_trades_parser import parse_traded
from decode.ethereum_marketplace_trades_query import TRADED_TOPIC

FIXTURE = Path(__file__).parent / "fixtures" / "marketplace_trades_2025.json"

CHECKS_T = (
    "(uint256,uint256,uint256,bytes32,uint256,uint256,uint256,"
    "address[],(address,bytes4,uint256,bool)[])"
)
ASSET_T = "(uint256,address,uint256,address,bytes)"
TRADE_T = f"(address,bytes,{CHECKS_T},{ASSET_T}[],{ASSET_T}[])"

_EMPTY_CHECKS = (0, 0, 0, b"\x00" * 32, 0, 0, 0, [], [])


def _topics(caller: str, trade_id: str) -> list[str]:
    return [
        TRADED_TOPIC,
        "0x" + "0" * 24 + caller.removeprefix("0x"),
        trade_id,
    ]


def _encode_trade(signer, sent, received) -> str:
    return "0x" + abi_encode(
        [TRADE_T], [(signer, b"", _EMPTY_CHECKS, sent, received)]
    ).hex()


def test_parser_matches_eth_abi_on_real_logs():
    records = json.loads(FIXTURE.read_text())
    assert len(records) == 4
    saw_extra = False
    for record in records:
        parsed = parse_traded(record["topics"], record["data"])
        (trade,) = abi_decode([TRADE_T], bytes.fromhex(record["data"][2:]))
        signer, _sig, _checks, sent, received = trade
        saw_extra = saw_extra or any(a[4] for a in sent + received)

        assert parsed["caller"] == "0x" + record["topics"][1][26:].lower()
        assert parsed["trade_id"] == record["topics"][2]
        assert parsed["signer"] == signer.lower()
        ref_nfts = [a for a in list(sent) + list(received) if a[0] in (3, 4)]
        ref_pays = [a for a in list(sent) + list(received) if a[0] in (1, 2)]
        assert parsed["nft_contract_addresses"] == [a[1].lower() for a in ref_nfts]
        assert parsed["nft_token_ids"] == [str(a[2]) for a in ref_nfts]
        assert parsed["nft_asset_types"] == [a[0] for a in ref_nfts]
        assert parsed["nft_beneficiaries"] == [a[3].lower() for a in ref_nfts]
        assert parsed["payment_currencies"] == [a[1].lower() for a in ref_pays]
        assert parsed["payment_amounts"] == [a[2] for a in ref_pays]
        assert parsed["payment_asset_types"] == [a[0] for a in ref_pays]
        assert parsed["payment_beneficiaries"] == [a[3].lower() for a in ref_pays]
        # all sampled logs are listings: NFTs sit in sent
        assert parsed["order_side"] == "listing"
        assert parsed["seller"] == parsed["signer"]
        assert parsed["buyer"] == parsed["caller"]
    # the fixture set must exercise the non-empty `extra` layout
    assert saw_extra


def test_parser_synthetic_bid_nft_in_received():
    # bid: signer offers MANA (sent), receives the NFT
    signer = "0x" + "a1" * 20
    caller = "0x" + "b2" * 20
    mana = "0x0f5d2fb29fb7d3cfee444a200298f468908cc942"
    land = "0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d"
    data = _encode_trade(
        signer,
        sent=[(1, mana, 5 * 10**18, "0x" + "c3" * 20, b"")],
        received=[(3, land, 42, signer, b"")],
    )
    parsed = parse_traded(_topics(caller, "0x" + "11" * 32), data)
    assert parsed["order_side"] == "bid"
    assert parsed["buyer"] == signer
    assert parsed["seller"] == caller
    assert parsed["nft_token_ids"] == ["42"]
    assert parsed["payment_amounts"] == [5 * 10**18]


def test_parser_usd_pegged_and_collection_item_types():
    signer = "0x" + "a1" * 20
    mana = "0x0f5d2fb29fb7d3cfee444a200298f468908cc942"
    coll = "0x" + "d4" * 20
    data = _encode_trade(
        signer,
        sent=[(4, coll, 7, "0x" + "b2" * 20, b"")],
        received=[(2, mana, 10 * 10**18, signer, b"")],
    )
    parsed = parse_traded(_topics("0x" + "b2" * 20, "0x" + "22" * 32), data)
    assert parsed["nft_asset_types"] == [4]
    assert parsed["payment_asset_types"] == [2]
    assert parsed["order_side"] == "listing"


def test_parser_unknown_side_nfts_both_sides():
    signer = "0x" + "a1" * 20
    land = "0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d"
    data = _encode_trade(
        signer,
        sent=[(3, land, 1, signer, b"")],
        received=[(3, land, 2, signer, b"")],
    )
    parsed = parse_traded(_topics("0x" + "b2" * 20, "0x" + "33" * 32), data)
    assert parsed["order_side"] == "unknown"
    assert parsed["buyer"] is None and parsed["seller"] is None


def test_parser_rejects_wrong_topic_and_bad_asset_type():
    with pytest.raises(ValueError):
        parse_traded(["0x" + "ff" * 32, "0x" + "0" * 64, "0x" + "0" * 64], "0x")
    signer = "0x" + "a1" * 20
    data = _encode_trade(
        signer, sent=[(9, "0x" + "d4" * 20, 1, signer, b"")], received=[]
    )
    with pytest.raises(ValueError):
        parse_traded(_topics("0x" + "b2" * 20, "0x" + "44" * 32), data)
```

- [ ] **Step 2: Save the fixture file**

Write `tests/fixtures/marketplace_trades_2025.json` with the four sampled records (from the design-session scratchpad or the query above).

- [ ] **Step 3: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_decode_ethereum_marketplace_trades_parser.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'decode.ethereum_marketplace_trades_parser'`

- [ ] **Step 4: Write the parser**

Create `decode/ethereum_marketplace_trades_parser.py`:

```python
"""Pure decoder for V3/V4 marketplace Traded logs.

No eth_abi at runtime: the parser walks the ABI layout word by word.
Trade = (signer, bytes signature, Checks checks, Asset[] sent,
Asset[] received); Asset = (assetType, contractAddress, value,
beneficiary, bytes extra). Because `extra` is dynamic, array elements
carry per-element offsets — unlike Seaport's static items. signature,
checks and extra are skipped entirely. The manual parse is validated
against eth_abi in tests.

sent = what the signer gives, received = what the signer gets, and the
indexed caller is always the executing counterparty — so buyer/seller
are never NULL (unlike Seaport's zero-address recipient case).
"""

from decode.ethereum_marketplace_trades_query import TRADED_TOPIC

_ASSET_NFT = {3, 4}  # ERC-721, collection item (primary mint)
_ASSET_PAYMENT = {1, 2}  # ERC-20, USD-pegged MANA


def _word(data_hex: str, index: int) -> str:
    word = data_hex[index * 64 : (index + 1) * 64]
    if len(word) < 64:
        raise ValueError(f"data truncated at word {index}")
    return word


def _uint(data_hex: str, index: int) -> int:
    return int(_word(data_hex, index), 16)


def _address(word: str) -> str:
    return ("0x" + word[-40:]).lower()


def _read_assets(data_hex: str, array_word: int) -> list[dict]:
    count = _uint(data_hex, array_word)
    # dynamic elements: per-element offsets, relative to the word after
    # the array length
    base = array_word + 1
    assets = []
    for i in range(count):
        elem = base + _uint(data_hex, base + i) // 32
        assets.append(
            {
                "asset_type": _uint(data_hex, elem),
                "contract": _address(_word(data_hex, elem + 1)),
                "value": _uint(data_hex, elem + 2),
                "beneficiary": _address(_word(data_hex, elem + 3)),
            }
        )
    return assets


def parse_traded(topics: list[str], data: str) -> dict:
    if not topics or topics[0] != TRADED_TOPIC:
        raise ValueError(f"not a Traded topic: {topics[:1]}")
    caller = _address(topics[1])
    trade_id = topics[2]

    data_hex = data.removeprefix("0x")
    trade = _uint(data_hex, 0) // 32  # offset to the Trade tuple
    signer = _address(_word(data_hex, trade))
    # tuple field offsets are relative to the tuple base; fields:
    # 0 signer, 1 signature, 2 checks, 3 sent, 4 received
    sent = _read_assets(data_hex, trade + _uint(data_hex, trade + 3) // 32)
    received = _read_assets(data_hex, trade + _uint(data_hex, trade + 4) // 32)

    nft_contracts: list[str] = []
    nft_token_ids: list[str] = []
    nft_asset_types: list[int] = []
    nft_beneficiaries: list[str] = []
    pay_currencies: list[str] = []
    pay_amounts: list[int] = []
    pay_asset_types: list[int] = []
    pay_beneficiaries: list[str] = []
    nft_in_sent = nft_in_received = False

    for side, assets in (("sent", sent), ("received", received)):
        for asset in assets:
            if asset["asset_type"] in _ASSET_NFT:
                if side == "sent":
                    nft_in_sent = True
                else:
                    nft_in_received = True
                nft_contracts.append(asset["contract"])
                nft_token_ids.append(str(asset["value"]))
                nft_asset_types.append(asset["asset_type"])
                nft_beneficiaries.append(asset["beneficiary"])
            elif asset["asset_type"] in _ASSET_PAYMENT:
                pay_currencies.append(asset["contract"])
                pay_amounts.append(asset["value"])
                pay_asset_types.append(asset["asset_type"])
                pay_beneficiaries.append(asset["beneficiary"])
            else:
                raise ValueError(
                    f"unexpected assetType {asset['asset_type']} in trade"
                )

    if nft_in_sent and not nft_in_received:
        order_side, buyer, seller = "listing", caller, signer
    elif nft_in_received and not nft_in_sent:
        order_side, buyer, seller = "bid", signer, caller
    else:
        order_side, buyer, seller = "unknown", None, None

    return {
        "trade_id": trade_id,
        "caller": caller,
        "signer": signer,
        "order_side": order_side,
        "buyer": buyer,
        "seller": seller,
        "nft_contract_addresses": nft_contracts,
        "nft_token_ids": nft_token_ids,
        "nft_asset_types": nft_asset_types,
        "nft_beneficiaries": nft_beneficiaries,
        "payment_currencies": pay_currencies,
        "payment_amounts": pay_amounts,
        "payment_asset_types": pay_asset_types,
        "payment_beneficiaries": pay_beneficiaries,
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_decode_ethereum_marketplace_trades_parser.py -v`
Expected: 5 PASS

- [ ] **Step 6: Commit**

```bash
git add decode/ethereum_marketplace_trades_parser.py \
    tests/test_decode_ethereum_marketplace_trades_parser.py \
    tests/fixtures/marketplace_trades_2025.json
git commit -m "feat: pure parser for marketplace traded events"
```

---

### Task 3: Handler module

**Files:**
- Create: `decode/ethereum_marketplace_trades_handler.py`
- Modify: `decode/common.py:1-2` (docstring: add marketplace trades to the Lambda list)
- Test: `tests/test_decode_ethereum_marketplace_trades_handler.py`

**Interfaces:**
- Consumes: `parse_event`, `build_query` (Task 1), `parse_traded` (Task 2).
- Produces: `handler(event, context) -> {"start_date", "end_date", "rows_by_dt"}` (entrypoint `decode.ethereum_marketplace_trades_handler.handler`, used by Task 4's Terraform) and `postprocess(df) -> df` + `_FINAL_COLUMNS`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_decode_ethereum_marketplace_trades_handler.py`:

```python
import json
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from decode.ethereum_marketplace_trades_handler import _FINAL_COLUMNS, postprocess

FIXTURE = Path(__file__).parent / "fixtures" / "marketplace_trades_2025.json"


def _df_from_fixture():
    records = json.loads(FIXTURE.read_text())
    return pd.DataFrame(
        {
            "transaction_hash": [r["transaction_hash"] for r in records],
            "log_index": [r["log_index"] for r in records],
            "block_timestamp": [pd.Timestamp("2025-06-03 10:00:00")] * len(records),
            "address": [r["address"] for r in records],
            "topics": [r["topics"] for r in records],
            "data": [r["data"] for r in records],
            "extracted_at": [pd.Timestamp("2026-08-27 06:00:00")] * len(records),
            "dt": [r["dt"] for r in records],
        }
    )


def test_postprocess_real_logs_columns_and_types():
    out = postprocess(_df_from_fixture())
    assert list(out.columns) == _FINAL_COLUMNS
    assert len(out) == 4  # all fixture logs are sales
    row = out.iloc[0]
    assert row["marketplace_address"] in (
        "0x2d6b3508f9aca32d2550f92b2addba932e73c1ff",
        "0x1b67d0e31eeb6b52d8eeed71d3616c2f5b33b8e7",
    )
    assert row["order_side"] == "listing"
    assert row["buyer"] and row["seller"]
    assert all(isinstance(a, Decimal) for a in row["payment_amounts"])
    assert all(t.isdigit() for t in row["nft_token_ids"])
    assert pd.notna(row["decoded_at"])


def test_postprocess_guardrail_raises_on_huge_amount(monkeypatch):
    import decode.ethereum_marketplace_trades_handler as h

    def _fake_parse(topics, data):
        return {
            "trade_id": "0x" + "0" * 64,
            "caller": "0x" + "b2" * 20,
            "signer": "0x" + "a1" * 20,
            "order_side": "listing",
            "buyer": "0x" + "b2" * 20,
            "seller": "0x" + "a1" * 20,
            "nft_contract_addresses": ["0x" + "d4" * 20],
            "nft_token_ids": ["1"],
            "nft_asset_types": [3],
            "nft_beneficiaries": ["0x" + "b2" * 20],
            "payment_currencies": ["0x" + "e5" * 20],
            "payment_amounts": [10**38],
            "payment_asset_types": [1],
            "payment_beneficiaries": ["0x" + "a1" * 20],
        }

    monkeypatch.setattr(h, "parse_traded", _fake_parse)
    df = _df_from_fixture().head(1)
    with pytest.raises(ValueError, match="decimal"):
        postprocess(df)


def test_postprocess_drops_unknown_and_zero_payment(monkeypatch):
    import decode.ethereum_marketplace_trades_handler as h

    base = {
        "trade_id": "0x" + "0" * 64,
        "caller": "0x" + "b2" * 20,
        "signer": "0x" + "a1" * 20,
        "nft_contract_addresses": [],
        "nft_token_ids": [],
        "nft_asset_types": [],
        "nft_beneficiaries": [],
        "payment_currencies": [],
        "payment_amounts": [],
        "payment_asset_types": [],
        "payment_beneficiaries": [],
    }
    results = iter(
        [
            {**base, "order_side": "unknown", "buyer": None, "seller": None},
            {
                **base,
                "order_side": "listing",
                "buyer": "0x" + "b2" * 20,
                "seller": "0x" + "a1" * 20,
            },
        ]
    )
    monkeypatch.setattr(h, "parse_traded", lambda topics, data: next(results))
    out = postprocess(_df_from_fixture().head(2))
    # first row unknown side, second row has no payments: both dropped
    assert out.empty


def test_handler_returns_empty_on_zero_row_unload(monkeypatch):
    import awswrangler as wr

    from decode import ethereum_marketplace_trades_handler

    def _raise(**kwargs):
        raise wr.exceptions.EmptyDataFrame("Query would return untyped, empty dataframe.")

    monkeypatch.setenv("LAKE_BUCKET", "test-bucket")
    monkeypatch.setattr(wr.athena, "read_sql_query", _raise)
    result = ethereum_marketplace_trades_handler.handler(
        {"start_date": "2025-01-01", "end_date": "2025-01-02"}, None
    )
    assert result == {
        "start_date": "2025-01-01",
        "end_date": "2025-01-02",
        "rows_by_dt": {},
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_decode_ethereum_marketplace_trades_handler.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write the handler**

Create `decode/ethereum_marketplace_trades_handler.py`:

```python
"""decode-ethereum-marketplace-trades Lambda: bronze -> staging.

The Athena query only filters (V3/V4 address + Traded topic0) and
fetches raw logs; all ABI decoding happens in
ethereum_marketplace_trades_parser. Only sales reach staging: trades
with order_side 'unknown' (NFTs on both sides or neither) or with no
payments are dropped here. wr.s3.to_parquet appends timestamped
parquets (bronze-style): re-runs add rows rather than replace them, so
downstream dbt dedups by (transaction_hash, log_index) keeping the
latest decoded_at.

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
from decode.ethereum_marketplace_trades_parser import parse_traded
from decode.ethereum_marketplace_trades_query import build_query

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")
GLUE_DATABASE = "staging"
GLUE_TABLE = "ethereum_marketplace_trades"

# Athena decimal(38,0) ceiling for payment amounts
_MAX_DECIMAL38 = 10**38

_FINAL_COLUMNS = [
    "transaction_hash",
    "log_index",
    "block_timestamp",
    "marketplace_address",
    "trade_id",
    "caller",
    "signer",
    "order_side",
    "buyer",
    "seller",
    "nft_contract_addresses",
    "nft_token_ids",
    "nft_asset_types",
    "nft_beneficiaries",
    "payment_currencies",
    "payment_amounts",
    "payment_asset_types",
    "payment_beneficiaries",
    "bronze_extracted_at",
    "decoded_at",
    "dt",
]


def _to_decimal_list(values, column, tx, log_index):
    for v in values:
        if v >= _MAX_DECIMAL38:
            raise ValueError(
                f"{column} exceeds decimal(38,0) in {tx} log_index {log_index}"
            )
    return [Decimal(v) for v in values]


def postprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    parsed = pd.DataFrame(
        [parse_traded(list(row.topics), row.data) for row in df.itertuples()],
        index=df.index,
    )
    out = pd.concat([df.drop(columns=["topics", "data"]), parsed], axis=1)

    is_sale = (out["order_side"] != "unknown") & (
        out["payment_amounts"].map(len) > 0
    )
    skipped = int((~is_sale).sum())
    if skipped:
        logger.info("skipping %d non-sale trades (swaps or zero-payment)", skipped)
    out = out[is_sale].copy()
    if out.empty:
        return out.reindex(columns=_FINAL_COLUMNS)

    out["marketplace_address"] = out["address"].str.lower()
    out["payment_amounts"] = out.apply(
        lambda r: _to_decimal_list(
            r["payment_amounts"],
            "payment_amounts",
            r["transaction_hash"],
            r["log_index"],
        ),
        axis=1,
    )
    out = out.rename(columns={"extracted_at": "bronze_extracted_at"})
    # floor to ms: the staging schema stores timestamp(ms) and pyarrow
    # refuses lossy casts from microseconds
    out["decoded_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("ms")
    return out[_FINAL_COLUMNS]


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
                f"marketplace_trades/{uuid.uuid4()}/"
            ),
            keep_files=False,
        )
    except wr.exceptions.EmptyDataFrame:
        # a zero-row UNLOAD raises instead of returning an empty frame
        return {"start_date": start, "end_date": end, "rows_by_dt": {}}
    if df.empty:
        return {"start_date": start, "end_date": end, "rows_by_dt": {}}

    df = postprocess(df)
    if df.empty:
        return {"start_date": start, "end_date": end, "rows_by_dt": {}}
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/staging/ethereum_marketplace_trades/",
        dataset=True,
        partition_cols=["dt"],
        mode="append",
        database=GLUE_DATABASE,
        table=GLUE_TABLE,
        filename_prefix=f"{stamp}_",
        compression="snappy",
        dtype={
            "payment_amounts": "array<decimal(38,0)>",
            "nft_asset_types": "array<bigint>",
            "payment_asset_types": "array<bigint>",
        },
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
wyvern, erc20, legacy marketplace and marketplace v2)."""
```

to:

```python
"""Helpers shared by all the decode Lambdas (nft transfers, seaport,
wyvern, erc20 and the dcl marketplaces: legacy, v2 and trades)."""
```

- [ ] **Step 5: Run tests, full suite and lint**

Run: `.venv/bin/pytest tests/test_decode_ethereum_marketplace_trades_handler.py -v && .venv/bin/pytest tests/ && .venv/bin/ruff check decode/ tests/`
Expected: 4 PASS, full suite green, no lint errors

- [ ] **Step 6: Commit**

```bash
git add decode/ethereum_marketplace_trades_handler.py decode/common.py \
    tests/test_decode_ethereum_marketplace_trades_handler.py
git commit -m "feat: decode handler for marketplace trades"
```

---

### Task 4: Terraform — Glue table + Lambda

**Files:**
- Create: `terraform/table_marketplace_trades.tf`
- Create: `terraform/lambda_decode_marketplace_trades.tf`

**Interfaces:**
- Consumes: existing Terraform symbols (same set as `lambda_decode_marketplace_v2.tf`); handler entrypoint `decode.ethereum_marketplace_trades_handler.handler` (Task 3).
- Produces: Lambda `decode-ethereum-marketplace-trades` and Glue table `staging.ethereum_marketplace_trades` (used by Task 5).

- [ ] **Step 1: Write the Glue table**

Create `terraform/table_marketplace_trades.tf` — pattern of `table_seaport_sales.tf`: database `staging`, name `ethereum_marketplace_trades`, location `s3://${local.bucket_name}/staging/ethereum_marketplace_trades/`, parquet serde, partition key `dt` (string), and these columns in order:

```
transaction_hash string, log_index bigint, block_timestamp timestamp,
marketplace_address string, trade_id string, caller string,
signer string, order_side string, buyer string, seller string,
nft_contract_addresses array<string>, nft_token_ids array<string>,
nft_asset_types array<bigint>, nft_beneficiaries array<string>,
payment_currencies array<string>, payment_amounts array<decimal(38,0)>,
payment_asset_types array<bigint>, payment_beneficiaries array<string>,
bronze_extracted_at timestamp, decoded_at timestamp
```

Same resource shape as `aws_glue_catalog_table.marketplace_v2_orders` (copy it and adjust name/location/columns).

- [ ] **Step 2: Write the Lambda + IAM**

Create `terraform/lambda_decode_marketplace_trades.tf` — exact mirror of `lambda_decode_marketplace_v2.tf` with these renames:

- locals: `decode_marketplace_trades_tags`
- archive: `decode_marketplace_trades`, output `build/decode_marketplace_trades.zip`, sources: `__init__.py`, `common.py`, `ethereum_marketplace_trades_query.py`, `ethereum_marketplace_trades_parser.py`, `ethereum_marketplace_trades_handler.py`
- IAM role `decode-ethereum-marketplace-trades-role` + identical policy statements
- log group `/aws/lambda/decode-ethereum-marketplace-trades`
- function `decode-ethereum-marketplace-trades`, handler `decode.ethereum_marketplace_trades_handler.handler`, same runtime/arch/timeout/memory/layer/env

- [ ] **Step 3: Validate and plan**

Run: `cd terraform && terraform validate && terraform plan`
Expected: valid; exactly 6 resources to add plus benign `source_code_hash` updates on the other decode Lambdas (the `common.py` docstring ships in their zips). Nothing destroyed. READ THE PLAN before continuing.

- [ ] **Step 4: Commit**

```bash
git add terraform/table_marketplace_trades.tf \
    terraform/lambda_decode_marketplace_trades.tf
git commit -m "feat: terraform for decode-ethereum-marketplace-trades lambda and staging table"
```

---

### Task 5: Deploy and smoke test

**Files:** none; deploy + verification only.

**Interfaces:**
- Consumes: Lambda and table from Task 4.
- Produces: verified rows in staging for a sample range.

- [ ] **Step 1: Apply**

Run: `cd terraform && terraform plan -out=/tmp/mp_trades.tfplan`, review, then `terraform apply /tmp/mp_trades.tfplan`
Expected: 6 added (+ benign hash updates).

- [ ] **Step 2: Invoke for a range containing the fixtures**

```bash
aws lambda invoke --function-name decode-ethereum-marketplace-trades \
  --payload '{"start_date":"2025-06-01","end_date":"2025-06-07"}' \
  --cli-binary-format raw-in-base64-out /dev/stdout
```

Expected: `rows_by_dt` with N > 0 for several days, no function error.

- [ ] **Step 3: Verify in Athena**

```sql
SELECT marketplace_address, order_side,
    count(*) AS trades,
    count_if(cardinality(nft_token_ids) > 0) AS with_nfts,
    count_if(cardinality(payment_amounts) > 0) AS with_payments,
    count_if(buyer IS NULL OR seller IS NULL) AS null_parties
FROM "staging"."ethereum_marketplace_trades"
WHERE dt BETWEEN '2025-06-01' AND '2025-06-07'
GROUP BY 1, 2
```

Expected: every row has `with_nfts = with_payments = trades` and `null_parties = 0` (only sales stored; buyer/seller always derived). Spot-check one trade's `nft_token_ids` and `payment_amounts` against the raw log.

- [ ] **Step 4: Verify the partition metadata convention**

```sql
SELECT max(dt) FROM "staging"."ethereum_marketplace_trades$partitions"
```

Expected: a dt inside the invoked range.

- [ ] **Step 5: Report**

Report invoke output and query results. Follow-ups pending user go-ahead: backfill (2024-12 → today, monthly invokes, 8 in parallel) and PR + squash merge.
