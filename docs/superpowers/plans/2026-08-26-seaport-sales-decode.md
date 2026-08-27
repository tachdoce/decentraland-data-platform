# Seaport Sales Decode (Ethereum) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode Seaport `OrderFulfilled` logs from `bronze.ethereum_logs` into `staging.ethereum_seaport_sales` (one row per fulfilled order) via a new zip Lambda `decode-ethereum-seaport-sales`.

**Architecture:** Athena (via awswrangler UNLOAD) filters bronze by topic0 and returns raw logs; a pure-Python word parser decodes the `data` blob (no eth_abi at runtime; eth_abi validates it in tests); awswrangler writes parquet with `overwrite_partitions` and registers `dt` partitions in Glue. Terraform defines the Glue table, Lambda, IAM, and log group, mirroring `lambda_decode_nft.tf`.

**Tech Stack:** Python 3.13 ARM64 zip Lambda + AWSSDKPandas layer (pandas/pyarrow/awswrangler), Athena, Glue, S3, Terraform, pytest + eth_abi (dev only).

**Spec:** `docs/superpowers/specs/2026-08-26-seaport-sales-decode-design.md`

## Global Constraints

- All artifacts in English; chat with the user in Spanish.
- Region `us-east-1`; Athena workgroup `decentraland-data-platform`.
- Never hardcode the bucket: `decentraland-data-platform-${account_id}` interpolated in Terraform; Lambdas get it via `LAKE_BUCKET`.
- Addresses lowercase at write time. Partition column is `dt` (never `date`).
- No partition projection on this table (keeps `"$partitions"` working); awswrangler registers partitions.
- SQL style: keywords UPPERCASE; `INNER JOIN` explicit (repo convention).
- Terraform only — never create resources via console; ALWAYS read `terraform plan` output before `apply`.
- Commit locally freely; `git push` only with explicit user confirmation.
- `OrderFulfilled` topic0: `0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31`.
- Verified fixture tx (dt=2026-08-17): `0xd8c5ed0ca8394ec6dace758237697e50ec2b39eb16c53c0cb9e7743166899fcb` — exactly 2 `OrderFulfilled` logs: log_index 497 (3-item listing) and 498 (1-item listing).

---

### Task 1: Real-log fixtures + eth_abi dev dependency

**Files:**
- Create: `tests/fixtures/seaport_order_fulfilled_2026-08-17.json`
- Modify: `requirements-dev.txt`

**Interfaces:**
- Produces: fixture JSON — a list of 2 objects `{"log_index": int, "address": str, "topics": [str, ...], "data": "0x..."}` for the verified tx, consumed by Tasks 2 and 4 tests. Adds `eth_abi==5.*` for tests.

- [ ] **Step 1: Add eth_abi to dev requirements and install**

Append to `requirements-dev.txt`:

```
eth_abi==5.*
```

Run: `pip install -r requirements-dev.txt`
Expected: eth_abi installed (pulls eth-typing/eth-utils/parsimonious — dev only, never shipped to the Lambda).

- [ ] **Step 2: Dump the two real logs from Athena into the fixture**

Write and run this one-off script (delete after; do not commit it):

```python
# scratch_fetch_fixture.py — one-off, delete after running
import json

import awswrangler as wr

TX = "0xd8c5ed0ca8394ec6dace758237697e50ec2b39eb16c53c0cb9e7743166899fcb"
TOPIC = "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"

df = wr.athena.read_sql_query(
    sql=f"""SELECT log_index, address, topics, data
FROM "bronze"."ethereum_logs"
WHERE dt = '2026-08-17'
    AND transaction_hash = '{TX}'
    AND topics[1] = '{TOPIC}'
ORDER BY log_index""",
    database="bronze",
    workgroup="decentraland-data-platform",
    ctas_approach=False,
)
records = [
    {
        "log_index": int(r.log_index),
        "address": r.address,
        "topics": list(r.topics),
        "data": r.data,
    }
    for r in df.itertuples()
]
assert len(records) == 2, f"expected 2 logs, got {len(records)}"
with open("tests/fixtures/seaport_order_fulfilled_2026-08-17.json", "w") as f:
    json.dump(records, f, indent=2)
print("wrote", [r["log_index"] for r in records])
```

Run: `python scratch_fetch_fixture.py && rm scratch_fetch_fixture.py`
Expected: prints `wrote [497, 498]`; JSON file exists with 2 records, each `topics` has 3 entries (OrderFulfilled has 2 indexed params + topic0) and `data` starts with `0x`.

- [ ] **Step 3: Commit**

```bash
git add tests/fixtures/seaport_order_fulfilled_2026-08-17.json requirements-dev.txt
git commit -m "test: real Seaport OrderFulfilled fixtures; eth_abi as dev dep"
```

---

### Task 2: Extract shared `parse_event` into `decode/common.py`

The seaport handler needs the same `{start_date, end_date}` parsing as `decode/handler.py`, but its zip will not include `handler.py` (which imports `decode.query`). Extract the shared function so both zips can include `common.py`.

**Files:**
- Create: `decode/common.py`
- Modify: `decode/handler.py` (remove `parse_event`, import it instead)
- Modify: `terraform/lambda_decode_nft.tf` (add `common.py` to the zip)
- Test: existing `tests/test_decode_nft_handler.py` (unchanged — must stay green)

**Interfaces:**
- Produces: `decode.common.parse_event(event: dict) -> tuple[str, str]` — same behavior as today: defaults to UTC today−2, raises `ValueError` if `start_date > end_date`. `decode.handler.parse_event` remains importable (re-export).

- [ ] **Step 1: Create `decode/common.py`**

```python
"""Helpers shared by the decode Lambdas (nft transfers, seaport sales)."""

from datetime import datetime, timedelta, timezone


def parse_event(event: dict) -> tuple[str, str]:
    event = event or {}
    default = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
    start = event.get("start_date") or default
    end = event.get("end_date") or start
    if start > end:
        raise ValueError(f"start_date {start} after end_date {end}")
    return start, end
```

- [ ] **Step 2: Update `decode/handler.py`**

Delete its `parse_event` function body and the now-unused `timedelta`/`timezone` imports if nothing else uses them (`datetime` is still used for the filename stamp; `timezone` is still used in `handler` and `postprocess` — keep those). Replace the function with a re-export so existing tests and callers keep working:

```python
from decode.common import parse_event  # noqa: F401  (re-exported for callers/tests)
```

- [ ] **Step 3: Add `common.py` to the nft Lambda zip**

In `terraform/lambda_decode_nft.tf`, inside `data "archive_file" "decode_nft"`, add:

```hcl
  source {
    content  = file("${path.module}/../decode/common.py")
    filename = "decode/common.py"
  }
```

- [ ] **Step 4: Run existing tests and lint**

Run: `pytest tests/test_decode_nft_handler.py tests/test_decode_nft_query.py -v && ruff check decode/`
Expected: all PASS, no lint errors.

- [ ] **Step 5: Validate terraform**

Run: `cd terraform && terraform validate`
Expected: `Success!`

- [ ] **Step 6: Commit**

```bash
git add decode/common.py decode/handler.py terraform/lambda_decode_nft.tf
git commit -m "refactor: extract parse_event to decode/common for reuse by seaport Lambda"
```

---

### Task 3: `decode/seaport_parser.py` — pure OrderFulfilled decoder

**Files:**
- Create: `decode/seaport_parser.py`
- Test: `tests/test_decode_seaport_parser.py`

**Interfaces:**
- Produces: `decode.seaport_parser.parse_order_fulfilled(topics: list[str], data: str) -> dict` with keys: `order_hash: str`, `offerer: str`, `recipient: str`, `order_side: str` (`"listing"`/`"bid"`/`"unknown"`), `buyer: str | None`, `seller: str | None`, `nft_contract_addresses: list[str]`, `nft_token_ids: list[str]` (decimal-as-string), `nft_quantities: list[int]`, `nft_froms: list[str | None]`, `nft_tos: list[str | None]`, `payment_currencies: list[str]`, `payment_amounts: list[int]`, `payment_recipients: list[str | None]`. All addresses lowercase, `0x`-prefixed. Also exports `ORDER_FULFILLED_TOPIC` and `ZERO_ADDRESS` constants (Task 4/5 import them).
- Consumes: fixture from Task 1.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_decode_seaport_parser.py`:

```python
import json
from pathlib import Path

import pytest
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode

from decode.seaport_parser import (
    ORDER_FULFILLED_TOPIC,
    ZERO_ADDRESS,
    parse_order_fulfilled,
)

FIXTURE = Path(__file__).parent / "fixtures" / "seaport_order_fulfilled_2026-08-17.json"

# abi types of the non-indexed OrderFulfilled params:
# (orderHash, recipient, SpentItem[] offer, ReceivedItem[] consideration)
ABI_TYPES = [
    "bytes32",
    "address",
    "(uint8,address,uint256,uint256)[]",
    "(uint8,address,uint256,uint256,address)[]",
]

WETH = "0x" + "ee" * 20
LAND = "0x" + "ab" * 20
ALICE = "0x" + "aa" * 20
BOB = "0x" + "bb" * 20
FEE_WALLET = "0x" + "fe" * 20


def _load_fixture():
    return json.loads(FIXTURE.read_text())


def _encode_log(order_hash, recipient, offer, consideration, offerer):
    """Build (topics, data) exactly as Seaport emits them."""
    data = "0x" + abi_encode(
        ABI_TYPES, [order_hash, recipient, offer, consideration]
    ).hex()
    topics = [
        ORDER_FULFILLED_TOPIC,
        "0x" + "00" * 12 + offerer[2:],  # indexed offerer, left-padded
        "0x" + "00" * 32,  # indexed zone (unused by the parser)
    ]
    return topics, data


def test_real_logs_match_eth_abi():
    for record in _load_fixture():
        parsed = parse_order_fulfilled(record["topics"], record["data"])
        order_hash, recipient, offer, consideration = abi_decode(
            ABI_TYPES, bytes.fromhex(record["data"][2:])
        )
        assert parsed["order_hash"] == "0x" + order_hash.hex()
        assert parsed["recipient"] == recipient.lower()
        n_items = len(offer) + len(consideration)
        n_parsed = len(parsed["nft_contract_addresses"]) + len(
            parsed["payment_amounts"]
        )
        assert n_parsed == n_items  # every item routed exactly once
        # parallel arrays stay parallel
        assert (
            len(parsed["nft_contract_addresses"])
            == len(parsed["nft_token_ids"])
            == len(parsed["nft_quantities"])
            == len(parsed["nft_froms"])
            == len(parsed["nft_tos"])
        )
        assert (
            len(parsed["payment_currencies"])
            == len(parsed["payment_amounts"])
            == len(parsed["payment_recipients"])
        )


def test_real_logs_are_listings_with_expected_item_counts():
    by_index = {r["log_index"]: r for r in _load_fixture()}
    three = parse_order_fulfilled(by_index[497]["topics"], by_index[497]["data"])
    one = parse_order_fulfilled(by_index[498]["topics"], by_index[498]["data"])
    assert three["order_side"] == "listing"
    assert len(three["nft_contract_addresses"]) == 3
    assert one["order_side"] == "listing"
    assert len(one["nft_contract_addresses"]) == 1
    # listing: buyer is the event recipient, seller is the offerer
    for parsed in (three, one):
        assert parsed["buyer"] == parsed["recipient"]
        assert parsed["seller"] == parsed["offerer"]
        assert all(f == parsed["offerer"] for f in parsed["nft_froms"])
        assert all(t == parsed["recipient"] for t in parsed["nft_tos"])
        assert parsed["payment_amounts"] and all(
            a > 0 for a in parsed["payment_amounts"]
        )


def test_synthetic_bid_roundtrip():
    # Bob (offerer) bids 1 WETH for Alice's LAND; Alice fulfills.
    topics, data = _encode_log(
        order_hash=b"\x01" * 32,
        recipient=ALICE,  # fulfiller
        offer=[(1, WETH, 0, 10**18)],  # ERC-20 -> payment side
        consideration=[
            (2, LAND, (10 << 128) | 20, 1, BOB),  # ERC-721 to Bob
            (1, WETH, 5 * 10**16, 5 * 10**16, FEE_WALLET),
        ],
        offerer=BOB,
    )
    parsed = parse_order_fulfilled(topics, data)
    assert parsed["order_side"] == "bid"
    assert parsed["buyer"] == BOB and parsed["seller"] == ALICE
    assert parsed["nft_contract_addresses"] == [LAND]
    assert parsed["nft_token_ids"] == [str((10 << 128) | 20)]  # decimal string
    assert parsed["nft_quantities"] == [1]
    assert parsed["nft_froms"] == [ALICE]  # event recipient fulfilled
    assert parsed["nft_tos"] == [BOB]  # item-level recipient
    assert parsed["payment_currencies"] == [WETH, WETH]
    assert parsed["payment_amounts"] == [10**18, 5 * 10**16]
    # offer-side payment goes to the fulfiller; consideration fee to its recipient
    assert parsed["payment_recipients"] == [ALICE, FEE_WALLET]


def test_zero_recipient_yields_nulls_not_guesses():
    topics, data = _encode_log(
        order_hash=b"\x02" * 32,
        recipient=ZERO_ADDRESS,  # matchOrders-style fulfillment
        offer=[(2, LAND, 7, 1)],
        consideration=[(0, ZERO_ADDRESS, 0, 10**18, ALICE)],
        offerer=ALICE,
    )
    parsed = parse_order_fulfilled(topics, data)
    assert parsed["order_side"] == "listing"
    assert parsed["seller"] == ALICE
    assert parsed["buyer"] is None
    assert parsed["nft_tos"] == [None]
    assert parsed["nft_froms"] == [ALICE]


def test_native_eth_payment_uses_zero_address_currency():
    topics, data = _encode_log(
        order_hash=b"\x03" * 32,
        recipient=BOB,
        offer=[(2, LAND, 7, 1)],
        consideration=[(0, ZERO_ADDRESS, 0, 10**18, ALICE)],
        offerer=ALICE,
    )
    parsed = parse_order_fulfilled(topics, data)
    assert parsed["payment_currencies"] == [ZERO_ADDRESS]


def test_nfts_on_both_sides_is_unknown():
    topics, data = _encode_log(
        order_hash=b"\x04" * 32,
        recipient=BOB,
        offer=[(2, LAND, 1, 1)],
        consideration=[(2, LAND, 2, 1, ALICE)],
        offerer=ALICE,
    )
    parsed = parse_order_fulfilled(topics, data)
    assert parsed["order_side"] == "unknown"
    assert parsed["buyer"] is None and parsed["seller"] is None


def test_wrong_topic_raises():
    with pytest.raises(ValueError, match="topic"):
        parse_order_fulfilled(["0x" + "00" * 32, "0x" + "00" * 32], "0x")


def test_truncated_data_raises():
    record = _load_fixture()[0]
    with pytest.raises(ValueError, match="truncated"):
        parse_order_fulfilled(record["topics"], record["data"][:200])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_decode_seaport_parser.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'decode.seaport_parser'`

- [ ] **Step 3: Implement the parser**

Create `decode/seaport_parser.py`:

```python
"""Pure decoder for Seaport OrderFulfilled logs.

No eth_abi at runtime: the event data has a regular ABI layout that a
word-by-word reader covers — head of 4 static words (orderHash,
recipient, offset of offer[], offset of consideration[]) followed by two
arrays of static structs (SpentItem = 4 words, ReceivedItem = 5). The
manual parse is validated against eth_abi in tests.

The parser mirrors the event honestly: no per-payment payer exists in
the log, so none is produced; buyer/seller/order_side are the only
derived fields, and they go NULL rather than guessed when the event's
recipient is the zero address.
"""

ORDER_FULFILLED_TOPIC = (
    "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"
)
ZERO_ADDRESS = "0x" + "0" * 40

_ITEM_NFT = {2, 3}  # ERC-721, ERC-1155
_ITEM_PAYMENT = {0, 1}  # native ETH, ERC-20


def _word(data_hex: str, index: int) -> str:
    word = data_hex[index * 64 : (index + 1) * 64]
    if len(word) < 64:
        raise ValueError(f"data truncated at word {index}")
    return word


def _address(word: str) -> str:
    return ("0x" + word[-40:]).lower()


def _read_items(data_hex: str, offset_word: int, words_per_item: int) -> list[dict]:
    count = int(_word(data_hex, offset_word), 16)
    items = []
    for i in range(count):
        base = offset_word + 1 + i * words_per_item
        item = {
            "item_type": int(_word(data_hex, base), 16),
            "token": _address(_word(data_hex, base + 1)),
            "identifier": int(_word(data_hex, base + 2), 16),
            "amount": int(_word(data_hex, base + 3), 16),
        }
        if words_per_item == 5:
            item["recipient"] = _address(_word(data_hex, base + 4))
        items.append(item)
    return items


def parse_order_fulfilled(topics: list[str], data: str) -> dict:
    if not topics or topics[0] != ORDER_FULFILLED_TOPIC:
        raise ValueError(f"not an OrderFulfilled topic: {topics[:1]}")
    offerer = _address(topics[1])

    data_hex = data[2:] if data.startswith("0x") else data
    order_hash = "0x" + _word(data_hex, 0)
    recipient = _address(_word(data_hex, 1))
    # offsets are byte positions from the start of data
    offer_at = int(_word(data_hex, 2), 16) // 32
    consideration_at = int(_word(data_hex, 3), 16) // 32
    offer = _read_items(data_hex, offer_at, 4)
    consideration = _read_items(data_hex, consideration_at, 5)

    recipient_or_none = None if recipient == ZERO_ADDRESS else recipient

    nft_contracts: list[str] = []
    nft_token_ids: list[str] = []
    nft_quantities: list[int] = []
    nft_froms: list[str | None] = []
    nft_tos: list[str | None] = []
    pay_currencies: list[str] = []
    pay_amounts: list[int] = []
    pay_recipients: list[str | None] = []
    nft_in_offer = nft_in_consideration = False

    for side, items in (("offer", offer), ("consideration", consideration)):
        for item in items:
            if item["item_type"] in _ITEM_NFT:
                if side == "offer":
                    nft_in_offer = True
                    nft_from, nft_to = offerer, recipient_or_none
                else:
                    nft_in_consideration = True
                    nft_from, nft_to = recipient_or_none, item["recipient"]
                nft_contracts.append(item["token"])
                nft_token_ids.append(str(item["identifier"]))
                nft_quantities.append(item["amount"])
                nft_froms.append(nft_from)
                nft_tos.append(nft_to)
            elif item["item_type"] in _ITEM_PAYMENT:
                pay_currencies.append(item["token"])
                pay_amounts.append(item["amount"])
                # offer-side payments (accepted bids) flow to the fulfiller
                pay_recipients.append(
                    item.get("recipient") if side == "consideration" else recipient_or_none
                )
            else:
                raise ValueError(
                    f"unexpected itemType {item['item_type']} in fulfilled order"
                )

    if nft_in_offer and not nft_in_consideration:
        order_side, buyer, seller = "listing", recipient_or_none, offerer
    elif nft_in_consideration and not nft_in_offer:
        order_side, buyer, seller = "bid", offerer, recipient_or_none
    else:
        order_side, buyer, seller = "unknown", None, None

    return {
        "order_hash": order_hash,
        "offerer": offerer,
        "recipient": recipient,
        "order_side": order_side,
        "buyer": buyer,
        "seller": seller,
        "nft_contract_addresses": nft_contracts,
        "nft_token_ids": nft_token_ids,
        "nft_quantities": nft_quantities,
        "nft_froms": nft_froms,
        "nft_tos": nft_tos,
        "payment_currencies": pay_currencies,
        "payment_amounts": pay_amounts,
        "payment_recipients": pay_recipients,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_decode_seaport_parser.py -v && ruff check decode/ tests/`
Expected: all PASS, no lint errors. If `test_real_logs_match_eth_abi` fails, the manual parser disagrees with eth_abi on real data — fix the parser, never the assertion.

- [ ] **Step 5: Commit**

```bash
git add decode/seaport_parser.py tests/test_decode_seaport_parser.py
git commit -m "feat: pure-Python Seaport OrderFulfilled parser validated against eth_abi"
```

---

### Task 4: `decode/seaport_query.py` — extraction query builder

**Files:**
- Create: `decode/seaport_query.py`
- Test: `tests/test_decode_seaport_query.py`

**Interfaces:**
- Consumes: `ORDER_FULFILLED_TOPIC` from `decode.seaport_parser`.
- Produces: `decode.seaport_query.build_query(start_date: str, end_date: str) -> str`; raises `ValueError` on non-`YYYY-MM-DD` input. Task 5's handler calls it.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_decode_seaport_query.py`:

```python
import pytest

from decode.seaport_parser import ORDER_FULFILLED_TOPIC
from decode.seaport_query import build_query


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_decode_seaport_query.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'decode.seaport_query'`

- [ ] **Step 3: Implement the query builder**

Create `decode/seaport_query.py`:

```python
"""Builds the Athena query that fetches raw Seaport OrderFulfilled logs.

Filter-and-fetch only: all ABI decoding happens in Python
(seaport_parser), because the data blob nests two dynamic arrays of
structs — unreadable as SQL substr arithmetic. No address filter:
topic0 already identifies the event.
"""

import re

from decode.seaport_parser import ORDER_FULFILLED_TOPIC

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
    AND topics[1] = '{ORDER_FULFILLED_TOPIC}'"""
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_decode_seaport_query.py -v && ruff check decode/ tests/`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add decode/seaport_query.py tests/test_decode_seaport_query.py
git commit -m "feat: seaport extraction query builder (filter-and-fetch, no SQL decoding)"
```

---

### Task 5: `decode/seaport_handler.py` — Lambda handler

**Files:**
- Create: `decode/seaport_handler.py`
- Test: `tests/test_decode_seaport_handler.py`

**Interfaces:**
- Consumes: `decode.common.parse_event`, `decode.seaport_query.build_query`, `decode.seaport_parser.parse_order_fulfilled`.
- Produces: `decode.seaport_handler.handler(event, context) -> dict` (`{"start_date", "end_date", "rows_by_dt"}`) and `decode.seaport_handler.postprocess(df: pd.DataFrame) -> pd.DataFrame` returning exactly the staging columns. Terraform (Task 6) points at `decode.seaport_handler.handler`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_decode_seaport_handler.py`:

```python
import json
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from decode.seaport_handler import _FINAL_COLUMNS, postprocess

FIXTURE = Path(__file__).parent / "fixtures" / "seaport_order_fulfilled_2026-08-17.json"


def _bronze_df():
    records = json.loads(FIXTURE.read_text())
    return pd.DataFrame(
        {
            "transaction_hash": ["0xd8c5" for _ in records],
            "log_index": [r["log_index"] for r in records],
            "block_timestamp": [pd.Timestamp("2026-08-17 12:00:00")] * len(records),
            "address": [r["address"].upper() for r in records],  # exercises lowercase
            "topics": [r["topics"] for r in records],
            "data": [r["data"] for r in records],
            "extracted_at": [pd.Timestamp("2026-08-19 06:10:00")] * len(records),
            "dt": ["2026-08-17"] * len(records),
        }
    )


def test_postprocess_shapes_and_types():
    out = postprocess(_bronze_df())
    assert list(out.columns) == _FINAL_COLUMNS
    assert len(out) == 2
    by_index = out.set_index("log_index")
    assert len(by_index.loc[497, "nft_contract_addresses"]) == 3
    assert len(by_index.loc[498, "nft_contract_addresses"]) == 1
    assert by_index.loc[497, "order_side"] == "listing"
    # decimals for parquet decimal128(38,0)
    assert all(
        isinstance(a, Decimal) for a in by_index.loc[497, "payment_amounts"]
    )
    assert all(isinstance(q, Decimal) for q in by_index.loc[497, "nft_quantities"])
    # seaport_address lowercased even if bronze ever carried mixed case
    assert by_index.loc[497, "seaport_address"] == (
        "0x0000000000000068f116a894984e2db1123eb395"
    )
    assert (out["bronze_extracted_at"] == pd.Timestamp("2026-08-19 06:10:00")).all()
    assert out["decoded_at"].notna().all()


def test_postprocess_rejects_amount_over_decimal38():
    df = _bronze_df()
    # patch one payment amount word beyond 10**38 inside the raw data:
    # simpler to assert via the guard directly on a parsed-like row
    from decode.seaport_handler import _to_decimal_list

    with pytest.raises(ValueError, match="decimal"):
        _to_decimal_list([10**38], "payment_amounts", "0xd8c5", 497)
    assert _to_decimal_list([1, 2], "payment_amounts", "0xd8c5", 497) == [
        Decimal(1),
        Decimal(2),
    ]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_decode_seaport_handler.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'decode.seaport_handler'`

- [ ] **Step 3: Implement the handler**

Create `decode/seaport_handler.py`:

```python
"""decode-ethereum-seaport-sales Lambda: bronze -> staging.

The Athena query only filters (topic0) and fetches raw logs; all ABI
decoding happens in seaport_parser. wr.s3.to_parquet overwrites only
the touched dt partitions and registers them in Glue.

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
from decode.seaport_parser import parse_order_fulfilled
from decode.seaport_query import build_query

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "decentraland-data-platform")
GLUE_DATABASE = "staging"
GLUE_TABLE = "ethereum_seaport_sales"

# Athena decimal(38,0) ceiling for amounts/quantities
_MAX_DECIMAL38 = 10**38

_FINAL_COLUMNS = [
    "transaction_hash",
    "log_index",
    "block_timestamp",
    "seaport_address",
    "order_hash",
    "offerer",
    "recipient",
    "order_side",
    "buyer",
    "seller",
    "nft_contract_addresses",
    "nft_token_ids",
    "nft_quantities",
    "nft_froms",
    "nft_tos",
    "payment_currencies",
    "payment_amounts",
    "payment_recipients",
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
        [
            parse_order_fulfilled(list(row.topics), row.data)
            for row in df.itertuples()
        ],
        index=df.index,
    )
    out = pd.concat([df.drop(columns=["topics", "data"]), parsed], axis=1)
    out["seaport_address"] = out["address"].str.lower()
    out["nft_quantities"] = out.apply(
        lambda r: _to_decimal_list(
            r["nft_quantities"], "nft_quantities", r["transaction_hash"], r["log_index"]
        ),
        axis=1,
    )
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
    df = wr.athena.read_sql_query(
        sql=sql,
        database="bronze",
        workgroup=ATHENA_WORKGROUP,
        ctas_approach=False,
        unload_approach=True,
        # unique per run: UNLOAD refuses an existing target directory
        s3_output=f"s3://{bucket}/athena-results/unload/seaport_sales/{uuid.uuid4()}/",
        keep_files=False,
    )
    if df.empty:
        return {"start_date": start, "end_date": end, "rows_by_dt": {}}

    df = postprocess(df)
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/staging/ethereum_seaport_sales/",
        dataset=True,
        partition_cols=["dt"],
        mode="overwrite_partitions",
        database=GLUE_DATABASE,
        table=GLUE_TABLE,
        filename_prefix=f"{stamp}_",
        compression="snappy",
        dtype={
            "nft_quantities": "array<decimal(38,0)>",
            "payment_amounts": "array<decimal(38,0)>",
        },
    )
    return {
        "start_date": start,
        "end_date": end,
        "rows_by_dt": df.groupby("dt").size().to_dict(),
    }
```

- [ ] **Step 4: Run the full test suite and lint**

Run: `pytest tests/ -v && ruff check decode/ tests/`
Expected: all PASS (including the pre-existing suites), no lint errors.

- [ ] **Step 5: Commit**

```bash
git add decode/seaport_handler.py tests/test_decode_seaport_handler.py
git commit -m "feat: decode-ethereum-seaport-sales handler (Athena UNLOAD -> parser -> staging)"
```

---

### Task 6: Terraform — Glue table + Lambda + IAM

**Files:**
- Create: `terraform/table_seaport_sales.tf`
- Create: `terraform/lambda_decode_seaport.tf`

**Interfaces:**
- Consumes: existing `aws_glue_catalog_database.staging`, `aws_glue_catalog_database.bronze`, `aws_glue_catalog_database.silver`, `aws_athena_workgroup.main`, `aws_s3_bucket.lake`, `local.bucket_name`, `local.awssdkpandas_layer_arn` (defined in `lambda_decode_nft.tf` locals — same module scope), handler `decode.seaport_handler.handler` from Task 5.
- Produces: Lambda `decode-ethereum-seaport-sales`, Glue table `staging.ethereum_seaport_sales`.

- [ ] **Step 1: Create `terraform/table_seaport_sales.tf`**

```hcl
# Staging table for decoded Seaport OrderFulfilled orders. No partition
# projection: the Lambda registers each dt explicitly, which keeps the
# "$partitions" metadata convention working.
resource "aws_glue_catalog_table" "seaport_sales" {
  database_name = aws_glue_catalog_database.staging.name
  name          = "ethereum_seaport_sales"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/staging/ethereum_seaport_sales/"
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
      name = "seaport_address"
      type = "string"
    }
    columns {
      name = "order_hash"
      type = "string"
    }
    columns {
      name = "offerer"
      type = "string"
    }
    columns {
      name = "recipient"
      type = "string"
    }
    columns {
      name = "order_side"
      type = "string"
    }
    columns {
      name = "buyer"
      type = "string"
    }
    columns {
      name = "seller"
      type = "string"
    }
    columns {
      name = "nft_contract_addresses"
      type = "array<string>"
    }
    columns {
      name = "nft_token_ids"
      type = "array<string>"
    }
    columns {
      name = "nft_quantities"
      type = "array<decimal(38,0)>"
    }
    columns {
      name = "nft_froms"
      type = "array<string>"
    }
    columns {
      name = "nft_tos"
      type = "array<string>"
    }
    columns {
      name = "payment_currencies"
      type = "array<string>"
    }
    columns {
      name = "payment_amounts"
      type = "array<decimal(38,0)>"
    }
    columns {
      name = "payment_recipients"
      type = "array<string>"
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

- [ ] **Step 2: Create `terraform/lambda_decode_seaport.tf`**

```hcl
locals {
  decode_seaport_tags = { component = "decode", layer = "staging" }
}

# source blocks (not source_dir) so the zip keeps the decode/ package
# directory and the handler resolves as decode.seaport_handler.handler.
# Only this Lambda's modules ship: query.py/handler.py stay out.
data "archive_file" "decode_seaport" {
  type        = "zip"
  output_path = "${path.module}/build/decode_seaport.zip"

  source {
    content  = file("${path.module}/../decode/__init__.py")
    filename = "decode/__init__.py"
  }
  source {
    content  = file("${path.module}/../decode/common.py")
    filename = "decode/common.py"
  }
  source {
    content  = file("${path.module}/../decode/seaport_parser.py")
    filename = "decode/seaport_parser.py"
  }
  source {
    content  = file("${path.module}/../decode/seaport_query.py")
    filename = "decode/seaport_query.py"
  }
  source {
    content  = file("${path.module}/../decode/seaport_handler.py")
    filename = "decode/seaport_handler.py"
  }
}

resource "aws_iam_role" "decode_seaport" {
  name = "decode-ethereum-seaport-sales-role"
  tags = local.decode_seaport_tags
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "decode_seaport" {
  name = "athena-glue-s3"
  role = aws_iam_role.decode_seaport.id
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
        # overwrite_partitions deletes and re-registers dt partitions
        Effect = "Allow"
        Action = [
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition",
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
        # wrangler UNLOAD scratch + staging output (overwrite needs delete)
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

resource "aws_iam_role_policy_attachment" "decode_seaport_logs" {
  role       = aws_iam_role.decode_seaport.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "decode_seaport" {
  name              = "/aws/lambda/decode-ethereum-seaport-sales"
  retention_in_days = 7
  tags              = local.decode_seaport_tags
}

resource "aws_lambda_function" "decode_seaport" {
  function_name    = "decode-ethereum-seaport-sales"
  role             = aws_iam_role.decode_seaport.arn
  filename         = data.archive_file.decode_seaport.output_path
  source_code_hash = data.archive_file.decode_seaport.output_base64sha256
  handler          = "decode.seaport_handler.handler"
  runtime          = "python3.13"
  architectures    = ["arm64"]
  timeout          = 300
  memory_size      = 2048
  layers           = [local.awssdkpandas_layer_arn]
  tags             = local.decode_seaport_tags

  environment {
    variables = {
      LAKE_BUCKET      = aws_s3_bucket.lake.id
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }
}
```

Note: unlike the nft Lambda, this role gets no `silver` Glue/S3 access — the seaport query never touches `dim_contracts`.

- [ ] **Step 3: Validate and plan**

Run: `cd terraform && terraform validate && terraform plan`
Expected: `Success!` then a plan that ONLY adds: 1 Glue table, 1 IAM role, 1 role policy, 1 policy attachment, 1 log group, 1 Lambda — and updates `aws_lambda_function.decode_nft` (new zip hash from `common.py` added in Task 2). READ the whole plan; if anything is destroyed or replaced, STOP and show the user.

- [ ] **Step 4: Apply (checkpoint — show the plan summary to the user first if anything looks off)**

Run: `cd terraform && terraform apply` (approve after re-reading the plan)
Expected: `Apply complete!` with resources added, 1 changed, 0 destroyed.

- [ ] **Step 5: Commit**

```bash
git add terraform/table_seaport_sales.tf terraform/lambda_decode_seaport.tf
git commit -m "infra: decode-ethereum-seaport-sales zip Lambda and staging Glue table"
```

---

### Task 7: Deploy verification against real data

**Files:** none (verification only)

**Interfaces:**
- Consumes: deployed Lambda `decode-ethereum-seaport-sales`, verified fixture tx from Global Constraints.

- [ ] **Step 1: Invoke the Lambda for the fixture day**

Run:

```bash
aws lambda invoke --function-name decode-ethereum-seaport-sales \
  --payload '{"start_date":"2026-08-17","end_date":"2026-08-17"}' \
  --cli-binary-format raw-in-base64-out /dev/stdout
```

Expected: JSON response with `"rows_by_dt": {"2026-08-17": <N>}` and N > 0, no `errorMessage`. If it errors, read the CloudWatch log group `/aws/lambda/decode-ethereum-seaport-sales` before changing anything.

- [ ] **Step 2: Verify the fixture tx in Athena**

Run via Athena (workgroup `decentraland-data-platform`), e.g. with `aws athena start-query-execution` + `get-query-results`:

```sql
SELECT log_index,
    order_side,
    buyer,
    seller,
    CARDINALITY(nft_contract_addresses) AS n_nfts,
    CARDINALITY(payment_amounts) AS n_payments
FROM "staging"."ethereum_seaport_sales"
WHERE dt = '2026-08-17'
    AND transaction_hash = '0xd8c5ed0ca8394ec6dace758237697e50ec2b39eb16c53c0cb9e7743166899fcb'
ORDER BY log_index
```

Expected: exactly 2 rows — log_index 497 with `n_nfts = 3` and log_index 498 with `n_nfts = 1`; both `order_side = 'listing'`, `buyer`/`seller` non-null, `n_payments >= 1`.

- [ ] **Step 3: Verify partition registration via metadata (zero bytes scanned)**

```sql
SELECT MAX(dt) FROM "staging"."ethereum_seaport_sales$partitions"
```

Expected: `2026-08-17`.

- [ ] **Step 4: Verify idempotency**

Re-run the Step 1 invoke, then re-run the Step 2 query.
Expected: same `rows_by_dt` count, still exactly 2 rows for the tx (overwrite, no duplicates).

- [ ] **Step 5: Report results to the user**

Summarize: rows decoded for 2026-08-17, the fixture-tx check, partition check, idempotency check. Do not push; ask the user whether to backfill more days (per-day invokes in batches of 10).
