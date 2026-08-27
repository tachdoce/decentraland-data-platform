import json
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from decode.seaport_handler import _FINAL_COLUMNS, _to_decimal_list, postprocess

FIXTURES = Path(__file__).parent / "fixtures"


def _bronze_df():
    """Both fixtures as one bronze extract: 2 swap legs + 1 plain listing."""
    swap = json.loads((FIXTURES / "seaport_order_fulfilled_2026-08-17.json").read_text())
    listing = json.loads((FIXTURES / "seaport_listing_2026-08-17.json").read_text())
    records = [
        {"transaction_hash": "0xd8c5", **r} for r in swap
    ] + [listing]
    return pd.DataFrame(
        {
            "transaction_hash": [r["transaction_hash"] for r in records],
            "log_index": [r["log_index"] for r in records],
            "block_timestamp": [pd.Timestamp("2026-08-17 12:00:00")] * len(records),
            "address": [r["address"].upper() for r in records],  # exercises lowercase
            "topics": [r["topics"] for r in records],
            "data": [r["data"] for r in records],
            "extracted_at": [pd.Timestamp("2026-08-19 06:10:00")] * len(records),
            "dt": ["2026-08-17"] * len(records),
        }
    )


def test_postprocess_keeps_only_sales():
    out = postprocess(_bronze_df())
    # both swap legs dropped (unknown side / zero payments); listing kept
    assert len(out) == 1
    row = out.iloc[0]
    assert row["transaction_hash"].startswith("0x905ace")
    assert row["order_side"] == "listing"
    assert row["buyer"] and row["seller"]


def test_postprocess_shapes_and_types():
    out = postprocess(_bronze_df())
    assert list(out.columns) == _FINAL_COLUMNS
    row = out.iloc[0]
    assert len(row["nft_contract_addresses"]) == 1
    assert row["nft_token_ids"] == ["5046"]
    # decimals for parquet decimal128(38,0)
    assert all(isinstance(a, Decimal) for a in row["payment_amounts"])
    assert all(isinstance(q, Decimal) for q in row["nft_quantities"])
    assert row["payment_amounts"] == [Decimal(172260000000000000)]
    # seaport_address lowercased even if bronze ever carried mixed case
    assert row["seaport_address"] == "0x0000000000000068f116a894984e2db1123eb395"
    assert row["bronze_extracted_at"] == pd.Timestamp("2026-08-19 06:10:00")
    assert pd.notna(row["decoded_at"])


def test_to_decimal_list_guards_decimal38():
    with pytest.raises(ValueError, match="decimal"):
        _to_decimal_list([10**38], "payment_amounts", "0xd8c5", 497)
    assert _to_decimal_list([1, 2], "payment_amounts", "0xd8c5", 497) == [
        Decimal(1),
        Decimal(2),
    ]
