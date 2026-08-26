from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd
import pytest

from decode.handler import parse_event, postprocess

# LAND-style id: x=10 in the high 128 bits, y=20 low -> needs bignum
LAND_HEX = format((10 << 128) | 20, "064x")


def _df(**overrides):
    base = {
        "transaction_hash": ["0xaa"],
        "log_index": [1],
        "block_timestamp": [pd.Timestamp("2018-06-01 10:00:00")],
        "contract_address": ["0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d"],
        "erc_type": [721],
        "token_id_hex": [LAND_HEX],
        "quantity_hex": ["1".zfill(64)],
        "from_address": ["0x" + "1" * 40],
        "to_address": ["0x" + "2" * 40],
        "bronze_extracted_at": [pd.Timestamp("2026-08-25 18:00:00")],
        "dt": ["2018-06-01"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def test_postprocess_token_id_exact_decimal_string():
    out = postprocess(_df())
    assert out.loc[0, "token_id"] == str((10 << 128) | 20)  # 39-digit exact
    assert out.loc[0, "quantity"] == Decimal(1)
    assert "token_id_hex" not in out.columns and "quantity_hex" not in out.columns
    assert "decoded_at" in out.columns


def test_postprocess_max_uint256_is_78_digits():
    out = postprocess(_df(token_id_hex=["f" * 64]))
    assert len(out.loc[0, "token_id"]) == 78


def test_postprocess_rejects_quantity_over_decimal38():
    with pytest.raises(ValueError, match="quantity"):
        postprocess(_df(quantity_hex=[format(10**38, "064x")]))


def test_parse_event_default_is_utc_today_minus_2():
    start, end = parse_event({})
    expected = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
    assert start == end == expected


def test_parse_event_range():
    assert parse_event({"start_date": "2018-06-01", "end_date": "2018-06-05"}) == (
        "2018-06-01",
        "2018-06-05",
    )
    with pytest.raises(ValueError):
        parse_event({"start_date": "2018-06-05", "end_date": "2018-06-01"})
