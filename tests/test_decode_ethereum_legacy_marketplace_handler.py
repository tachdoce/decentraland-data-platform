from decimal import Decimal

import pandas as pd
import pytest

from decode.ethereum_legacy_marketplace_handler import _FINAL_COLUMNS, postprocess

# expires hex is real (AuctionCreated fixture, tx 0x25f0...); price is synthetic
_CREATED_PRICE_HEX = format(20000 * 10**18, "064x")
# 1524182400000 ms = 2018-04-20 00:00 UTC
_EXPIRES_MS_HEX = format(0x162E059BC00, "064x")
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
