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
