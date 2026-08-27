from decimal import Decimal

import pandas as pd
import pytest

from decode.ethereum_wyvern_handler import _FINAL_COLUMNS, postprocess


def _df(**overrides):
    base = {
        "transaction_hash": ["0xb158"],
        "log_index": [1],
        "block_timestamp": [pd.Timestamp("2021-08-15 10:00:00")],
        "wyvern_address": ["0x7BE8076f4EA4A4AD08075C2508e481d6C946D12b"],
        "buy_hash": ["0x" + "0" * 64],
        "sell_hash": ["0x" + "ab" * 32],
        "maker": ["0x" + "1" * 40],
        "taker": ["0x" + "2" * 40],
        "price_hex": [format(25 * 10**16, "064x")],  # 0.25 ETH in wei
        "metadata": ["0x" + "0" * 64],
        "bronze_extracted_at": [pd.Timestamp("2026-08-25 18:00:00")],
        "dt": ["2021-08-15"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def test_postprocess_price_decimal_and_columns():
    out = postprocess(_df())
    assert list(out.columns) == _FINAL_COLUMNS
    assert out.loc[0, "price"] == Decimal(25 * 10**16)
    assert isinstance(out.loc[0, "price"], Decimal)
    # addresses lowercased at write time
    assert out.loc[0, "wyvern_address"] == (
        "0x7be8076f4ea4a4ad08075c2508e481d6c946d12b"
    )
    assert "price_hex" not in out.columns
    assert pd.notna(out.loc[0, "decoded_at"])


def test_postprocess_rejects_price_over_decimal38():
    with pytest.raises(ValueError, match="price"):
        postprocess(_df(price_hex=[format(10**38, "064x")]))


def test_handler_returns_empty_on_zero_row_unload(monkeypatch):
    # a zero-row UNLOAD makes awswrangler raise EmptyDataFrame instead of
    # returning an empty frame; the handler must treat it as "no data"
    import awswrangler as wr

    from decode import ethereum_wyvern_handler

    def _raise(**kwargs):
        raise wr.exceptions.EmptyDataFrame("Query would return untyped, empty dataframe.")

    monkeypatch.setenv("LAKE_BUCKET", "test-bucket")
    monkeypatch.setattr(wr.athena, "read_sql_query", _raise)
    result = ethereum_wyvern_handler.handler(
        {"start_date": "2017-01-01", "end_date": "2017-01-10"}, None
    )
    assert result == {
        "start_date": "2017-01-01",
        "end_date": "2017-01-10",
        "rows_by_dt": {},
    }
