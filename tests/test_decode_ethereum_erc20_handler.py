from decimal import Decimal

import pandas as pd

from decode.ethereum_erc20_handler import _FINAL_COLUMNS, postprocess


def _df(**overrides):
    base = {
        "transaction_hash": ["0xb849"],
        "log_index": [302],
        "block_timestamp": [pd.Timestamp("2021-08-15 10:00:00")],
        "token_address": ["0x0f5d2fb29fb7d3cfee444a200298f468908cc942"],
        "from_address": ["0x" + "1" * 40],
        "to_address": ["0x" + "2" * 40],
        "amount_hex": [format(25 * 10**18, "064x")],  # 25 MANA in wei
        "bronze_extracted_at": [pd.Timestamp("2026-08-25 18:00:00")],
        "dt": ["2021-08-15"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def test_postprocess_amount_decimal_and_columns():
    out = postprocess(_df())
    assert list(out.columns) == _FINAL_COLUMNS
    assert out.loc[0, "amount_raw"] == Decimal(25 * 10**18)
    assert isinstance(out.loc[0, "amount_raw"], Decimal)
    assert "amount_hex" not in out.columns
    assert pd.notna(out.loc[0, "decoded_at"])


def test_handler_returns_empty_on_zero_row_unload(monkeypatch):
    # a zero-row UNLOAD makes awswrangler raise EmptyDataFrame instead of
    # returning an empty frame; the handler must treat it as "no data"
    import awswrangler as wr

    from decode import ethereum_erc20_handler

    def _raise(**kwargs):
        raise wr.exceptions.EmptyDataFrame("Query would return untyped, empty dataframe.")

    monkeypatch.setenv("LAKE_BUCKET", "test-bucket")
    monkeypatch.setattr(wr.athena, "read_sql_query", _raise)
    result = ethereum_erc20_handler.handler(
        {"start_date": "2017-09-06", "end_date": "2017-09-10"}, None
    )
    assert result == {
        "start_date": "2017-09-06",
        "end_date": "2017-09-10",
        "rows_by_dt": {},
    }
