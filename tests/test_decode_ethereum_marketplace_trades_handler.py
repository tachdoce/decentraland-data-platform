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
