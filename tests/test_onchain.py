import datetime
import io

import pyarrow.parquet as pq
import pytest

from ingestion.onchain.extract import (
    CHAINS,
    build_query,
    object_key,
    parse_event,
    rows_to_parquet,
)

TODAY = datetime.date(2026, 8, 23)


class TestParseEvent:
    def test_explicit_date_and_chain(self):
        chain_id, run_date = parse_event({"chain_id": 137, "date": "2026-08-19"})
        assert chain_id == 137
        assert run_date == datetime.date(2026, 8, 19)

    def test_date_defaults_to_two_days_before_today(self):
        _, run_date = parse_event({"chain_id": 1}, today=TODAY)
        assert run_date == datetime.date(2026, 8, 21)

    def test_missing_chain_id_raises(self):
        with pytest.raises(ValueError, match="chain_id is required"):
            parse_event({"date": "2026-08-19"})

    def test_unknown_chain_id_raises(self):
        with pytest.raises(ValueError, match="chain_id must be one of"):
            parse_event({"chain_id": 56})

    def test_malformed_date_raises(self):
        with pytest.raises(ValueError):
            parse_event({"chain_id": 1, "date": "19/08/2026"})


class TestBuildQuery:
    def test_ethereum_dataset_and_two_step_shape(self):
        sql = build_query(1)
        assert "goog_blockchain_ethereum_mainnet_us.logs" in sql
        assert "@dt" in sql and "UNNEST(@addresses)" in sql
        assert "ARRAY_LENGTH(topics) >= 1" in sql
        assert "ORDER BY block_timestamp, log_index" in sql

    def test_polygon_dataset(self):
        assert "goog_blockchain_polygon_mainnet_us.logs" in build_query(137)

    def test_never_interpolates_values(self):
        # parameters only: no quoted dates or addresses in the SQL text
        assert "2026" not in build_query(1)


class TestObjectKey:
    def test_key_shape_dashes_in_time(self):
        extracted_at = datetime.datetime(
            2026, 8, 20, 20, 34, 59, tzinfo=datetime.timezone.utc
        )
        key = object_key(1, datetime.date(2026, 8, 19), extracted_at)
        assert key == "bronze/ethereum_logs/dt=2026-08-19/2026-08-20_20-34-59.parquet"

    def test_polygon_table_prefix(self):
        extracted_at = datetime.datetime(
            2026, 8, 20, 1, 2, 3, tzinfo=datetime.timezone.utc
        )
        key = object_key(137, datetime.date(2026, 8, 19), extracted_at)
        assert key.startswith("bronze/polygon_logs/dt=2026-08-19/")


class TestRowsToParquet:
    EXTRACTED_AT = datetime.datetime(
        2026, 8, 20, 20, 34, 59, tzinfo=datetime.timezone.utc
    )

    def _row(self):
        return {
            "transaction_hash": "0xabc",
            "log_index": 7,
            "block_timestamp": datetime.datetime(
                2026, 8, 19, 12, 0, 0, tzinfo=datetime.timezone.utc
            ),
            "address": "0x0f5d2fb29fb7d3cfee444a200298f468908cc942",
            "topics": ["0xddf2", "0x0001"],
            "data": "0x00",
            "extracted_at": self.EXTRACTED_AT,
        }

    def test_roundtrip_preserves_columns(self):
        table = pq.read_table(io.BytesIO(rows_to_parquet([self._row()])))
        assert table.num_rows == 1
        assert table.column("topics").to_pylist() == [["0xddf2", "0x0001"]]
        assert table.column("extracted_at").to_pylist() == [self.EXTRACTED_AT]

    def test_empty_rows_keep_full_schema(self):
        table = pq.read_table(io.BytesIO(rows_to_parquet([])))
        assert table.num_rows == 0
        assert table.schema.names == [
            "transaction_hash",
            "log_index",
            "block_timestamp",
            "address",
            "topics",
            "data",
            "extracted_at",
        ]


def test_chains_config():
    assert set(CHAINS) == {1, 137}
    assert CHAINS[1]["table"] == "ethereum_logs"
    assert CHAINS[137]["table"] == "polygon_logs"
