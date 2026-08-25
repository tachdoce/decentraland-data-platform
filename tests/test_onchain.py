import datetime
import io

import pyarrow.parquet as pq
import pytest

from ingestion.onchain.extract import (
    CHAINS,
    build_query,
    object_key,
    parse_event,
    parse_hours,
    rows_to_parquet,
    window_bounds,
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


class TestParseHours:
    def test_absent_hours_returns_none(self):
        assert parse_hours({"chain_id": 1, "date": "2026-08-19"}) is None

    def test_valid_window(self):
        assert parse_hours({"chain_id": 1, "hour_start": 0, "hour_end": 6}) == (0, 6)

    def test_last_window_of_the_day(self):
        assert parse_hours({"hour_start": 18, "hour_end": 24}) == (18, 24)

    def test_only_one_bound_raises(self):
        with pytest.raises(ValueError, match="hour_start and hour_end"):
            parse_hours({"hour_start": 0})

    def test_inverted_window_raises(self):
        with pytest.raises(ValueError, match="hour_start < hour_end"):
            parse_hours({"hour_start": 12, "hour_end": 6})

    def test_out_of_range_raises(self):
        with pytest.raises(ValueError, match="between 0 and 24"):
            parse_hours({"hour_start": 0, "hour_end": 25})


class TestWindowBounds:
    def test_utc_timestamps_from_hours(self):
        start, end = window_bounds(datetime.date(2019, 11, 16), (0, 6))
        assert start == datetime.datetime(
            2019, 11, 16, 0, 0, 0, tzinfo=datetime.timezone.utc
        )
        assert end == datetime.datetime(
            2019, 11, 16, 6, 0, 0, tzinfo=datetime.timezone.utc
        )

    def test_hour_24_lands_on_next_midnight(self):
        _, end = window_bounds(datetime.date(2019, 11, 16), (18, 24))
        assert end == datetime.datetime(
            2019, 11, 17, 0, 0, 0, tzinfo=datetime.timezone.utc
        )


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

    def test_default_has_no_window_filter(self):
        assert "@ts_start" not in build_query(1)

    def test_windowed_filters_both_query_steps(self):
        # the window must bound the tx CTE (what makes the day cheap) AND
        # the outer log fetch; a tx's logs share one block so no tx splits
        sql = build_query(1, windowed=True)
        assert sql.count("block_timestamp >= @ts_start") == 2
        assert sql.count("block_timestamp < @ts_end") == 2

    def test_windowed_never_interpolates_values(self):
        assert "2026" not in build_query(1, windowed=True)


class TestObjectKey:
    def test_key_shape_dashes_in_time(self):
        extracted_at = datetime.datetime(
            2026, 8, 20, 20, 34, 59, tzinfo=datetime.timezone.utc
        )
        key = object_key(1, datetime.date(2026, 8, 19), extracted_at)
        assert key == "bronze/ethereum_logs/dt=2026-08-19/2026-08-20_20-34-59.parquet"

    def test_windowed_key_carries_hour_suffix(self):
        extracted_at = datetime.datetime(
            2026, 8, 25, 15, 0, 0, tzinfo=datetime.timezone.utc
        )
        key = object_key(1, datetime.date(2019, 11, 16), extracted_at, hours=(0, 6))
        assert key == (
            "bronze/ethereum_logs/dt=2019-11-16/2026-08-25_15-00-00_h00-06.parquet"
        )

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


class FakeAthenaResults:
    """start/poll/paginate contract like the real Athena client."""

    def __init__(self, pages, state="SUCCEEDED"):
        self.pages = pages  # list of lists of address strings (per page)
        self.state = state
        self.queries = []

    def start_query_execution(self, QueryString, WorkGroup):
        self.queries.append({"sql": QueryString, "workgroup": WorkGroup})
        return {"QueryExecutionId": "fake-id"}

    def get_query_execution(self, QueryExecutionId):
        return {
            "QueryExecution": {
                "Status": {"State": self.state, "StateChangeReason": "fake reason"}
            }
        }

    def get_query_results(self, QueryExecutionId, NextToken=None):
        index = 0 if NextToken is None else int(NextToken)
        rows = []
        if index == 0:  # Athena's first page starts with the header row
            rows.append({"Data": [{"VarCharValue": "contract_address"}]})
        # a None address models Athena's NULL: a Data cell with no VarCharValue
        rows += [
            {"Data": [{"VarCharValue": a} if a is not None else {}]}
            for a in self.pages[index]
        ]
        result = {"ResultSet": {"Rows": rows}}
        if index + 1 < len(self.pages):
            result["NextToken"] = str(index + 1)
        return result


class TestFetchContractAddresses:
    def test_reads_addresses_across_pages_skipping_header(self):
        from ingestion.onchain.handler import fetch_contract_addresses

        fake = FakeAthenaResults(pages=[["0xaaa", "0xbbb"], ["0xccc"]])
        addresses = fetch_contract_addresses(fake, 137)
        assert addresses == ["0xaaa", "0xbbb", "0xccc"]
        assert "chain_id = 137" in fake.queries[0]["sql"]
        assert fake.queries[0]["workgroup"] == "decentraland-data-platform"

    def test_raises_on_failed_query(self):
        from ingestion.onchain.handler import fetch_contract_addresses

        fake = FakeAthenaResults(pages=[[]], state="FAILED")
        with pytest.raises(RuntimeError, match="FAILED.*fake reason"):
            fetch_contract_addresses(fake, 1)

    def test_null_address_raises_clear_error(self):
        # Athena renders a NULL cell as Data without VarCharValue; that
        # must surface as a clear error, not a KeyError.
        from ingestion.onchain.handler import fetch_contract_addresses

        fake = FakeAthenaResults(pages=[["0xaaa", None]])
        with pytest.raises(RuntimeError, match="NULL contract_address"):
            fetch_contract_addresses(fake, 1)
