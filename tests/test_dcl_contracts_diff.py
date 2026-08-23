import pytest

from ingestion.dcl_contracts_diff.transform import diff_snapshots, summarize


def row(chain_id, address, name):
    return {
        "chain_id": chain_id,
        "contract_address": address,
        "contract_name": name,
    }


BASE = [
    row(1, "0xaaa", "LANDProxy"),
    row(1, "0xbbb", "MANAToken"),
    row(137, "0xccc", "MarketplaceV2"),
]


def test_identical_snapshots_have_no_changes():
    changes = diff_snapshots(BASE, BASE)
    assert changes == {"added": [], "removed": [], "renamed": []}


def test_new_contract_is_added():
    today = BASE + [row(137, "0xddd", "CollectionStore")]
    changes = diff_snapshots(today, BASE)
    assert changes["added"] == [row(137, "0xddd", "CollectionStore")]
    assert changes["removed"] == []
    assert changes["renamed"] == []


def test_missing_contract_is_removed():
    today = [r for r in BASE if r["contract_address"] != "0xbbb"]
    changes = diff_snapshots(today, BASE)
    assert changes["removed"] == [row(1, "0xbbb", "MANAToken")]
    assert changes["added"] == []


def test_same_key_new_name_is_renamed():
    today = [
        row(1, "0xaaa", "LANDProxy"),
        row(1, "0xbbb", "MANATokenV2"),
        row(137, "0xccc", "MarketplaceV2"),
    ]
    changes = diff_snapshots(today, BASE)
    assert changes["renamed"] == [
        {
            "chain_id": 1,
            "contract_address": "0xbbb",
            "old_name": "MANAToken",
            "new_name": "MANATokenV2",
        }
    ]
    assert changes["added"] == []
    assert changes["removed"] == []


def test_same_address_on_other_chain_is_added_not_renamed():
    # (chain_id, address) is the key: the same address appearing on a new
    # chain is an addition, never a rename.
    today = BASE + [row(137, "0xaaa", "LANDProxy")]
    changes = diff_snapshots(today, BASE)
    assert changes["added"] == [row(137, "0xaaa", "LANDProxy")]
    assert changes["renamed"] == []


def test_summarize_counts_and_lists_changes():
    today = BASE + [row(137, "0xddd", "CollectionStore")]
    changes = diff_snapshots(today, BASE)
    text = summarize(changes)
    assert "1 added, 0 removed, 0 renamed" in text
    assert "added chain_id=137 0xddd CollectionStore" in text


def test_summarize_shows_rename_old_and_new_name():
    today = [
        row(1, "0xaaa", "LANDProxy"),
        row(1, "0xbbb", "MANATokenV2"),
        row(137, "0xccc", "MarketplaceV2"),
    ]
    changes = diff_snapshots(today, BASE)
    text = summarize(changes)
    assert "renamed chain_id=1 0xbbb MANAToken -> MANATokenV2" in text


def test_summarize_caps_listed_changes(caplog):
    today = BASE + [row(137, f"0x{i:03x}", f"Contract{i}") for i in range(50)]
    changes = diff_snapshots(today, BASE)
    text = summarize(changes, max_lines=10)
    # counts header + at most 10 change lines + ellipsis marker
    assert "50 added, 0 removed, 0 renamed" in text
    assert len(text.splitlines()) == 12
    assert text.splitlines()[-1] == "... 40 more"


def test_diff_rejects_empty_today_snapshot():
    with pytest.raises(ValueError):
        diff_snapshots([], BASE)


# --- handler ---------------------------------------------------------------


def snapshot_bytes(rows):
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buf)
    return buf.getvalue()


class FakeS3:
    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        import io

        if Key not in self.objects:
            raise self.exceptions.NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key])}


class FakeSNS:
    def __init__(self, published):
        self.published = published

    def publish(self, TopicArn, Message):
        self.published.append({"topic": TopicArn, "message": Message})


def make_handler_env(monkeypatch, objects, published):
    import ingestion.dcl_contracts_diff.handler as h

    def mock_boto3_client(service):
        return FakeS3(objects) if service == "s3" else FakeSNS(published)

    monkeypatch.setenv("LAKE_BUCKET", "test-bucket")
    monkeypatch.setenv("ALERTS_TOPIC_ARN", "arn:aws:sns:us-east-1:123:alerts")
    monkeypatch.setattr(h.boto3, "client", mock_boto3_client)
    return h


def test_handler_publishes_info_alert_on_changes(monkeypatch):
    import json

    published = []
    objects = {
        "bronze/dcl_contracts/dt=2026-08-23/contracts.parquet": snapshot_bytes(
            BASE + [row(137, "0xddd", "CollectionStore")]
        ),
        "bronze/dcl_contracts/dt=2026-08-22/contracts.parquet": snapshot_bytes(BASE),
    }
    h = make_handler_env(monkeypatch, objects, published)

    result = h.handler({"date": "2026-08-23"}, None)

    assert result == {
        "compared": True,
        "added": 1,
        "removed": 0,
        "renamed": 0,
        "notified": True,
    }
    assert len(published) == 1
    msg = json.loads(published[0]["message"])
    assert msg["source"] == "diff-dcl-contracts"
    assert msg["component"] == "dcl_contracts"
    assert msg["status"] == "INFO"
    assert "1 added, 0 removed, 0 renamed" in msg["detail"]
    assert "2026-08-23" in msg["detail"] and "2026-08-22" in msg["detail"]


def test_handler_silent_when_no_changes(monkeypatch):
    published = []
    objects = {
        "bronze/dcl_contracts/dt=2026-08-23/contracts.parquet": snapshot_bytes(BASE),
        "bronze/dcl_contracts/dt=2026-08-22/contracts.parquet": snapshot_bytes(BASE),
    }
    h = make_handler_env(monkeypatch, objects, published)

    result = h.handler({"date": "2026-08-23"}, None)

    assert result["compared"] is True
    assert result["notified"] is False
    assert published == []


def test_handler_skips_when_yesterday_missing(monkeypatch):
    published = []
    objects = {
        "bronze/dcl_contracts/dt=2026-08-23/contracts.parquet": snapshot_bytes(BASE),
    }
    h = make_handler_env(monkeypatch, objects, published)

    result = h.handler({"date": "2026-08-23"}, None)

    assert result["compared"] is False
    assert published == []


def test_handler_fails_when_today_missing(monkeypatch):
    # The extract step ran before us in the state machine; a missing snapshot
    # for the run date is a pipeline failure, not a skippable condition.
    h = make_handler_env(monkeypatch, {}, [])

    with pytest.raises(RuntimeError):
        h.handler({"date": "2026-08-23"}, None)


def test_handler_rejects_bad_date(monkeypatch):
    h = make_handler_env(monkeypatch, {}, [])

    with pytest.raises(ValueError):
        h.handler({"date": "23/08/2026"}, None)
