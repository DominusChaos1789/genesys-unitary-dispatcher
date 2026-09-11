import logging

import src.s3_utils as s3_utils
from test.conftest import PROVIDERS_LANDING_BUCKET as BUCKET
from test.conftest import SOURCE_PREFIX as PREFIX


def test_list_json_keys_filters_to_json_only(aws, seeded_source_files):
    s3 = aws["s3"]
    s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/notes.txt", Body=b"not json")

    keys = s3_utils.list_json_keys(s3, BUCKET, PREFIX)

    assert sorted(keys) == sorted(seeded_source_files)


def test_read_json_roundtrip(aws):
    s3 = aws["s3"]
    s3_utils.write_json(s3, BUCKET, "some/key.json", {"a": 1, "b": "\u00f1"})

    assert s3_utils.read_json(s3, BUCKET, "some/key.json") == {"a": 1, "b": "\u00f1"}


def test_read_json_tolerates_a_byte_order_mark(aws):
    s3 = aws["s3"]
    s3.put_object(Bucket=BUCKET, Key="bom.json", Body='\ufeff{"a": 1}'.encode("utf-8"))

    assert s3_utils.read_json(s3, BUCKET, "bom.json") == {"a": 1}


def test_read_json_files_skips_unreadable_files_and_keeps_good_ones(aws, seeded_source_files):
    s3 = aws["s3"]
    empty_key = f"{PREFIX}/empty.json"
    not_utf8_key = f"{PREFIX}/not_utf8.json"
    s3.put_object(Bucket=BUCKET, Key=empty_key, Body=b"")
    s3.put_object(Bucket=BUCKET, Key=not_utf8_key, Body=b"\xff\xfe not utf-8")

    records, successful_keys, skipped_keys = s3_utils.read_json_files(
        s3, BUCKET, seeded_source_files + [empty_key, not_utf8_key]
    )

    assert len(records) == len(seeded_source_files)
    assert successful_keys == seeded_source_files
    assert skipped_keys == [empty_key, not_utf8_key]


def test_delete_objects_removes_all_given_keys(aws, seeded_source_files):
    s3 = aws["s3"]

    deleted = s3_utils.delete_objects(s3, BUCKET, seeded_source_files)

    assert sorted(deleted) == sorted(seeded_source_files)
    assert s3_utils.list_json_keys(s3, BUCKET, PREFIX) == []


def test_delete_objects_sends_batches_no_larger_than_the_s3_limit(aws, monkeypatch):
    s3 = aws["s3"]
    keys = [f"{PREFIX}/{i}.json" for i in range(5)]
    for key in keys:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"{}")
    monkeypatch.setattr(s3_utils, "DELETE_BATCH_SIZE", 2)
    batch_sizes = []
    real_delete = s3.delete_objects

    def counting_delete(**kwargs):
        batch_sizes.append(len(kwargs["Delete"]["Objects"]))
        return real_delete(**kwargs)

    monkeypatch.setattr(s3, "delete_objects", counting_delete)

    assert sorted(s3_utils.delete_objects(s3, BUCKET, keys)) == sorted(keys)
    assert batch_sizes == [2, 2, 1]


def test_delete_objects_logs_keys_that_could_not_be_deleted(caplog):
    class FakeS3:
        def delete_objects(self, **kwargs):
            return {"Deleted": [{"Key": "a.json"}], "Errors": [{"Key": "b.json", "Message": "AccessDenied"}]}

    with caplog.at_level(logging.ERROR):
        deleted = s3_utils.delete_objects(FakeS3(), BUCKET, ["a.json", "b.json"])

    assert deleted == ["a.json"]
    assert "b.json" in caplog.text
    assert "AccessDenied" in caplog.text


def test_delete_objects_with_no_keys_makes_no_call():
    class NoCalls:
        def delete_objects(self, **kwargs):
            raise AssertionError("no keys, no call")

    assert s3_utils.delete_objects(NoCalls(), BUCKET, []) == []


def test_list_common_prefixes_returns_the_folders_directly_under_a_prefix(aws):
    s3 = aws["s3"]
    for key in (
        "root/org_id=2/year=2026/a.json",
        "root/org_id=1/year=2026/b.json",
        "root/org_id=1/c.json",
        "root/file.json",
    ):
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"{}")

    assert s3_utils.list_common_prefixes(s3, BUCKET, "root/") == ["root/org_id=1/", "root/org_id=2/"]
