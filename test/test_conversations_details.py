import json
from types import SimpleNamespace

import pytest

import src.s3_utils as s3_utils
from src.conversations_details import organization_id_from_folder, parse_date
from src.main import handler
from src.sources import EventError
from test.conftest import LANDING_BUCKET, load_fixture, payload_by_org, read_payload

PREFIX = "transacciones/genesys/api/conversations_details/"
DAY = "2026-08-13"
DAY_PATH = "year=2026/month=08/day=13/"
EVENT = {"tag": "surveys", "ids_source": "conversations_details", "date": DAY}
SAMPLE_IDS = sorted(
    c["conversationId"] for c in load_fixture("conversations_details_sample.json")["endpoint"]
)


def _context(request_id: str = "req-details"):
    return SimpleNamespace(aws_request_id=request_id)


def _by_org(result: dict) -> dict:
    return payload_by_org(result)


def _put(s3, key: str, body) -> None:
    s3.put_object(Bucket=LANDING_BUCKET, Key=key, Body=body if isinstance(body, bytes) else json.dumps(body))


def _details(*conversation_ids) -> dict:
    return {"endpoint": [{"conversationId": cid, "participants": []} for cid in conversation_ids]}


def test_ids_are_read_per_organization_for_the_event_date_and_files_are_kept(aws):
    s3 = aws["s3"]
    _put(
        s3, f"{PREFIX}org_id=1/{DAY_PATH}conversations_details_2026-08-13_00:00:00_1.json", _details("a", "b")
    )
    _put(
        s3, f"{PREFIX}org_id=1/{DAY_PATH}conversations_details_2026-08-13_00:00:00_2.json", _details("b", "c")
    )
    _put(
        s3,
        f"{PREFIX}org_id=3/{DAY_PATH}conversations_details_2026-08-13_00:00:00_1.json",
        load_fixture("conversations_details_sample.json"),
    )
    # Another day for the same organization must not be read.
    _put(s3, f"{PREFIX}org_id=1/year=2026/month=08/day=12/conversations_details_old.json", _details("old"))

    result = handler(EVENT, _context())

    organizations = _by_org(result)
    assert organizations["org-1"]["ids"] == ["a", "b", "c"]
    assert organizations["org-3"]["ids"] == SAMPLE_IDS
    assert organizations["org-1"]["request_context"]["headers"]["Authorization"] == "Bearer token-for-org-1"
    assert organizations["org-3"]["request_context"]["base_url"] == "https://api.usw2.pure.cloud"

    summary = result["conversations_details"]
    assert summary == {
        "date": DAY,
        "bucket": LANDING_BUCKET,
        "files_read": 3,
        "skipped_files": [],
        "conversations": {"org-1": 3, "org-3": len(SAMPLE_IDS)},
    }

    # Nothing is deleted or moved: the download process owns these files.
    assert len(s3_utils.list_json_keys(s3, LANDING_BUCKET, PREFIX)) == 4


def test_unreadable_or_unexpected_files_are_skipped_and_reported(aws):
    s3 = aws["s3"]
    empty = f"{PREFIX}org_id=1/{DAY_PATH}empty.json"
    no_endpoint = f"{PREFIX}org_id=1/{DAY_PATH}no_endpoint.json"
    _put(s3, f"{PREFIX}org_id=1/{DAY_PATH}good.json", _details("a"))
    _put(s3, empty, b"")
    _put(s3, no_endpoint, {"conversations": [{"conversationId": "x"}]})
    _put(s3, f"{PREFIX}org_id=1/{DAY_PATH}notes.txt", b"not json, not listed")

    result = handler(EVENT, _context())

    assert _by_org(result)["org-1"]["ids"] == ["a"]
    assert result["conversations_details"]["files_read"] == 1
    assert sorted(result["conversations_details"]["skipped_files"]) == sorted([empty, no_endpoint])


def test_conversations_without_an_id_are_ignored(aws):
    body = {
        "endpoint": [{"conversationId": "a"}, {"participants": []}, "not-an-object", {"conversationId": ""}]
    }
    _put(aws["s3"], f"{PREFIX}org_id=2/{DAY_PATH}mixed.json", body)

    result = handler(EVENT, _context())

    assert _by_org(result)["org-2"]["ids"] == ["a"]


def test_folders_that_are_not_org_partitions_are_ignored(aws):
    s3 = aws["s3"]
    _put(s3, f"{PREFIX}org_id=1/{DAY_PATH}a.json", _details("a"))
    _put(s3, f"{PREFIX}_tmp/{DAY_PATH}b.json", _details("b"))

    result = handler(EVENT, _context())

    assert set(_by_org(result)) == {"org-1"}


def test_a_day_without_files_produces_an_empty_payload_without_touching_genesys(aws, genesys_api):
    result = handler(EVENT, _context())

    assert read_payload(result)["organization"] == []
    assert result["conversations_details"]["files_read"] == 0
    assert genesys_api["load_config"] == []


@pytest.mark.parametrize("event_date", [None, "13-08-2026", "2026-02-30", 20260813])
def test_the_date_is_required_as_yyyy_mm_dd(aws, event_date):
    event = {"tag": "surveys", "ids_source": "conversations_details"}
    if event_date is not None:
        event["date"] = event_date

    with pytest.raises(EventError, match='needs "date" as YYYY-MM-DD'):
        handler(event, _context())


def test_parse_date():
    assert parse_date("2026-08-13").isoformat() == "2026-08-13"


@pytest.mark.parametrize(
    "folder, expected",
    [
        (f"{PREFIX}org_id=1/", "org-1"),
        (f"{PREFIX}org_id=4/", "org-4"),
        (f"{PREFIX}_tmp/", None),
        (f"{PREFIX}org_id=/", None),
    ],
)
def test_organization_id_comes_from_the_org_id_folder(folder, expected):
    assert organization_id_from_folder(folder) == expected
