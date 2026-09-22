import json
from types import SimpleNamespace

import pytest

from src.main import run
from src.sources import EventError
from test.conftest import LANDING_BUCKET, payload_by_org

PREFIX = "funcionarios/genesys/api/management_unit_list/"
DAY = "2026-08-13"
DAY_PATH = "year=2026/month=08/day=13/"
EVENT = {"tag": "funcionarios_adherencia", "ids_source": "management_unit_list", "date": DAY}


def _context(request_id: str = "req-mu"):
    return SimpleNamespace(aws_request_id=request_id)


def _by_org(result: dict) -> dict:
    return payload_by_org(result)


def _put(s3, key: str, body) -> None:
    s3.put_object(Bucket=LANDING_BUCKET, Key=key, Body=body if isinstance(body, bytes) else json.dumps(body))


def _units(*unit_ids) -> dict:
    return {
        "endpoint": [{"id": mu_id, "name": f"MU_{mu_id}"} for mu_id in unit_ids],
        "status": "success",
    }


def test_management_unit_ids_are_read_per_organization_for_the_event_date(aws):
    s3 = aws["s3"]
    _put(s3, f"{PREFIX}org_id=1/{DAY_PATH}a.json", _units("mu-b", "mu-a"))
    _put(s3, f"{PREFIX}org_id=3/{DAY_PATH}b.json", _units("mu-c"))
    # Another day for the same organization must not be read.
    _put(s3, f"{PREFIX}org_id=1/year=2026/month=08/day=12/old.json", _units("mu-old"))

    result = run(EVENT, _context())

    organizations = _by_org(result)
    assert organizations["org-1"]["ids"] == ["mu-a", "mu-b"]
    assert organizations["org-3"]["ids"] == ["mu-c"]
    assert result["management_unit_list"] == {
        "date": DAY,
        "bucket": LANDING_BUCKET,
        "files_read": 2,
        "skipped_files": [],
        "management_units": {"org-1": 2, "org-3": 1},
    }
    # The event's own date, not today's, is what goes in the payload.
    assert organizations["org-1"]["date"] == DAY


def test_units_without_an_id_are_ignored(aws):
    body = {"endpoint": [{"id": "mu-a"}, {"name": "no id"}, "not-an-object", {"id": ""}]}
    _put(aws["s3"], f"{PREFIX}org_id=2/{DAY_PATH}mixed.json", body)

    result = run(EVENT, _context())

    assert _by_org(result)["org-2"]["ids"] == ["mu-a"]


def test_unreadable_or_unexpected_files_are_skipped_and_reported(aws):
    s3 = aws["s3"]
    empty = f"{PREFIX}org_id=1/{DAY_PATH}empty.json"
    no_endpoint = f"{PREFIX}org_id=1/{DAY_PATH}no_endpoint.json"
    _put(s3, f"{PREFIX}org_id=1/{DAY_PATH}good.json", _units("mu-a"))
    _put(s3, empty, b"")
    _put(s3, no_endpoint, {"units": [{"id": "x"}]})

    result = run(EVENT, _context())

    assert _by_org(result)["org-1"]["ids"] == ["mu-a"]
    assert result["management_unit_list"]["files_read"] == 1
    assert sorted(result["management_unit_list"]["skipped_files"]) == sorted([empty, no_endpoint])


def test_the_date_is_required_as_yyyy_mm_dd(aws):
    event = {"tag": "funcionarios_adherencia", "ids_source": "management_unit_list"}

    with pytest.raises(EventError, match='ids_source "management_unit_list" needs "date" as YYYY-MM-DD'):
        run(event, _context())


def test_a_day_without_files_produces_an_empty_payload_without_touching_genesys(aws, genesys_api):
    result = run(EVENT, _context())

    assert result["responses"] == []
    assert result["management_unit_list"]["files_read"] == 0
    assert genesys_api["load_config"] == []
