"""transacciones_pronosticos and funcionarios_grupos_planificacion: one call
per business unit, ids read off the management units download -- by date, or
from the one file an EventBridge S3 event names."""

import json
from types import SimpleNamespace

import pytest

from src.main import run
from src.management_units import organization_from_key
from src.sources import EventError, date_from_key
from test.conftest import GENESYS_BASE_PATH, GENESYS_BASE_PATH_WFM, LANDING_BUCKET, payload_by_org

PREFIX = "funcionarios/genesys/api/management_unit_list/"
DAY_PATH = "year=2026/month=08/day=13/"
FILE_KEY = f"{PREFIX}org_id=3/{DAY_PATH}management_unit_list_2026_08_13.json"
TAGS = ["transacciones_pronosticos", "funcionarios_grupos_planificacion"]
BU = "/api/v2/workforcemanagement/businessunits/{businessUnitId}"


def _context():
    return SimpleNamespace(aws_request_id="req-bu")


def _units(*pairs) -> dict:
    """(management unit id, business unit id or None) -> the download's shape."""
    endpoint = []
    for mu_id, bu_id in pairs:
        unit = {"id": mu_id, "name": f"MU_{mu_id}"}
        if bu_id is not None:
            unit["businessUnit"] = {
                "id": bu_id,
                "selfUri": f"/api/v2/workforcemanagement/businessunits/{bu_id}",
            }
        endpoint.append(unit)
    return {"endpoint": endpoint, "status": "success"}


def _put(s3, key, body):
    s3.put_object(
        Bucket=LANDING_BUCKET, Key=key, Body=json.dumps(body) if not isinstance(body, bytes) else body
    )


def _s3_event(key=FILE_KEY, tags=TAGS):
    """What the EventBridge rule's input transformer sends (docs/eventbridge)."""
    return {
        "tags": tags,
        "ids_source": "management_unit_list",
        "ids_location": {"bucket": "augusta-nexa-dev-landing", "key": key},
    }


def test_by_date_each_organization_gets_its_distinct_business_units(aws):
    s3 = aws["s3"]
    _put(
        s3, f"{PREFIX}org_id=3/{DAY_PATH}a.json", _units(("mu-1", "bu-b"), ("mu-2", "bu-a"), ("mu-3", "bu-b"))
    )
    _put(s3, f"{PREFIX}org_id=1/{DAY_PATH}b.json", _units(("mu-9", "bu-z"), ("mu-8", None)))

    result = run({"tags": TAGS, "ids_source": "management_unit_list", "date": "2026-08-13"}, _context())

    forecasts = payload_by_org(result, "transacciones_pronosticos")
    planning = payload_by_org(result, "funcionarios_grupos_planificacion")
    assert forecasts["org-3"]["ids"] == planning["org-3"]["ids"] == ["bu-a", "bu-b"]
    assert forecasts["org-1"]["ids"] == ["bu-z"]
    assert result["management_unit_list"]["management_units"] == {"org-1": 1, "org-3": 2}


def test_forecasts_list_then_fetch_one_forecasts_data(aws):
    _put(aws["s3"], FILE_KEY, _units(("mu-1", "bu-1")))

    result = run(_s3_event(tags=["transacciones_pronosticos"]), _context())

    payload = payload_by_org(result)["org-3"]
    assert payload["request_context"]["url"] == f"{BU}/weeks/{{weekDateId}}/shorttermforecasts"
    assert payload["request_context"]["result_data"] == "selfUri"
    assert (
        payload["request_status"]["url"]
        == f"{BU}/weeks/{{weekDateId}}/shorttermforecasts/{{forecastId}}/data"
    )
    assert payload["request_context"]["base_path"] == payload["request_status"]["base_path"]
    assert result["responses"][0]["stages"] == ["request_context", "request_status"]


def test_planning_groups_is_one_call_per_business_unit(aws):
    _put(aws["s3"], FILE_KEY, _units(("mu-1", "bu-1")))

    result = run(_s3_event(tags=["funcionarios_grupos_planificacion"]), _context())

    payload = payload_by_org(result)["org-3"]
    assert payload["request_context"]["url"] == f"{BU}/planninggroups"
    assert result["responses"][0]["stages"] == ["request_context"]


def test_the_s3_event_reads_only_its_own_file_and_takes_its_date(aws):
    s3 = aws["s3"]
    _put(s3, FILE_KEY, _units(("mu-1", "bu-1")))
    # Other organizations and other files the same day aren't read.
    _put(s3, f"{PREFIX}org_id=1/{DAY_PATH}other.json", _units(("mu-9", "bu-9")))

    result = run(_s3_event(), _context())

    assert set(payload_by_org(result, "transacciones_pronosticos")) == {"org-3"}
    assert payload_by_org(result, "transacciones_pronosticos")["org-3"]["date"] == "2026-08-13"
    assert result["management_unit_list"]["file"] == FILE_KEY


def test_the_raw_eventbridge_s3_event_works_too(aws):
    _put(aws["s3"], FILE_KEY, _units(("mu-1", "bu-1")))
    event = {
        "detail-type": "Object Created",
        "source": "aws.s3",
        "detail": {
            "bucket": {"name": LANDING_BUCKET},
            "object": {"key": FILE_KEY},
            "tags": TAGS,
        },
        "ids_source": "management_unit_list",
    }

    result = run(event, _context())

    assert {(r["tag"], r["organization_id"]) for r in result["responses"]} == {
        ("transacciones_pronosticos", "org-3"),
        ("funcionarios_grupos_planificacion", "org-3"),
    }


def test_the_event_date_wins_over_the_files_day(aws):
    _put(aws["s3"], FILE_KEY, _units(("mu-1", "bu-1")))

    result = run({**_s3_event(), "date": "2026-08-20"}, _context())

    assert payload_by_org(result, "transacciones_pronosticos")["org-3"]["date"] == "2026-08-20"


def test_existing_management_unit_flows_can_use_the_s3_event(aws):
    _put(aws["s3"], FILE_KEY, _units(("mu-1", "bu-1"), ("mu-2", "bu-1")))

    result = run(_s3_event(tags=["funcionarios_adherencia", "funcionarios_programaciones"]), _context())

    assert payload_by_org(result, "funcionarios_adherencia")["org-3"]["ids"] == ["mu-1", "mu-2"]
    assert payload_by_org(result, "funcionarios_programaciones")["org-3"]["ids"] == [
        {"managementUnitId": "mu-1", "businessUnitId": "bu-1"},
        {"managementUnitId": "mu-2", "businessUnitId": "bu-1"},
    ]


def test_a_file_with_no_business_units_writes_nothing(aws):
    _put(aws["s3"], FILE_KEY, _units(("mu-1", None)))

    result = run(_s3_event(), _context())

    assert result["responses"] == []


def test_a_file_outside_an_org_folder_is_rejected(aws):
    key = f"{PREFIX}{DAY_PATH}loose.json"
    _put(aws["s3"], key, _units(("mu-1", "bu-1")))

    with pytest.raises(EventError, match="not under an org_id= folder"):
        run(_s3_event(key=key), _context())


def test_an_unreadable_file_is_rejected(aws):
    _put(aws["s3"], FILE_KEY, b"not json")

    with pytest.raises(EventError, match="is unreadable"):
        run(_s3_event(), _context())


def test_a_tag_the_download_cannot_feed_is_rejected(aws):
    _put(aws["s3"], FILE_KEY, _units(("mu-1", "bu-1")))

    with pytest.raises(EventError, match="cannot feed id_kind 'survey'"):
        run(_s3_event(tags=["surveys"]), _context())


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        (FILE_KEY, "2026-08-13"),
        ("x/year=2026/month=8/day=3/f.json", "2026-08-03"),
        ("x/no-partitions/f.json", None),
    ],
)
def test_date_from_key(key, expected):
    assert date_from_key(key) == expected


@pytest.mark.parametrize(
    ("key", "expected"),
    [(FILE_KEY, "org-3"), ("org_id=12/x.json", "org-12"), ("x/notorg_id=3/x.json", None), ("x/f.json", None)],
)
def test_organization_from_key(key, expected):
    assert organization_from_key(key) == expected


def test_a_business_unit_shared_by_several_management_units_is_sent_once(aws):
    _put(aws["s3"], FILE_KEY, _units(("mu-1", "bu-2"), ("mu-2", "bu-1"), ("mu-3", "bu-2")))

    result = run(_s3_event(), _context())

    assert payload_by_org(result, "transacciones_pronosticos")["org-3"]["ids"] == ["bu-1", "bu-2"]


def test_forecasts_save_under_transacciones_and_planning_groups_under_funcionarios(aws):
    _put(aws["s3"], FILE_KEY, _units(("mu-1", "bu-1")))

    result = run(_s3_event(), _context())

    forecast = payload_by_org(result, "transacciones_pronosticos")["org-3"]
    planning = payload_by_org(result, "funcionarios_grupos_planificacion")["org-3"]
    assert forecast["request_context"]["base_path"] == GENESYS_BASE_PATH
    assert forecast["request_status"]["base_path"] == GENESYS_BASE_PATH
    assert planning["request_context"]["base_path"] == GENESYS_BASE_PATH_WFM
