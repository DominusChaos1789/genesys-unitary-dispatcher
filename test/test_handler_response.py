import json
import logging
from types import SimpleNamespace

import pytest

from src.main import handler
from test.conftest import (
    CORE_CONFIG,
    CORE_CONFIG_KEY,
    ENDPOINTS_PREFIX,
    LOGS_BUCKET,
    RESOURCES_BUCKET,
    add_dispatcher_flow,
)

PREFIX = "transacciones/genesys/api/payload_request_unitary"
ORGS = [
    {"organization_id": "org-3", "ids": ["conv-3"]},
    {"organization_id": "org-1", "ids": ["conv-1"]},
]
RESPONSE_KEYS = ["bucket", "payload_location", "organization_id", "stages", "failed_organizations", "tag"]


def _context(request_id: str = "req-1"):
    return SimpleNamespace(aws_request_id=request_id)


def _read(s3, response: dict) -> dict:
    return json.loads(
        s3.get_object(Bucket=response["bucket"], Key=response["payload_location"])["Body"].read()
    )


def _add_recordings_flow(s3) -> None:
    s3.put_object(
        Bucket=RESOURCES_BUCKET,
        Key=f"{ENDPOINTS_PREFIX}/recordings.json",
        Body=json.dumps(
            {
                "conversation_recordings": {
                    "tag": "recordings",
                    "type": "unitary",
                    "method": "GET",
                    "url": "/api/v2/conversations/{conversationId}/recordings",
                }
            }
        ),
    )
    core = {**CORE_CONFIG, "unitary": [*CORE_CONFIG["unitary"], f"{ENDPOINTS_PREFIX}/recordings.json"]}
    s3.put_object(Bucket=RESOURCES_BUCKET, Key=CORE_CONFIG_KEY, Body=json.dumps(core))
    add_dispatcher_flow(s3, "recordings")


def test_handler_always_returns_a_list_one_entry_per_tag_and_organization(aws):
    responses = handler({"tag": "surveys", "organizations": ORGS}, _context("req-1"))

    assert isinstance(responses, list)
    by_org = {r["organization_id"]: r for r in responses}
    assert set(by_org) == {"org-1", "org-3"}
    assert list(by_org["org-1"]) == RESPONSE_KEYS
    assert by_org["org-1"] == {
        "bucket": LOGS_BUCKET,
        # A fixed key per (tag, organization) -- no execution id -- so a
        # later run overwrites this same file rather than adding a new one.
        "payload_location": f"{PREFIX}/surveys/org-1.json",
        "organization_id": "org-1",
        "stages": ["request_context"],
        "failed_organizations": [],
        "tag": "surveys",
    }


def test_a_single_organization_run_still_returns_a_one_item_list(aws):
    responses = handler({"tag": "surveys", "organizations": [ORGS[0]]}, _context())

    assert isinstance(responses, list)
    assert len(responses) == 1
    assert responses[0]["organization_id"] == "org-3"


def test_the_payload_file_is_flat_with_no_organization_array(aws):
    responses = handler({"tag": "surveys", "organizations": ORGS}, _context())
    response = next(r for r in responses if r["organization_id"] == "org-1")

    written = _read(aws["s3"], response)
    assert set(written) == {"tag", "organization_id", "ids", "request_context", "failed_organizations"}
    assert written["organization_id"] == "org-1"
    assert written["ids"] == ["conv-1"]
    assert written["request_context"]["url"] == "/api/v2/quality/surveys/{surveyId}"


def test_tags_produce_a_response_entry_per_tag_and_organization(aws):
    _add_recordings_flow(aws["s3"])

    responses = handler({"tags": ["surveys", "recordings"], "organizations": ORGS}, _context("req-2"))

    locations = sorted(r["payload_location"] for r in responses)
    assert locations == [
        f"{PREFIX}/recordings/org-1.json",
        f"{PREFIX}/recordings/org-3.json",
        f"{PREFIX}/surveys/org-1.json",
        f"{PREFIX}/surveys/org-3.json",
    ]
    for response in responses:
        assert list(response) == RESPONSE_KEYS
        assert _read(aws["s3"], response)["tag"] == response["tag"]


@pytest.mark.parametrize("selection", [{"tag": "surveys"}, {"tags": ["surveys"]}, {"tags": "all"}])
def test_the_response_shape_is_always_a_list_whatever_the_event_key(aws, selection):
    responses = handler({**selection, "organizations": [ORGS[0]]}, _context())

    assert isinstance(responses, list)


def test_failed_organizations_are_counts_in_the_response_and_ids_in_the_file(aws):
    event = {"tag": "surveys", "organizations": [*ORGS, {"organization_id": "org-9", "ids": ["conv-9"]}]}

    responses = handler(event, _context())

    assert {r["organization_id"] for r in responses} == {"org-1", "org-3"}
    assert responses[0]["failed_organizations"] == [
        {"organization_id": "org-9", "id_count": 1, "error": "no servers entry for org_9"}
    ]
    assert _read(aws["s3"], responses[0])["failed_organizations"] == [
        {"organization_id": "org-9", "ids": ["conv-9"], "error": "no servers entry for org_9"}
    ]


def test_a_later_run_overwrites_the_same_organizations_file(aws):
    first = handler(
        {"tag": "surveys", "organizations": [{"organization_id": "org-1", "ids": ["conv-a"]}]}, _context()
    )
    second = handler(
        {"tag": "surveys", "organizations": [{"organization_id": "org-1", "ids": ["conv-b"]}]}, _context()
    )

    assert first[0]["payload_location"] == second[0]["payload_location"] == f"{PREFIX}/surveys/org-1.json"
    assert _read(aws["s3"], second[0])["ids"] == ["conv-b"]


def test_a_wholly_failed_run_returns_an_empty_list(aws):
    event = {"tag": "surveys", "organizations": [{"organization_id": "org-9", "ids": ["conv-9"]}]}

    assert handler(event, _context()) == []


def test_the_detailed_run_summary_goes_to_the_logs(aws, caplog):
    with caplog.at_level(logging.INFO, logger="src.main"):
        handler({"tag": "surveys", "organizations": ORGS}, _context())

    summaries = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Run summary: ")]
    assert len(summaries) == 1
    summary = json.loads(summaries[0].removeprefix("Run summary: "))
    assert {r["organization_id"] for r in summary["responses"]} == {"org-1", "org-3"}
