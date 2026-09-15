import json
import logging
from types import SimpleNamespace

import pytest

from src.main import handler
from test.conftest import CORE_CONFIG, CORE_CONFIG_KEY, ENDPOINTS_PREFIX, LOGS_BUCKET, RESOURCES_BUCKET

PREFIX = "transacciones/genesys/api/payload_request_unitary"
ORGS = [
    {"organization_id": "org-3", "ids": ["conv-3"]},
    {"organization_id": "org-1", "ids": ["conv-1"]},
]
RESPONSE_KEYS = ["execution_id", "bucket", "payload_location", "stages", "failed_organizations", "tag"]


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


def test_a_single_tag_returns_exactly_the_response_fields(aws):
    response = handler({"tag": "surveys", "organizations": ORGS}, _context("req-1"))

    assert list(response) == RESPONSE_KEYS
    assert response == {
        "execution_id": "req-1",
        "bucket": LOGS_BUCKET,
        "payload_location": f"{PREFIX}/surveys/req-1.json",
        "stages": ["request_context"],
        "failed_organizations": [],
        "tag": "surveys",
    }


def test_the_payload_file_is_left_as_it_was(aws):
    response = handler({"tag": "surveys", "organizations": ORGS}, _context())

    written = _read(aws["s3"], response)
    assert set(written) == {"tag", "organization", "failed_organizations"}
    assert [entry["organization_id"] for entry in written["organization"]] == ["org-1", "org-3"]
    assert written["organization"][0]["ids"] == ["conv-1"]
    assert written["organization"][0]["request_context"]["url"] == (
        "/api/v2/quality/conversations/{conversationId}/surveys"
    )


def test_tags_return_one_response_per_tag(aws):
    _add_recordings_flow(aws["s3"])

    responses = handler({"tags": ["surveys", "recordings"], "organizations": ORGS}, _context("req-2"))

    assert [r["tag"] for r in responses] == ["surveys", "recordings"]
    assert [r["payload_location"] for r in responses] == [
        f"{PREFIX}/surveys/req-2.json",
        f"{PREFIX}/recordings/req-2.json",
    ]
    for response in responses:
        assert list(response) == RESPONSE_KEYS
        assert response["execution_id"] == "req-2"
        assert _read(aws["s3"], response)["tag"] == response["tag"]


@pytest.mark.parametrize(
    "selection", [{"tags": ["surveys"]}, {"tags": "all"}, {"detail": {"tags": ["surveys"]}}]
)
def test_the_shape_follows_the_event_key_not_the_number_of_tags(aws, selection):
    response = handler({**selection, "organizations": ORGS}, _context())

    assert isinstance(response, list)
    assert [r["tag"] for r in response] == ["surveys"]


def test_failed_organizations_are_counts_in_the_response_and_ids_in_the_file(aws):
    event = {"tag": "surveys", "organizations": [*ORGS, {"organization_id": "org-9", "ids": ["conv-9"]}]}

    response = handler(event, _context())

    assert response["failed_organizations"] == [
        {"organization_id": "org-9", "id_count": 1, "error": "no servers entry for org_9"}
    ]
    assert _read(aws["s3"], response)["failed_organizations"] == [
        {"organization_id": "org-9", "ids": ["conv-9"], "error": "no servers entry for org_9"}
    ]


def test_the_detailed_run_summary_goes_to_the_logs(aws, caplog):
    with caplog.at_level(logging.INFO, logger="src.main"):
        handler({"tag": "surveys", "organizations": ORGS}, _context())

    summaries = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Run summary: ")]
    assert len(summaries) == 1
    summary = json.loads(summaries[0].removeprefix("Run summary: "))
    assert summary["payloads"][0]["organizations"] == {"org-1": 1, "org-3": 1}
