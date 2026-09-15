import json
import re
from types import SimpleNamespace

import pytest

from src.main import run
from src.sources import EventError
from test.conftest import (
    CORE_CONFIG,
    CORE_CONFIG_KEY,
    ENDPOINTS_PREFIX,
    GENESYS_BASE_PATH,
    GENESYS_BASE_PATH_WFM,
    LANDING_BUCKET,
    LOGS_BUCKET,
    RESOURCES_BUCKET,
    load_fixture,
    payload_by_org,
    read_payload,
)

PAYLOAD_PREFIX = "transacciones/genesys/api/payload_request_unitary"
TWO_ORGS = [
    {"organization_id": "org-3", "ids": ["conv-3b", "conv-3a"]},
    {"organization_id": "org-1", "ids": ["conv-1a"]},
]
RECORDINGS_ENDPOINT = {
    "conversation_recordings": {
        "tag": "recordings",
        "url": "/api/v2/conversations/{conversationId}/recordings",
        "type": "unitary",
        "method": "GET",
        "path": "conversation_recordings",
        "result_data": "state",
    }
}


def _context(request_id: str = "req-123"):
    return SimpleNamespace(aws_request_id=request_id)


def _add_recordings_flow(s3) -> None:
    """A second conversation flow next to surveys, in its own endpoint file."""
    s3.put_object(
        Bucket=RESOURCES_BUCKET,
        Key=f"{ENDPOINTS_PREFIX}/recordings.json",
        Body=json.dumps(RECORDINGS_ENDPOINT),
    )
    core = {**CORE_CONFIG, "unitary": [*CORE_CONFIG["unitary"], f"{ENDPOINTS_PREFIX}/recordings.json"]}
    s3.put_object(Bucket=RESOURCES_BUCKET, Key=CORE_CONFIG_KEY, Body=json.dumps(core))


def test_surveys_payload_has_one_direct_stage_per_organization(aws, genesys_api):
    result = run({"tag": "surveys", "organizations": TWO_ORGS}, _context())

    assert result["tags"] == ["surveys"]
    assert result["failed_organizations"] == []
    assert result["payloads"] == [
        {
            "tag": "surveys",
            "bucket": LOGS_BUCKET,
            "key": f"{PAYLOAD_PREFIX}/surveys/req-123.json",
            "payload_location": f"s3://{LOGS_BUCKET}/{PAYLOAD_PREFIX}/surveys/req-123.json",
            "stages": ["request_context"],
            "organizations": {"org-1": 1, "org-3": 2},
        }
    ]

    payload = read_payload(result)
    assert [entry["organization_id"] for entry in payload["organization"]] == ["org-1", "org-3"]
    assert payload_by_org(result)["org-3"] == {
        "organization_id": "org-3",
        "ids": ["conv-3a", "conv-3b"],
        "request_context": {
            "base_url": "https://api.usw2.pure.cloud",
            # Left unrendered: the id is substituted downstream, per call.
            "url": "/api/v2/quality/conversations/{conversationId}/surveys",
            "method": "GET",
            "headers": {"Authorization": "Bearer token-for-org-3", "Content-Type": "application/json"},
            "type": "unitary",
            "path": "conversations_surveys",
            "result_data": "state",
            "base_path": GENESYS_BASE_PATH,
            "server_path": "org_id=3/",
        },
    }
    assert genesys_api["load_config"] == [("/augusta-nexa-dev/genesys/api", "us-east-2")]
    assert genesys_api["resource_name"] == "augusta-nexa-dev-genesys-api-unitary-request"


def test_the_response_carries_locations_and_counts_never_the_payload(aws):
    """Step Functions caps state data at 256 KB. A day of conversations is more
    than that, so the ids only live in the payload file."""
    ids = [f"conversation-{n:05d}-0000-4000-8000-000000000000" for n in range(6000)]

    result = run({"tag": "surveys", "organizations": [{"organization_id": "org-1", "ids": ids}]}, _context())

    assert "organization" not in result
    assert len(json.dumps(result)) < 2_000
    assert result["payloads"][0]["organizations"] == {"org-1": 6000}
    assert len(read_payload(result)["organization"][0]["ids"]) == 6000


def test_adherence_is_one_job_per_management_unit(aws):
    s3 = aws["s3"]
    key = "funcionarios/genesys/management_units/2026-09-11.json"
    s3.put_object(
        Bucket=LANDING_BUCKET,
        Key=key,
        Body=json.dumps({"organizations": [{"organization_id": "org-1", "ids": ["mu-2", "mu-1"]}]}),
    )

    result = run(
        {"tag": "funcionarios_adherencia", "ids_location": {"bucket": "landing", "key": key}},
        _context(),
    )

    # No users listing stage: the job itself covers every user in the management unit.
    assert result["payloads"][0]["stages"] == ["request_init", "request_status"]
    entry = payload_by_org(result)["org-1"]
    assert entry["ids"] == ["mu-1", "mu-2"]
    assert "request_context" not in entry

    init = entry["request_init"]
    assert init["url"] == "/api/v2/workforcemanagement/adherence/historical/bulk"
    # POST, as the endpoint definition says -- the example payload's GET was wrong.
    assert init["method"] == "POST"
    assert init["type"] == "init"
    assert init["result_data"] == "jobId"
    assert init["payload"] == load_fixture("unitary.json")["adherence_historical_init"]["body_templante"]
    # userIds omitted: Genesys queries every user in the management unit.
    assert init["payload"]["items"][0]["managementUnitId"] == "{mu_id}"
    assert "userIds" not in init["payload"]["items"][0]

    status = entry["request_status"]
    assert status["url"] == "/api/v2/workforcemanagement/adherence/historical/bulk/jobs/{jobId}"
    assert status["path"] == "adherence_agent_details"
    assert status["result_data"] == "status"


def test_every_stage_uses_its_own_organizations_token_and_region(aws):
    """The adherence example had org-3's init/status stages carrying org-1's
    token and sae1 region -- each organization must use its own throughout."""
    event = {
        "tag": "funcionarios_adherencia",
        "organizations": [
            {"organization_id": "org-1", "ids": ["mu-1"]},
            {"organization_id": "org-3", "ids": ["mu-3"]},
        ],
    }

    result = run(event, _context())

    expected = {"org-1": ("sae1", "org-1", "org_id=1/"), "org-3": ("usw2", "org-3", "org_id=3/")}
    for organization_id, entry in payload_by_org(result).items():
        region_id, oauth, server_path = expected[organization_id]
        for stage_key in result["payloads"][0]["stages"]:
            stage = entry[stage_key]
            assert stage["base_url"] == f"https://api.{region_id}.pure.cloud"
            assert stage["headers"]["Authorization"] == f"Bearer token-for-{oauth}"
            assert stage["server_path"] == server_path


def test_one_token_per_organization_not_per_stage(aws, genesys_api):
    event = {
        "tag": "funcionarios_adherencia",
        "organizations": [
            {"organization_id": "org-1", "ids": ["mu-1"]},
            {"organization_id": "org-3", "ids": ["mu-3"]},
        ],
    }

    run(event, _context())

    assert genesys_api["requested_orgs"] == ["org-1", "org-3"]


def test_payload_file_is_written_under_its_tag_and_execution_id(aws):
    event = {"tag": "surveys", "organizations": [{"organization_id": "org-1", "ids": ["conv-1"]}]}

    result = run(event, _context("req-abc"))

    key = f"{PAYLOAD_PREFIX}/surveys/req-abc.json"
    assert result["payloads"][0]["payload_location"] == f"s3://{LOGS_BUCKET}/{key}"
    written = json.loads(aws["s3"].get_object(Bucket=LOGS_BUCKET, Key=key)["Body"].read())
    assert set(written) == {"tag", "organization", "failed_organizations"}
    assert written["tag"] == "surveys"
    assert written["failed_organizations"] == []


def test_payloads_for_different_tags_do_not_overwrite_each_other(aws):
    s3 = aws["s3"]
    organizations = [{"organization_id": "org-1", "ids": ["x"]}]

    run({"tag": "surveys", "organizations": organizations}, _context("same-id"))
    run({"tag": "funcionarios_adherencia", "organizations": organizations}, _context("same-id"))

    surveys = s3.get_object(Bucket=LOGS_BUCKET, Key=f"{PAYLOAD_PREFIX}/surveys/same-id.json")
    adherence = s3.get_object(
        Bucket=LOGS_BUCKET, Key=f"{PAYLOAD_PREFIX}/funcionarios_adherencia/same-id.json"
    )
    assert json.loads(surveys["Body"].read())["tag"] == "surveys"
    assert json.loads(adherence["Body"].read())["tag"] == "funcionarios_adherencia"


def test_several_tags_share_the_ids_and_one_token_per_organization(aws, genesys_api):
    _add_recordings_flow(aws["s3"])

    result = run({"tags": ["surveys", "recordings"], "organizations": TWO_ORGS}, _context("req-multi"))

    assert result["tags"] == ["surveys", "recordings"]
    assert [p["payload_location"] for p in result["payloads"]] == [
        f"s3://{LOGS_BUCKET}/{PAYLOAD_PREFIX}/surveys/req-multi.json",
        f"s3://{LOGS_BUCKET}/{PAYLOAD_PREFIX}/recordings/req-multi.json",
    ]
    surveys = payload_by_org(result, "surveys")["org-3"]
    recordings = payload_by_org(result, "recordings")["org-3"]
    assert surveys["request_context"]["url"] == "/api/v2/quality/conversations/{conversationId}/surveys"
    assert recordings["request_context"]["url"] == "/api/v2/conversations/{conversationId}/recordings"
    assert recordings["ids"] == surveys["ids"] == ["conv-3a", "conv-3b"]
    # Two tags, two organizations: one config load and one token per organization.
    assert len(genesys_api["load_config"]) == 1
    assert genesys_api["requested_orgs"] == ["org-1", "org-3"]


def test_each_tag_saves_under_its_own_domain_prefix_in_the_same_run(aws):
    event = {
        "tags": ["surveys", "funcionarios_adherencia"],
        "organizations": [{"organization_id": "org-1", "ids": ["x"]}],
    }

    result = run(event, _context())

    assert payload_by_org(result, "surveys")["org-1"]["request_context"]["base_path"] == GENESYS_BASE_PATH
    adherence = payload_by_org(result, "funcionarios_adherencia")["org-1"]
    assert adherence["request_init"]["base_path"] == GENESYS_BASE_PATH_WFM


def test_all_tags_runs_every_conversation_flow_and_nothing_else(aws):
    _add_recordings_flow(aws["s3"])

    result = run({"tags": "all", "organizations": TWO_ORGS}, _context())

    # funcionarios_adherencia takes {mu_id}, not conversation ids, so it isn't included.
    assert result["tags"] == ["recordings", "surveys"]
    assert [p["tag"] for p in result["payloads"]] == ["recordings", "surveys"]


def test_all_tags_without_any_conversation_flow_is_an_error(aws):
    aws["s3"].put_object(
        Bucket=RESOURCES_BUCKET, Key=CORE_CONFIG_KEY, Body=json.dumps({**CORE_CONFIG, "unitary": []})
    )

    with pytest.raises(EventError, match="found no endpoints taking"):
        run({"tags": "all", "organizations": TWO_ORGS}, _context())


def test_every_tag_is_validated_before_any_ids_are_read(aws):
    # The ids file doesn't exist: the bad tag must be rejected before it's read.
    event = {"tags": ["surveys", "nope"], "ids_location": {"bucket": "landing", "key": "does/not/exist.json"}}

    with pytest.raises(ValueError, match="No endpoints are tagged 'nope'"):
        run(event, _context())


def test_execution_id_falls_back_to_a_uuid_without_a_lambda_context(aws):
    result = run({"tag": "surveys", "organizations": []}, None)

    assert re.fullmatch(r"[0-9a-f-]{36}", result["execution_id"])
    assert result["payloads"][0]["payload_location"].endswith(f"/surveys/{result['execution_id']}.json")


def test_s3_object_created_event_through_eventbridge(aws):
    key = "empatia/conversations/2026-09-11.json"
    aws["s3"].put_object(
        Bucket=LANDING_BUCKET,
        Key=key,
        Body=json.dumps([{"organization_id": "org-3", "ids": ["conv-a", "conv-b"]}]),
    )
    event = {
        "source": "aws.s3",
        "detail-type": "Object Created",
        "detail": {"tag": "surveys", "bucket": {"name": LANDING_BUCKET}, "object": {"key": key}},
    }

    result = run(event, _context())

    assert payload_by_org(result)["org-3"]["ids"] == ["conv-a", "conv-b"]


def test_an_organization_without_a_servers_entry_is_reported_not_fatal(aws, genesys_api):
    event = {
        "tag": "surveys",
        "organizations": [
            {"organization_id": "org-1", "ids": ["conv-1"]},
            {"organization_id": "org-9", "ids": ["conv-9"]},
        ],
    }

    result = run(event, _context())

    assert list(payload_by_org(result)) == ["org-1"]
    assert result["failed_organizations"] == [
        {"organization_id": "org-9", "id_count": 1, "error": "no servers entry for org_9"}
    ]
    assert read_payload(result)["failed_organizations"] == [
        {"organization_id": "org-9", "ids": ["conv-9"], "error": "no servers entry for org_9"}
    ]
    assert genesys_api["requested_orgs"] == ["org-1"]


def test_an_organization_that_fails_is_left_out_of_every_payload(aws, monkeypatch):
    _add_recordings_flow(aws["s3"])

    def flaky_get_token(secret, connection, base_path, server, resource_name, now=None):
        if server["oauth"] == "org-1":
            raise RuntimeError("oauth down for org-1")
        return {"access_token": f"token-for-{server['oauth']}"}

    monkeypatch.setattr("src.main.get_token", flaky_get_token)

    result = run({"tags": ["surveys", "recordings"], "organizations": TWO_ORGS}, _context())

    assert result["failed_organizations"] == [
        {"organization_id": "org-1", "id_count": 1, "error": "oauth down for org-1"}
    ]
    for tag in ("surveys", "recordings"):
        payload = read_payload(result, tag)
        assert [entry["organization_id"] for entry in payload["organization"]] == ["org-3"]
        # The file keeps the failed organization's ids, so a re-run has them.
        assert payload["failed_organizations"] == [
            {"organization_id": "org-1", "ids": ["conv-1a"], "error": "oauth down for org-1"}
        ]


def test_no_ids_writes_empty_payloads_without_touching_genesys(aws, genesys_api):
    result = run({"tag": "surveys", "organizations": []}, _context("req-empty"))

    assert result["payloads"][0]["organizations"] == {}
    assert read_payload(result) == {"tag": "surveys", "organization": [], "failed_organizations": []}
    assert genesys_api["load_config"] == []


def test_unknown_tag_fails_before_reading_any_ids(aws):
    event = {"tag": "nope", "ids_location": {"bucket": "landing", "key": "does/not/exist.json"}}

    with pytest.raises(ValueError, match="No endpoints are tagged 'nope'"):
        run(event, _context())


def test_funcionarios_flows_are_saved_under_base_path_wfm(aws):
    event = {
        "tag": "funcionarios_adherencia",
        "organizations": [{"organization_id": "org-1", "ids": ["mu-1"]}],
    }

    result = run(event, _context())

    entry = payload_by_org(result)["org-1"]
    for stage_key in result["payloads"][0]["stages"]:
        assert entry[stage_key]["base_path"] == GENESYS_BASE_PATH_WFM


def test_the_token_cache_key_stays_on_base_path_for_every_flow(aws, genesys_api):
    """Tokens are per organization: keying the cache on base_path_wfm for
    funcionarios flows would miss the cached token and mint a second one."""
    organizations = [{"organization_id": "org-1", "ids": ["x"]}]

    run({"tag": "funcionarios_adherencia", "organizations": organizations}, _context("a"))
    run({"tag": "surveys", "organizations": organizations}, _context("b"))

    assert genesys_api["token_base_paths"] == [GENESYS_BASE_PATH, GENESYS_BASE_PATH]
