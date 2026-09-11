import json
import re
from types import SimpleNamespace

import pytest

from src.main import handler
from test.conftest import GENESYS_BASE_PATH, GENESYS_BASE_PATH_WFM, LANDING_BUCKET, LOGS_BUCKET, load_fixture

PAYLOAD_PREFIX = "transacciones/genesys/api/payload_request_unitary"


def _context(request_id: str = "req-123"):
    return SimpleNamespace(aws_request_id=request_id)


def _by_org(result: dict) -> dict:
    return {entry["organization_id"]: entry for entry in result["organization"]}


def _read(s3, bucket: str, key: str):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def test_surveys_payload_has_one_direct_stage_per_organization(aws, genesys_api):
    event = {
        "tag": "surveys",
        "organizations": [
            {"organization_id": "org-3", "ids": ["conv-3b", "conv-3a"]},
            {"organization_id": "org-1", "ids": ["conv-1a"]},
        ],
    }

    result = handler(event, _context())

    assert result["tag"] == "surveys"
    assert result["stages"] == ["request_context"]
    assert result["failed_organizations"] == []
    assert [entry["organization_id"] for entry in result["organization"]] == ["org-1", "org-3"]

    assert _by_org(result)["org-3"] == {
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


def test_adherence_payload_has_context_init_and_status_stages(aws):
    s3 = aws["s3"]
    key = "funcionarios/genesys/management_units/2026-09-11.json"
    s3.put_object(
        Bucket=LANDING_BUCKET,
        Key=key,
        Body=json.dumps({"organizations": [{"organization_id": "org-1", "ids": ["mu-2", "mu-1"]}]}),
    )

    result = handler(
        {"tag": "funcionarios_adherencia", "ids_location": {"bucket": "landing", "key": key}},
        _context(),
    )

    assert result["stages"] == ["request_context", "request_init", "request_status"]
    entry = _by_org(result)["org-1"]
    assert entry["ids"] == ["mu-1", "mu-2"]
    assert entry["request_context"]["url"] == "/api/v2/workforcemanagement/managementunits/{mu_id}/users"
    assert entry["request_context"]["path"] == "users_managment_unit"

    init = entry["request_init"]
    assert init["url"] == "/api/v2/workforcemanagement/adherence/historical/bulk"
    # POST, as the endpoint definition says -- the example payload's GET was wrong.
    assert init["method"] == "POST"
    assert init["type"] == "init"
    assert init["result_data"] == "jobId"
    assert init["payload"] == load_fixture("unitary.json")["adherence_historical_init"]["body_templante"]

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

    result = handler(event, _context())

    expected = {"org-1": ("sae1", "org-1", "org_id=1/"), "org-3": ("usw2", "org-3", "org_id=3/")}
    for organization_id, entry in _by_org(result).items():
        region_id, oauth, server_path = expected[organization_id]
        for stage_key in result["stages"]:
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

    handler(event, _context())

    assert genesys_api["requested_orgs"] == ["org-1", "org-3"]


def test_payload_is_written_under_its_tag_and_execution_id(aws):
    s3 = aws["s3"]
    event = {"tag": "surveys", "organizations": [{"organization_id": "org-1", "ids": ["conv-1"]}]}

    result = handler(event, _context("req-abc"))

    key = f"{PAYLOAD_PREFIX}/surveys/req-abc.json"
    assert result["payload_location"] == f"s3://{LOGS_BUCKET}/{key}"
    assert _read(s3, LOGS_BUCKET, key) == {"tag": "surveys", "organization": result["organization"]}


def test_payloads_for_different_tags_do_not_overwrite_each_other(aws):
    s3 = aws["s3"]
    organizations = [{"organization_id": "org-1", "ids": ["x"]}]

    handler({"tag": "surveys", "organizations": organizations}, _context("same-id"))
    handler({"tag": "funcionarios_adherencia", "organizations": organizations}, _context("same-id"))

    assert _read(s3, LOGS_BUCKET, f"{PAYLOAD_PREFIX}/surveys/same-id.json")["tag"] == "surveys"
    adherence = _read(s3, LOGS_BUCKET, f"{PAYLOAD_PREFIX}/funcionarios_adherencia/same-id.json")
    assert adherence["tag"] == "funcionarios_adherencia"


def test_execution_id_falls_back_to_a_uuid_without_a_lambda_context(aws):
    result = handler({"tag": "surveys", "organizations": []}, None)

    assert re.fullmatch(r"[0-9a-f-]{36}", result["execution_id"])
    assert result["payload_location"].endswith(f"/surveys/{result['execution_id']}.json")


def test_s3_object_created_event_through_eventbridge(aws):
    s3 = aws["s3"]
    key = "empatia/conversations/2026-09-11.json"
    s3.put_object(
        Bucket=LANDING_BUCKET,
        Key=key,
        Body=json.dumps([{"organization_id": "org-3", "ids": ["conv-a", "conv-b"]}]),
    )
    event = {
        "source": "aws.s3",
        "detail-type": "Object Created",
        "detail": {"tag": "surveys", "bucket": {"name": LANDING_BUCKET}, "object": {"key": key}},
    }

    result = handler(event, _context())

    assert _by_org(result)["org-3"]["ids"] == ["conv-a", "conv-b"]


def test_an_organization_without_a_servers_entry_is_reported_not_fatal(aws, genesys_api):
    event = {
        "tag": "surveys",
        "organizations": [
            {"organization_id": "org-1", "ids": ["conv-1"]},
            {"organization_id": "org-9", "ids": ["conv-9"]},
        ],
    }

    result = handler(event, _context())

    assert list(_by_org(result)) == ["org-1"]
    assert result["failed_organizations"] == [
        {"organization_id": "org-9", "error": "no servers entry for org_9"}
    ]
    assert genesys_api["requested_orgs"] == ["org-1"]


def test_a_token_failure_for_one_organization_does_not_drop_the_others(aws, monkeypatch):
    def flaky_get_token(secret, connection, base_path, server, resource_name, now=None):
        if server["oauth"] == "org-1":
            raise RuntimeError("oauth down for org-1")
        return {"access_token": f"token-for-{server['oauth']}"}

    monkeypatch.setattr("src.main.get_token", flaky_get_token)
    event = {
        "tag": "surveys",
        "organizations": [
            {"organization_id": "org-1", "ids": ["conv-1"]},
            {"organization_id": "org-3", "ids": ["conv-3"]},
        ],
    }

    result = handler(event, _context())

    assert list(_by_org(result)) == ["org-3"]
    assert result["failed_organizations"] == [{"organization_id": "org-1", "error": "oauth down for org-1"}]


def test_no_ids_writes_an_empty_payload_without_touching_genesys(aws, genesys_api):
    s3 = aws["s3"]

    result = handler({"tag": "surveys", "organizations": []}, _context("req-empty"))

    assert result["organization"] == []
    assert genesys_api["load_config"] == []
    assert _read(s3, LOGS_BUCKET, f"{PAYLOAD_PREFIX}/surveys/req-empty.json") == {
        "tag": "surveys",
        "organization": [],
    }


def test_unknown_tag_fails_before_reading_any_ids(aws):
    # The ids file doesn't exist: the tag must be rejected before it's read.
    event = {"tag": "nope", "ids_location": {"bucket": "landing", "key": "does/not/exist.json"}}

    with pytest.raises(ValueError, match="No endpoints are tagged 'nope'"):
        handler(event, _context())


def test_funcionarios_flows_are_saved_under_base_path_wfm(aws):
    event = {
        "tag": "funcionarios_adherencia",
        "organizations": [{"organization_id": "org-1", "ids": ["mu-1"]}],
    }

    result = handler(event, _context())

    entry = _by_org(result)["org-1"]
    for stage_key in result["stages"]:
        assert entry[stage_key]["base_path"] == GENESYS_BASE_PATH_WFM


def test_the_token_cache_key_stays_on_base_path_for_every_flow(aws, genesys_api):
    """Tokens are per organization: keying the cache on base_path_wfm for
    funcionarios flows would miss the cached token and mint a second one."""
    organizations = [{"organization_id": "org-1", "ids": ["x"]}]

    handler({"tag": "funcionarios_adherencia", "organizations": organizations}, _context("a"))
    handler({"tag": "surveys", "organizations": organizations}, _context("b"))

    assert genesys_api["token_base_paths"] == [GENESYS_BASE_PATH, GENESYS_BASE_PATH]
