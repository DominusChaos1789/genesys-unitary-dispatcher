import json
import re
from types import SimpleNamespace

import pytest

from src.dispatcher_config import DispatcherConfigError
from src.main import handler, run
from src.sources import EventError
from test.conftest import (
    GENESYS_BASE_PATH,
    GENESYS_BASE_PATH_WFM,
    LANDING_BUCKET,
    LOGS_BUCKET,
    RESOURCES_BUCKET,
    add_dispatcher_flow,
    payload_by_org,
    read_payload,
    responses_for,
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
    """A second conversation flow next to surveys: its endpoint plus its
    dispatcher.json entry."""
    s3.put_object(
        Bucket=RESOURCES_BUCKET,
        Key="params/genesys/api/recordings.json",
        Body=json.dumps(RECORDINGS_ENDPOINT),
    )
    from test.conftest import CORE_CONFIG, CORE_CONFIG_KEY, ENDPOINTS_PREFIX

    core = {**CORE_CONFIG, "unitary": [*CORE_CONFIG["unitary"], f"{ENDPOINTS_PREFIX}/recordings.json"]}
    s3.put_object(Bucket=RESOURCES_BUCKET, Key=CORE_CONFIG_KEY, Body=json.dumps(core))
    add_dispatcher_flow(s3, "recordings")


def test_surveys_writes_one_flat_file_per_organization(aws, genesys_api):
    result = run({"tag": "surveys", "organizations": TWO_ORGS}, _context())

    assert result["tags"] == ["surveys"]
    assert result["failed_organizations"] == []
    responses = sorted(result["responses"], key=lambda r: r["organization_id"])
    assert responses == [
        {
            "bucket": LOGS_BUCKET,
            "payload_location": f"{PAYLOAD_PREFIX}/surveys/org-1.json",
            "organization_id": "org-1",
            "stages": ["request_context"],
            "failed_organizations": [],
            "tag": "surveys",
        },
        {
            "bucket": LOGS_BUCKET,
            "payload_location": f"{PAYLOAD_PREFIX}/surveys/org-3.json",
            "organization_id": "org-3",
            "stages": ["request_context"],
            "failed_organizations": [],
            "tag": "surveys",
        },
    ]

    # The file itself is flat: no "organization" array to unpack.
    assert read_payload(result, organization_id="org-3") == {
        "tag": "surveys",
        "organization_id": "org-3",
        "ids": ["conv-3a", "conv-3b"],
        "failed_organizations": [],
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


def test_the_response_carries_locations_never_the_payload(aws):
    """Step Functions caps state data at 256 KB. A day of conversations is more
    than that, so the ids only live in the payload file."""
    ids = [f"conversation-{n:05d}-0000-4000-8000-000000000000" for n in range(6000)]

    result = run({"tag": "surveys", "organizations": [{"organization_id": "org-1", "ids": ids}]}, _context())

    assert len(json.dumps(result["responses"])) < 1_000
    assert len(read_payload(result)["ids"]) == 6000


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
    assert responses_for(result)[0]["stages"] == ["request_init", "request_status"]
    entry = read_payload(result)
    assert entry["ids"] == ["mu-1", "mu-2"]
    assert "request_context" not in entry

    init = entry["request_init"]
    assert init["url"] == "/api/v2/workforcemanagement/adherence/historical/bulk"
    # POST, as the endpoint definition says.
    assert init["method"] == "POST"
    assert init["type"] == "init"
    assert init["result_data"] == "jobId"
    # userIds omitted: Genesys queries every user in the management unit.
    assert init["payload"]["items"][0]["managementUnitId"] == "{mu_id}"
    assert "userIds" not in init["payload"]["items"][0]
    assert init["base_path"] == GENESYS_BASE_PATH_WFM

    status = entry["request_status"]
    assert status["url"] == "/api/v2/workforcemanagement/adherence/historical/bulk/jobs/{jobId}"
    assert status["path"] == "adherence_agent_details"
    assert status["result_data"] == "status"


def test_transcripts_is_search_then_url(aws):
    result = run(
        {"tag": "transcripts", "organizations": [{"organization_id": "org-1", "ids": ["conv-1"]}]}, _context()
    )

    assert responses_for(result)[0]["stages"] == ["request_context", "request_url"]
    entry = read_payload(result)
    search = entry["request_context"]
    assert search["url"] == "/api/v2/speechandtextanalytics/transcripts/search"
    assert search["method"] == "POST"
    assert search["type"] == "unitary"

    url_stage = entry["request_url"]
    assert url_stage["url"].startswith(
        "/api/v2/speechandtextanalytics/conversations/{conversationId}/communications/"
    )
    assert url_stage["url"].endswith("/transcripturl")
    assert url_stage["type"] == "url"
    # communicationId only comes from the search result, so it stays a placeholder.
    assert "{communicationId}" in url_stage["url"]


def test_every_stage_uses_its_own_organizations_token_and_region(aws):
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
        for stage_key in ("request_init", "request_status"):
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


def test_payload_file_is_written_under_tag_and_organization(aws):
    event = {"tag": "surveys", "organizations": [{"organization_id": "org-1", "ids": ["conv-1"]}]}

    result = run(event, _context("req-abc"))

    key = f"{PAYLOAD_PREFIX}/surveys/org-1.json"
    assert responses_for(result)[0]["payload_location"] == key
    written = json.loads(aws["s3"].get_object(Bucket=LOGS_BUCKET, Key=key)["Body"].read())
    assert set(written) == {"tag", "organization_id", "ids", "request_context", "failed_organizations"}
    assert written["tag"] == "surveys"
    assert written["failed_organizations"] == []


def test_a_second_run_for_the_same_tag_and_organization_overwrites_the_file(aws):
    event_a = {"tag": "surveys", "organizations": [{"organization_id": "org-1", "ids": ["conv-1"]}]}
    event_b = {"tag": "surveys", "organizations": [{"organization_id": "org-1", "ids": ["conv-2"]}]}

    result_a = run(event_a, _context("req-a"))
    result_b = run(event_b, _context("req-b"))

    # Same fixed key both times -- no execution id in the path.
    key = f"{PAYLOAD_PREFIX}/surveys/org-1.json"
    assert responses_for(result_a)[0]["payload_location"] == key
    assert responses_for(result_b)[0]["payload_location"] == key
    # The second run's content replaced the first's.
    assert read_payload(result_b)["ids"] == ["conv-2"]


def test_payloads_for_different_tags_do_not_overwrite_each_other(aws):
    s3 = aws["s3"]
    organizations = [{"organization_id": "org-1", "ids": ["x"]}]

    run({"tag": "surveys", "organizations": organizations}, _context("same-id"))
    run({"tag": "funcionarios_adherencia", "organizations": organizations}, _context("same-id"))

    surveys = s3.get_object(Bucket=LOGS_BUCKET, Key=f"{PAYLOAD_PREFIX}/surveys/org-1.json")
    adherence = s3.get_object(Bucket=LOGS_BUCKET, Key=f"{PAYLOAD_PREFIX}/funcionarios_adherencia/org-1.json")
    assert json.loads(surveys["Body"].read())["tag"] == "surveys"
    assert json.loads(adherence["Body"].read())["tag"] == "funcionarios_adherencia"


def test_several_tags_share_the_ids_and_one_token_per_organization(aws, genesys_api):
    _add_recordings_flow(aws["s3"])

    result = run({"tags": ["surveys", "recordings"], "organizations": TWO_ORGS}, _context("req-multi"))

    assert result["tags"] == ["surveys", "recordings"]
    locations = sorted(r["payload_location"] for r in result["responses"])
    assert locations == [
        f"{PAYLOAD_PREFIX}/recordings/org-1.json",
        f"{PAYLOAD_PREFIX}/recordings/org-3.json",
        f"{PAYLOAD_PREFIX}/surveys/org-1.json",
        f"{PAYLOAD_PREFIX}/surveys/org-3.json",
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

    # funcionarios_adherencia takes management-unit ids, not conversation ids.
    assert result["tags"] == ["recordings", "surveys", "transcripts"]


def test_all_tags_without_any_conversation_flow_is_an_error(aws):
    s3 = aws["s3"]
    from test.conftest import DISPATCHER_CONFIG_KEY

    config = json.loads(s3.get_object(Bucket=RESOURCES_BUCKET, Key=DISPATCHER_CONFIG_KEY)["Body"].read())
    for flow in config["flows"].values():
        flow["enabled"] = flow.get("id_kind") not in ("conversation", "division")
    s3.put_object(Bucket=RESOURCES_BUCKET, Key=DISPATCHER_CONFIG_KEY, Body=json.dumps(config))

    with pytest.raises(EventError, match='found no enabled flow with id_kind "conversation" or "division"'):
        run({"tags": "all", "organizations": TWO_ORGS}, _context())


def test_a_tag_not_declared_in_dispatcher_json_is_rejected(aws):
    with pytest.raises(DispatcherConfigError, match="not declared in dispatcher.json"):
        run({"tag": "not_a_real_flow", "organizations": TWO_ORGS}, _context())


def test_a_disabled_tag_is_rejected_before_any_ids_are_read(aws):
    s3 = aws["s3"]
    from test.conftest import DISPATCHER_CONFIG_KEY

    config = json.loads(s3.get_object(Bucket=RESOURCES_BUCKET, Key=DISPATCHER_CONFIG_KEY)["Body"].read())
    config["flows"]["surveys"]["enabled"] = False
    s3.put_object(Bucket=RESOURCES_BUCKET, Key=DISPATCHER_CONFIG_KEY, Body=json.dumps(config))

    with pytest.raises(DispatcherConfigError, match="'surveys' is disabled"):
        run(
            {"tag": "surveys", "ids_location": {"bucket": "landing", "key": "does/not/exist.json"}},
            _context(),
        )


def test_every_tag_is_validated_before_any_ids_are_read(aws):
    event = {"tags": ["surveys", "nope"], "ids_location": {"bucket": "landing", "key": "does/not/exist.json"}}

    with pytest.raises(DispatcherConfigError, match="not declared in dispatcher.json"):
        run(event, _context())


def test_execution_id_falls_back_to_a_uuid_without_a_lambda_context(aws):
    result = run({"tag": "surveys", "organizations": []}, None)

    assert re.fullmatch(r"[0-9a-f-]{36}", result["execution_id"])
    assert result["responses"] == []


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

    assert read_payload(result)["ids"] == ["conv-a", "conv-b"]


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
    # The surviving organization's own file still lists the failure, with ids,
    # so a re-run has them.
    assert read_payload(result)["failed_organizations"] == [
        {"organization_id": "org-9", "ids": ["conv-9"], "error": "no servers entry for org_9"}
    ]
    assert genesys_api["requested_orgs"] == ["org-1"]


def test_a_failing_organization_gets_no_file_in_any_tag(aws, monkeypatch):
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
        by_org = payload_by_org(result, tag)
        assert list(by_org) == ["org-3"]
        assert by_org["org-3"]["failed_organizations"] == [
            {"organization_id": "org-1", "ids": ["conv-1a"], "error": "oauth down for org-1"}
        ]


def test_no_ids_writes_no_files_and_does_not_touch_genesys(aws, genesys_api):
    result = run({"tag": "surveys", "organizations": []}, _context("req-empty"))

    assert result["responses"] == []
    assert genesys_api["load_config"] == []


def test_unknown_tag_fails_before_reading_any_ids(aws):
    event = {"tag": "nope", "ids_location": {"bucket": "landing", "key": "does/not/exist.json"}}

    with pytest.raises(DispatcherConfigError, match="not declared in dispatcher.json"):
        run(event, _context())


def test_the_token_cache_key_stays_on_base_path_for_every_flow(aws, genesys_api):
    """Tokens are per organization: keying the cache on base_path_wfm for
    funcionarios flows would miss the cached token and mint a second one."""
    organizations = [{"organization_id": "org-1", "ids": ["x"]}]

    run({"tag": "funcionarios_adherencia", "organizations": organizations}, _context("a"))
    run({"tag": "surveys", "organizations": organizations}, _context("b"))

    assert genesys_api["token_base_paths"] == [GENESYS_BASE_PATH, GENESYS_BASE_PATH]


def test_handler_always_returns_a_flat_list(aws):
    single = handler(
        {"tag": "surveys", "organizations": [{"organization_id": "org-1", "ids": ["conv-1"]}]}, _context()
    )
    assert isinstance(single, list)
    assert len(single) == 1
    assert single[0]["organization_id"] == "org-1"

    two_orgs = handler({"tag": "surveys", "organizations": TWO_ORGS}, _context())
    assert isinstance(two_orgs, list)
    assert len(two_orgs) == 2
