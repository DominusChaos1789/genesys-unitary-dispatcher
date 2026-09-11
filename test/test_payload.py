from src.payload import build_organization_entry, build_payload, build_stage
from test.conftest import GENESYS_BASE_PATH, GENESYS_CONNECTION, GENESYS_SERVERS

TOKEN = {"access_token": "abc"}
SERVER = GENESYS_SERVERS["org_3"]


def _stage(spec):
    return build_stage(
        spec, token=TOKEN, connection=GENESYS_CONNECTION, server=SERVER, base_path=GENESYS_BASE_PATH
    )


def test_only_the_region_and_token_are_rendered():
    stage = _stage({"url": "/api/v2/things/{conversationId}", "method": "GET", "type": "unitary"})

    assert stage["base_url"] == "https://api.usw2.pure.cloud"
    assert stage["headers"] == {"Authorization": "Bearer abc", "Content-Type": "application/json"}
    assert stage["url"] == "/api/v2/things/{conversationId}"
    assert stage["server_path"] == "org_id=3/"
    assert stage["base_path"] == GENESYS_BASE_PATH


def test_a_stage_without_a_body_or_params_has_neither_key():
    stage = _stage({"url": "/x", "method": "GET", "type": "unitary"})

    assert "payload" not in stage
    assert "params" not in stage


def test_body_templante_becomes_the_stage_payload_unrendered():
    body = {"items": [{"managementUnitId": "{mu_id}", "startDate": "{star_date}"}]}

    stage = _stage({"url": "/x", "method": "POST", "type": "init", "body_templante": body})

    assert stage["payload"] == body


def test_the_correctly_spelled_body_template_is_accepted_too():
    stage = _stage({"url": "/x", "method": "POST", "type": "init", "body_template": {"a": "{b}"}})

    assert stage["payload"] == {"a": "{b}"}


def test_params_template_is_carried_as_params():
    stage = _stage(
        {"url": "/x", "method": "GET", "type": "unitary", "params_template": {"pageSize": "{page_size}"}}
    )

    assert stage["params"] == {"pageSize": "{page_size}"}


def test_missing_optional_fields_are_none_not_absent():
    stage = _stage({"url": "/x"})

    assert stage["method"] is None
    assert stage["type"] is None
    assert stage["path"] is None
    assert stage["result_data"] is None


def test_organization_entry_has_ids_then_one_key_per_stage():
    stages = {
        "request_context": ("ctx", {"url": "/ctx", "method": "GET", "type": "unitary"}),
        "request_init": ("init", {"url": "/init", "method": "POST", "type": "init"}),
    }

    entry = build_organization_entry(
        "org-3",
        ["a", "b"],
        stages,
        token=TOKEN,
        connection=GENESYS_CONNECTION,
        server=SERVER,
        base_path=GENESYS_BASE_PATH,
    )

    assert list(entry) == ["organization_id", "ids", "request_context", "request_init"]
    assert entry["request_init"]["url"] == "/init"


def test_payload_shape():
    assert build_payload("surveys", [{"organization_id": "org-1"}]) == {
        "tag": "surveys",
        "organization": [{"organization_id": "org-1"}],
    }
