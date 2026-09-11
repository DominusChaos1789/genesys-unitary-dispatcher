import json

import pytest

from src.config import load_settings
from src.endpoints import load_endpoint_catalog, select_stages
from test.conftest import CORE_CONFIG_KEY, ENDPOINTS_PREFIX, RESOURCES_BUCKET, load_fixture


def _catalog() -> dict:
    return {**load_fixture("unitary.json"), **load_fixture("status.json")}


def test_catalog_loads_only_the_configured_groups(aws):
    # "ondemand" points at upload.json, which was never uploaded: reading it would raise.
    catalog = load_endpoint_catalog(aws["s3"], load_settings())

    assert "conversations_surveys" in catalog  # unitary group
    assert "adherence_agent_status" in catalog  # status group
    assert "conversations_details_init" not in catalog  # daily group, not configured


def test_catalog_groups_are_configurable(aws, monkeypatch):
    monkeypatch.setenv("ENDPOINT_GROUPS", "unitary, daily")

    catalog = load_endpoint_catalog(aws["s3"], load_settings())

    assert "conversations_details_init" in catalog
    assert "adherence_agent_status" not in catalog


def test_a_group_missing_from_core_config_is_an_error(aws, monkeypatch):
    monkeypatch.setenv("ENDPOINT_GROUPS", "unitary,actions")

    with pytest.raises(ValueError, match="no 'actions' group"):
        load_endpoint_catalog(aws["s3"], load_settings())


def test_core_config_may_wrap_groups_and_use_single_string_references(aws):
    aws["s3"].put_object(
        Bucket=RESOURCES_BUCKET,
        Key=CORE_CONFIG_KEY,
        Body=json.dumps(
            {
                "endpoints": {
                    "unitary": f"{ENDPOINTS_PREFIX}/unitary.json",
                    "status": [f"{ENDPOINTS_PREFIX}/status.json"],
                }
            }
        ),
    )

    catalog = load_endpoint_catalog(aws["s3"], load_settings())

    assert "conversations_surveys" in catalog
    assert "adherence_agent_status" in catalog


def test_an_endpoint_defined_differently_in_two_files_is_an_error(aws):
    s3 = aws["s3"]
    s3.put_object(
        Bucket=RESOURCES_BUCKET,
        Key=f"{ENDPOINTS_PREFIX}/other.json",
        Body=json.dumps(
            {"conversations_surveys": {"url": "/different", "tag": "surveys", "type": "unitary"}}
        ),
    )
    s3.put_object(
        Bucket=RESOURCES_BUCKET,
        Key=CORE_CONFIG_KEY,
        Body=json.dumps(
            {
                "unitary": [f"{ENDPOINTS_PREFIX}/unitary.json", f"{ENDPOINTS_PREFIX}/other.json"],
                "status": [f"{ENDPOINTS_PREFIX}/status.json"],
            }
        ),
    )

    with pytest.raises(ValueError, match="'conversations_surveys' is defined differently"):
        load_endpoint_catalog(s3, load_settings())


def test_the_same_file_referenced_twice_is_tolerated(aws):
    s3 = aws["s3"]
    s3.put_object(
        Bucket=RESOURCES_BUCKET,
        Key=CORE_CONFIG_KEY,
        Body=json.dumps(
            {
                "unitary": [f"{ENDPOINTS_PREFIX}/unitary.json", f"{ENDPOINTS_PREFIX}/unitary.json"],
                "status": [f"{ENDPOINTS_PREFIX}/status.json"],
            }
        ),
    )

    assert "conversations_surveys" in load_endpoint_catalog(s3, load_settings())


def test_surveys_is_a_single_direct_stage():
    stages = select_stages(_catalog(), "surveys")

    assert list(stages) == ["request_context"]
    assert stages["request_context"][0] == "conversations_surveys"


def test_adherence_is_init_then_status_without_a_users_listing():
    stages = select_stages(_catalog(), "funcionarios_adherencia")

    assert list(stages) == ["request_init", "request_status"]
    assert [name for name, _ in stages.values()] == ["adherence_historical_init", "adherence_agent_status"]


def test_stages_come_out_in_flow_order_whatever_the_catalog_order():
    catalog = {
        "demo_status": {"tag": "demo", "type": "status", "url": "/s"},
        "demo_init": {"tag": "demo", "type": "init", "url": "/i"},
        "demo_context": {"tag": "demo", "type": "unitary", "url": "/c"},
    }

    assert list(select_stages(catalog, "demo")) == ["request_context", "request_init", "request_status"]


def test_untagged_endpoints_belong_to_no_flow():
    # status.json holds several untagged "status" endpoints. Had any been picked
    # up, adherence would have two request_status endpoints and raise.
    stages = select_stages(_catalog(), "funcionarios_adherencia")

    assert stages["request_status"][0] == "adherence_agent_status"


def test_unknown_tag_is_an_error():
    with pytest.raises(ValueError, match="No endpoints are tagged 'nope'"):
        select_stages(_catalog(), "nope")


def test_two_endpoints_for_the_same_stage_is_an_error():
    catalog = _catalog()
    catalog["another_surveys"] = {"tag": "surveys", "type": "unitary", "url": "/x"}

    with pytest.raises(ValueError, match="two request_context endpoints"):
        select_stages(catalog, "surveys")


def test_a_tagged_endpoint_whose_type_maps_to_no_stage_is_an_error():
    catalog = _catalog()
    catalog["surveys_results"] = {"tag": "surveys", "type": "result", "url": "/x"}

    with pytest.raises(ValueError, match="'result', which maps to no stage"):
        select_stages(catalog, "surveys")
