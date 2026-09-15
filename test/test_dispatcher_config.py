import json

import pytest

from src.config import load_settings
from src.dispatcher_config import (
    DispatcherConfigError,
    conversation_tags,
    flow_config,
    load_dispatcher_config,
    output_base_path_key,
)
from test.conftest import DISPATCHER_CONFIG_KEY, RESOURCES_BUCKET

CONFIG = {
    "domains": {
        "transacciones": {"output_base_path_key": "base_path"},
        "funcionarios": {"output_base_path_key": "base_path_wfm"},
    },
    "flows": {
        "surveys": {"enabled": True, "domain": "transacciones", "id_kind": "conversation"},
        "transcripts": {"enabled": True, "domain": "transacciones", "id_kind": "conversation"},
        "funcionarios_adherencia": {"enabled": True, "domain": "funcionarios", "id_kind": "management_unit"},
        "retired_flow": {"enabled": False, "domain": "transacciones", "id_kind": "conversation"},
    },
}


def test_load_dispatcher_config_reads_and_validates(aws):
    config = load_dispatcher_config(aws["s3"], load_settings())

    assert "surveys" in config["flows"]
    assert config["domains"]["funcionarios"]["output_base_path_key"] == "base_path_wfm"


@pytest.mark.parametrize(
    "broken, message",
    [
        ({}, "needs a top-level 'domains'"),
        ({"domains": {}}, "needs a top-level 'flows'"),
        ({"domains": {"x": {}}, "flows": {}}, "needs a non-empty 'output_base_path_key'"),
        ({"domains": {}, "flows": {"surveys": {}}}, "needs an 'enabled' boolean"),
        (
            {
                "domains": {"a": {"output_base_path_key": "base_path"}},
                "flows": {"surveys": {"enabled": True, "domain": "nope"}},
            },
            "is not in 'domains'",
        ),
    ],
)
def test_malformed_dispatcher_config_is_rejected(aws, broken, message):
    aws["s3"].put_object(Bucket=RESOURCES_BUCKET, Key=DISPATCHER_CONFIG_KEY, Body=json.dumps(broken))

    with pytest.raises(DispatcherConfigError, match=message):
        load_dispatcher_config(aws["s3"], load_settings())


def test_flow_config_returns_the_declared_flow():
    assert flow_config(CONFIG, "surveys")["domain"] == "transacciones"


def test_flow_config_rejects_an_undeclared_tag():
    with pytest.raises(DispatcherConfigError, match="'nope' is not declared"):
        flow_config(CONFIG, "nope")


def test_flow_config_rejects_a_disabled_flow():
    with pytest.raises(DispatcherConfigError, match="'retired_flow' is disabled"):
        flow_config(CONFIG, "retired_flow")


def test_output_base_path_key_follows_the_flows_domain():
    assert output_base_path_key(CONFIG, "surveys") == "base_path"
    assert output_base_path_key(CONFIG, "funcionarios_adherencia") == "base_path_wfm"


def test_conversation_tags_are_every_enabled_conversation_id_kind_flow():
    # retired_flow is id_kind "conversation" but disabled, so it's excluded.
    assert conversation_tags(CONFIG) == ["surveys", "transcripts"]
