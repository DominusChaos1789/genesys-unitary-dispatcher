import pytest

from src.config import load_settings, normalize_env_token


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("dev", "dev"),
        ("stg", "stg"),
        ("pro", "pro"),
        ("augusta-nexa-pro", "pro"),
        ("augusta-nexa-stg-", "stg"),
        (" dev- ", "dev"),
    ],
)
def test_normalize_env_token(raw, expected):
    assert normalize_env_token(raw) == expected


def test_defaults_to_dev_when_nothing_is_set(monkeypatch):
    monkeypatch.delenv("ENVIRONMENT", raising=False)

    assert load_settings().env == "dev"


def test_env_prefix_takes_precedence_over_environment(monkeypatch):
    monkeypatch.setenv("ENV_PREFIX", "pro")
    monkeypatch.setenv("ENVIRONMENT", "stg")

    assert load_settings().env == "pro"


def test_stack_id_is_the_last_fallback(monkeypatch):
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setenv("STACK_ID", "augusta-nexa-pro")

    assert load_settings().env == "pro"


def test_every_default_follows_the_environment(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "stg")

    settings = load_settings()

    assert settings.resources_bucket == "augusta-nexa-stg-resources"
    assert settings.payload_log_bucket == "augusta-nexa-stg-logs"
    assert settings.api_genesys_params == "/augusta-nexa-stg/genesys/api"
    assert settings.resource_name == "augusta-nexa-stg-genesys-api-unitary-request"
    assert settings.core_config_key == "params/genesys/api/core.json"
    assert settings.endpoint_groups == ("unitary", "status")
    assert settings.region == "us-east-2"


def test_bucket_variables_accept_logical_or_full_names(monkeypatch):
    # The pipeline passes RESOURCES_BUCKET: "resources" -- a logical name.
    monkeypatch.setenv("RESOURCES_BUCKET", "resources")
    monkeypatch.setenv("PAYLOAD_LOG_BUCKET", "augusta-nexa-dev-logs-archive")

    settings = load_settings()

    assert settings.resources_bucket == "augusta-nexa-dev-resources"
    assert settings.payload_log_bucket == "augusta-nexa-dev-logs-archive"


def test_explicit_values_override_the_derived_defaults(monkeypatch):
    monkeypatch.setenv("API_GENESYS_PARAMS", "/custom/path")
    monkeypatch.setenv("RESOURCE_NAME", "custom-resource")
    monkeypatch.setenv("REGION", "us-west-2")
    monkeypatch.setenv("CORE_CONFIG_KEY", "params/other/core.json")

    settings = load_settings()

    assert settings.api_genesys_params == "/custom/path"
    assert settings.resource_name == "custom-resource"
    assert settings.region == "us-west-2"
    assert settings.core_config_key == "params/other/core.json"


def test_endpoint_groups_are_a_comma_separated_list(monkeypatch):
    monkeypatch.setenv("ENDPOINT_GROUPS", " unitary , status,, daily ")

    assert load_settings().endpoint_groups == ("unitary", "status", "daily")


def test_resolve_bucket():
    settings = load_settings()

    assert settings.resolve_bucket("landing") == "augusta-nexa-dev-landing"
    assert settings.resolve_bucket("augusta-nexa-dev-landing") == "augusta-nexa-dev-landing"


def test_payload_log_key_template(monkeypatch):
    # A fixed key per (tag, organization) -- no execution id -- so each run
    # overwrites the previous payload for that pair.
    assert load_settings().payload_log_key("surveys", "org-3") == (
        "transacciones/genesys/api/payload_request_unitary/surveys/org-3.json"
    )

    monkeypatch.setenv("PAYLOAD_LOG_KEY_TEMPLATE", "logs/{tag}/{organization_id}.json")

    assert load_settings().payload_log_key("surveys", "org-3") == "logs/surveys/org-3.json"


def test_dispatcher_config_key_default_and_override(monkeypatch):
    assert load_settings().dispatcher_config_key == "params/genesys/api/dispatcher.json"

    monkeypatch.setenv("DISPATCHER_CONFIG_KEY", "params/other/dispatcher.json")

    assert load_settings().dispatcher_config_key == "params/other/dispatcher.json"


def test_contracts_process_defaults():
    settings = load_settings()

    assert settings.contracts_prefix == "contracts/entrada/transacciones/empatia/transcripciones/"
    assert settings.contract_key == ""


def test_contracts_process_overrides(monkeypatch):
    monkeypatch.setenv("CONTRACTS_PREFIX", "contracts/other/")
    monkeypatch.setenv("CONTRACT_KEY", "contracts/other/x/sac/transcripcion.json")

    settings = load_settings()

    assert settings.contracts_prefix == "contracts/other/"
    assert settings.contract_key == "contracts/other/x/sac/transcripcion.json"


def test_conversations_details_defaults_follow_the_environment(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "stg")

    settings = load_settings()

    assert settings.conversations_details_bucket == "augusta-nexa-stg-landing"
    assert settings.conversations_details_prefix == "transacciones/genesys/api/conversations_details/"


def test_conversations_details_overrides(monkeypatch):
    monkeypatch.setenv("CONVERSATIONS_DETAILS_BUCKET", "augusta-nexa-dev-landing-archive")
    monkeypatch.setenv("CONVERSATIONS_DETAILS_PREFIX", "other/prefix/")

    settings = load_settings()

    assert settings.conversations_details_bucket == "augusta-nexa-dev-landing-archive"
    assert settings.conversations_details_prefix == "other/prefix/"
