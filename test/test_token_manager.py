import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import src.token_manager as tm
from test.conftest import GENESYS_CONNECTION, GENESYS_SECRETS, GENESYS_SERVERS

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)
SERVER = GENESYS_SERVERS["org_3"]
BASE_PATH = "transacciones/genesys/api"
DATASET = f"{BASE_PATH}/org_id=3/"


def _cache_expiring_in(seconds: int) -> dict:
    expires = (NOW + timedelta(seconds=seconds)).strftime(tm.TOKEN_EXPIRY_FORMAT)
    return {"data": {"access_token": "cached", "date_expire": expires}}


@pytest.mark.parametrize("last_success", [None, {}, {"data": None}, {"data": {"access_token": "x"}}])
def test_no_usable_cache_entry(last_success):
    assert tm._cached_token(last_success, NOW) is None


def test_a_cached_token_inside_the_refresh_margin_is_not_reused():
    assert tm._cached_token(_cache_expiring_in(tm.TOKEN_REFRESH_MARGIN_SECONDS), NOW) is None


def test_a_cached_token_with_time_left_is_reused():
    token = tm._cached_token(_cache_expiring_in(tm.TOKEN_REFRESH_MARGIN_SECONDS + 60), NOW)

    assert token["access_token"] == "cached"


def test_get_token_reuses_a_valid_cached_token(monkeypatch):
    seen = {}

    def fake_last_execution(resource_name, dataset):
        seen.update(resource_name=resource_name, dataset=dataset)
        return _cache_expiring_in(3600)

    def must_not_mint(*args, **kwargs):
        raise AssertionError("a valid cached token must not be re-minted")

    monkeypatch.setattr(tm, "get_last_execution_dynamo", fake_last_execution)
    monkeypatch.setattr(tm, "_mint_token", must_not_mint)

    token = tm.get_token(GENESYS_SECRETS, GENESYS_CONNECTION, BASE_PATH, SERVER, resource_name="rn", now=NOW)

    assert token["access_token"] == "cached"
    assert seen == {"resource_name": "rn", "dataset": DATASET}


def test_get_token_mints_and_persists_when_the_cache_is_stale(monkeypatch):
    persisted = {}

    monkeypatch.setattr(
        tm, "get_last_execution_dynamo", lambda resource_name, dataset: _cache_expiring_in(60)
    )
    monkeypatch.setattr(tm, "_mint_token", lambda secret, connection, server: {"access_token": "fresh"})
    monkeypatch.setattr(
        tm, "write_log_token", lambda token, dataset: persisted.update(token=token, dataset=dataset)
    )

    token = tm.get_token(GENESYS_SECRETS, GENESYS_CONNECTION, BASE_PATH, SERVER, resource_name="rn", now=NOW)

    assert token == {"access_token": "fresh"}
    assert persisted == {"token": {"access_token": "fresh"}, "dataset": DATASET}


@pytest.mark.parametrize(
    "secret",
    [
        {},
        {"org-3": {"error": "AccessDenied"}},
        {"org-3": {"value": {"client_id": "id-3"}}},
    ],
)
def test_minting_requires_both_credentials(secret):
    with pytest.raises(ValueError, match=r"Credenciales invalidas para OAuth \(org-3\)"):
        tm._mint_token(secret, GENESYS_CONNECTION, SERVER)


def test_minting_calls_the_organizations_regional_oauth_endpoint(monkeypatch):
    captured = {}

    class FakeClient:
        def __init__(self, base_url):
            captured["base_url"] = base_url

        def call(self, request_params):
            captured["request"] = request_params
            return {"access_token": "fresh", "expires_in": 86400}

    monkeypatch.setattr(tm, "GenesysClient", FakeClient)

    token = tm._mint_token(GENESYS_SECRETS, GENESYS_CONNECTION, SERVER)

    assert token["access_token"] == "fresh"
    assert captured["base_url"] == "https://login.usw2.pure.cloud/oauth/"
    assert captured["request"] == {
        "url": "token",
        # Sent as query-string params by GenesysClient, as in the original code.
        "params_template": {
            "grant_type": "client_credentials",
            "client_id": "id-3",
            "client_secret": "secret-3",
        },
        "headers": {"Content-Type": "application/x-www-form-urlencoded"},
        "method": "POST",
    }


def test_write_log_token_stamps_expiry_and_logs_to_runtime_control(monkeypatch):
    logged = {}
    monkeypatch.setattr(
        tm, "_runtime_control", lambda: SimpleNamespace(log_process=lambda **kw: logged.update(kw))
    )
    token = {"access_token": "fresh", "expires_in": 3600}

    tm.write_log_token(token, DATASET)

    expires = datetime.strptime(token["date_expire"], tm.TOKEN_EXPIRY_FORMAT).replace(tzinfo=timezone.utc)
    assert timedelta(minutes=59) < expires - datetime.now(timezone.utc) <= timedelta(hours=1)
    assert logged["dataset"] == DATASET
    assert logged["token"] is token
    assert logged["status"] == "SUCCESS"
    assert logged["log_type"] is False


def test_get_last_execution_queries_runtime_control_for_the_dataset(monkeypatch):
    queried = {}

    def fake_get_last_execution(**kwargs):
        queried.update(kwargs)
        return {"data": None}

    monkeypatch.setattr(
        tm, "_runtime_control", lambda: SimpleNamespace(get_last_execution=fake_get_last_execution)
    )

    assert tm.get_last_execution_dynamo("rn", DATASET) == {"data": None}
    assert queried == {"resource_name": "rn", "dataset": DATASET, "log_type": False, "full_item": True}


def test_runtime_control_is_only_required_when_a_token_is_looked_up():
    # Importing the module worked above without the layer; using it without the layer fails loudly.
    with pytest.raises(ModuleNotFoundError, match="runtime_control"):
        tm._runtime_control()


def test_load_config_returns_the_sections_the_dispatcher_uses(monkeypatch):
    created = {}

    class FakeLoader:
        def __init__(self, path, region_name):
            created.update(path=path, region_name=region_name)

        def get_all_parameters(self):
            return {"connection": {"c": 1}, "servers": {"s": 1}, "config": {"k": 1}, "runtime": {"r": 1}}

        def get_all_secrets(self):
            return {"org-3": {"value": {}}}

    monkeypatch.setattr(tm.params, "AWSConfigLoader", FakeLoader)

    config = tm.load_config("/augusta-nexa-dev/genesys/api", region_name="us-east-2")

    assert created == {"path": "/augusta-nexa-dev/genesys/api", "region_name": "us-east-2"}
    assert config["connection"] == {"c": 1}
    assert config["servers"] == {"s": 1}
    assert config["config"] == {"k": 1}
    assert config["secret"] == {"org-3": {"value": {}}}


def test_runtime_control_is_imported_from_the_layer_when_present(monkeypatch):
    layer = SimpleNamespace(__name__="runtime_control")
    monkeypatch.setitem(sys.modules, "runtime_control", layer)

    assert tm._runtime_control() is layer
