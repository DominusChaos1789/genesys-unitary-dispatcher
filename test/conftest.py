import json
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

FIXTURES_DIR = Path(__file__).parent / "fixtures"

RESOURCES_BUCKET = "augusta-nexa-dev-resources"
LANDING_BUCKET = "augusta-nexa-dev-landing"
LOGS_BUCKET = "augusta-nexa-dev-logs"
CORE_CONFIG_KEY = "params/genesys/api/core.json"
ENDPOINTS_PREFIX = "params/genesys/api"

CORE_CONFIG = {
    "daily": [f"{ENDPOINTS_PREFIX}/jobs.json"],
    # Referenced but deliberately never uploaded: the default ENDPOINT_GROUPS
    # (unitary,status) must not read groups it wasn't asked for.
    "ondemand": [f"{ENDPOINTS_PREFIX}/upload.json"],
    "status": [f"{ENDPOINTS_PREFIX}/status.json"],
    "unitary": [f"{ENDPOINTS_PREFIX}/unitary.json"],
}

_ENV_VARS_UNDER_TEST = (
    "ENV_PREFIX",
    "ENVIRONMENT",
    "PROFILE",
    "STACK_ID",
    "RESOURCES_BUCKET",
    "CORE_CONFIG_KEY",
    "ENDPOINT_GROUPS",
    "PAYLOAD_LOG_BUCKET",
    "PAYLOAD_LOG_KEY_TEMPLATE",
    "API_GENESYS_PARAMS",
    "REGION",
    "RESOURCE_NAME",
)


def load_fixture(name: str):
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def aws_env(monkeypatch):
    for name in _ENV_VARS_UNDER_TEST:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    # What the deploy pipeline actually sets -- not ENV_PREFIX.
    monkeypatch.setenv("ENVIRONMENT", "dev")


@pytest.fixture
def aws(aws_env):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        for bucket in (RESOURCES_BUCKET, LANDING_BUCKET, LOGS_BUCKET):
            s3.create_bucket(Bucket=bucket)

        s3.put_object(Bucket=RESOURCES_BUCKET, Key=CORE_CONFIG_KEY, Body=json.dumps(CORE_CONFIG))
        for name in ("unitary.json", "status.json", "jobs.json"):
            s3.put_object(
                Bucket=RESOURCES_BUCKET,
                Key=f"{ENDPOINTS_PREFIX}/{name}",
                Body=(FIXTURES_DIR / name).read_bytes(),
            )

        yield {"s3": s3}


# --- Genesys API config (SSM) + OAuth, stubbed ----------------------------
# The real versions read SSM/Secrets Manager and mint tokens over HTTP
# through the runtime-control layer, none of which exists in tests.

GENESYS_BASE_PATH = "transacciones/genesys/api"
GENESYS_BASE_PATH_WFM = "funcionarios/genesys/api"
GENESYS_CONNECTION = {
    "base_url": "https://api.{region_id}.pure.cloud",
    "header_template": {
        "Authorization": "Bearer {access_token}",
        "Content-Type": "application/json",
    },
    "auth": {
        "url": "https://login.{region_id}.pure.cloud/oauth/token",
        "method": "POST",
        "header_template": {"Content-Type": "application/x-www-form-urlencoded"},
        "body_template": {
            "grant_type": "client_credentials",
            "client_id": "{client_id}",
            "client_secret": "{client_secret}",
        },
    },
}
GENESYS_SERVERS = {
    "org_1": {"region": "sa-east-1", "region_id": "sae1", "relative_path": "org_id=1/", "oauth": "org-1"},
    "org_3": {"region": "us-west-2", "region_id": "usw2", "relative_path": "org_id=3/", "oauth": "org-3"},
}
GENESYS_SECRETS = {
    "org-1": {"value": {"client_id": "id-1", "client_secret": "secret-1"}},
    "org-3": {"value": {"client_id": "id-3", "client_secret": "secret-3"}},
}
GENESYS_CONFIG = {
    "parameters": {},
    "secret": GENESYS_SECRETS,
    "connection": GENESYS_CONNECTION,
    "servers": GENESYS_SERVERS,
    "config": {"output": {"base_path": GENESYS_BASE_PATH, "base_path_wfm": GENESYS_BASE_PATH_WFM}},
}


@pytest.fixture(autouse=True)
def genesys_api(monkeypatch):
    """Stubs the SSM config load and the per-organization token, recording
    how the handler used them."""
    calls = {"load_config": [], "requested_orgs": [], "token_base_paths": []}

    def fake_load_config(path_params, region_name="us-east-2"):
        calls["load_config"].append((path_params, region_name))
        return GENESYS_CONFIG

    def fake_get_token(secret, connection, base_path, server, resource_name, now=None):
        calls["requested_orgs"].append(server["oauth"])
        calls["token_base_paths"].append(base_path)
        calls["resource_name"] = resource_name
        return {"access_token": f"token-for-{server['oauth']}", "expires_in": 86400}

    monkeypatch.setattr("src.main.load_config", fake_load_config)
    monkeypatch.setattr("src.main.get_token", fake_get_token)
    return calls
