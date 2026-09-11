import json
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

FIXTURES_DIR = Path(__file__).parent / "fixtures"

RESOURCES_BUCKET = "augusta-nexa-dev-resources"
LANDING_BUCKET = "augusta-nexa-dev-landing"
LOGS_BUCKET = "augusta-nexa-dev-logs"
# Contracts process: transcriptions land in providers-landing, parquet goes to refined.
PROVIDERS_LANDING_BUCKET = "augusta-nexa-dev-providers-landing"
REFINED_BUCKET = "augusta-nexa-dev-refined"

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

CONTRACTS_PREFIX = "contracts/entrada/transacciones/empatia/transcripciones/"
CONTRACT_KEY = f"{CONTRACTS_PREFIX}bdo/sac/transcripcion.json"
SOURCE_PREFIX = "external/datanexa/transacciones/empatia/transcripciones/BDO"

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
    "CONTRACTS_PREFIX",
    "CONTRACT_KEY",
    "CONVERSATIONS_DETAILS_BUCKET",
    "CONVERSATIONS_DETAILS_PREFIX",
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
        for bucket in (
            RESOURCES_BUCKET,
            LANDING_BUCKET,
            LOGS_BUCKET,
            PROVIDERS_LANDING_BUCKET,
            REFINED_BUCKET,
        ):
            s3.create_bucket(Bucket=bucket)

        s3.put_object(Bucket=RESOURCES_BUCKET, Key=CORE_CONFIG_KEY, Body=json.dumps(CORE_CONFIG))
        for name in ("unitary.json", "status.json", "jobs.json"):
            s3.put_object(
                Bucket=RESOURCES_BUCKET,
                Key=f"{ENDPOINTS_PREFIX}/{name}",
                Body=(FIXTURES_DIR / name).read_bytes(),
            )
        s3.put_object(
            Bucket=RESOURCES_BUCKET,
            Key=CONTRACT_KEY,
            Body=(FIXTURES_DIR / "transcripcion.json").read_bytes(),
        )

        yield {"s3": s3}


@pytest.fixture
def seeded_source_files(aws):
    """Two BDO transcription files in providers-landing."""
    s3 = aws["s3"]
    keys = []
    for name in ("sample_transcription_1.json", "sample_transcription_2.json"):
        key = f"{SOURCE_PREFIX}/{name}"
        s3.put_object(Bucket=PROVIDERS_LANDING_BUCKET, Key=key, Body=(FIXTURES_DIR / name).read_bytes())
        keys.append(key)
    return keys


PEL_CONTRACT_KEY = f"{CONTRACTS_PREFIX}pel/sac/transcripcion.json"
PEL_SOURCE_PREFIX = "external/datanexa/transacciones/empatia/transcripciones/PEL"
PEL_CONVERSATION_ID = "9c3ea7d1-0000-4d2b-8e11-5f6a7b8c9d01"


@pytest.fixture
def seeded_second_provider(aws, seeded_source_files):
    """A second provider (PEL/SAC, org-2) alongside BDO (org-3): its own
    contract, landing prefix and conversation id."""
    s3 = aws["s3"]

    contract = load_fixture("transcripcion.json")
    contract["client_prefix"] = "PEL"
    contract["genesys_cloud_organization"] = "org-2"
    contract["source"]["prefix_pattern"] = PEL_SOURCE_PREFIX
    s3.put_object(
        Bucket=RESOURCES_BUCKET,
        Key=PEL_CONTRACT_KEY,
        Body=json.dumps(contract, ensure_ascii=False).encode("utf-8"),
    )

    record = load_fixture("sample_transcription_1.json")
    record["genesys_cloud_id"] = PEL_CONVERSATION_ID
    source_key = f"{PEL_SOURCE_PREFIX}/sample_pel.json"
    s3.put_object(
        Bucket=PROVIDERS_LANDING_BUCKET,
        Key=source_key,
        Body=json.dumps(record, ensure_ascii=False).encode("utf-8"),
    )

    return {"contract_key": PEL_CONTRACT_KEY, "source_key": source_key}


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
    "org_2": {"region": "sa-east-1", "region_id": "sae1", "relative_path": "org_id=2/", "oauth": "org-2"},
    "org_3": {"region": "us-west-2", "region_id": "usw2", "relative_path": "org_id=3/", "oauth": "org-3"},
}
GENESYS_SECRETS = {
    "org-1": {"value": {"client_id": "id-1", "client_secret": "secret-1"}},
    "org-2": {"value": {"client_id": "id-2", "client_secret": "secret-2"}},
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


# --- Reading back what a run wrote ------------------------------------------
# The response only carries each payload's location and counts; the payload
# itself lives in S3.


def read_payload(result: dict, tag: str | None = None) -> dict:
    """The payload a run wrote for `tag` -- or for its only tag, when omitted."""
    payloads = result["payloads"]
    if tag is None:
        assert len(payloads) == 1, f"expected one payload, got tags {[p['tag'] for p in payloads]}"
        item = payloads[0]
    else:
        item = next(p for p in payloads if p["tag"] == tag)
    bucket, key = item["payload_location"].removeprefix("s3://").split("/", 1)
    body = boto3.client("s3", region_name="us-east-1").get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)


def payload_by_org(result: dict, tag: str | None = None) -> dict:
    return {entry["organization_id"]: entry for entry in read_payload(result, tag)["organization"]}
