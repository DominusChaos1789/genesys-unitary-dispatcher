import json
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

from src.params import AWSConfigLoader

PATH = "/augusta-nexa-dev/genesys/api"
REGION = "us-east-2"


@pytest.fixture
def aws_params(aws_env):
    with mock_aws():
        yield (
            boto3.client("ssm", region_name=REGION),
            boto3.client("secretsmanager", region_name=REGION),
        )


def test_parameters_under_the_path_are_parsed_and_keyed_relative_to_it(aws_params):
    ssm, _ = aws_params
    servers = {"org_3": {"region_id": "usw2", "relative_path": "org_id=3/", "oauth": "org-3"}}
    ssm.put_parameter(Name=f"{PATH}/servers", Value=json.dumps(servers), Type="String")
    ssm.put_parameter(Name=f"{PATH}/connection", Value=json.dumps({"base_url": "b"}), Type="SecureString")
    ssm.put_parameter(Name=f"{PATH}/plain", Value="not-json", Type="String")
    ssm.put_parameter(Name="/augusta-nexa-dev/other/servers", Value="{}", Type="String")

    # A trailing slash on the path is tolerated.
    parameters = AWSConfigLoader(path=f"{PATH}/", region_name=REGION).get_all_parameters()

    assert parameters == {"servers": servers, "connection": {"base_url": "b"}, "plain": "not-json"}


def test_secrets_are_limited_to_the_path_prefix(aws_params):
    _, secrets = aws_params
    credentials = {"client_id": "id-3", "client_secret": "secret-3"}
    secrets.create_secret(Name=f"{PATH}/org-3", SecretString=json.dumps(credentials))
    secrets.create_secret(Name="/augusta-nexa-dev/elsewhere/org-9", SecretString="{}")

    result = AWSConfigLoader(path=PATH, region_name=REGION).get_all_secrets()

    assert list(result) == ["org-3"]
    assert result["org-3"]["value"] == credentials
    assert result["org-3"]["arn"].startswith("arn:aws:secretsmanager:")


def test_a_secret_that_cannot_be_read_is_reported_per_secret(aws_params, monkeypatch):
    _, secrets = aws_params
    secrets.create_secret(Name=f"{PATH}/org-3", SecretString="{}")
    loader = AWSConfigLoader(path=PATH, region_name=REGION)

    def access_denied(**kwargs):
        raise RuntimeError("AccessDenied")

    monkeypatch.setattr(loader.secrets, "get_secret_value", access_denied)

    assert loader.get_all_secrets() == {"org-3": {"error": "AccessDenied"}}


def test_a_local_profile_session_is_used_when_ssl_verification_is_off(monkeypatch):
    sessions = []

    class FakeSession:
        def __init__(self, profile_name):
            sessions.append(profile_name)

        def client(self, service, region_name, verify):
            return f"{service}@{region_name}/verify={verify}"

    # Patch the module's boto3 reference, not boto3.Session itself: boto3.client()
    # builds its default session through that same class.
    fake_boto3 = SimpleNamespace(
        client=lambda service, region_name, verify: f"default-{service}",
        Session=FakeSession,
    )
    monkeypatch.setattr("src.params.boto3", fake_boto3)

    loader = AWSConfigLoader(path=PATH, region_name=REGION, profile_name="dev", verify=False)

    assert sessions == ["dev"]
    assert loader.ssm == "ssm@us-east-2/verify=False"
    assert loader.secrets == "secretsmanager@us-east-2/verify=False"
