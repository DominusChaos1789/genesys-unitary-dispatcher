"""Genesys OAuth token handling, per organization.

Each organization ("org-1".."org-4") selects an entry from the `servers` SSM
parameter (region, oauth secret name, relative_path) and, through it, that
organization's client_id/client_secret in Secrets Manager. Tokens are cached
in DynamoDB through the runtime-control layer and only re-minted when the
cached one is within TOKEN_REFRESH_MARGIN_SECONDS of expiring.

`runtime_control` ships as a Lambda layer (augusta-nexa-<env>-runtime-control),
so it is imported lazily -- importing this module must not require the layer.
"""

import logging
from datetime import datetime, timedelta, timezone

from src import params
from src.client_request import GenesysClient
from src.templates import render_template

logger = logging.getLogger(__name__)

UTC = timezone.utc

# Re-mint the token when the cached one expires within this many seconds.
TOKEN_REFRESH_MARGIN_SECONDS = 900
TOKEN_EXPIRY_FORMAT = "%Y-%m-%d %H:%M:%S"


def _runtime_control():
    """The DynamoDB execution-log layer, imported only when actually used."""
    import runtime_control as rl

    return rl


def load_config(path_params: str, region_name: str = "us-east-2") -> dict:
    """The Genesys API configuration from SSM + the OAuth secrets.

    Narrower than the ingestion pipeline's own loader: the endpoint catalog
    is resolved from S3 (see endpoints.py), so only the connection, servers
    and config parameters are needed here.
    """
    loader = params.AWSConfigLoader(path=path_params, region_name=region_name)
    parameters = loader.get_all_parameters()

    return {
        "parameters": parameters,
        "secret": loader.get_all_secrets(),
        "connection": parameters["connection"],
        "servers": parameters["servers"],
        "config": parameters["config"],
    }


def get_last_execution_dynamo(resource_name: str, dataset: str) -> dict:
    rl = _runtime_control()
    logger.info("Reading last execution for dataset %s", dataset)
    return rl.get_last_execution(resource_name=resource_name, dataset=dataset, log_type=False, full_item=True)


def _cached_token(last_success: dict | None, now: datetime) -> dict | None:
    """The cached token, when there is one and it isn't about to expire."""
    last_token = (last_success or {}).get("data")
    if not last_token or not last_token.get("date_expire"):
        return None

    expire_token = datetime.strptime(last_token["date_expire"], TOKEN_EXPIRY_FORMAT).replace(tzinfo=UTC)
    remaining = (expire_token - now).total_seconds()
    logger.info("Cached token expires at %s (%.0fs remaining)", expire_token, remaining)
    if remaining <= TOKEN_REFRESH_MARGIN_SECONDS:
        return None
    return last_token


def _mint_token(secret: dict, connection: dict, server: dict) -> dict:
    secret_org = secret.get(server.get("oauth", "")) or {}
    credentials = secret_org.get("value", {}) or {}
    client_id = credentials.get("client_id", "")
    client_secret = credentials.get("client_secret", "")
    if not client_id or not client_secret:
        raise ValueError(f"Credenciales invalidas para OAuth ({server.get('oauth')})")

    auth = connection.get("auth", {})
    url = render_template(auth.get("url", ""), region_id=server.get("region_id", ""))
    endpoint_token = url.split("/")[-1]
    base_url = url.rsplit("/", 1)[0] + "/"
    body = render_template(
        auth.get("body_template", {}),
        client_id=client_id,
        client_secret=client_secret,
    )
    request_params = {
        "url": endpoint_token,
        "params_template": body,
        "headers": auth.get("header_template", {}),
        "method": auth["method"],
    }
    return GenesysClient(base_url=base_url).call(request_params)


def write_log_token(token: dict, dataset: str) -> None:
    rl = _runtime_control()
    token["date_expire"] = (datetime.now(UTC) + timedelta(seconds=token["expires_in"])).strftime(
        TOKEN_EXPIRY_FORMAT
    )
    logger.info("Persisting token for dataset %s", dataset)
    rl.log_process(
        status="SUCCESS",
        status_code="200",
        status_detail=None,
        client_id=None,
        operation_id=None,
        segment_id=None,
        data_mode="",
        dataset=dataset,
        start_timestamp=0,
        final_timestamp=0,
        payload_source={},
        payload_target={},
        output_name=None,
        output_type=None,
        output_records=None,
        output_path=None,
        error=None,
        retries=0,
        log_type=False,
        metrics=None,
        token=token,
        delay_time=1,
    )


def get_token(
    secret: dict,
    connection: dict,
    base_path: str,
    server: dict,
    resource_name: str,
    now: datetime | None = None,
) -> dict:
    """A usable OAuth token for `server`'s organization, reusing the cached
    one from DynamoDB unless it is close to expiring."""
    now = now or datetime.now(UTC)
    dataset = f"{base_path}/{server.get('relative_path', '')}"

    cached = _cached_token(get_last_execution_dynamo(resource_name, dataset), now)
    if cached:
        return cached

    token = _mint_token(secret, connection, server)
    write_log_token(token, dataset)
    return token
