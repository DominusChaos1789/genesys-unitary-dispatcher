"""Environment-driven configuration for the Request Unitary dispatcher."""

import os
from dataclasses import dataclass

BUCKET_NAMESPACE = "augusta-nexa"
DEFAULT_CORE_CONFIG_KEY = "params/genesys/api/core.json"
DEFAULT_DISPATCHER_CONFIG_KEY = "params/genesys/api/dispatcher.json"
# Only the core.json groups that hold tagged endpoints. core.json also lists
# daily/ondemand/actions files that this Lambda has no use for.
DEFAULT_ENDPOINT_GROUPS = ("unitary", "status")
# Tag + organization id in the key -- one fixed location per (tag,
# organization) pair, not one per execution: Unitary Status and Unitary
# Download always read the same, latest path, and each run's payload
# overwrites the previous one for that pair. Each organization gets its own
# flat file (see payload.build_organization_payload), so two different
# organizations never collide with each other.
DEFAULT_PAYLOAD_LOG_KEY_TEMPLATE = (
    "transacciones/genesys/api/payload_request_unitary/{tag}/{organization_id}.json"
)
# Every .json under this prefix is a contract, one per provider/operation pair.
DEFAULT_CONTRACTS_PREFIX = "contracts/entrada/transacciones/empatia/transcripciones/"
# Where the Genesys conversations download leaves its files:
# <prefix>org_id=<N>/year=YYYY/month=MM/day=DD/*.json
DEFAULT_CONVERSATIONS_DETAILS_PREFIX = "transacciones/genesys/api/conversations_details/"
# Where the real-time conversation-event process leaves its files, one per
# event: <prefix>org_id=<N>/<event_id>.json -- no date partitioning, since
# events arrive continuously rather than once a day.
DEFAULT_CONVERSATIONS_EVENTS_PREFIX = "transacciones/genesys/events/"


def normalize_env_token(value: str) -> str:
    """ "dev", "augusta-nexa-dev" and "augusta-nexa-dev-" all mean "dev"."""
    token = value.strip().strip("-")
    prefix = f"{BUCKET_NAMESPACE}-"
    if token.startswith(prefix):
        token = token[len(prefix) :]
    return token.strip("-")


def _resolve_env_token() -> str:
    """The deploy pipeline sets ENVIRONMENT/PROFILE (dev/stg/pro) and STACK_ID
    (augusta-nexa-<env>) rather than ENV_PREFIX, so fall back through all of
    them -- defaulting straight to dev would point stg/pro at dev buckets."""
    for name in ("ENV_PREFIX", "ENVIRONMENT", "PROFILE", "STACK_ID"):
        value = os.environ.get(name, "").strip()
        if value:
            return normalize_env_token(value)
    return "dev"


def _split_csv(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in value.split(",") if part.strip())


@dataclass(frozen=True)
class Settings:
    env: str
    resources_bucket: str
    core_config_key: str
    dispatcher_config_key: str
    endpoint_groups: tuple[str, ...]
    payload_log_bucket: str
    payload_log_key_template: str
    # SSM path holding the Genesys connection/servers/config parameters; the
    # per-organization OAuth secrets share the same prefix in Secrets Manager.
    api_genesys_params: str
    region: str
    # Identifies this Lambda in the runtime-control DynamoDB log, which is
    # where OAuth tokens are cached between runs.
    resource_name: str
    # Contracts process (ids_source "contracts"): every .json under
    # contracts_prefix is a contract; contract_key, when set, pins the run to
    # that single contract instead.
    contracts_prefix: str
    contract_key: str
    # Conversations-details ids source: the bucket and prefix the Genesys
    # conversations download writes to.
    conversations_details_bucket: str
    conversations_details_prefix: str
    # transcript_events (id_kind "transcript_event"): the bucket and prefix
    # the real-time conversation-event process writes to.
    conversations_events_bucket: str
    conversations_events_prefix: str

    def resolve_bucket(self, name: str) -> str:
        """ "landing" -> "augusta-nexa-dev-landing"; full names pass through."""
        if name.startswith(f"{BUCKET_NAMESPACE}-"):
            return name
        return f"{BUCKET_NAMESPACE}-{self.env}-{name}"

    def payload_log_key(self, tag: str, organization_id: str) -> str:
        return self.payload_log_key_template.format(tag=tag, organization_id=organization_id)


def load_settings() -> Settings:
    env = _resolve_env_token()
    stack = f"{BUCKET_NAMESPACE}-{env}"

    def bucket(env_name: str, logical_name: str) -> str:
        # The pipeline passes logical names ("resources", "logs"); a full
        # bucket name is accepted as-is.
        value = os.environ.get(env_name, logical_name).strip()
        return value if value.startswith(f"{BUCKET_NAMESPACE}-") else f"{stack}-{value}"

    return Settings(
        env=env,
        resources_bucket=bucket("RESOURCES_BUCKET", "resources"),
        core_config_key=os.environ.get("CORE_CONFIG_KEY", DEFAULT_CORE_CONFIG_KEY),
        dispatcher_config_key=os.environ.get("DISPATCHER_CONFIG_KEY", DEFAULT_DISPATCHER_CONFIG_KEY),
        endpoint_groups=_split_csv(os.environ.get("ENDPOINT_GROUPS", ",".join(DEFAULT_ENDPOINT_GROUPS))),
        payload_log_bucket=bucket("PAYLOAD_LOG_BUCKET", "logs"),
        payload_log_key_template=os.environ.get("PAYLOAD_LOG_KEY_TEMPLATE", DEFAULT_PAYLOAD_LOG_KEY_TEMPLATE),
        api_genesys_params=os.environ.get("API_GENESYS_PARAMS", f"/{stack}/genesys/api"),
        region=os.environ.get("REGION", "us-east-2"),
        resource_name=os.environ.get("RESOURCE_NAME", f"{stack}-genesys-api-unitary-request"),
        contracts_prefix=os.environ.get("CONTRACTS_PREFIX", DEFAULT_CONTRACTS_PREFIX),
        contract_key=os.environ.get("CONTRACT_KEY", ""),
        conversations_details_bucket=bucket("CONVERSATIONS_DETAILS_BUCKET", "landing"),
        conversations_details_prefix=os.environ.get(
            "CONVERSATIONS_DETAILS_PREFIX", DEFAULT_CONVERSATIONS_DETAILS_PREFIX
        ),
        conversations_events_bucket=bucket("CONVERSATIONS_EVENTS_BUCKET", "landing"),
        conversations_events_prefix=os.environ.get(
            "CONVERSATIONS_EVENTS_PREFIX", DEFAULT_CONVERSATIONS_EVENTS_PREFIX
        ),
    )
