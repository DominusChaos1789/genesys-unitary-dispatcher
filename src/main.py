"""Request Unitary: the multi-tag Genesys Cloud dispatcher.

One invocation runs one tag. The tag selects which endpoints make up the flow
(endpoints.py), the event supplies the ids per Genesys organization
(sources.py), and the result is one payload with an entry per organization.
Each entry carries that organization's bearer token, regional base_url and a
request template for every stage of the flow (payload.py):

    surveys                  -> request_context
    funcionarios_adherencia  -> request_context, request_init, request_status

The payload is written to the logs bucket under a key holding the tag and
this invocation's execution id -- Unitary Status and Unitary Download read
it from there -- and is also returned for the Step Function.

An organization that can't be served (no `servers` entry, token failure) is
recorded in `failed_organizations`; the others still get their entries.
"""

import logging
import uuid

import boto3

import src.s3_utils as s3_utils
from src.config import Settings, load_settings
from src.endpoints import load_endpoint_catalog, select_stages
from src.payload import build_organization_entry, build_payload
from src.sources import resolve_ids_by_organization, resolve_tag
from src.token_manager import get_token, load_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _server_key(organization_id: str) -> str:
    """The `servers` param uses "org_3" where organizations are "org-3"."""
    return organization_id.replace("-", "_")


def _build_organizations(
    settings: Settings, ids_by_organization: dict[str, list[str]], stages: dict[str, tuple[str, dict]]
) -> tuple[list[dict], list[dict]]:
    """(organization entries, failed organizations). One token per organization."""
    if not ids_by_organization:
        return [], []

    config = load_config(settings.api_genesys_params, region_name=settings.region)
    base_path = config["config"]["output"]["base_path"]
    connection = config["connection"]

    organizations: list[dict] = []
    failed: list[dict] = []

    for organization_id, ids in sorted(ids_by_organization.items()):
        server = config["servers"].get(_server_key(organization_id))
        if server is None:
            logger.error("No 'servers' entry for organization %s", organization_id)
            failed.append(
                {
                    "organization_id": organization_id,
                    "error": f"no servers entry for {_server_key(organization_id)}",
                }
            )
            continue

        try:
            token = get_token(
                config["secret"], connection, base_path, server, resource_name=settings.resource_name
            )
            organizations.append(
                build_organization_entry(
                    organization_id,
                    ids,
                    stages,
                    token=token,
                    connection=connection,
                    server=server,
                    base_path=base_path,
                )
            )
        except Exception as exc:  # noqa: BLE001 -- one organization must not stop the rest
            logger.exception("Could not build the payload for organization %s", organization_id)
            failed.append({"organization_id": organization_id, "error": str(exc)})

    return organizations, failed


def handler(event, context):
    settings = load_settings()
    s3_client = boto3.client("s3")
    execution_id = getattr(context, "aws_request_id", None) or str(uuid.uuid4())

    tag = resolve_tag(event)
    # Validate the tag against the catalog before reading any ids, so a typo'd
    # tag fails fast instead of after an S3 read.
    stages = select_stages(load_endpoint_catalog(s3_client, settings), tag)
    ids_by_organization = resolve_ids_by_organization(s3_client, settings, event)
    logger.info("Tag %s: stages %s, %d organization(s)", tag, list(stages), len(ids_by_organization))

    organizations, failed = _build_organizations(settings, ids_by_organization, stages)
    payload = build_payload(tag, organizations)

    key = settings.payload_log_key(tag, execution_id)
    s3_utils.write_json(s3_client, settings.payload_log_bucket, key, payload)
    logger.info("Wrote %s payload to s3://%s/%s", tag, settings.payload_log_bucket, key)

    return {
        "execution_id": execution_id,
        "payload_location": f"s3://{settings.payload_log_bucket}/{key}",
        "stages": list(stages),
        "failed_organizations": failed,
        **payload,
    }
