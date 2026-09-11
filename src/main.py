"""Request Unitary: the multi-tag Genesys Cloud dispatcher.

One invocation runs one or more tags. Each tag selects the endpoints of a flow
(endpoints.py), the ids come from the event or from an ids source, and each tag
gets one payload with an entry per Genesys organization. An entry carries that
organization's bearer token, regional base_url and a request template for every
stage of the flow (payload.py):

    surveys                  -> request_context
    funcionarios_adherencia  -> request_init, request_status (one job per management unit)

`"tags": [...]` runs several flows over the same ids, and `"tags": "all"` runs
every conversation flow (every tag with an endpoint taking {conversationId}).
The ids are read once and each organization's token is requested once, however
many tags run.

Ids sources:
- none: the ids come in the event, inline or as an S3 file (sources.py).
- "contracts": the contracts process -- transcription files to parquet,
  conversation ids grouped by each contract's organization (contracts_process.py).
- "conversations_details": the conversation ids the Genesys conversations
  download left in the landing bucket for the event's `date`, grouped by their
  org_id= folder; those files are left untouched (conversations_details.py).

Each payload is written to the logs bucket under its tag and this invocation's
execution id; Unitary Status and Unitary Download read it from there. The
response only says where each payload is and how many ids it holds -- never the
payloads themselves, which could exceed the Step Functions 256 KB limit.

An organization that can't be served (no `servers` entry, token failure) is left
out of every payload. Each payload file lists it with its ids under
`failed_organizations`, so a re-run has them; the response gives its id count.
"""

import logging
import uuid

import boto3

import src.s3_utils as s3_utils
from src.config import Settings, load_settings
from src.conversations_details import collect_conversation_ids, parse_date
from src.endpoints import conversation_tags, load_endpoint_catalog, select_stages
from src.payload import build_organization_entry, build_payload, output_base_path
from src.sources import ALL_TAGS, EventError, resolve_ids_by_organization, resolve_tag_selection
from src.token_manager import get_token, load_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CONTRACTS_IDS_SOURCE = "contracts"
CONVERSATIONS_DETAILS_IDS_SOURCE = "conversations_details"
IDS_SOURCES = (CONTRACTS_IDS_SOURCE, CONVERSATIONS_DETAILS_IDS_SOURCE)


def _server_key(organization_id: str) -> str:
    """The `servers` param uses "org_3" where organizations are "org-3"."""
    return organization_id.replace("-", "_")


def _expand_tags(selection: list[str] | str, catalog: dict[str, dict]) -> list[str]:
    if selection != ALL_TAGS:
        return selection
    tags = conversation_tags(catalog)
    if not tags:
        raise EventError('"tags": "all" found no endpoints taking {conversationId}')
    return tags


def _resolve_ids(s3_client, settings: Settings, event: dict, execution_id: str):
    """(ids by organization, source summary or None).

    Without `ids_source` the ids come from the event itself. With one, they come
    from that process, and its summary goes into the response under its name.
    The contracts process is imported here, not at module level: it's the only
    path that needs polars, so the other flows never load it.
    """
    ids_source = event.get("ids_source")
    if ids_source is None:
        return resolve_ids_by_organization(s3_client, settings, event), None

    if ids_source == CONTRACTS_IDS_SOURCE:
        from src.contracts_process import run_contracts

        run = run_contracts(s3_client, settings, execution_id)
    elif ids_source == CONVERSATIONS_DETAILS_IDS_SOURCE:
        run = collect_conversation_ids(s3_client, settings, parse_date(event.get("date")))
    else:
        raise EventError(f"Unknown ids_source {ids_source!r}; expected one of {sorted(IDS_SOURCES)}")
    return run["ids_by_organization"], run["summary"]


def _build_organizations(
    settings: Settings,
    stages_by_tag: dict[str, dict[str, tuple[str, dict]]],
    ids_by_organization: dict[str, list[str]],
) -> tuple[dict[str, list[dict]], list[dict]]:
    """({tag: organization entries}, failed organizations).

    One token per organization for all tags. An organization's entries for every
    tag are built before any is kept, so it is either in every payload or failed.
    """
    organizations: dict[str, list[dict]] = {tag: [] for tag in stages_by_tag}
    if not ids_by_organization:
        return organizations, []

    config = load_config(settings.api_genesys_params, region_name=settings.region)
    output = config["config"]["output"]
    # Prefix each flow's downloads are saved under: transacciones vs funcionarios.
    base_paths = {tag: output_base_path(output, tag) for tag in stages_by_tag}
    # Cached tokens live under base_path whatever the flow (as in the original
    # token flow). Tokens are per organization, so keying the cache on
    # base_path_wfm would miss the cached token and mint a second one.
    token_base_path = output["base_path"]
    connection = config["connection"]
    failed: list[dict] = []

    for organization_id, ids in sorted(ids_by_organization.items()):
        server = config["servers"].get(_server_key(organization_id))
        if server is None:
            logger.error("No 'servers' entry for organization %s", organization_id)
            error = f"no servers entry for {_server_key(organization_id)}"
            failed.append({"organization_id": organization_id, "ids": ids, "error": error})
            continue

        try:
            token = get_token(
                config["secret"], connection, token_base_path, server, resource_name=settings.resource_name
            )
            entries = {
                tag: build_organization_entry(
                    organization_id,
                    ids,
                    stages,
                    token=token,
                    connection=connection,
                    server=server,
                    base_path=base_paths[tag],
                )
                for tag, stages in stages_by_tag.items()
            }
        except Exception as exc:  # noqa: BLE001 -- one organization must not stop the rest
            logger.exception("Could not build the payload for organization %s", organization_id)
            failed.append({"organization_id": organization_id, "ids": ids, "error": str(exc)})
            continue

        for tag, entry in entries.items():
            organizations[tag].append(entry)

    return organizations, failed


def _write_payload(s3_client, settings: Settings, tag, execution_id, stages, organizations, failed) -> dict:
    """Writes one tag's payload and returns what the Step Function needs to find
    it: its location and counts, never the payload itself."""
    key = settings.payload_log_key(tag, execution_id)
    s3_utils.write_json(
        s3_client, settings.payload_log_bucket, key, build_payload(tag, organizations, failed)
    )
    logger.info("Wrote %s payload to s3://%s/%s", tag, settings.payload_log_bucket, key)
    return {
        "tag": tag,
        "payload_location": f"s3://{settings.payload_log_bucket}/{key}",
        "stages": list(stages),
        "organizations": {entry["organization_id"]: len(entry["ids"]) for entry in organizations},
    }


def handler(event, context):
    settings = load_settings()
    s3_client = boto3.client("s3")
    execution_id = getattr(context, "aws_request_id", None) or str(uuid.uuid4())

    selection = resolve_tag_selection(event)
    catalog = load_endpoint_catalog(s3_client, settings)
    tags = _expand_tags(selection, catalog)
    # Validate every tag before reading ids -- and before the contracts process
    # deletes any source files -- so a typo'd tag fails fast.
    stages_by_tag = {tag: select_stages(catalog, tag) for tag in tags}
    ids_by_organization, source_summary = _resolve_ids(s3_client, settings, event, execution_id)
    logger.info("Tags %s, %d organization(s)", tags, len(ids_by_organization))

    organizations_by_tag, failed = _build_organizations(settings, stages_by_tag, ids_by_organization)

    response = {
        "execution_id": execution_id,
        "tags": tags,
        "payloads": [
            _write_payload(
                s3_client, settings, tag, execution_id, stages_by_tag[tag], organizations_by_tag[tag], failed
            )
            for tag in tags
        ],
        "failed_organizations": [
            {"organization_id": f["organization_id"], "id_count": len(f["ids"]), "error": f["error"]}
            for f in failed
        ],
    }
    if source_summary is not None:
        response[event["ids_source"]] = source_summary
    return response
