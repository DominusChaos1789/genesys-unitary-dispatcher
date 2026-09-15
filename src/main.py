"""Request Unitary: the multi-tag Genesys Cloud dispatcher.

One invocation runs one or more tags. dispatcher.json says which tags may run
and where their output goes (dispatcher_config.py); each tag's endpoints
(tagged in unitary.json/status.json) say which stages it has (endpoints.py);
the ids come from the event or from an ids source. Every organization that
gets ids for a tag gets its own flat payload file with a request template for
every stage of that flow (payload.py):

    surveys                  -> request_context
    transcripts              -> request_context, request_url
    funcionarios_adherencia  -> request_init, request_status (one job per management unit)

`"tags": [...]` runs several flows over the same ids, and `"tags": "all"` runs
every enabled flow whose dispatcher.json id_kind is "conversation". The ids
are read once and each organization's token is requested once, however many
tags run.

Ids sources:
- none: the ids come in the event, inline or as an S3 file (sources.py).
- "contracts": the contracts process -- transcription files to parquet,
  conversation ids grouped by each contract's organization (contracts_process.py).
- "conversations_details": the conversation ids the Genesys conversations
  download left in the landing bucket for the event's `date`, grouped by their
  org_id= folder; those files are left untouched (conversations_details.py).

Each organization's payload is written to the logs bucket under its tag, this
invocation's execution id and its own organization id -- one flat file, no
"organization" array, so a Step Function can read it straight into a Map
state. The handler returns a list with one entry per (tag, organization) pair
that got a file -- {execution_id, bucket, payload_location, organization_id,
stages, failed_organizations, tag} -- never the payload itself, which could
exceed the Step Functions 256 KB limit. The detailed run summary goes to the
logs.

An organization that can't be served (no `servers` entry, token failure) gets
no file for this run. Every other organization's file lists it under
`failed_organizations` with its ids, so a re-run has them.
"""

import json
import logging
import uuid

import boto3

import src.s3_utils as s3_utils
from src.config import Settings, load_settings
from src.conversations_details import collect_conversation_ids, parse_date
from src.dispatcher_config import conversation_tags, flow_config, load_dispatcher_config, output_base_path_key
from src.endpoints import load_endpoint_catalog, select_stages
from src.payload import build_organization_entry, build_organization_payload, output_base_path
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


def _expand_tags(selection: list[str] | str, dispatcher: dict) -> list[str]:
    if selection != ALL_TAGS:
        return selection
    tags = conversation_tags(dispatcher)
    if not tags:
        raise EventError('"tags": "all" found no enabled flow with id_kind "conversation"')
    return tags


def _resolve_ids(s3_client, settings: Settings, event: dict, execution_id: str):
    """(ids by organization, source summary or None).

    Without `ids_source` the ids come from the event itself. With one, they come
    from that process, and its summary goes into the logged run summary.
    The contracts process is imported here, not at module level: it's the only
    path that needs polars, so the other flows never load it.
    """
    ids_source = event.get("ids_source")
    if ids_source is None:
        return resolve_ids_by_organization(s3_client, settings, event), None

    if ids_source == CONTRACTS_IDS_SOURCE:
        from src.contracts_process import run_contracts

        outcome = run_contracts(s3_client, settings, execution_id)
    elif ids_source == CONVERSATIONS_DETAILS_IDS_SOURCE:
        outcome = collect_conversation_ids(s3_client, settings, parse_date(event.get("date")))
    else:
        raise EventError(f"Unknown ids_source {ids_source!r}; expected one of {sorted(IDS_SOURCES)}")
    return outcome["ids_by_organization"], outcome["summary"]


def _build_organizations(
    settings: Settings,
    stages_by_tag: dict[str, dict[str, tuple[str, dict]]],
    base_path_keys: dict[str, str],
    ids_by_organization: dict[str, list[str]],
) -> tuple[dict[str, list[dict]], list[dict]]:
    """({tag: organization entries}, failed organizations).

    One token per organization for all tags. An organization's entries for every
    tag are built before any is kept, so it is either in every tag's files or
    failed entirely (never present for one tag and missing for another).
    """
    organizations: dict[str, list[dict]] = {tag: [] for tag in stages_by_tag}
    if not ids_by_organization:
        return organizations, []

    config = load_config(settings.api_genesys_params, region_name=settings.region)
    output = config["config"]["output"]
    # Prefix each flow's downloads are saved under (its dispatcher.json domain).
    base_paths = {tag: output_base_path(output, key) for tag, key in base_path_keys.items()}
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


def _write_payloads(
    s3_client,
    settings: Settings,
    execution_id: str,
    stages_by_tag: dict[str, dict[str, tuple[str, dict]]],
    organizations_by_tag: dict[str, list[dict]],
    failed: list[dict],
) -> list[dict]:
    """Writes one flat file per (tag, organization) pair and returns, for each
    one, only where it went -- never the payload itself."""
    # Every organization's file carries the same list, in full detail (with
    # ids), so a re-run has whatever couldn't be served this time.
    failed_for_files = [
        {"organization_id": f["organization_id"], "ids": f["ids"], "error": f["error"]} for f in failed
    ]
    # The response stays small (Step Functions caps state data at 256 KB): a
    # count instead of the ids themselves.
    failed_for_response = [
        {"organization_id": f["organization_id"], "id_count": len(f["ids"]), "error": f["error"]}
        for f in failed
    ]

    responses = []
    for tag, entries in organizations_by_tag.items():
        stages = list(stages_by_tag[tag])
        for entry in entries:
            organization_id = entry["organization_id"]
            key = settings.payload_log_key(tag, execution_id, organization_id)
            payload = build_organization_payload(tag, entry, failed_for_files)
            s3_utils.write_json(s3_client, settings.payload_log_bucket, key, payload)
            logger.info(
                "Wrote %s/%s payload to s3://%s/%s", tag, organization_id, settings.payload_log_bucket, key
            )
            responses.append(
                {
                    "execution_id": execution_id,
                    "bucket": settings.payload_log_bucket,
                    "payload_location": key,
                    "organization_id": organization_id,
                    "stages": stages,
                    "failed_organizations": failed_for_response,
                    "tag": tag,
                }
            )
    return responses


def handler(event, context):
    """Runs the dispatcher and returns only where each payload was written.

    Always a list, one entry per (tag, organization) pair that got a file --
    including a single-tag single-organization run -- so a Step Function Map
    state can iterate it the same way regardless of how many tags or
    organizations were involved. The full run summary (ids per organization,
    the ids source's counts) goes to the logs instead.
    """
    result = run(event, context)
    logger.info("Run summary: %s", json.dumps(result, ensure_ascii=False, default=str))
    return result["responses"]


def run(event, context) -> dict:
    """The whole dispatch: validate the tags against dispatcher.json and the
    endpoint catalog, resolve the ids, build and write one payload file per
    (tag, organization) pair. Returns the detailed summary the handler logs."""
    settings = load_settings()
    s3_client = boto3.client("s3")
    execution_id = getattr(context, "aws_request_id", None) or str(uuid.uuid4())

    selection = resolve_tag_selection(event)
    catalog = load_endpoint_catalog(s3_client, settings)
    dispatcher = load_dispatcher_config(s3_client, settings)
    tags = _expand_tags(selection, dispatcher)

    # Validate every tag -- enabled in dispatcher.json, has endpoints for every
    # stage it needs -- before reading ids or deleting anything (the contracts
    # process deletes source files once it's done).
    stages_by_tag: dict[str, dict[str, tuple[str, dict]]] = {}
    base_path_keys: dict[str, str] = {}
    for tag in tags:
        flow_config(dispatcher, tag)
        base_path_keys[tag] = output_base_path_key(dispatcher, tag)
        stages_by_tag[tag] = select_stages(catalog, tag)

    ids_by_organization, source_summary = _resolve_ids(s3_client, settings, event, execution_id)
    logger.info("Tags %s, %d organization(s)", tags, len(ids_by_organization))

    organizations_by_tag, failed = _build_organizations(
        settings, stages_by_tag, base_path_keys, ids_by_organization
    )
    responses = _write_payloads(
        s3_client, settings, execution_id, stages_by_tag, organizations_by_tag, failed
    )

    result = {
        "execution_id": execution_id,
        "tags": tags,
        "responses": responses,
        "failed_organizations": [
            {"organization_id": f["organization_id"], "id_count": len(f["ids"]), "error": f["error"]}
            for f in failed
        ],
    }
    if source_summary is not None:
        result[event["ids_source"]] = source_summary
    return result
