"""Request Unitary: the multi-tag Genesys Cloud dispatcher.

One invocation runs one or more tags. dispatcher.json says which tags may run
and where their output goes (dispatcher_config.py); each tag's endpoints
(tagged in unitary.json/status.json) say which stages it has (endpoints.py);
the ids come from the event or from an ids source. Every organization that
gets ids for a tag gets its own flat payload file with a request template for
every stage of that flow (payload.py):

    surveys                  -> request_context
    transcripts              -> request_url
    transcript_events        -> request_url (same endpoint, ids from real-time events)
    funcionarios_adherencia  -> request_init, request_status (one job per management unit)

`"tags": [...]` runs several flows over their own ids, and `"tags": "all"` runs
every enabled flow whose dispatcher.json id_kind is "conversation", "survey"
or "transcript_session" (dispatcher_config.py). Ids are resolved once per
distinct id_kind among the tags requested -- not once overall -- since a
"survey" tag like surveys needs different ids from a "transcript_session" tag
like transcripts even in the same run; each organization's token is still
requested once however many tags/kinds run.

Ids sources:
- none: the ids come in the event, inline or as an S3 file (sources.py);
  the same event ids are used for whatever id_kind(s) the requested tags need.
- "contracts": the contracts process -- transcription files to parquet,
  conversation ids grouped by each contract's organization (contracts_process.py).
- "conversations_details": ids the Genesys conversations download left in the
  landing bucket for the event's `date`, grouped by their org_id= folder --
  Finished surveyIds, (conversationId, communicationId) session pairs, or the
  conversation ids themselves, depending on the tag's id_kind
  (conversations_details.py). Those files are left untouched.

transcript_events (id_kind "transcript_event") is a further step on top of
whichever ids source above supplied its ids (normally the event's own inline
ids): those ids are event ids, one file each under the real-time
conversation-event process's landing prefix, resolved here into
{conversationId, communicationId} pairs the same way "transcript_session"
does from a whole day's conversations_details download
(transcript_events.py). It isn't part of `"tags": "all"`, since it isn't
driven by a `date` the way the other three kinds are.

Each organization's payload is written to the logs bucket under its tag and
its own organization id -- one flat file, no "organization" array, at a fixed
location Unitary Status/Download can always find: each run **overwrites**
the previous payload for that (tag, organization) pair rather than keeping a
copy per execution. The handler returns a list with one entry per
(tag, organization) pair that got a file -- {bucket, payload_location,
organization_id, stages, failed_organizations, tag} -- never the payload
itself, which could exceed the Step Functions 256 KB limit. The detailed run
summary (including the execution id) goes to the logs.

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
from src.conversations_details import (
    collect_conversation_ids,
    collect_survey_ids,
    collect_transcript_session_ids,
    parse_date,
)
from src.dispatcher_config import (
    SURVEY_ID_KIND,
    TRANSCRIPT_EVENT_ID_KIND,
    TRANSCRIPT_SESSION_ID_KIND,
    conversation_tags,
    flow_config,
    flow_id_kind,
    load_dispatcher_config,
    output_base_path_key,
)
from src.endpoints import load_endpoint_catalog, select_stages
from src.payload import build_organization_entry, build_organization_payload, output_base_path
from src.sources import ALL_TAGS, EventError, resolve_ids_by_organization, resolve_tag_selection
from src.token_manager import get_token, load_config
from src.transcript_events import resolve_transcript_events

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
        raise EventError(
            '"tags": "all" found no enabled flow with id_kind '
            '"conversation", "survey" or "transcript_session"'
        )
    return tags


def _resolve_ids(s3_client, settings: Settings, event: dict, execution_id: str, id_kind: str):
    """(ids by organization, source summary or None, summary key or None).

    Without `ids_source` the ids come from the event itself, whatever they
    are for the tags requested. With one, they come from that process for the
    given `id_kind`, and its summary goes into the logged run summary under
    `summary key`. The contracts process is imported here, not at module
    level: it's the only path that needs polars, so the other flows never
    load it.

    "transcript_event" is a further step on top of this: whatever ids came
    out above (normally the event's own inline ids) are event ids, one file
    each under the real-time conversation-event process's landing prefix --
    resolved here into the {conversationId, communicationId} pairs
    transcripts_url needs, with their own summary.
    """
    ids_source = event.get("ids_source")
    summary = None
    summary_key = None

    if ids_source is None:
        ids_by_organization = resolve_ids_by_organization(s3_client, settings, event)
    elif ids_source == CONTRACTS_IDS_SOURCE:
        from src.contracts_process import run_contracts

        outcome = run_contracts(s3_client, settings, execution_id)
        ids_by_organization, summary, summary_key = (
            outcome["ids_by_organization"],
            outcome["summary"],
            ids_source,
        )
    elif ids_source == CONVERSATIONS_DETAILS_IDS_SOURCE:
        day = parse_date(event.get("date"))
        if id_kind == SURVEY_ID_KIND:
            outcome = collect_survey_ids(s3_client, settings, day)
        elif id_kind == TRANSCRIPT_SESSION_ID_KIND:
            outcome = collect_transcript_session_ids(s3_client, settings, day)
        else:
            outcome = collect_conversation_ids(s3_client, settings, day)
        ids_by_organization, summary, summary_key = (
            outcome["ids_by_organization"],
            outcome["summary"],
            ids_source,
        )
    else:
        raise EventError(f"Unknown ids_source {ids_source!r}; expected one of {sorted(IDS_SOURCES)}")

    if id_kind == TRANSCRIPT_EVENT_ID_KIND:
        outcome = resolve_transcript_events(s3_client, settings, ids_by_organization)
        ids_by_organization, summary, summary_key = (
            outcome["ids_by_organization"],
            outcome["summary"],
            "transcript_events",
        )

    return ids_by_organization, summary, summary_key


def _resolve_ids_by_kind(s3_client, settings: Settings, event: dict, execution_id: str, id_kinds: set[str]):
    """({id_kind: ids by organization}, {id_kind: (summary key, summary)}).

    Ids are resolved once per distinct id_kind among the tags requested (e.g.
    "tags": "all" mixes "survey" tags like surveys with "transcript_session"
    tags like transcripts, which need different ids from the same
    conversations_details day). Without `ids_source`, the event supplies one
    set of ids that's used for every kind -- the event author is responsible
    for sending ids that fit whatever tag(s) they asked for.
    """
    ids_by_kind: dict[str, dict[str, list[str]]] = {}
    summaries_by_kind: dict[str, tuple[str, dict]] = {}
    for id_kind in sorted(id_kinds):
        ids_by_organization, summary, summary_key = _resolve_ids(
            s3_client, settings, event, execution_id, id_kind
        )
        ids_by_kind[id_kind] = ids_by_organization
        if summary is not None:
            summaries_by_kind[id_kind] = (summary_key, summary)
    return ids_by_kind, summaries_by_kind


def _combined_ids(ids_by_kind: dict[str, dict[str, list]], organization_id: str) -> list:
    """Every id an organization has across every id_kind, only to report a
    failure so a re-run has them. Ids are opaque here -- plain strings for
    most kinds, {conversationId, communicationId} pairs for
    "transcript_session" -- so this doesn't dedupe or sort across kinds."""
    combined: list = []
    for ids in ids_by_kind.values():
        combined.extend(ids.get(organization_id, []))
    return combined


def _build_organizations(
    settings: Settings,
    stages_by_tag: dict[str, dict[str, tuple[str, dict]]],
    base_path_keys: dict[str, str],
    id_kinds_by_tag: dict[str, str],
    ids_by_kind: dict[str, dict[str, list[str]]],
) -> tuple[dict[str, list[dict]], list[dict]]:
    """({tag: organization entries}, failed organizations).

    One token per organization for all tags, but each tag draws its ids from
    its own id_kind -- a "transcript_session" tag like transcripts and a
    "survey" tag like surveys never share an ids list. An organization with a
    token failure is failed for every tag; one with no ids for a given tag's
    kind (e.g. no finished surveys that day) simply gets no file for that
    tag, not a failure.
    """
    organizations: dict[str, list[dict]] = {tag: [] for tag in stages_by_tag}
    all_organization_ids = sorted({org for ids in ids_by_kind.values() for org in ids})
    if not all_organization_ids:
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

    for organization_id in all_organization_ids:
        server = config["servers"].get(_server_key(organization_id))
        if server is None:
            logger.error("No 'servers' entry for organization %s", organization_id)
            error = f"no servers entry for {_server_key(organization_id)}"
            ids = _combined_ids(ids_by_kind, organization_id)
            failed.append({"organization_id": organization_id, "ids": ids, "error": error})
            continue

        try:
            token = get_token(
                config["secret"], connection, token_base_path, server, resource_name=settings.resource_name
            )
            entries = {}
            for tag, stages in stages_by_tag.items():
                tag_ids = ids_by_kind.get(id_kinds_by_tag[tag], {}).get(organization_id, [])
                if not tag_ids:
                    continue
                entries[tag] = build_organization_entry(
                    organization_id,
                    tag_ids,
                    stages,
                    token=token,
                    connection=connection,
                    server=server,
                    base_path=base_paths[tag],
                )
        except Exception as exc:  # noqa: BLE001 -- one organization must not stop the rest
            logger.exception("Could not build the payload for organization %s", organization_id)
            ids = _combined_ids(ids_by_kind, organization_id)
            failed.append({"organization_id": organization_id, "ids": ids, "error": str(exc)})
            continue

        for tag, entry in entries.items():
            organizations[tag].append(entry)

    return organizations, failed


def _write_payloads(
    s3_client,
    settings: Settings,
    stages_by_tag: dict[str, dict[str, tuple[str, dict]]],
    organizations_by_tag: dict[str, list[dict]],
    failed: list[dict],
) -> list[dict]:
    """Writes one flat file per (tag, organization) pair -- overwriting
    whatever was there from a previous run -- and returns, for each one, only
    where it went -- never the payload itself."""
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
            key = settings.payload_log_key(tag, organization_id)
            payload = build_organization_payload(tag, entry, failed_for_files)
            s3_utils.write_json(s3_client, settings.payload_log_bucket, key, payload)
            logger.info(
                "Wrote %s/%s payload to s3://%s/%s", tag, organization_id, settings.payload_log_bucket, key
            )
            responses.append(
                {
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
    id_kinds_by_tag: dict[str, str] = {}
    for tag in tags:
        flow_config(dispatcher, tag)
        base_path_keys[tag] = output_base_path_key(dispatcher, tag)
        stages_by_tag[tag] = select_stages(catalog, tag)
        id_kinds_by_tag[tag] = flow_id_kind(dispatcher, tag)

    ids_by_kind, summaries_by_kind = _resolve_ids_by_kind(
        s3_client, settings, event, execution_id, set(id_kinds_by_tag.values())
    )
    logger.info("Tags %s, ids: %s", tags, {kind: len(ids) for kind, ids in ids_by_kind.items()})

    organizations_by_tag, failed = _build_organizations(
        settings, stages_by_tag, base_path_keys, id_kinds_by_tag, ids_by_kind
    )
    responses = _write_payloads(s3_client, settings, stages_by_tag, organizations_by_tag, failed)

    result = {
        "execution_id": execution_id,
        "tags": tags,
        "responses": responses,
        "failed_organizations": [
            {"organization_id": f["organization_id"], "id_count": len(f["ids"]), "error": f["error"]}
            for f in failed
        ],
    }
    # Group summaries by their key (usually `ids_source`, but "transcript_events"
    # for id_kind "transcript_event", which isn't gated by one). The common
    # case is one id_kind per key: keep that summary flat under the key, as
    # before. Only nest it by id_kind when a run (e.g. "tags": "all") actually
    # mixed more than one under the same key.
    summaries_by_summary_key: dict[str, dict[str, dict]] = {}
    for id_kind, (summary_key, summary) in summaries_by_kind.items():
        summaries_by_summary_key.setdefault(summary_key, {})[id_kind] = summary
    for summary_key, per_kind in summaries_by_summary_key.items():
        result[summary_key] = next(iter(per_kind.values())) if len(per_kind) == 1 else per_kind
    return result
