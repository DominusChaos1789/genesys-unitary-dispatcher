"""transcript_events: real-time conversation events, one JSON file per event.

An external process writes each Genesys Cloud conversation event (its
"customer.end" notification) into the landing bucket as its own file, one
folder per organization, no date partitioning -- events arrive continuously
rather than once a day:

    augusta-nexa-<env>-landing/transacciones/genesys/events/org_id=<N>/<event_id>.json

Each file is the EventBridge event Genesys Cloud emits, shaped roughly like
{"detail": {"eventBody": {"conversationId": ..., "sessionId": ..., ...}}}.
An SQS queue feeds a Step Function that hands this Lambda the ids of the
events to read -- not the event bodies themselves -- the same way any other
tag's ids are supplied:

    {"tag": "transcript_events",
     "organizations": [{"organization_id": "org-1", "ids": ["<event_id>", ...]}]}

This resolves each given event id into the {conversationId, communicationId}
pair transcripts_url needs -- the same shape the batch "transcripts" tag
(id_kind "transcript_session") builds from a whole day's conversations_details
download, just sourced one real-time event at a time. An event id whose file
can't be read, or that's missing conversationId/sessionId, is skipped and
listed in the summary; nothing is deleted here.
"""

import logging

import src.s3_utils as s3_utils
from src.config import Settings

logger = logging.getLogger(__name__)


def _event_key(prefix: str, organization_id: str, event_id: str) -> str:
    """ "org-3" -> "org_id=3/<event_id>.json", under `prefix`."""
    org_number = organization_id.rsplit("-", 1)[-1]
    return f"{prefix.rstrip('/')}/org_id={org_number}/{event_id}.json"


def _conversation_session_pair(document) -> dict | None:
    event_body = ((document or {}).get("detail") or {}).get("eventBody") or {}
    conversation_id = event_body.get("conversationId")
    session_id = event_body.get("sessionId")
    if not conversation_id or not session_id:
        return None
    return {"conversationId": conversation_id, "communicationId": session_id}


def resolve_transcript_events(s3_client, settings: Settings, event_ids_by_organization: dict) -> dict:
    """{organization_id: [event_id, ...]} -> {"ids_by_organization":
    {organization_id: [{conversationId, communicationId}, ...]}, "summary": ...}."""
    bucket = settings.conversations_events_bucket
    resolved: dict[str, list[dict]] = {}
    skipped: list[str] = []
    events_read = 0

    for organization_id, event_ids in event_ids_by_organization.items():
        pairs = []
        for event_id in event_ids:
            key = _event_key(settings.conversations_events_prefix, organization_id, event_id)
            try:
                pair = _conversation_session_pair(s3_utils.read_json(s3_client, bucket, key))
            except Exception as exc:  # noqa: BLE001 -- one bad event must not stop the rest
                logger.warning("Skipping unreadable transcript event s3://%s/%s: %s", bucket, key, exc)
                skipped.append(key)
                continue
            if pair is None:
                logger.warning("s3://%s/%s has no conversationId/sessionId; skipping", bucket, key)
                skipped.append(key)
                continue
            events_read += 1
            pairs.append(pair)
        if pairs:
            resolved[organization_id] = sorted(
                pairs, key=lambda p: (p["conversationId"], p["communicationId"])
            )

    logger.info(
        "Read %d transcript event(s): %s", events_read, {org: len(ids) for org, ids in resolved.items()}
    )
    return {
        "ids_by_organization": resolved,
        "summary": {
            "bucket": bucket,
            "events_read": events_read,
            "skipped_files": skipped,
            "conversations": {org: len(ids) for org, ids in resolved.items()},
        },
    }
