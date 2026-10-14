"""Ids from the Genesys conversations download.

A separate process downloads conversation details from Genesys Cloud into the
landing bucket, one folder per organization and day:

    augusta-nexa-<env>-landing/transacciones/genesys/api/conversations_details/
        org_id=<N>/year=YYYY/month=MM/day=DD/conversations_details_<...>.json

Each file is {"endpoint": [{"conversationId": ..., "surveys": [...],
"participants": [...], ...}, ...]}. For a run with `"ids_source":
"conversations_details"` and `"date": "YYYY-MM-DD"`, this reads that day's
files for every organization and collects ids -- which field depends on the
tag's id_kind:

- "survey" (surveys): each conversation's `surveys[]` entries with
  `surveyStatus == "Finished"`, by their `surveyId` -- Unitary consumes
  `conversations_surveys_result` (`/api/v2/quality/surveys/{surveyId}`), not
  the conversation itself.
- "transcript_session" (transcripts): one (conversationId, communicationId)
  pair per **recorded voice** participant session on the conversation --
  `recording: true` and `mediaType: "voice"` -- since `transcripts_url`
  (`/api/v2/speechandtextanalytics/conversations/{conversationId}/communications/{communicationId}/transcripturl`)
  needs both ids and only a recorded voice session has a transcript to fetch.
  A conversation can carry more than one qualifying communication; its other,
  non-recorded sessions (ivr, acd routing, ...) are skipped so they don't
  multiply the volume downstream Lambdas process for nothing.

Unlike the contracts process there is nothing to transform or write: the ids
only feed the payload. The files belong to the download process, so they are
never modified or deleted here.

The org_id=/date-partitioned walk itself is shared with management_units.py,
see landing_partitions.py.
"""

import json
from datetime import date, datetime

from src.config import Settings
from src.dispatcher_config import CONVERSATION_ID_KIND, SURVEY_ID_KIND, TRANSCRIPT_SESSION_ID_KIND
from src.landing_partitions import collect_ids, organization_id_from_folder
from src.sources import EventError

__all__ = [
    "parse_date",
    "organization_id_from_folder",
    "collect_conversation_ids",
    "collect_survey_ids",
    "collect_transcript_session_ids",
    "unitary_requests",
    "collect_for_conversations",
]

CONVERSATIONS_KEY = "endpoint"


def parse_date(value, ids_source: str = "conversations_details") -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        raise EventError(f'ids_source "{ids_source}" needs "date" as YYYY-MM-DD, got {value!r}') from None


def _conversations(document) -> list[dict]:
    conversations = document.get(CONVERSATIONS_KEY) if isinstance(document, dict) else None
    if not isinstance(conversations, list):
        raise ValueError(f"expected an object with an {CONVERSATIONS_KEY!r} list")
    return conversations


def _conversation_ids(document) -> list[str]:
    return [
        c["conversationId"]
        for c in _conversations(document)
        if isinstance(c, dict) and c.get("conversationId")
    ]


def _survey_ids(document) -> list[str]:
    """Finished surveys' surveyId, off every conversation's `surveys[]`."""
    return [
        survey["surveyId"]
        for c in _conversations(document)
        if isinstance(c, dict)
        for survey in (c.get("surveys") or [])
        if isinstance(survey, dict) and survey.get("surveyStatus") == "Finished" and survey.get("surveyId")
    ]


def _transcript_session_pairs(document) -> list[tuple[str, str]]:
    """One (conversationId, sessionId) pair per recorded voice session -- a
    conversation with several qualifying communications yields several pairs.
    Only sessions with `recording: true` and `mediaType: "voice"` count:
    the other, non-recorded sessions (ivr, acd routing, ...) on the same
    conversation have no transcript to fetch, and would otherwise multiply
    the volume the downstream Lambdas have to process for no reason."""
    pairs = []
    for c in _conversations(document):
        if not isinstance(c, dict) or not c.get("conversationId"):
            continue
        for participant in c.get("participants") or []:
            if not isinstance(participant, dict):
                continue
            for session in participant.get("sessions") or []:
                if (
                    isinstance(session, dict)
                    and session.get("sessionId")
                    and session.get("recording") is True
                    and session.get("mediaType") == "voice"
                ):
                    pairs.append((c["conversationId"], session["sessionId"]))
    return pairs


def collect_conversation_ids(s3_client, settings: Settings, day: date) -> dict:
    """The day's conversation ids grouped by organization -- for the generic
    "conversation" id_kind."""
    return collect_ids(
        s3_client,
        settings.conversations_details_bucket,
        settings.conversations_details_prefix,
        day,
        _conversation_ids,
        "conversationId",
    )


def collect_survey_ids(s3_client, settings: Settings, day: date) -> dict:
    """The day's Finished surveyIds grouped by organization -- for "survey"
    id_kind flows (surveys)."""
    return collect_ids(
        s3_client,
        settings.conversations_details_bucket,
        settings.conversations_details_prefix,
        day,
        _survey_ids,
        "surveyId",
    )


def collect_transcript_session_ids(s3_client, settings: Settings, day: date) -> dict:
    """The day's (conversationId, communicationId) pairs grouped by
    organization -- for "transcript_session" id_kind flows (transcripts)."""
    return collect_ids(
        s3_client,
        settings.conversations_details_bucket,
        settings.conversations_details_prefix,
        day,
        _transcript_session_pairs,
        "conversationId+communicationId",
        render=lambda pair: {"conversationId": pair[0], "communicationId": pair[1]},
    )


UNITARY_CONVERSATIONS_IDS_SOURCE = "unitary_conversations"
# id_kind -> (extractor, label, render) for the conversations_details download.
_SPECS = {
    CONVERSATION_ID_KIND: (_conversation_ids, "conversationId", lambda item: item),
    SURVEY_ID_KIND: (_survey_ids, "surveyId", lambda item: item),
    TRANSCRIPT_SESSION_ID_KIND: (
        _transcript_session_pairs,
        "conversationId+communicationId",
        lambda pair: {"conversationId": pair[0], "communicationId": pair[1]},
    ),
}


def unitary_requests(document) -> list[dict]:
    """The unitary_process file -- {"tag", "date", "organization_id", "ids"},
    or a list of those -- as validated requests. `ids` are conversation ids to
    look up in that organization's conversations_details folder for `date`."""
    entries = document if isinstance(document, list) else [document]
    requests = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise EventError(f"unitary_process entry must be an object, got {entry!r}")
        organization_id = entry.get("organization_id")
        ids = entry.get("ids")
        if not organization_id or not isinstance(ids, list):
            raise EventError(f"unitary_process entry needs 'organization_id' and an 'ids' list: {entry!r}")
        day = parse_date(entry.get("date"), UNITARY_CONVERSATIONS_IDS_SOURCE)
        requests.append({"organization_id": organization_id, "day": day, "ids": set(ids)})
    return requests


def _only_conversations(extractor, wanted: set):
    def extract(document):
        conversations = [
            c for c in _conversations(document) if isinstance(c, dict) and c.get("conversationId") in wanted
        ]
        return extractor({CONVERSATIONS_KEY: conversations})

    return extract


def collect_for_conversations(s3_client, settings: Settings, id_kind: str, requests: list[dict]) -> dict:
    """Like the collect_* functions above, but only for the given conversations:
    each request names an organization, a day and the conversation ids to look
    up in that organization's conversations_details folder for that day. The
    ids found are whatever `id_kind` needs off those conversations (surveyIds,
    session pairs, ...). Requests are merged per organization."""
    if id_kind not in _SPECS:
        raise EventError(
            f'ids_source "{UNITARY_CONVERSATIONS_IDS_SOURCE}" cannot feed id_kind {id_kind!r}; '
            f"expected one of {sorted(_SPECS)}"
        )
    extractor, label, render = _SPECS[id_kind]

    merged: dict[str, dict[str, object]] = {}
    files_read = 0
    skipped: list[str] = []
    for request in requests:
        outcome = collect_ids(
            s3_client,
            settings.conversations_details_bucket,
            settings.conversations_details_prefix,
            request["day"],
            _only_conversations(extractor, request["ids"]),
            label,
            render=render,
            only_organization=request["organization_id"],
        )
        files_read += outcome["summary"]["files_read"]
        skipped.extend(outcome["summary"]["skipped_files"])
        for organization_id, ids in outcome["ids_by_organization"].items():
            bucket = merged.setdefault(organization_id, {})
            for item in ids:
                bucket[json.dumps(item, sort_keys=True)] = item

    ids_by_organization = {org: [items[key] for key in sorted(items)] for org, items in merged.items()}
    return {
        "ids_by_organization": ids_by_organization,
        "summary": {
            "bucket": settings.conversations_details_bucket,
            "requests": len(requests),
            "files_read": files_read,
            "skipped_files": skipped,
            "conversations": {org: len(ids) for org, ids in ids_by_organization.items()},
        },
    }
