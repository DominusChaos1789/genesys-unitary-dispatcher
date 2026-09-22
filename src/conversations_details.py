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
  pair per participant session on the conversation -- a single conversation
  can have several, since `transcripts_url`
  (`/api/v2/speechandtextanalytics/conversations/{conversationId}/communications/{communicationId}/transcripturl`)
  needs both ids and a conversation can carry more than one communication.

Unlike the contracts process there is nothing to transform or write: the ids
only feed the payload. The files belong to the download process, so they are
never modified or deleted here.

The org_id=/date-partitioned walk itself is shared with management_units.py,
see landing_partitions.py.
"""

from datetime import date, datetime

from src.config import Settings
from src.landing_partitions import collect_ids, organization_id_from_folder
from src.sources import EventError

__all__ = [
    "parse_date",
    "organization_id_from_folder",
    "collect_conversation_ids",
    "collect_survey_ids",
    "collect_transcript_session_ids",
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
    """One (conversationId, sessionId) pair per participant session -- a
    conversation with several communications yields several pairs."""
    pairs = []
    for c in _conversations(document):
        if not isinstance(c, dict) or not c.get("conversationId"):
            continue
        for participant in c.get("participants") or []:
            if not isinstance(participant, dict):
                continue
            for session in participant.get("sessions") or []:
                if isinstance(session, dict) and session.get("sessionId"):
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
