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
"""

import logging
import re
from datetime import date, datetime
from typing import Callable

import src.s3_utils as s3_utils
from src.config import Settings
from src.sources import EventError

logger = logging.getLogger(__name__)

CONVERSATIONS_KEY = "endpoint"
_ORG_FOLDER = re.compile(r"org_id=([^/]+)/$")


def parse_date(value) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        raise EventError(
            f'ids_source "conversations_details" needs "date" as YYYY-MM-DD, got {value!r}'
        ) from None


def organization_id_from_folder(folder: str) -> str | None:
    """ "<prefix>org_id=1/" -> "org-1"; folders that aren't org partitions -> None."""
    match = _ORG_FOLDER.search(folder)
    return f"org-{match.group(1)}" if match else None


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


def _collect_ids(
    s3_client, settings: Settings, day: date, extractor: Callable, label: str, render: Callable = lambda x: x
) -> dict:
    """The day's ids (as picked out by `extractor`, deduplicated as whatever
    hashable value it returns) grouped by organization, plus a summary for
    the Lambda's response. `render` turns each deduplicated item into the
    value that actually goes in `ids_by_organization` -- identity for plain
    ids, tuple-to-dict for pairs. Files are read one at a time, keeping only
    the ids."""
    bucket = settings.conversations_details_bucket
    prefix = settings.conversations_details_prefix.rstrip("/") + "/"
    day_path = f"year={day.year:04d}/month={day.month:02d}/day={day.day:02d}/"

    grouped: dict[str, set] = {}
    files_read = 0
    skipped: list[str] = []

    for folder in s3_utils.list_common_prefixes(s3_client, bucket, prefix):
        organization_id = organization_id_from_folder(folder)
        if organization_id is None:
            logger.warning("Ignoring s3://%s/%s: not an org_id= folder", bucket, folder)
            continue
        for key in s3_utils.list_json_keys(s3_client, bucket, f"{folder}{day_path}"):
            try:
                ids = extractor(s3_utils.read_json(s3_client, bucket, key))
            except ValueError as exc:  # also covers JSONDecodeError and UnicodeDecodeError
                logger.warning("Skipping unreadable conversations file s3://%s/%s: %s", bucket, key, exc)
                skipped.append(key)
                continue
            files_read += 1
            grouped.setdefault(organization_id, set()).update(ids)

    ids_by_organization = {org: [render(item) for item in sorted(ids)] for org, ids in grouped.items() if ids}
    logger.info(
        "Read %d conversations file(s) for %s (%s): %s",
        files_read,
        day.isoformat(),
        label,
        {org: len(ids) for org, ids in ids_by_organization.items()},
    )
    return {
        "ids_by_organization": ids_by_organization,
        "summary": {
            "date": day.isoformat(),
            "bucket": bucket,
            "files_read": files_read,
            "skipped_files": skipped,
            "conversations": {org: len(ids) for org, ids in ids_by_organization.items()},
        },
    }


def collect_conversation_ids(s3_client, settings: Settings, day: date) -> dict:
    """The day's conversation ids grouped by organization -- for the generic
    "conversation" id_kind."""
    return _collect_ids(s3_client, settings, day, _conversation_ids, "conversationId")


def collect_survey_ids(s3_client, settings: Settings, day: date) -> dict:
    """The day's Finished surveyIds grouped by organization -- for "survey"
    id_kind flows (surveys)."""
    return _collect_ids(s3_client, settings, day, _survey_ids, "surveyId")


def collect_transcript_session_ids(s3_client, settings: Settings, day: date) -> dict:
    """The day's (conversationId, communicationId) pairs grouped by
    organization -- for "transcript_session" id_kind flows (transcripts)."""
    return _collect_ids(
        s3_client,
        settings,
        day,
        _transcript_session_pairs,
        "conversationId+communicationId",
        render=lambda pair: {"conversationId": pair[0], "communicationId": pair[1]},
    )
