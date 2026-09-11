"""Conversation ids from the Genesys conversations download.

A separate process downloads conversation details from Genesys Cloud into the
landing bucket, one folder per organization and day:

    augusta-nexa-<env>-landing/transacciones/genesys/api/conversations_details/
        org_id=<N>/year=YYYY/month=MM/day=DD/conversations_details_<...>.json

Each file is {"endpoint": [{"conversationId": ..., "participants": [...], ...}, ...]}.
For a run with `"ids_source": "conversations_details"` and `"date": "YYYY-MM-DD"`,
this reads that day's files for every organization and collects the
conversation ids. Unlike the contracts process there is nothing to transform or
write: the ids only feed the payload. The files belong to the download process,
so they are never modified or deleted here.
"""

import logging
import re
from datetime import date, datetime

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


def _conversation_ids(document) -> list[str]:
    conversations = document.get(CONVERSATIONS_KEY) if isinstance(document, dict) else None
    if not isinstance(conversations, list):
        raise ValueError(f"expected an object with an {CONVERSATIONS_KEY!r} list")
    return [c["conversationId"] for c in conversations if isinstance(c, dict) and c.get("conversationId")]


def collect_conversation_ids(s3_client, settings: Settings, day: date) -> dict:
    """The day's conversation ids grouped by organization, plus a summary for
    the Lambda's response. Files are read one at a time, keeping only the ids."""
    bucket = settings.conversations_details_bucket
    prefix = settings.conversations_details_prefix.rstrip("/") + "/"
    day_path = f"year={day.year:04d}/month={day.month:02d}/day={day.day:02d}/"

    grouped: dict[str, set[str]] = {}
    files_read = 0
    skipped: list[str] = []

    for folder in s3_utils.list_common_prefixes(s3_client, bucket, prefix):
        organization_id = organization_id_from_folder(folder)
        if organization_id is None:
            logger.warning("Ignoring s3://%s/%s: not an org_id= folder", bucket, folder)
            continue
        for key in s3_utils.list_json_keys(s3_client, bucket, f"{folder}{day_path}"):
            try:
                ids = _conversation_ids(s3_utils.read_json(s3_client, bucket, key))
            except ValueError as exc:  # also covers JSONDecodeError and UnicodeDecodeError
                logger.warning("Skipping unreadable conversations file s3://%s/%s: %s", bucket, key, exc)
                skipped.append(key)
                continue
            files_read += 1
            grouped.setdefault(organization_id, set()).update(ids)

    ids_by_organization = {org: sorted(ids) for org, ids in grouped.items() if ids}
    logger.info(
        "Read %d conversations file(s) for %s: %s",
        files_read,
        day.isoformat(),
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
