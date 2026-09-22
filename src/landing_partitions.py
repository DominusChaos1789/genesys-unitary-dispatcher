"""Shared reader for the "one folder per organization, one folder per day"
layout the Genesys downloads use:

    <prefix>org_id=<N>/year=YYYY/month=MM/day=DD/*.json

conversations_details.py (conversations) and management_units.py (management
units) both walk this same layout -- same folders, same day partition, just a
different bucket/prefix and a different per-file extractor.
"""

import logging
import re
from datetime import date
from typing import Callable

import src.s3_utils as s3_utils

logger = logging.getLogger(__name__)

_ORG_FOLDER = re.compile(r"org_id=([^/]+)/$")


def organization_id_from_folder(folder: str) -> str | None:
    """ "<prefix>org_id=1/" -> "org-1"; folders that aren't org partitions -> None."""
    match = _ORG_FOLDER.search(folder)
    return f"org-{match.group(1)}" if match else None


def collect_ids(
    s3_client,
    bucket: str,
    prefix: str,
    day: date,
    extractor: Callable,
    label: str,
    render: Callable = lambda x: x,
    counts_key: str = "conversations",
) -> dict:
    """The day's ids (as picked out by `extractor`, deduplicated as whatever
    hashable value it returns) grouped by organization, plus a summary for
    the Lambda's response. `render` turns each deduplicated item into the
    value that actually goes in `ids_by_organization` -- identity for plain
    ids, tuple-to-dict for pairs. Files are read one at a time, keeping only
    the ids."""
    prefix = prefix.rstrip("/") + "/"
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
                logger.warning("Skipping unreadable file s3://%s/%s: %s", bucket, key, exc)
                skipped.append(key)
                continue
            files_read += 1
            grouped.setdefault(organization_id, set()).update(ids)

    ids_by_organization = {org: [render(item) for item in sorted(ids)] for org, ids in grouped.items() if ids}
    logger.info(
        "Read %d file(s) for %s (%s): %s",
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
            counts_key: {org: len(ids) for org, ids in ids_by_organization.items()},
        },
    }
