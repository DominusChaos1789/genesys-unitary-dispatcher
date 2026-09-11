"""Which tag a run executes, and where its ids come from.

The event names the tag and supplies the ids grouped by Genesys
organization, either inline or as a pointer to a JSON file in S3:

    {"tag": "surveys",
     "organizations": [{"organization_id": "org-3", "ids": ["..."]}]}

    {"tag": "funcionarios_adherencia",
     "ids_location": {"bucket": "landing", "key": "..."}}

An S3 "Object Created" event delivered through EventBridge also works: the
bucket and key come from `detail`, and the tag from `tag` or `detail.tag`
(set by the rule's input transformer). The file holds the same
organizations list, bare or as {"organizations": [...]}.
"""

import src.s3_utils as s3_utils
from src.config import Settings


class EventError(ValueError):
    """The event doesn't say what to run or which ids to run it for."""


def resolve_tag(event: dict) -> str:
    tag = event.get("tag") or (event.get("detail") or {}).get("tag")
    if not tag:
        raise EventError("Event has no 'tag' (top-level or under 'detail')")
    return tag


def _ids_location(event: dict) -> tuple[str, str] | None:
    location = event.get("ids_location")
    if location:
        return location["bucket"], location["key"]

    detail = event.get("detail") or {}
    bucket = (detail.get("bucket") or {}).get("name")
    key = (detail.get("object") or {}).get("key")
    if bucket and key:
        return bucket, key
    return None


def _group(organizations) -> dict[str, list[str]]:
    """{organization_id: sorted unique ids}. Repeated organizations merge;
    organizations left with no ids are dropped."""
    if isinstance(organizations, dict):
        organizations = organizations.get("organizations", [])

    grouped: dict[str, set[str]] = {}
    for entry in organizations:
        organization_id = entry.get("organization_id")
        if not organization_id:
            raise EventError(f"Organization entry without 'organization_id': {entry!r}")
        if "ids" not in entry:
            raise EventError(f"Organization {organization_id!r} has no 'ids' list")
        grouped.setdefault(organization_id, set()).update(entry["ids"])

    return {organization_id: sorted(ids) for organization_id, ids in grouped.items() if ids}


def resolve_ids_by_organization(s3_client, settings: Settings, event: dict) -> dict[str, list[str]]:
    if "organizations" in event:
        return _group(event["organizations"])

    location = _ids_location(event)
    if location is None:
        raise EventError(
            "Event carries no ids: expected 'organizations', 'ids_location', or an S3 event 'detail'"
        )

    bucket, key = location
    return _group(s3_utils.read_json(s3_client, settings.resolve_bucket(bucket), key))
