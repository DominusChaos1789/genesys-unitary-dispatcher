"""Which tags a run executes, and where its ids come from.

The event names the tag(s) and supplies the ids grouped by Genesys
organization, either inline or as a pointer to a JSON file in S3:

    {"tag": "surveys",
     "organizations": [{"organization_id": "org-3", "ids": ["..."]}]}

    {"tags": ["surveys", "recordings"],
     "ids_location": {"bucket": "landing", "key": "..."}}

`"tags": "all"` means every conversation flow (see endpoints.conversation_tags).
An S3 "Object Created" event delivered through EventBridge also works: the
bucket and key come from `detail`, and the tag(s) from the top level or from
`detail` (set by the rule's input transformer). The file holds the same
organizations list, bare or as {"organizations": [...]}.
"""

import csv
import io
import json

import src.s3_utils as s3_utils
from src.config import Settings

ALL_TAGS = "all"


class EventError(ValueError):
    """The event doesn't say what to run or which ids to run it for."""


def _tags_list(tags) -> list[str] | str:
    if tags == ALL_TAGS:
        return ALL_TAGS
    if not isinstance(tags, list) or not tags or not all(isinstance(tag, str) and tag for tag in tags):
        raise EventError(f"'tags' must be \"all\" or a non-empty list of tag names, got {tags!r}")
    return list(dict.fromkeys(tags))


def _tag_source(event: dict) -> dict:
    """Where the tag fields are: the top level, or EventBridge's `detail`."""
    return event if ("tag" in event or "tags" in event) else (event.get("detail") or {})


def resolve_tag_selection(event: dict) -> list[str] | str:
    """The tags to run, in order and without repeats -- or ALL_TAGS.

    Accepts `tag` (one) or `tags` (a list, or "all"), top-level or under
    `detail`, but not both keys at once.
    """
    source = _tag_source(event)
    if "tag" in source and "tags" in source:
        raise EventError("Event has both 'tag' and 'tags'; send one of them")
    if "tags" in source:
        return _tags_list(source["tags"])
    tag = source.get("tag")
    if not isinstance(tag, str) or not tag:
        raise EventError("Event has no 'tag' or 'tags' (top-level or under 'detail')")
    return [tag]


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


def _sort_key(item):
    """Ids are usually plain strings, but a "transcript_session" tag's ids
    are {conversationId, communicationId} objects -- not hashable or
    orderable on their own, so dicts sort by their JSON form instead."""
    return item if isinstance(item, str) else json.dumps(item, sort_keys=True)


def has_ids_location(event: dict) -> bool:
    """Whether the event points at a file (`ids_location` or an S3 event's `detail`)."""
    return _ids_location(event) is not None


def parse_json_documents(text: str):
    """One JSON document, or -- for Firehose delivery files, which have no
    extension and hold one record per line (or records back to back with no
    separator at all) -- the list of every record in the file."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    decoder = json.JSONDecoder()
    records = []
    position = 0
    while True:
        while position < len(text) and text[position].isspace():
            position += 1
        if position >= len(text):
            return records
        record, position = decoder.raw_decode(text, position)
        records.append(record)


def read_ids_document(s3_client, settings: Settings, event: dict):
    """The file the event points at (`ids_location`, or an S3 "Object
    Created" event's `detail`), for ids sources whose file isn't the plain
    organizations list: a CSV (as rows), a JSON document, or a Firehose file
    of JSON records (as a list of them)."""
    location = _ids_location(event)
    if location is None:
        raise EventError("Event carries no file: expected 'ids_location' or an S3 event 'detail'")
    bucket, key = location
    bucket = settings.resolve_bucket(bucket)
    text = s3_utils.read_text(s3_client, bucket, key)
    if key.lower().endswith(".csv"):
        return list(csv.DictReader(io.StringIO(text))), key
    return parse_json_documents(text), key


def _group(organizations) -> dict[str, list]:
    """{organization_id: unique ids, sorted}. Repeated organizations merge;
    organizations left with no ids are dropped."""
    if isinstance(organizations, dict):
        organizations = organizations.get("organizations", [])

    grouped: dict[str, list] = {}
    seen: dict[str, set] = {}
    for entry in organizations:
        organization_id = entry.get("organization_id")
        if not organization_id:
            raise EventError(f"Organization entry without 'organization_id': {entry!r}")
        if "ids" not in entry:
            raise EventError(f"Organization {organization_id!r} has no 'ids' list")
        ids = grouped.setdefault(organization_id, [])
        seen_keys = seen.setdefault(organization_id, set())
        for item in entry["ids"]:
            key = _sort_key(item)
            if key not in seen_keys:
                seen_keys.add(key)
                ids.append(item)

    return {organization_id: sorted(ids, key=_sort_key) for organization_id, ids in grouped.items() if ids}


def resolve_ids_by_organization(s3_client, settings: Settings, event: dict) -> dict[str, list]:
    if "organizations" in event:
        return _group(event["organizations"])

    location = _ids_location(event)
    if location is None:
        raise EventError(
            "Event carries no ids: expected 'organizations', 'ids_location', or an S3 event 'detail'"
        )

    bucket, key = location
    return _group(s3_utils.read_json(s3_client, settings.resolve_bucket(bucket), key))
