"""Runs every AWS console test event in events/ through the handler.

Keeps the events usable: if the event format changes, the files that no longer
work fail here instead of in the console.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.main import handler
from test.conftest import LANDING_BUCKET, LOGS_BUCKET, load_fixture

EVENTS_DIR = Path(__file__).resolve().parent.parent / "events"
CONVERSATIONS_DETAILS_KEY = (
    "transacciones/genesys/api/conversations_details/org_id=1/year=2026/month=08/day=13/"
    "conversations_details_2026-08-13_00:00:00_1.json"
)
MANAGEMENT_UNITS_KEY = "funcionarios/genesys/api/management_units/2026-08-13.json"
TRANSCRIPT_EVENT_KEY = "transacciones/genesys/events/org_id=1/00000000-0000-4000-8000-000000000003.json"

# event file -> {(tag, organization_id): id_count} for every payload file the run must write.
SUCCEEDS = {
    "01-smoke-empty.json": {},
    "02-surveys-inline.json": {("surveys", "org-1"): 1},
    "03-surveys-conv-details.json": {("surveys", "org-1"): 2},
    "04-tags-list.json": {("surveys", "org-1"): 2},
    # "tags": "all" -- every enabled conversation-id flow in dispatcher.json,
    # today surveys and transcripts (adherence takes management-unit ids).
    "05-tags-all.json": {("surveys", "org-1"): 2, ("transcripts", "org-1"): 2},
    "06-surveys-contracts.json": {("surveys", "org-3"): 2},
    "07-adherence-inline.json": {("funcionarios_adherencia", "org-1"): 1},
    "08-adherence-s3-file.json": {("funcionarios_adherencia", "org-1"): 1},
    "09-eventbridge-s3.json": {("funcionarios_adherencia", "org-1"): 1},
    "15-transcripts-inline.json": {("transcripts", "org-1"): 1},
    "16-transcript-events.json": {("transcript_events", "org-1"): 1},
}
# event file -> error message the run must fail with
FAILS = {
    "10-err-unknown-tag.json": "not declared in dispatcher.json",
    "11-err-unknown-source.json": "Unknown ids_source 'contract'",
    "12-err-tag-and-tags.json": "both 'tag' and 'tags'",
    "13-err-missing-date.json": 'needs "date" as YYYY-MM-DD',
    "14-err-no-ids.json": "carries no ids",
}


def _load(name: str) -> dict:
    return json.loads((EVENTS_DIR / name).read_text(encoding="utf-8"))


@pytest.fixture
def seeded_landing(aws, seeded_source_files):
    """What the events expect to find: a conversations download for 2026-08-13
    (org_id=1), the management units file, and BDO transcriptions."""
    s3 = aws["s3"]
    s3.put_object(
        Bucket=LANDING_BUCKET,
        Key=CONVERSATIONS_DETAILS_KEY,
        Body=json.dumps(load_fixture("conversations_details_sample.json")),
    )
    s3.put_object(
        Bucket=LANDING_BUCKET,
        Key=MANAGEMENT_UNITS_KEY,
        Body=(EVENTS_DIR / "s3-objects" / "management_units_2026-08-13.json").read_bytes(),
    )
    s3.put_object(
        Bucket=LANDING_BUCKET,
        Key=TRANSCRIPT_EVENT_KEY,
        Body=json.dumps(
            {
                "detail": {
                    "topicName": "v2.detail.events.conversation.conv-1.customer.end",
                    "eventBody": {"conversationId": "conv-1", "sessionId": "sess-1"},
                }
            }
        ),
    )
    return s3


def test_every_event_file_has_an_expectation():
    assert {p.name for p in EVENTS_DIR.glob("*.json")} == set(SUCCEEDS) | set(FAILS)


def test_event_names_fit_the_console_limit():
    # The Lambda console accepts event names of up to 25 characters.
    assert all(len(p.stem) <= 25 for p in EVENTS_DIR.glob("*.json"))


@pytest.mark.parametrize("name", sorted(SUCCEEDS))
def test_event_runs(seeded_landing, name):
    expected = SUCCEEDS[name]

    responses = handler(_load(name), SimpleNamespace(aws_request_id=f"console-{name}"))

    # Always a flat list: one entry per (tag, organization) pair that got a file.
    assert isinstance(responses, list)
    assert {(r["tag"], r["organization_id"]) for r in responses} == set(expected)
    for item in responses:
        assert item["failed_organizations"] == []
        assert item["bucket"] == LOGS_BUCKET
        written = json.loads(
            seeded_landing.get_object(Bucket=item["bucket"], Key=item["payload_location"])["Body"].read()
        )
        assert len(written["ids"]) == expected[(item["tag"], item["organization_id"])]


@pytest.mark.parametrize("name", sorted(FAILS))
def test_event_fails_with_a_clear_error(seeded_landing, name):
    with pytest.raises(ValueError, match=FAILS[name].replace("(", r"\(")):
        handler(_load(name), SimpleNamespace(aws_request_id=f"console-{name}"))
