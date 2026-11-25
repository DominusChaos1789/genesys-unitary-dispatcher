"""Runs every AWS console test event in events/ through the handler.

Keeps the events usable: if the event format changes, the files that no longer
work fail here instead of in the console.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import src.recorded_conversations as recorded_conversations
from src.main import handler
from test.conftest import LANDING_BUCKET, LOGS_BUCKET, load_fixture
from test.test_recordings import FakeAthena

EVENTS_DIR = Path(__file__).resolve().parent.parent / "events"
CONVERSATIONS_DETAILS_KEY = (
    "transacciones/genesys/api/conversations_details/org_id=1/year=2026/month=08/day=13/"
    "conversations_details_2026-08-13_00:00:00_1.json"
)
MANAGEMENT_UNITS_KEY = "funcionarios/genesys/api/management_units/2026-08-13.json"
TRANSCRIPT_EVENT_KEY = (
    "transacciones/genesys/real_time_tmp_events/transcript_events/"
    "org_id=1/00000000-0000-4000-8000-000000000003.json"
)
UNITARY_PROCESS_KEY = "transacciones/genesys/unitary_process/request-1.json"
UNITARY_CSV_KEY = "transacciones/genesys/unitary_process/transcripts/4d16aedd-20ab-4750-a6bb-b726f4181462.csv"
OUTPUT_BUCKET = "augusta-nexa-dev-output"
FIREHOSE_KEY = (
    "transacciones/genesys/events/transcript/year=2026/month=11/day=23/"
    "augusta-nexa-dev-firehose-genesys-transcript-events-2-2026-11-23-00-01-58-"
    "75cb5d15-3fce-4a00-934b-48b1a5bb5871"
)
RECORDINGS_CSV_KEY = (
    "transacciones/genesys/unitary_process/recordings/4d16aedd-20ab-4750-a6bb-b726f4181462.csv"
)
MANAGEMENT_UNIT_LIST_KEY = (
    "funcionarios/genesys/api/management_unit_list/org_id=1/year=2026/month=08/day=13/units.json"
)

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
    "17-adherence-mu-list.json": {("funcionarios_adherencia", "org-1"): 2},
    "18-programaciones-inline.json": {("funcionarios_programaciones", "org-1"): 1},
    "19-programaciones-mu-list.json": {("funcionarios_programaciones", "org-1"): 2},
    "20-unitary-conversations.json": {("surveys", "org-1"): 1},
    "21-unitary-csv.json": {("transcripts", "org-1"): 2, ("transcripts", "org-3"): 1},
    "22-transcript-events-fh.json": {("transcript_events", "org-3"): 2, ("transcript_events", "org-1"): 1},
    "23-recordings-csv.json": {("recordings", "org-3"): 2},
    # Athena is faked below: two recorded BDO conversations.
    "24-recordings-by-date.json": {("recordings", "org-3"): 2},
    # What the EventBridge rule sends when the management units download lands
    # (docs/eventbridge/): one call per business unit, two flows.
    "25-forecast-s3-event.json": {
        ("funcionarios_pronosticos", "org-1"): 2,
        ("funcionarios_grupos_planificacion", "org-1"): 2,
    },
    "26-planning-mu-list.json": {("funcionarios_grupos_planificacion", "org-1"): 2},
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
    s3.put_object(
        Bucket=LANDING_BUCKET,
        Key=MANAGEMENT_UNIT_LIST_KEY,
        Body=json.dumps(
            {
                "endpoint": [
                    {"id": "mu-1", "name": "MU_ONE", "businessUnit": {"id": "bu-1"}},
                    {"id": "mu-2", "name": "MU_TWO", "businessUnit": {"id": "bu-2"}},
                ]
            }
        ),
    )
    s3.put_object(
        Bucket=LANDING_BUCKET,
        Key=UNITARY_PROCESS_KEY,
        Body=json.dumps(
            {
                "tag": "surveys",
                "date": "2026-08-13",
                "organization_id": "org-1",
                "ids": ["0c1d2e3f-1111-4aaa-8bbb-000000000001"],
            }
        ),
    )
    s3.create_bucket(Bucket=OUTPUT_BUCKET)
    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=UNITARY_CSV_KEY,
        Body="\n".join(
            [
                "conversationId,communicationId,cliente_prefijo,Organizacion",
                "c1,P1,BDB,org-1",
                "c2,P2,BDB,org-1",
                "c2,P2,BDB,org-1",
                "c3,P3,BPP,org-3",
            ]
        ).encode("utf-8"),
    )
    firehose_records = [
        ("96cd5ccd-1808-419f-9ce4-54691e2aa02e", "conv-a", "comm-a1"),
        ("96cd5ccd-1808-419f-9ce4-54691e2aa02e", "conv-a", "comm-a2"),
        ("a96b4397-4a1e-44de-bc1e-0365e163eb39", "conv-b", "comm-b1"),
    ]
    s3.put_object(
        Bucket=LANDING_BUCKET,
        Key=FIREHOSE_KEY,
        Body="".join(
            json.dumps(
                {
                    "tag": "transcript_events",
                    "organizations": [
                        {"organization_id": org, "ids": [{"conversationId": c, "communicationId": m}]}
                    ],
                }
            )
            + "\n"
            for org, c, m in firehose_records
        ).encode("utf-8"),
    )
    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=RECORDINGS_CSV_KEY,
        Body="\n".join(
            [
                '"conversacion_id","cliente_prefijo","Organizacion"',
                '"r1","BDO","org-3"',
                '"r1","BDO","org-3"',
                '"r2","BDO","org-3"',
            ]
        ).encode("utf-8"),
    )
    return s3


@pytest.fixture(autouse=True)
def fake_athena(monkeypatch):
    client = FakeAthena([("r1", "org-3"), ("r2", "org-3")])
    monkeypatch.setattr(recorded_conversations, "_athena_client", lambda settings: client)
    monkeypatch.setattr(recorded_conversations.time, "sleep", lambda seconds: None)
    return client


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
