"""transcript_events fed by a Firehose delivery file (ids_source unitary_conversations)."""

import json
from types import SimpleNamespace

import pytest

from src.main import run
from src.sources import EventError, parse_json_documents
from src.transcript_events import collect_from_firehose, organization_alias
from test.conftest import LANDING_BUCKET, payload_by_org

KEY = (
    "transacciones/genesys/events/transcript/year=2026/month=11/day=23/"
    "augusta-nexa-dev-firehose-genesys-transcript-events-2-2026-11-23-00-01-58-75cb5d15"
)
EVENT = {
    "tag": "transcript_events",
    "ids_source": "unitary_conversations",
    "ids_location": {"bucket": "landing", "key": KEY},
}
ORG_1 = "a96b4397-4a1e-44de-bc1e-0365e163eb39"
ORG_3 = "96cd5ccd-1808-419f-9ce4-54691e2aa02e"
ORG_UNKNOWN = "00000000-0000-0000-0000-000000000000"


def _record(organization_id, *pairs):
    return {
        "tag": "transcript_events",
        "organizations": [
            {
                "organization_id": organization_id,
                "ids": [{"conversationId": c, "communicationId": m} for c, m in pairs],
            }
        ],
    }


def _put(s3, *records, separator="\n"):
    body = separator.join(json.dumps(r) for r in records) + "\n"
    s3.put_object(Bucket=LANDING_BUCKET, Key=KEY, Body=body.encode("utf-8"))


def _context():
    return SimpleNamespace(aws_request_id="req-firehose")


def test_each_line_becomes_a_pair_of_its_mapped_organization(aws):
    _put(
        aws["s3"],
        _record(ORG_3, ("c1", "m1")),
        _record(ORG_3, ("c1", "m2")),
        _record(ORG_1, ("c9", "m9")),
        _record(ORG_3, ("c1", "m1")),  # repeated event
    )

    result = run(EVENT, _context(), today=None)

    by_org = payload_by_org(result)
    assert by_org["org-3"]["ids"] == [
        {"conversationId": "c1", "communicationId": "m1"},
        {"conversationId": "c1", "communicationId": "m2"},
    ]
    assert by_org["org-1"]["ids"] == [{"conversationId": "c9", "communicationId": "m9"}]
    assert by_org["org-3"]["request_url"]["url"].endswith("/communications/{communicationId}/transcripturl")
    summary = result["unitary_conversations"]
    assert summary["records"] == 4
    assert summary["conversations"] == {"org-1": 1, "org-3": 2}


def test_no_event_file_is_looked_up_for_firehose_pairs(aws):
    # resolve_transcript_events would read <prefix>/org_id=3/<id>.json and find nothing.
    _put(aws["s3"], _record(ORG_3, ("c1", "m1")))

    result = run(EVENT, _context())

    assert "transcript_events" not in result
    assert payload_by_org(result)["org-3"]["ids"] == [{"conversationId": "c1", "communicationId": "m1"}]


def test_unknown_organizations_are_skipped_and_listed(aws):
    _put(aws["s3"], _record(ORG_UNKNOWN, ("c1", "m1")), _record(ORG_3, ("c2", "m2")))

    result = run(EVENT, _context())

    assert set(payload_by_org(result)) == {"org-3"}
    assert result["unitary_conversations"]["unknown_organizations"] == [ORG_UNKNOWN]


def test_records_written_back_to_back_without_newlines_are_read(aws):
    _put(aws["s3"], _record(ORG_3, ("c1", "m1")), _record(ORG_3, ("c2", "m2")), separator="")

    result = run(EVENT, _context())

    assert len(payload_by_org(result)["org-3"]["ids"]) == 2


def test_incomplete_pairs_are_dropped():
    records = [
        _record(ORG_3, ("c1", "m1")),
        {"organizations": [{"organization_id": ORG_3, "ids": [{"conversationId": "c2"}]}]},
    ]

    outcome = collect_from_firehose(records)

    assert outcome["ids_by_organization"] == {"org-3": [{"conversationId": "c1", "communicationId": "m1"}]}


def test_a_single_json_record_is_accepted():
    outcome = collect_from_firehose(_record(ORG_1, ("c1", "m1")))

    assert outcome["ids_by_organization"] == {"org-1": [{"conversationId": "c1", "communicationId": "m1"}]}


def test_a_record_that_is_not_an_object_is_rejected():
    with pytest.raises(EventError, match="must be an object"):
        collect_from_firehose([["not", "a", "record"]])


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (ORG_1, "org-1"),
        ("432897ee-dad8-4ede-8c64-f489e4c8f8f3", "org-2"),
        (ORG_3, "org-3"),
        ("956b5c93-38a7-4087-89a3-da3edf40b138", "org-4"),
        ("org-2", "org-2"),
        (ORG_UNKNOWN, None),
        (None, None),
    ],
)
def test_organization_alias(value, expected):
    assert organization_alias(value) == expected


def test_parse_json_documents():
    assert parse_json_documents('{"a": 1}') == {"a": 1}
    assert parse_json_documents('{"a": 1}\n{"a": 2}\n') == [{"a": 1}, {"a": 2}]
    assert parse_json_documents('{"a": 1}{"a": 2}') == [{"a": 1}, {"a": 2}]
    assert parse_json_documents("[1, 2]") == [1, 2]
