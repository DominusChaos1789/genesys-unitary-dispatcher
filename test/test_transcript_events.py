import json
from types import SimpleNamespace

from src.main import run
from test.conftest import LANDING_BUCKET, payload_by_org

PREFIX = "transacciones/genesys/events/"


def _context(request_id: str = "req-events"):
    return SimpleNamespace(aws_request_id=request_id)


def _put_event(s3, organization_id: str, event_id: str, conversation_id=None, session_id=None) -> None:
    org_number = organization_id.rsplit("-", 1)[-1]
    key = f"{PREFIX}org_id={org_number}/{event_id}.json"
    body = {
        "detail": {
            "topicName": f"v2.detail.events.conversation.{conversation_id}.customer.end",
            "eventBody": {
                "eventTime": 1789603204752,
                "conversationId": conversation_id,
                "sessionId": session_id,
            },
        }
    }
    s3.put_object(Bucket=LANDING_BUCKET, Key=key, Body=json.dumps(body))


def _event(organization_id: str, *event_ids: str) -> dict:
    return {
        "tag": "transcript_events",
        "organizations": [{"organization_id": organization_id, "ids": list(event_ids)}],
    }


def test_event_ids_resolve_to_conversation_session_pairs(aws):
    s3 = aws["s3"]
    _put_event(s3, "org-1", "evt-1", conversation_id="conv-1", session_id="sess-1")

    result = run(_event("org-1", "evt-1"), _context())

    entry = payload_by_org(result)["org-1"]
    assert entry["ids"] == [{"conversationId": "conv-1", "communicationId": "sess-1"}]
    assert result["transcript_events"] == {
        "bucket": LANDING_BUCKET,
        "events_read": 1,
        "skipped_files": [],
        "conversations": {"org-1": 1},
    }


def test_several_event_ids_for_one_organization_yield_several_pairs(aws):
    s3 = aws["s3"]
    _put_event(s3, "org-1", "evt-1", conversation_id="conv-1", session_id="sess-1")
    _put_event(s3, "org-1", "evt-2", conversation_id="conv-2", session_id="sess-2")

    result = run(_event("org-1", "evt-1", "evt-2"), _context())

    entry = payload_by_org(result)["org-1"]
    assert entry["ids"] == [
        {"conversationId": "conv-1", "communicationId": "sess-1"},
        {"conversationId": "conv-2", "communicationId": "sess-2"},
    ]


def test_missing_or_incomplete_events_are_skipped_and_reported(aws):
    s3 = aws["s3"]
    _put_event(s3, "org-1", "evt-good", conversation_id="conv-1", session_id="sess-1")
    _put_event(s3, "org-1", "evt-no-session", conversation_id="conv-2", session_id=None)
    # evt-missing is never written -- its file simply doesn't exist.

    result = run(_event("org-1", "evt-good", "evt-no-session", "evt-missing"), _context())

    entry = payload_by_org(result)["org-1"]
    assert entry["ids"] == [{"conversationId": "conv-1", "communicationId": "sess-1"}]
    summary = result["transcript_events"]
    assert summary["events_read"] == 1
    assert sorted(summary["skipped_files"]) == sorted(
        [
            f"{PREFIX}org_id=1/evt-no-session.json",
            f"{PREFIX}org_id=1/evt-missing.json",
        ]
    )


def test_transcript_events_is_not_part_of_tags_all(aws):
    result = run({"tags": "all", "organizations": [{"organization_id": "org-1", "ids": ["x"]}]}, _context())

    tags_present = {r["tag"] for r in result["responses"]}
    assert "transcript_events" not in tags_present
