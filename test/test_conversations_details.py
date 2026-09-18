import json
from types import SimpleNamespace

import pytest

import src.s3_utils as s3_utils
from src.conversations_details import organization_id_from_folder, parse_date
from src.main import run
from src.sources import EventError
from test.conftest import LANDING_BUCKET, load_fixture, payload_by_org

PREFIX = "transacciones/genesys/api/conversations_details/"
DAY = "2026-08-13"
DAY_PATH = "year=2026/month=08/day=13/"
EVENT = {"tag": "surveys", "ids_source": "conversations_details", "date": DAY}
SAMPLE_IDS = sorted(
    survey["surveyId"]
    for c in load_fixture("conversations_details_sample.json")["endpoint"]
    for survey in c.get("surveys", [])
    if survey.get("surveyStatus") == "Finished"
)


def _context(request_id: str = "req-details"):
    return SimpleNamespace(aws_request_id=request_id)


def _by_org(result: dict) -> dict:
    return payload_by_org(result)


def _put(s3, key: str, body) -> None:
    s3.put_object(Bucket=LANDING_BUCKET, Key=key, Body=body if isinstance(body, bytes) else json.dumps(body))


def _details(*survey_ids) -> dict:
    """A conversations_details file with one Finished survey per conversation
    -- surveys' id_kind is "survey", so what matters here is `surveyId`, not
    the (made-up) conversationId."""
    return {
        "endpoint": [
            {"conversationId": f"conv-{sid}", "surveys": [{"surveyId": sid, "surveyStatus": "Finished"}]}
            for sid in survey_ids
        ]
    }


def _transcript_details(*pairs) -> dict:
    """A conversations_details file with one participant session per given
    (conversationId, communicationId) pair -- transcripts' id_kind is
    "transcript_session"."""
    return {
        "endpoint": [
            {"conversationId": conversation_id, "participants": [{"sessions": [{"sessionId": session_id}]}]}
            for conversation_id, session_id in pairs
        ]
    }


def test_ids_are_read_per_organization_for_the_event_date_and_files_are_kept(aws):
    s3 = aws["s3"]
    _put(
        s3, f"{PREFIX}org_id=1/{DAY_PATH}conversations_details_2026-08-13_00:00:00_1.json", _details("a", "b")
    )
    _put(
        s3, f"{PREFIX}org_id=1/{DAY_PATH}conversations_details_2026-08-13_00:00:00_2.json", _details("b", "c")
    )
    _put(
        s3,
        f"{PREFIX}org_id=3/{DAY_PATH}conversations_details_2026-08-13_00:00:00_1.json",
        load_fixture("conversations_details_sample.json"),
    )
    # Another day for the same organization must not be read.
    _put(s3, f"{PREFIX}org_id=1/year=2026/month=08/day=12/conversations_details_old.json", _details("old"))

    result = run(EVENT, _context())

    organizations = _by_org(result)
    assert organizations["org-1"]["ids"] == ["a", "b", "c"]
    assert organizations["org-3"]["ids"] == SAMPLE_IDS
    assert organizations["org-1"]["request_context"]["headers"]["Authorization"] == "Bearer token-for-org-1"
    assert organizations["org-3"]["request_context"]["base_url"] == "https://api.usw2.pure.cloud"

    summary = result["conversations_details"]
    assert summary == {
        "date": DAY,
        "bucket": LANDING_BUCKET,
        "files_read": 3,
        "skipped_files": [],
        "conversations": {"org-1": 3, "org-3": len(SAMPLE_IDS)},
    }

    # Nothing is deleted or moved: the download process owns these files.
    assert len(s3_utils.list_json_keys(s3, LANDING_BUCKET, PREFIX)) == 4


def test_unreadable_or_unexpected_files_are_skipped_and_reported(aws):
    s3 = aws["s3"]
    empty = f"{PREFIX}org_id=1/{DAY_PATH}empty.json"
    no_endpoint = f"{PREFIX}org_id=1/{DAY_PATH}no_endpoint.json"
    _put(s3, f"{PREFIX}org_id=1/{DAY_PATH}good.json", _details("a"))
    _put(s3, empty, b"")
    _put(s3, no_endpoint, {"conversations": [{"conversationId": "x"}]})
    _put(s3, f"{PREFIX}org_id=1/{DAY_PATH}notes.txt", b"not json, not listed")

    result = run(EVENT, _context())

    assert _by_org(result)["org-1"]["ids"] == ["a"]
    assert result["conversations_details"]["files_read"] == 1
    assert sorted(result["conversations_details"]["skipped_files"]) == sorted([empty, no_endpoint])


def test_only_finished_surveys_with_a_surveyid_are_collected(aws):
    body = {
        "endpoint": [
            {"conversationId": "c1", "surveys": [{"surveyId": "a", "surveyStatus": "Finished"}]},
            {"conversationId": "c2", "surveys": [{"surveyId": "b", "surveyStatus": "Expired"}]},
            {"conversationId": "c3", "surveys": [{"surveyStatus": "Finished"}]},
            {"conversationId": "c4", "surveys": []},
            {"conversationId": "c5"},
            "not-an-object",
        ]
    }
    _put(aws["s3"], f"{PREFIX}org_id=2/{DAY_PATH}mixed.json", body)

    result = run(EVENT, _context())

    assert _by_org(result)["org-2"]["ids"] == ["a"]


def test_transcripts_reads_conversation_session_pairs_off_the_same_records(aws):
    _put(
        aws["s3"],
        f"{PREFIX}org_id=3/{DAY_PATH}sample.json",
        load_fixture("conversations_details_sample.json"),
    )
    sample_pairs = sorted(
        (c["conversationId"], session["sessionId"])
        for c in load_fixture("conversations_details_sample.json")["endpoint"]
        for participant in c.get("participants", [])
        for session in participant.get("sessions", [])
    )
    expected = [{"conversationId": cid, "communicationId": sid} for cid, sid in sample_pairs]

    result = run({"tag": "transcripts", "ids_source": "conversations_details", "date": DAY}, _context())

    assert _by_org(result)["org-3"]["ids"] == expected
    assert result["conversations_details"]["conversations"] == {"org-3": len(expected)}


def test_a_conversation_with_several_sessions_yields_a_pair_per_session(aws):
    _put(
        aws["s3"],
        f"{PREFIX}org_id=1/{DAY_PATH}multi.json",
        {
            "endpoint": [
                {
                    "conversationId": "conv-1",
                    "participants": [
                        {"sessions": [{"sessionId": "s1"}, {"sessionId": "s2"}]},
                        {"sessions": [{"sessionId": "s3"}]},
                    ],
                }
            ]
        },
    )

    result = run({"tag": "transcripts", "ids_source": "conversations_details", "date": DAY}, _context())

    assert _by_org(result)["org-1"]["ids"] == [
        {"conversationId": "conv-1", "communicationId": "s1"},
        {"conversationId": "conv-1", "communicationId": "s2"},
        {"conversationId": "conv-1", "communicationId": "s3"},
    ]


def test_unreadable_transcript_files_are_skipped_and_reported(aws):
    s3 = aws["s3"]
    no_endpoint = f"{PREFIX}org_id=1/{DAY_PATH}no_endpoint.json"
    _put(s3, f"{PREFIX}org_id=1/{DAY_PATH}good.json", _transcript_details(("c1", "s1")))
    _put(s3, no_endpoint, {"conversations": [{"conversationId": "x"}]})

    result = run({"tag": "transcripts", "ids_source": "conversations_details", "date": DAY}, _context())

    assert _by_org(result)["org-1"]["ids"] == [{"conversationId": "c1", "communicationId": "s1"}]
    assert result["conversations_details"]["skipped_files"] == [no_endpoint]


def test_malformed_transcript_records_are_ignored(aws):
    body = {
        "endpoint": [
            {
                "conversationId": "c1",
                "participants": [{"sessions": [{"sessionId": "s1"}]}, "not-a-participant"],
            },
            {"conversationId": "", "participants": [{"sessions": [{"sessionId": "s2"}]}]},
            "not-an-object",
        ]
    }
    _put(aws["s3"], f"{PREFIX}org_id=1/{DAY_PATH}mixed.json", body)

    result = run({"tag": "transcripts", "ids_source": "conversations_details", "date": DAY}, _context())

    assert _by_org(result)["org-1"]["ids"] == [{"conversationId": "c1", "communicationId": "s1"}]


def test_the_generic_conversation_id_kind_reads_conversation_ids(aws):
    from test.conftest import DISPATCHER_CONFIG_KEY, RESOURCES_BUCKET

    s3 = aws["s3"]
    config = json.loads(s3.get_object(Bucket=RESOURCES_BUCKET, Key=DISPATCHER_CONFIG_KEY)["Body"].read())
    config["flows"]["surveys"]["id_kind"] = "conversation"
    s3.put_object(Bucket=RESOURCES_BUCKET, Key=DISPATCHER_CONFIG_KEY, Body=json.dumps(config))
    _put(s3, f"{PREFIX}org_id=1/{DAY_PATH}a.json", {"endpoint": [{"conversationId": "conv-x"}]})

    result = run(EVENT, _context())

    assert _by_org(result)["org-1"]["ids"] == ["conv-x"]


def test_a_tags_all_run_skips_transcripts_for_an_organization_with_no_sessions(aws):
    _put(aws["s3"], f"{PREFIX}org_id=1/{DAY_PATH}a.json", _details("a"))

    result = run({"tags": "all", "ids_source": "conversations_details", "date": DAY}, _context())

    tags_present = {r["tag"] for r in result["responses"]}
    assert tags_present == {"surveys"}
    assert result["failed_organizations"] == []


def test_folders_that_are_not_org_partitions_are_ignored(aws):
    s3 = aws["s3"]
    _put(s3, f"{PREFIX}org_id=1/{DAY_PATH}a.json", _details("a"))
    _put(s3, f"{PREFIX}_tmp/{DAY_PATH}b.json", _details("b"))

    result = run(EVENT, _context())

    assert set(_by_org(result)) == {"org-1"}


def test_a_day_without_files_produces_an_empty_payload_without_touching_genesys(aws, genesys_api):
    result = run(EVENT, _context())

    assert result["responses"] == []
    assert result["conversations_details"]["files_read"] == 0
    assert genesys_api["load_config"] == []


@pytest.mark.parametrize("event_date", [None, "13-08-2026", "2026-02-30", 20260813])
def test_the_date_is_required_as_yyyy_mm_dd(aws, event_date):
    event = {"tag": "surveys", "ids_source": "conversations_details"}
    if event_date is not None:
        event["date"] = event_date

    with pytest.raises(EventError, match='needs "date" as YYYY-MM-DD'):
        run(event, _context())


def test_parse_date():
    assert parse_date("2026-08-13").isoformat() == "2026-08-13"


@pytest.mark.parametrize(
    "folder, expected",
    [
        (f"{PREFIX}org_id=1/", "org-1"),
        (f"{PREFIX}org_id=4/", "org-4"),
        (f"{PREFIX}_tmp/", None),
        (f"{PREFIX}org_id=/", None),
    ],
)
def test_organization_id_comes_from_the_org_id_folder(folder, expected):
    assert organization_id_from_folder(folder) == expected
