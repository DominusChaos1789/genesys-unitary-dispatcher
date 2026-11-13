import json
from types import SimpleNamespace

import pytest

from src.main import run
from src.sources import EventError
from test.conftest import LANDING_BUCKET, payload_by_org

DETAILS = "transacciones/genesys/api/conversations_details/"
UNITARY_KEY = "transacciones/genesys/unitary_process/request-1.json"
EVENT = {
    "tag": "surveys",
    "ids_source": "unitary_conversations",
    "ids_location": {"bucket": "landing", "key": UNITARY_KEY},
}


def _context():
    return SimpleNamespace(aws_request_id="req-unitary")


def _put(s3, key, body):
    s3.put_object(Bucket=LANDING_BUCKET, Key=key, Body=json.dumps(body))


def _conversation(conversation_id, survey_id=None, session_id=None):
    record = {"conversationId": conversation_id}
    if survey_id:
        record["surveys"] = [{"surveyId": survey_id, "surveyStatus": "Finished"}]
    if session_id:
        record["participants"] = [
            {"sessions": [{"sessionId": session_id, "recording": True, "mediaType": "voice"}]}
        ]
    return record


def _day(organization, day, *conversations):
    return f"{DETAILS}org_id={organization}/year=2026/month=10/day={day}/part.json", {
        "endpoint": list(conversations)
    }


def _request(**overrides):
    return {
        "tag": "surveys",
        "date": "2026-10-14",
        "organization_id": "org-3",
        "ids": ["c1", "c3"],
        **overrides,
    }


def test_only_the_listed_conversations_feed_the_tags_ids(aws):
    s3 = aws["s3"]
    _put(s3, UNITARY_KEY, _request())
    key, body = _day(3, 14, _conversation("c1", "s1"), _conversation("c2", "s2"), _conversation("c3", "s3"))
    _put(s3, key, body)

    result = run(EVENT, _context())

    entry = payload_by_org(result)["org-3"]
    assert entry["ids"] == ["s1", "s3"]
    assert result["unitary_conversations"]["conversations"] == {"org-3": 2}
    assert result["unitary_conversations"]["requests"] == 1


def test_transcripts_gets_session_pairs_for_those_conversations(aws):
    s3 = aws["s3"]
    _put(s3, UNITARY_KEY, _request(tag="transcripts", ids=["c1"]))
    key, body = _day(3, 14, _conversation("c1", session_id="x1"), _conversation("c2", session_id="x2"))
    _put(s3, key, body)

    result = run({**EVENT, "tag": "transcripts"}, _context())

    assert payload_by_org(result)["org-3"]["ids"] == [{"conversationId": "c1", "communicationId": "x1"}]


def test_only_the_requests_organization_and_day_are_read(aws):
    s3 = aws["s3"]
    _put(s3, UNITARY_KEY, _request())
    for organization, day in ((3, 14), (3, 13), (1, 14)):
        key, body = _day(organization, day, _conversation("c1", f"s-{organization}-{day}"))
        _put(s3, key, body)

    result = run(EVENT, _context())

    assert payload_by_org(result)["org-3"]["ids"] == ["s-3-14"]
    assert list(payload_by_org(result)) == ["org-3"]


def test_a_list_of_requests_is_merged_per_organization(aws):
    s3 = aws["s3"]
    _put(
        s3, UNITARY_KEY, [_request(ids=["c1"]), _request(date="2026-10-15", ids=["c9"]), _request(ids=["c1"])]
    )
    for day, conversation in ((14, "c1"), (15, "c9")):
        key, body = _day(3, day, _conversation(conversation, f"s-{conversation}"))
        _put(s3, key, body)

    result = run(EVENT, _context())

    assert payload_by_org(result)["org-3"]["ids"] == ["s-c1", "s-c9"]
    assert result["unitary_conversations"]["requests"] == 3


def test_the_file_can_come_from_an_s3_object_created_event(aws):
    s3 = aws["s3"]
    _put(s3, UNITARY_KEY, _request())
    key, body = _day(3, 14, _conversation("c1", "s1"))
    _put(s3, key, body)
    event = {
        "tag": "surveys",
        "ids_source": "unitary_conversations",
        "detail": {"bucket": {"name": LANDING_BUCKET}, "object": {"key": UNITARY_KEY}},
    }

    assert payload_by_org(run(event, _context()))["org-3"]["ids"] == ["s1"]


def test_conversations_not_found_produce_no_payload(aws):
    _put(aws["s3"], UNITARY_KEY, _request(ids=["missing"]))

    result = run(EVENT, _context())

    assert result["responses"] == []


def test_the_event_must_point_at_a_file(aws):
    with pytest.raises(EventError, match="Event carries no file"):
        run({"tag": "surveys", "ids_source": "unitary_conversations"}, _context())


@pytest.mark.parametrize(
    "body, message",
    [
        ({"date": "2026-10-14", "ids": []}, "needs 'organization_id' and an 'ids' list"),
        ({"date": "2026-10-14", "organization_id": "org-3"}, "needs 'organization_id' and an 'ids' list"),
        ({"date": "14-10-2026", "organization_id": "org-3", "ids": []}, 'needs "date" as YYYY-MM-DD'),
        (["not-an-object"], "must be an object"),
    ],
)
def test_a_malformed_file_is_rejected(aws, body, message):
    _put(aws["s3"], UNITARY_KEY, body)

    with pytest.raises(EventError, match=message):
        run(EVENT, _context())


def test_an_id_kind_conversations_details_cannot_feed_is_rejected(aws):
    _put(aws["s3"], UNITARY_KEY, _request())

    with pytest.raises(EventError, match="cannot feed id_kind 'management_unit'"):
        run({**EVENT, "tag": "funcionarios_adherencia"}, _context())


CSV_KEY = "transacciones/genesys/unitary_process/transcripts/req.csv"
CSV_EVENT = {
    "tag": "transcripts",
    "ids_source": "unitary_conversations",
    "ids_location": {"bucket": "output", "key": CSV_KEY},
}
CSV_HEADER = "conversationId,communicationId,cliente_prefijo,Organizacion\n"


def _put_csv(s3, *rows, key=CSV_KEY, header=CSV_HEADER):
    body = header + "".join(f"{row}\n" for row in rows)
    s3.put_object(Bucket="augusta-nexa-dev-output", Key=key, Body=body.encode("utf-8"))


@pytest.fixture
def output_bucket(aws):
    aws["s3"].create_bucket(Bucket="augusta-nexa-dev-output")
    return aws["s3"]


def test_a_csv_builds_each_organizations_pairs(output_bucket):
    _put_csv(
        output_bucket,
        "c1,P1,BPP,org-3",
        "c2,P2,BDB,org-2",
        "c3,P3,BDO,org-3",
        "c3,P4,BDO,org-3",
    )

    result = run(CSV_EVENT, _context())

    by_org = payload_by_org(result)
    assert by_org["org-3"]["ids"] == [
        {"conversationId": "c1", "communicationId": "P1"},
        {"conversationId": "c3", "communicationId": "P3"},
        {"conversationId": "c3", "communicationId": "P4"},
    ]
    assert by_org["org-2"]["ids"] == [{"conversationId": "c2", "communicationId": "P2"}]
    assert result["unitary_conversations"]["rows"] == 4
    assert result["unitary_conversations"]["conversations"] == {"org-2": 1, "org-3": 3}


def test_duplicate_csv_rows_collapse(output_bucket):
    _put_csv(output_bucket, "c1,P1,BDB,org-2", "c1,P1,BDB,org-2", "c1,P1,BDB,org-2")

    result = run(CSV_EVENT, _context())

    assert payload_by_org(result)["org-2"]["ids"] == [{"conversationId": "c1", "communicationId": "P1"}]


def test_csv_rows_missing_a_value_are_skipped_and_counted(output_bucket):
    _put_csv(output_bucket, "c1,P1,BDB,org-2", "c2,,BDB,org-2", ",P3,BDB,org-2", "c4,P4,BDB,")

    result = run(CSV_EVENT, _context())

    assert payload_by_org(result)["org-2"]["ids"] == [{"conversationId": "c1", "communicationId": "P1"}]
    assert result["unitary_conversations"]["skipped_rows"] == 3


def test_a_csv_can_feed_plain_conversation_ids(output_bucket):
    s3 = output_bucket
    _put_csv(s3, "c1,,BDB,org-2", "c2,P2,BDB,org-2")
    config = json.loads(
        s3.get_object(Bucket="augusta-nexa-dev-resources", Key="params/genesys/api/dispatcher.json")[
            "Body"
        ].read()
    )
    config["flows"]["surveys"]["id_kind"] = "conversation"
    s3.put_object(
        Bucket="augusta-nexa-dev-resources", Key="params/genesys/api/dispatcher.json", Body=json.dumps(config)
    )

    result = run({**CSV_EVENT, "tag": "surveys"}, _context())

    assert payload_by_org(result)["org-2"]["ids"] == ["c1", "c2"]


def test_a_csv_cannot_feed_surveys(output_bucket):
    _put_csv(output_bucket, "c1,P1,BDB,org-2")

    with pytest.raises(EventError, match="CSV cannot feed id_kind 'survey'"):
        run({**CSV_EVENT, "tag": "surveys"}, _context())


def test_a_csv_without_the_expected_columns_is_rejected(output_bucket):
    _put_csv(output_bucket, "c1,P1", header="conversationId,communicationId\n")

    with pytest.raises(EventError, match="needs the columns"):
        run(CSV_EVENT, _context())


def test_an_empty_csv_produces_no_payload(output_bucket):
    _put_csv(output_bucket)

    assert run(CSV_EVENT, _context())["responses"] == []
