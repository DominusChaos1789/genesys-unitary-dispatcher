"""recordings: list a conversation's recordings, then each one's download URL.

Ids (recorded conversation ids) come from a CSV, from Athena by date, or from
conversations_details / the unitary JSON file like the other flows.
"""

import json
from datetime import date
from types import SimpleNamespace

import pytest

import src.recorded_conversations as recorded_conversations
from src.config import load_settings
from src.main import run
from src.recorded_conversations import build_query, collect_recorded_conversations
from src.sources import EventError
from test.conftest import LANDING_BUCKET, payload_by_org

OUTPUT_BUCKET = "augusta-nexa-dev-output"
CSV_KEY = "transacciones/genesys/unitary_process/recordings/4d16aedd.csv"
CSV_EVENT = {
    "tag": "recordings",
    "ids_source": "unitary_conversations",
    "ids_location": {"bucket": "output", "key": CSV_KEY},
}
DATE_EVENT = {"tag": "recordings", "ids_source": "unitary_conversations", "date": "2026-11-19"}
LIST_URL = "/api/v2/conversations/{conversationId}/recordings?formatId=MP3&mediaFormats=MP3&locale=en-US"
DOWNLOAD_URL = (
    "/api/v2/conversations/{conversationId}/recordings/{recordingId}"
    "?formatId=MP3&download=true&locale=en-US&mediaFormats=MP3"
)


def _context():
    return SimpleNamespace(aws_request_id="req-recordings")


ATHENA_COLUMNS = (
    "conversacion_id",
    "cola_nombre",
    "bucket_nombre",
    "prefix_nombre",
    "cliente_prefijo",
    "caso_uso_nombre",
    "organizacion",
)
GRABACIONES = "/clientes/bdo/sac/operacion/genesys/grabaciones/"


def _athena_row(
    conversation_id, organization, caso_uso="BANCA PERSONAS", bucket="output-transit", prefix=GRABACIONES
):
    return (conversation_id, "Q_BDO_SAC", bucket, prefix, "BDO", caso_uso, organization)


def _cell(value):
    # Athena leaves VarCharValue out for a NULL.
    return {} if value is None else {"VarCharValue": value}


class FakeAthena:
    """Just enough of the Athena client: queued states, then paged rows."""

    def __init__(self, rows, states=("RUNNING", "SUCCEEDED"), reason=None):
        self.rows = rows
        self.states = list(states)
        self.reason = reason
        self.started = []
        self.stopped = []

    def start_query_execution(self, **request):
        self.started.append(request)
        return {"QueryExecutionId": "q-1"}

    def get_query_execution(self, QueryExecutionId):
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        status = {"State": state}
        if self.reason:
            status["StateChangeReason"] = self.reason
        return {"QueryExecution": {"Status": status}}

    def stop_query_execution(self, QueryExecutionId):
        self.stopped.append(QueryExecutionId)

    def get_paginator(self, name):
        assert name == "get_query_results"
        fake = self

        class _Paginator:
            def paginate(self, QueryExecutionId):
                # The query's columns; Athena lower-cases the "Organizacion" alias.
                header = {"Data": [{"VarCharValue": name} for name in ATHENA_COLUMNS]}
                rows = [{"Data": [_cell(value) for value in _athena_row(*row)]} for row in fake.rows]
                # Two pages; only the first one starts with the header.
                yield {"ResultSet": {"Rows": [header, *rows[:1]]}}
                yield {"ResultSet": {"Rows": rows[1:]}}

        return _Paginator()


@pytest.fixture
def athena(monkeypatch):
    holder = {}

    def install(rows, **kwargs):
        holder["client"] = FakeAthena(rows, **kwargs)
        monkeypatch.setattr(recorded_conversations, "_athena_client", lambda settings: holder["client"])
        monkeypatch.setattr(recorded_conversations.time, "sleep", lambda seconds: None)
        return holder["client"]

    return install


@pytest.fixture
def output_bucket(aws):
    aws["s3"].create_bucket(Bucket=OUTPUT_BUCKET)
    return aws["s3"]


def _put_csv(s3, *rows, header='"conversacion_id","cliente_prefijo","Organizacion"'):
    body = "\n".join([header, *rows]) + "\n"
    s3.put_object(Bucket=OUTPUT_BUCKET, Key=CSV_KEY, Body=body.encode("utf-8"))


# --- CSV ---------------------------------------------------------------------


def test_a_recordings_csv_builds_both_stages_per_organization(output_bucket):
    _put_csv(
        output_bucket,
        '"c1","BDO","org-3"',
        '"c1","BDO","org-3"',
        '"c2","BDO","org-3"',
        '"c9","BDB","org-2"',
    )

    result = run(CSV_EVENT, _context())

    by_org = payload_by_org(result)
    assert by_org["org-3"]["ids"] == ["c1", "c2"]
    assert by_org["org-2"]["ids"] == ["c9"]
    payload = by_org["org-3"]
    assert payload["request_context"]["url"].endswith(LIST_URL)
    assert payload["request_url"]["url"].endswith(DOWNLOAD_URL)
    assert payload["request_context"]["result_data"] == "selfUri"
    assert result["unitary_conversations"]["rows"] == 4


def test_a_recordings_csv_needs_the_organization_column(output_bucket):
    _put_csv(output_bucket, '"c1","BDO"', header='"conversacion_id","cliente_prefijo"')

    with pytest.raises(EventError, match="needs a conversation id column"):
        run(CSV_EVENT, _context())


def test_a_transcripts_csv_still_needs_the_communication_column(output_bucket):
    _put_csv(output_bucket, '"c1","BDO","org-3"')

    with pytest.raises(EventError, match="communicationId"):
        run({**CSV_EVENT, "tag": "transcripts"}, _context())


def test_rows_without_a_conversation_are_skipped(output_bucket):
    _put_csv(output_bucket, '"","BDO","org-3"', '"c1","BDO","org-3"')

    result = run(CSV_EVENT, _context())

    assert payload_by_org(result)["org-3"]["ids"] == ["c1"]
    assert result["unitary_conversations"]["skipped_rows"] == 1


# --- Athena by date ------------------------------------------------------------


def test_by_date_queries_athena_and_groups_by_organization(aws, athena):
    client = athena([("c1", "org-3"), ("c2", "org-3"), ("c3", "org-2")])

    result = run(DATE_EVENT, _context())

    by_org = payload_by_org(result)
    assert by_org["org-3"]["ids"] == ["c1", "c2"]
    assert by_org["org-2"]["ids"] == ["c3"]
    assert by_org["org-3"]["date"] == "2026-11-19"
    request = client.started[0]
    assert request["QueryExecutionContext"] == {"Database": "augusta-nexa-dev-transacciones-curated"}
    assert request["WorkGroup"] == "primary"
    assert "ResultConfiguration" not in request
    assert "DATE '2026-11-19'" in request["QueryString"]
    assert result["unitary_conversations"]["query_execution_id"] == "q-1"
    assert result["unitary_conversations"]["conversations"] == {"org-2": 1, "org-3": 2}


def test_by_date_uses_the_configured_output_location(aws, athena, monkeypatch):
    monkeypatch.setenv("ATHENA_OUTPUT_LOCATION", "s3://results/athena/")
    client = athena([("c1", "org-3")])

    run(DATE_EVENT, _context())

    assert client.started[0]["ResultConfiguration"] == {"OutputLocation": "s3://results/athena/"}


def test_by_date_filters_go_into_the_query(aws, athena):
    client = athena([("c1", "org-3")])
    event = {**DATE_EVENT, "cliente_prefijo": "BDO", "operacion_prefijo": "SAC", "organization_id": "org-3"}

    result = run(event, _context())

    query = client.started[0]["QueryString"]
    assert "cliente_prefijo = 'BDO'" in query
    assert "operacion_prefijo = 'SAC'" in query
    assert "replace(lower(b.organizacion_id), '_0', '-') = 'org-3'" in query
    assert result["unitary_conversations"]["filters"] == {
        "cliente_prefijo": "BDO",
        "operacion_prefijo": "SAC",
        "organization_id": "org-3",
    }


@pytest.mark.parametrize(
    ("key", "value"),
    [("cliente_prefijo", "BDO' OR '1'='1"), ("operacion_prefijo", 7), ("organization_id", "3")],
)
def test_by_date_rejects_unsafe_filters(aws, athena, key, value):
    client = athena([])

    with pytest.raises(EventError, match=key):
        run({**DATE_EVENT, key: value}, _context())
    assert client.started == []


def test_by_date_needs_a_valid_date(aws, athena):
    athena([])

    with pytest.raises(EventError, match='needs "date" as YYYY-MM-DD'):
        run({**DATE_EVENT, "date": "19/11/2026"}, _context())


def test_by_date_only_feeds_recordings(aws, athena):
    athena([])

    with pytest.raises(EventError, match='only feeds id_kind "recording"'):
        run({**DATE_EVENT, "tag": "transcripts"}, _context())


def test_a_failed_query_fails_the_run(aws, athena):
    athena([], states=("FAILED",), reason="TABLE_NOT_FOUND")

    with pytest.raises(RuntimeError, match="FAILED: TABLE_NOT_FOUND"):
        run(DATE_EVENT, _context())


def test_a_query_past_the_timeout_is_stopped():
    client = FakeAthena([], states=("RUNNING",))

    with pytest.raises(RuntimeError, match="still RUNNING"):
        collect_recorded_conversations(
            load_settings(), date(2026, 11, 19), {}, client=client, timeout_seconds=0, sleep=lambda s: None
        )
    assert client.stopped == ["q-1"]


def test_no_rows_writes_no_payload(aws, athena):
    athena([])

    result = run(DATE_EVENT, _context())

    assert result["responses"] == []


def test_the_query_joins_sessions_segments_and_the_parametric_catalog():
    query = build_query(load_settings(), date(2026, 11, 19), {})

    assert 'FROM "augusta-nexa-dev-transacciones-curated"."genesys-conversaciones-sesiones-core" a' in query
    assert (
        '"augusta-nexa-dev-transacciones-curated"."genesys-conversaciones-sesiones-segmentos-core" s' in query
    )
    assert '"augusta-nexa-dev-catalogos-curated"."parametrica-core" WHERE grabacion_estado = \'1\'' in query
    assert "lower(s.cola_atencion_id) = lower(p.cola_id)" in query
    assert "replace(lower(b.organizacion_id), '_0', '-') AS Organizacion" in query
    assert "a.interaccion_grabada = true" in query
    assert "CAST(a.conversacion_fecha_inicio AS DATE) = DATE '2026-11-19'" in query
    for column in ("b.cola_nombre", "b.bucket_nombre", "b.prefix_nombre", "b.caso_uso_nombre"):
        assert column in query
    assert "= 'org-" not in query


def test_the_query_keeps_each_conversations_first_recorded_queue():
    query = build_query(load_settings(), date(2026, 11, 19), {})

    assert "PARTITION BY lower(s.conversacion_id)" in query
    assert "ORDER BY s.conversaciones_fecha_inicio ASC" in query
    assert "WHERE queue_order = 1" in query


def test_the_segment_order_column_can_be_configured(monkeypatch):
    monkeypatch.setenv("ATHENA_SEGMENT_ORDER_COLUMN", "segmento_fecha_inicio")

    assert "ORDER BY s.segmento_fecha_inicio ASC" in build_query(load_settings(), date(2026, 11, 19), {})


def test_an_unsafe_segment_order_column_is_rejected(monkeypatch):
    monkeypatch.setenv("ATHENA_SEGMENT_ORDER_COLUMN", "x; DROP TABLE y")

    with pytest.raises(ValueError, match="plain column name"):
        build_query(load_settings(), date(2026, 11, 19), {})


def test_the_catalog_tables_can_be_configured(monkeypatch):
    monkeypatch.setenv("ATHENA_CATALOG_DATABASE", "my-catalog")
    monkeypatch.setenv("ATHENA_CATALOG_TABLE", "my-param")
    monkeypatch.setenv("ATHENA_SEGMENTS_TABLE", "my-segments")

    query = build_query(load_settings(), date(2026, 11, 19), {})

    assert '"my-catalog"."my-param"' in query
    assert '"augusta-nexa-dev-transacciones-curated"."my-segments"' in query


def test_by_date_splits_by_caso_de_uso_like_the_csv(aws, athena):
    athena(
        [
            ("c1", "org-3", "BANCA PERSONAS"),
            ("c2", "org-3", "BANCA EMPRESAS", "output-transit", "/empresas/"),
            ("c3", "org-3", None, None, None),
        ]
    )

    result = run(DATE_EVENT, _context())

    by_location = {r["payload_location"].split("recordings/", 1)[1]: r for r in result["responses"]}
    assert sorted(by_location) == [
        "BANCA EMPRESAS/org-3.json",
        "BANCA PERSONAS/org-3.json",
        "CASO_USO_UNKNOWN/org-3.json",
    ]
    assert by_location["BANCA EMPRESAS/org-3.json"]["prefix_nombre"] == "/empresas/"
    assert by_location["BANCA PERSONAS/org-3.json"]["bucket_nombre"] == "output-transit"
    assert by_location["CASO_USO_UNKNOWN/org-3.json"]["bucket_nombre"] == ""
    summary = result["unitary_conversations"]
    assert summary["caso_uso_nombre"] == {
        "org-3": {"BANCA EMPRESAS": 1, "BANCA PERSONAS": 1, "CASO_USO_UNKNOWN": 1}
    }
    assert "file" not in summary


# --- conversations_details / unitary JSON ---------------------------------------


def _details(*conversations):
    return json.dumps({"endpoint": list(conversations)})


def _conversation(conversation_id, recorded):
    return {
        "conversationId": conversation_id,
        "participants": [{"sessions": [{"sessionId": "s", "recording": recorded, "mediaType": "voice"}]}],
    }


def test_conversations_details_feeds_only_recorded_conversations(aws):
    key = "transacciones/genesys/api/conversations_details/org_id=3/year=2026/month=11/day=19/a.json"
    aws["s3"].put_object(
        Bucket=LANDING_BUCKET,
        Key=key,
        Body=_details(_conversation("c1", True), _conversation("c2", False), _conversation("c3", "true")),
    )

    result = run(
        {"tag": "recordings", "ids_source": "conversations_details", "date": "2026-11-19"}, _context()
    )

    assert payload_by_org(result)["org-3"]["ids"] == ["c1"]


def test_the_unitary_json_file_feeds_recordings_too(aws):
    details = "transacciones/genesys/api/conversations_details/org_id=3/year=2026/month=11/day=19/a.json"
    aws["s3"].put_object(
        Bucket=LANDING_BUCKET,
        Key=details,
        Body=_details(_conversation("c1", True), _conversation("c2", True)),
    )
    request_key = "transacciones/genesys/unitary_process/r.json"
    aws["s3"].put_object(
        Bucket=LANDING_BUCKET,
        Key=request_key,
        Body=json.dumps(
            {"tag": "recordings", "date": "2026-11-19", "organization_id": "org-3", "ids": ["c2"]}
        ),
    )

    result = run(
        {
            "tag": "recordings",
            "ids_source": "unitary_conversations",
            "ids_location": {"bucket": "landing", "key": request_key},
        },
        _context(),
    )

    assert payload_by_org(result)["org-3"]["ids"] == ["c2"]


def test_the_athena_client_uses_the_configured_region(aws):
    client = recorded_conversations._athena_client(load_settings())

    assert client.meta.service_model.service_name == "athena"
    assert client.meta.region_name == "us-east-2"


def test_a_file_wins_over_a_date(output_bucket, athena):
    client = athena([("athena-only", "org-3")])
    _put_csv(output_bucket, '"c1","BDO","org-3"')

    result = run({**CSV_EVENT, "date": "2026-11-19"}, _context())

    assert payload_by_org(result)["org-3"]["ids"] == ["c1"]
    assert payload_by_org(result)["org-3"]["date"] == "2026-11-19"
    assert client.started == []
