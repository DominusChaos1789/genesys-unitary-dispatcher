"""recordings CSV with caso_uso_nombre: one payload per (caso de uso,
organization), under recordings/<caso_uso_nombre>/, carrying where Download
saves that group's recordings (bucket_nombre, prefix_nombre)."""

import json
from types import SimpleNamespace

import pytest

from src.main import run
from src.sources import EventError
from test.conftest import LOGS_BUCKET

OUTPUT_BUCKET = "augusta-nexa-dev-output"
CSV_KEY = "transacciones/genesys/unitary_process/recordings/37f23823-25ac-4e51-8c2b-c7bd685bbfbc.csv"
EVENT = {
    "tag": "recordings",
    "ids_source": "unitary_conversations",
    "ids_location": {"bucket": "output", "key": CSV_KEY},
}
PREFIX = "transacciones/genesys/api/payload_request_unitary/recordings"
HEADER = (
    '"conversacion_id","cola_nombre","bucket_nombre","prefix_nombre","cliente_prefijo","caso_uso_nombre",'
    '"Organizacion"'
)
GRABACIONES = "/clientes/bdo/sac/operacion/genesys/grabaciones/"


def _row(conversation_id, caso_uso, organization="org-3", bucket="output-transit", prefix=GRABACIONES):
    return f'"{conversation_id}","Q_BDO_SAC","{bucket}","{prefix}","BDO","{caso_uso}","{organization}"'


@pytest.fixture
def output_bucket(aws):
    aws["s3"].create_bucket(Bucket=OUTPUT_BUCKET)
    return aws["s3"]


def _put(s3, *rows):
    s3.put_object(Bucket=OUTPUT_BUCKET, Key=CSV_KEY, Body=("\n".join([HEADER, *rows]) + "\n").encode("utf-8"))


def _read(s3, key):
    return json.loads(s3.get_object(Bucket=LOGS_BUCKET, Key=key)["Body"].read())


def _context():
    return SimpleNamespace(aws_request_id="req-caso-uso")


def test_one_payload_per_caso_de_uso_and_organization(output_bucket):
    _put(
        output_bucket,
        _row("c2", "BANCA PERSONAS"),
        _row("c1", "BANCA PERSONAS"),
        _row("c1", "BANCA PERSONAS"),
        _row("c3", "BANCA EMPRESAS", prefix="/clientes/bdo/empresas/grabaciones/"),
        _row("c4", "BANCA PERSONAS", organization="org-1"),
    )

    result = run(EVENT, _context())

    locations = sorted(r["payload_location"] for r in result["responses"])
    assert locations == [
        f"{PREFIX}/BANCA EMPRESAS/org-3.json",
        f"{PREFIX}/BANCA PERSONAS/org-1.json",
        f"{PREFIX}/BANCA PERSONAS/org-3.json",
    ]
    personas = _read(output_bucket, f"{PREFIX}/BANCA PERSONAS/org-3.json")
    assert personas["ids"] == ["c1", "c2"]
    assert personas["tag"] == "recordings"
    assert personas["organization_id"] == "org-3"
    assert personas["caso_uso_nombre"] == "BANCA PERSONAS"
    assert personas["bucket_nombre"] == "output-transit"
    assert personas["prefix_nombre"] == GRABACIONES
    assert set(personas) >= {"request_context", "request_url", "date", "failed_organizations"}
    empresas = _read(output_bucket, f"{PREFIX}/BANCA EMPRESAS/org-3.json")
    assert empresas["ids"] == ["c3"]
    assert empresas["prefix_nombre"] == "/clientes/bdo/empresas/grabaciones/"


def test_the_response_says_which_caso_de_uso_each_file_is(output_bucket):
    _put(output_bucket, _row("c1", "BANCA PERSONAS"))

    result = run(EVENT, _context())

    [response] = result["responses"]
    assert response["tag"] == "recordings"
    assert response["caso_uso_nombre"] == "BANCA PERSONAS"
    assert response["bucket_nombre"] == "output-transit"
    assert response["prefix_nombre"] == GRABACIONES
    assert result["unitary_conversations"]["caso_uso_nombre"] == {"org-3": {"BANCA PERSONAS": 1}}


def test_one_token_per_organization_however_many_casos_de_uso(output_bucket, genesys_api):
    _put(output_bucket, _row("c1", "A"), _row("c2", "B"), _row("c3", "C"))

    run(EVENT, _context())

    assert genesys_api["requested_orgs"] == ["org-3"]


def test_rows_without_a_caso_de_uso_are_skipped(output_bucket):
    _put(output_bucket, _row("c1", ""), _row("c2", "BANCA PERSONAS"))

    result = run(EVENT, _context())

    assert [r["payload_location"] for r in result["responses"]] == [f"{PREFIX}/BANCA PERSONAS/org-3.json"]
    assert result["unitary_conversations"]["skipped_rows"] == 1


def test_one_caso_de_uso_cannot_name_two_destinations(output_bucket):
    _put(output_bucket, _row("c1", "BANCA PERSONAS"), _row("c2", "BANCA PERSONAS", bucket="other-bucket"))

    with pytest.raises(EventError, match="names more than one bucket/prefix"):
        run(EVENT, _context())


def test_the_same_caso_de_uso_may_differ_between_organizations(output_bucket):
    _put(
        output_bucket,
        _row("c1", "BANCA PERSONAS", organization="org-1", prefix="/org1/"),
        _row("c2", "BANCA PERSONAS", organization="org-3", prefix="/org3/"),
    )

    result = run(EVENT, _context())

    assert len(result["responses"]) == 2
    assert _read(output_bucket, f"{PREFIX}/BANCA PERSONAS/org-1.json")["prefix_nombre"] == "/org1/"


def test_a_slash_in_the_caso_de_uso_does_not_add_a_folder(output_bucket):
    _put(output_bucket, _row("c1", "BANCA/PERSONAS"))

    result = run(EVENT, _context())

    [response] = result["responses"]
    assert response["payload_location"] == f"{PREFIX}/BANCA-PERSONAS/org-3.json"
    assert response["caso_uso_nombre"] == "BANCA/PERSONAS"


def test_a_csv_without_caso_uso_nombre_keeps_one_file_per_organization(output_bucket):
    output_bucket.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=CSV_KEY,
        Body=b'"conversacion_id","cliente_prefijo","Organizacion"\n"c1","BDO","org-3"\n',
    )

    result = run(EVENT, _context())

    [response] = result["responses"]
    assert response["payload_location"] == f"{PREFIX}/org-3.json"
    assert "caso_uso_nombre" not in response
    assert "bucket_nombre" not in _read(output_bucket, response["payload_location"])


def test_a_failed_organization_is_reported_once_for_all_its_casos_de_uso(output_bucket, monkeypatch):
    _put(
        output_bucket,
        _row("c1", "A", organization="org-1"),
        _row("c2", "B", organization="org-1"),
        _row("c3", "A", organization="org-3"),
    )

    def flaky_get_token(secret, connection, base_path, server, **kwargs):
        if server["oauth"] == "org-1":
            raise RuntimeError("oauth down for org-1")
        return {"access_token": "{token}"}

    monkeypatch.setattr("src.main.get_token", flaky_get_token)

    result = run(EVENT, _context())

    assert [r["payload_location"] for r in result["responses"]] == [f"{PREFIX}/A/org-3.json"]
    assert result["failed_organizations"] == [
        {"organization_id": "org-1", "id_count": 2, "error": "oauth down for org-1"}
    ]
