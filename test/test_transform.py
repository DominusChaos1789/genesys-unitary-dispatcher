import json

import pytest

from src.contract import Contract
from src.transform import build_rows, dedup_rows
from test.conftest import load_fixture


@pytest.fixture
def contract():
    raw = load_fixture("transcripcion.json")
    return Contract(raw=raw)


@pytest.fixture
def records():
    return [
        load_fixture("sample_transcription_1.json"),
        load_fixture("sample_transcription_2.json"),
    ]


def _find(rows, interaccion_id):
    return next(r for r in rows if r["interaccion_id"] == interaccion_id)


def test_build_rows_returns_one_row_per_record(records, contract):
    rows, _ = build_rows(records, contract)
    assert len(rows) == 2


def test_conversation_ids_are_collected_from_genesys_cloud_id(records, contract):
    _, conversation_ids = build_rows(records, contract)
    assert conversation_ids == sorted(
        ["2a0db425-2370-4786-be61-1a9ee8e89855", "7b1fa3c2-91aa-4e2b-9d3a-0b2f6a7c1234"]
    )


def test_default_value_columns_are_stamped(records, contract):
    rows, _ = build_rows(records, contract)
    for row in rows:
        assert row["cliente_prefijo"] == "BDO"
        assert row["operacion_prefijo"] == "SAC"
        assert row["caso_uso_id"] == "temporal_id"
        assert row["canal"] == "VOICEBOT"
        assert row["origen"] == "GENESYS_CLOUD"


def test_trim_uppercase_and_remove_accents_applied_in_order(records, contract):
    rows, _ = build_rows(records, contract)
    row2 = _find(rows, "aa11bb22cc33dd44ee55ff6677889900")

    # "  Andres  " -> trim -> "Andres" -> uppercase -> "ANDRES" -> no accents to strip.
    assert row2["consumidor_nombre"] == "ANDRES"
    # "Niño" -> trim (no-op) -> uppercase -> "NIÑO" -> remove_accents -> "NINO"
    assert row2["consumidor_apellido"] == "NINO"


def test_timestamp_columns_are_normalized_to_utc_iso_strings(records, contract):
    rows, _ = build_rows(records, contract)
    row1 = _find(rows, "5624561b59ae99a0fae1e65cc206e4a1")

    # "2026-08-16T15:53:07-05:00" -> UTC -> "2026-08-16T20:53:07+00:00"
    assert row1["interaccion_fecha_inicio"] == "2026-08-16T20:53:07+00:00"


def test_json_column_mensajes_is_serialized_to_a_string(records, contract):
    rows, _ = build_rows(records, contract)
    row1 = _find(rows, "5624561b59ae99a0fae1e65cc206e4a1")

    messages = json.loads(row1["mensajes"])
    assert messages[0]["role"] == "assistant"
    assert len(messages) == 4


def test_non_required_missing_field_is_none(contract):
    record = load_fixture("sample_transcription_1.json")
    record.pop("interaction_result")
    rows, _ = build_rows([record], contract)
    assert rows[0]["interaccion_resultado"] in (None, "")


def test_dedup_rows_keeps_latest_fecha_fin(contract):
    older = load_fixture("sample_transcription_1.json")
    newer = dict(older)
    newer["exported_at"] = "2026-08-16T20:00:00-05:00"
    newer["interaction_result"] = "actualizado"

    rows, _ = build_rows([older, newer], contract)
    deduped = dedup_rows(rows, contract.deduplication)

    assert len(deduped) == 1
    assert deduped[0]["interaccion_resultado"] == "actualizado"


def test_dedup_rows_keeps_distinct_keys(records, contract):
    rows, _ = build_rows(records, contract)
    deduped = dedup_rows(rows, contract.deduplication)
    assert len(deduped) == 2


def test_dedup_rows_disabled_returns_all_rows(records, contract):
    rows, _ = build_rows(records, contract)
    deduped = dedup_rows(rows, {"enabled": False})
    assert len(deduped) == len(rows)


def test_dedup_rows_accepts_order_type_as_plain_string(contract):
    # The new contract shape has "order_type": "desc" (a string), not a list.
    older = load_fixture("sample_transcription_1.json")
    newer = dict(older)
    newer["exported_at"] = "2026-08-16T20:00:00-05:00"

    rows, _ = build_rows([older, newer], contract)
    assert isinstance(contract.deduplication["order_type"], str)
    deduped = dedup_rows(rows, contract.deduplication)
    assert len(deduped) == 1
