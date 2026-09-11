import hashlib
from datetime import datetime, timezone

import pytest

from src.contract import Contract
from src.technical_columns import build_output_rows, compute_technical_columns
from test.conftest import load_fixture


@pytest.fixture
def now():
    return datetime(2026, 8, 21, 15, 30, 45, tzinfo=timezone.utc)


def test_execution_id_returns_the_given_execution_id(now):
    spec = {"ejecucion_id": {"calculus_type": "execution_id"}}
    computed = compute_technical_columns(spec, {}, {}, "file.json", now, "exec-42")
    assert computed["ejecucion_id"] == "exec-42"


def test_uniqe_returns_a_fresh_sha256_hex_each_call(now):
    spec = {"registro_id": {"calculus_type": "uniqe"}}
    first = compute_technical_columns(spec, {}, {}, "file.json", now, "exec")["registro_id"]
    second = compute_technical_columns(spec, {}, {}, "file.json", now, "exec")["registro_id"]

    assert len(first) == 64
    int(first, 16)  # valid hex
    assert first != second  # random per call, not deterministic


def test_concatetation_hashes_joined_column_values(now):
    spec = {
        "combo": {
            "calculus_type": "concatetation",
            "delimiter": "#",
            "columns": ["a", "b"],
        }
    }
    computed = compute_technical_columns(spec, {}, {"a": "X", "b": "Y"}, "file.json", now, "exec")
    assert computed["combo"] == hashlib.sha256(b"X#Y").hexdigest()


def test_concatetation_treats_missing_values_as_empty_string(now):
    spec = {
        "combo": {
            "calculus_type": "concatetation",
            "delimiter": "#",
            "columns": ["a", "missing"],
        }
    }
    computed = compute_technical_columns(spec, {}, {"a": "X"}, "file.json", now, "exec")
    assert computed["combo"] == hashlib.sha256(b"X#").hexdigest()


def test_field_mapping_resolves_from_row_then_contract_then_bracket(now):
    spec = {
        "from_row": {"calculus_type": "field_mapping", "columns": ["row_field"]},
        "from_contract": {"calculus_type": "field_mapping", "columns": ["top_level_field"]},
        "from_bracket": {"calculus_type": "field_mapping", "columns": ["nested[inner]"]},
    }
    contract_raw = {"top_level_field": "CONTRACT_VALUE", "nested": {"inner": "BRACKET_VALUE"}}
    row = {"row_field": "ROW_VALUE"}

    computed = compute_technical_columns(spec, contract_raw, row, "file.json", now, "exec")
    assert computed["from_row"] == "ROW_VALUE"
    assert computed["from_contract"] == "CONTRACT_VALUE"
    assert computed["from_bracket"] == "BRACKET_VALUE"


def test_field_mapping_skips_no_apply_sentinel(now):
    spec = {"col": {"calculus_type": "field_mapping", "columns": ["No apply"]}}
    computed = compute_technical_columns(spec, {}, {}, "file.json", now, "exec")
    assert computed["col"] is None


def test_field_mapping_can_reference_another_technical_column(now):
    # registro_hash-style: depends on a technical column defined *after* it
    # in the dict, so this exercises the lazy/memoized resolution, not just
    # dict-order iteration.
    spec = {
        "depends_on_later": {"calculus_type": "field_mapping", "columns": ["computed_later"]},
        "computed_later": {"calculus_type": "field_mapping", "columns": ["source"]},
    }
    computed = compute_technical_columns(spec, {"source": "VALUE"}, {}, "file.json", now, "exec")
    assert computed["depends_on_later"] == "VALUE"
    assert computed["computed_later"] == "VALUE"


def test_circular_technical_column_dependency_raises(now):
    spec = {
        "a": {"calculus_type": "field_mapping", "columns": ["b"]},
        "b": {"calculus_type": "field_mapping", "columns": ["a"]},
    }
    with pytest.raises(ValueError, match="Circular"):
        compute_technical_columns(spec, {}, {}, "file.json", now, "exec")


def test_file_name_returns_the_source_filename(now):
    spec = {"base_nombre": {"calculus_type": "file_name"}}
    computed = compute_technical_columns(spec, {}, {}, "sample_transcription_1.json", now, "exec")
    assert computed["base_nombre"] == "sample_transcription_1.json"


def test_file_landing_date_id_suffix_is_date_only(now):
    spec = {"cargue_fecha_id": {"calculus_type": "file_landing_date"}}
    computed = compute_technical_columns(spec, {}, {}, "file.json", now, "exec")
    assert computed["cargue_fecha_id"] == "2026-08-21"


def test_file_landing_date_without_id_suffix_is_full_iso_datetime(now):
    spec = {"cargue_fecha": {"calculus_type": "file_landing_date"}}
    computed = compute_technical_columns(spec, {}, {}, "file.json", now, "exec")
    assert computed["cargue_fecha"] == now.isoformat()


def test_attribute_calculus_types_are_not_computed_here(now):
    spec = {"atributo_nombre": {"calculus_type": "column_name"}}
    computed = compute_technical_columns(spec, {}, {}, "file.json", now, "exec")
    assert "atributo_nombre" not in computed


@pytest.fixture
def contract():
    raw = load_fixture("transcripcion.json")
    return Contract(raw=raw)


def test_build_output_rows_produces_one_core_row_and_one_atts_row_per_column(now, contract):
    rows = [{"interaccion_id": "abc", "consumidor_id": "1", "_source_key": "a/f1.json"}]

    core_rows, atts_rows = build_output_rows(rows, contract, now, "exec-1")

    assert len(core_rows) == 1
    assert len(atts_rows) == len(contract.columns)
    assert "_source_key" not in core_rows[0]


def test_build_output_rows_atts_rows_carry_atributo_fields_for_every_column(now, contract):
    rows = [{"interaccion_id": "abc", "consumidor_id": "1", "_source_key": "a/f1.json"}]
    _, atts_rows = build_output_rows(rows, contract, now, "exec-1")

    names_seen = {r["atributo_nombre"] for r in atts_rows}
    assert names_seen == {c["name"] for c in contract.columns}

    consumidor_id_row = next(r for r in atts_rows if r["atributo_nombre"] == "consumidor_id")
    assert consumidor_id_row["atributo_valor"] == "1"
    assert consumidor_id_row["atributo_tipo"] == "string"


def test_build_output_rows_base_nombre_uses_source_key_basename(now, contract):
    rows = [{"_source_key": "external/datanexa/BDO/sample_transcription_1.json"}]
    core_rows, _ = build_output_rows(rows, contract, now, "exec-1")
    assert core_rows[0]["base_nombre"] == "sample_transcription_1.json"
