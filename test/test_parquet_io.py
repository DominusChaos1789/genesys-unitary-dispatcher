import json
import os
import tempfile
from datetime import datetime, timezone

import polars as pl
import pytest

from src.contract import Contract
from src.parquet_io import write_hive_parquet
from src.transform import build_rows
from test.conftest import load_fixture


def _read_parquet_rows(s3_client, bucket: str, key: str) -> list[dict]:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, "part.parquet")
        with open(local_path, "wb") as f:
            f.write(body)
        return pl.read_parquet(local_path).to_dicts()


def _expected_date_path() -> str:
    now = datetime.now(timezone.utc)
    return f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}"


@pytest.fixture
def contract():
    raw = load_fixture("transcripcion.json")
    return Contract(raw=raw)


def test_write_hive_parquet_partitions_by_configured_columns(aws, contract):
    s3 = aws["s3"]
    records = [load_fixture("sample_transcription_1.json"), load_fixture("sample_transcription_2.json")]
    rows, _ = build_rows(records, contract)

    keys = write_hive_parquet(
        s3,
        rows,
        "augusta-nexa-dev-refined",
        "transacciones/empatia/transcripciones",
        partition_cols=contract.partition_by,
        column_types=contract.column_types,
        filename_prefix="transcripciones",
    )

    assert len(keys) >= 1
    expected_prefix = (
        f"transacciones/empatia/transcripciones/cliente_prefijo=BDO/"
        f"operacion_prefijo=SAC/{_expected_date_path()}/"
    )
    for key in keys:
        assert key.startswith(expected_prefix)
        filename = key.rsplit("/", 1)[-1]
        assert filename.startswith("transcripciones_")
        assert filename.endswith(".parquet")

    total_rows = sum(len(_read_parquet_rows(s3, "augusta-nexa-dev-refined", key)) for key in keys)
    assert total_rows == 2

    # Partition columns are encoded in the path, not duplicated in the file.
    written = _read_parquet_rows(s3, "augusta-nexa-dev-refined", keys[0])[0]
    assert "cliente_prefijo" not in written
    assert "operacion_prefijo" not in written


def test_write_hive_parquet_does_not_infer_uuid_type_for_string_columns(aws, contract):
    # conversacion_id holds UUID-formatted strings (genesys_cloud_id).
    # Parquet readers besides DuckDB itself (e.g. AWS S3 Select) can't read
    # a UUID-typed column at all, so it must round-trip as a plain string
    # column regardless of which engine happens to write the file.
    s3 = aws["s3"]
    records = [load_fixture("sample_transcription_1.json")]
    rows, _ = build_rows(records, contract)

    keys = write_hive_parquet(
        s3,
        rows,
        "augusta-nexa-dev-refined",
        "p",
        partition_cols=contract.partition_by,
        column_types=contract.column_types,
    )

    body = s3.get_object(Bucket="augusta-nexa-dev-refined", Key=keys[0])["Body"].read()
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, "part.parquet")
        with open(local_path, "wb") as f:
            f.write(body)
        schema = pl.read_parquet_schema(local_path)

    assert schema["conversacion_id"] == pl.Utf8


def test_write_hive_parquet_casts_timestamp_columns(aws, contract):
    s3 = aws["s3"]
    records = [load_fixture("sample_transcription_1.json")]
    rows, _ = build_rows(records, contract)

    keys = write_hive_parquet(
        s3,
        rows,
        "augusta-nexa-dev-refined",
        "p",
        partition_cols=contract.partition_by,
        column_types=contract.column_types,
    )

    written = _read_parquet_rows(s3, "augusta-nexa-dev-refined", keys[0])[0]
    import datetime

    assert isinstance(written["interaccion_fecha_inicio"], datetime.datetime)


def test_write_hive_parquet_empty_rows_writes_nothing(aws, contract):
    s3 = aws["s3"]
    keys = write_hive_parquet(
        s3,
        [],
        "augusta-nexa-dev-refined",
        "p",
        partition_cols=contract.partition_by,
    )
    assert keys == []


def test_write_hive_parquet_without_partition_columns_writes_a_single_file(aws):
    s3 = aws["s3"]
    rows = [{"a": "x", "b": 1}, {"a": "y", "b": 2}]

    keys = write_hive_parquet(s3, rows, "augusta-nexa-dev-refined", "flat")

    assert len(keys) == 1
    assert keys[0].startswith(f"flat/{_expected_date_path()}/part_")
    assert keys[0].endswith(".parquet")
    written_rows = _read_parquet_rows(s3, "augusta-nexa-dev-refined", keys[0])
    assert len(written_rows) == 2


def test_write_hive_parquet_uses_custom_filename_prefix(aws):
    s3 = aws["s3"]
    rows = [{"a": "x"}]

    keys = write_hive_parquet(s3, rows, "augusta-nexa-dev-refined", "p", filename_prefix="atributos")

    assert keys[0].rsplit("/", 1)[-1].startswith("atributos_")


def test_write_hive_parquet_json_column_round_trips(aws, contract):
    s3 = aws["s3"]
    records = [load_fixture("sample_transcription_1.json")]
    rows, _ = build_rows(records, contract)

    keys = write_hive_parquet(
        s3,
        rows,
        "augusta-nexa-dev-refined",
        "p",
        partition_cols=contract.partition_by,
        column_types=contract.column_types,
    )

    written = _read_parquet_rows(s3, "augusta-nexa-dev-refined", keys[0])[0]
    messages = json.loads(written["mensajes"])
    assert messages[0]["role"] == "assistant"
    assert len(messages) == 4


def test_write_hive_parquet_ignores_column_types_not_present_in_rows(aws):
    # column_types may legitimately be broader than what a given set of
    # rows carries (e.g. output_core's technical types reused for
    # output_atts rows) -- extra entries should be silently ignored, not
    # crash with a Polars "column not found" error on the cast.
    s3 = aws["s3"]
    rows = [{"a": "x"}]

    keys = write_hive_parquet(
        s3,
        rows,
        "augusta-nexa-dev-refined",
        "p",
        column_types={"a": "string", "atributo_valor": "string"},
    )

    written = _read_parquet_rows(s3, "augusta-nexa-dev-refined", keys[0])
    assert written == [{"a": "x"}]
