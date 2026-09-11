from src.config import load_settings
from src.contract import list_contract_keys, load_contract
from test.conftest import CONTRACT_KEY, CONTRACTS_PREFIX, RESOURCES_BUCKET


def test_load_contract_exposes_source_and_output(aws):
    contract = load_contract(aws["s3"], load_settings(), CONTRACT_KEY)

    assert contract.source_bucket_logical == "providers-landing"
    assert contract.source_prefix == "external/datanexa/transacciones/empatia/transcripciones/BDO"
    assert contract.partition_by == ["cliente_prefijo", "operacion_prefijo"]
    assert {c["name"] for c in contract.columns} >= {"conversacion_id", "consumidor_id", "mensajes"}
    assert set(contract.timestamp_columns) == {"interaccion_fecha_inicio", "interaccion_fecha_fin"}

    assert contract.output_core_bucket_logical == "refined"
    assert contract.output_core_prefix == "transacciones/parquet/transcripciones_core"
    assert contract.output_core_filename == "transcripciones"
    assert "registro_hash" in contract.output_core_technical_columns

    assert contract.output_atts_bucket_logical == "refined"
    assert contract.output_atts_prefix == "transacciones/parquet/transcripciones_atts"
    assert contract.output_atts_filename == "transcripciones"
    assert "atributo_valor" in contract.output_atts_technical_columns


def test_technical_columns_are_exposed(aws):
    contract = load_contract(aws["s3"], load_settings(), CONTRACT_KEY)

    assert contract.technical_columns["registro_hash"]["calculus_type"] == "concatetation"
    # gestion_tipo/gestion_canal are referenced by output_core.technical_col_keep
    # but have no technical_columns entry -- a real gap in the contract -- and
    # still get a safe default type rather than crashing.
    assert contract.technical_column_types["gestion_tipo"] == "string"
    assert contract.technical_column_types["registro_hash"] == "string"
    assert contract.technical_column_types["atributo_posicion"] == "integer"
    assert contract.technical_column_types["cargue_fecha"] == "timestamp"


def test_contract_organization_and_its_servers_key(aws):
    contract = load_contract(aws["s3"], load_settings(), CONTRACT_KEY)

    assert contract.gne_organization_id == "org-3"
    assert contract.server_key == "org_3"


def test_every_json_under_the_prefix_is_a_contract_in_stable_order(aws):
    s3 = aws["s3"]
    for provider in ("pel/sac", "bpp/cob"):
        s3.put_object(
            Bucket=RESOURCES_BUCKET, Key=f"{CONTRACTS_PREFIX}{provider}/transcripcion.json", Body=b"{}"
        )
    s3.put_object(Bucket=RESOURCES_BUCKET, Key=f"{CONTRACTS_PREFIX}README.txt", Body=b"not a contract")

    assert list_contract_keys(s3, load_settings()) == [
        f"{CONTRACTS_PREFIX}bdo/sac/transcripcion.json",
        f"{CONTRACTS_PREFIX}bpp/cob/transcripcion.json",
        f"{CONTRACTS_PREFIX}pel/sac/transcripcion.json",
    ]


def test_contract_key_pins_the_run_to_one_contract(aws, monkeypatch):
    s3 = aws["s3"]
    pel_key = f"{CONTRACTS_PREFIX}pel/sac/transcripcion.json"
    s3.put_object(Bucket=RESOURCES_BUCKET, Key=pel_key, Body=b"{}")
    monkeypatch.setenv("CONTRACT_KEY", CONTRACT_KEY)

    settings = load_settings()

    assert list_contract_keys(s3, settings) == [CONTRACT_KEY]
    assert load_contract(s3, settings).gne_organization_id == "org-3"
