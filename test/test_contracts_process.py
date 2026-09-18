import json
import os
import tempfile
from datetime import datetime, timezone
from types import SimpleNamespace

import polars as pl
import pytest

import src.s3_utils as s3_utils
from src.main import run
from src.sources import EventError
from test.conftest import (
    CONTRACT_KEY,
    CONTRACTS_PREFIX,
    GENESYS_BASE_PATH,
    PEL_CONTRACT_KEY,
    PEL_CONVERSATION_ID,
    PROVIDERS_LANDING_BUCKET,
    REFINED_BUCKET,
    RESOURCES_BUCKET,
    SOURCE_PREFIX,
    load_fixture,
    payload_by_org,
)

EVENT = {"tag": "surveys", "ids_source": "contracts"}
BDO_IDS = ["2a0db425-2370-4786-be61-1a9ee8e89855", "7b1fa3c2-91aa-4e2b-9d3a-0b2f6a7c1234"]


def _context(request_id: str = "req-contracts"):
    return SimpleNamespace(aws_request_id=request_id)


def _by_org(result: dict) -> dict:
    return payload_by_org(result)


def _date_path() -> str:
    now = datetime.now(timezone.utc)
    return f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}"


def _read_parquet_rows(s3, bucket: str, key: str) -> list[dict]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, "part.parquet")
        with open(local_path, "wb") as f:
            f.write(body)
        return pl.read_parquet(local_path).to_dicts()


def _landing_keys(s3) -> list[str]:
    return sorted(s3_utils.list_json_keys(s3, PROVIDERS_LANDING_BUCKET, SOURCE_PREFIX))


def test_contracts_run_writes_parquet_deletes_sources_and_builds_the_surveys_payload(
    aws, seeded_source_files
):
    s3 = aws["s3"]

    result = run(EVENT, _context())

    contracts = result["contracts"]
    assert contracts["contracts_processed"] == 1
    assert contracts["failed_contracts"] == []
    assert contracts["processed_files"] == 2
    bdo = contracts["results"][0]
    assert bdo["contract_key"] == CONTRACT_KEY
    assert bdo["organization_id"] == "org-3"
    assert bdo["deleted_source_files"] == 2
    # The response carries a count, not the ids themselves.
    assert bdo["conversation_count"] == 2
    assert "conversation_ids" not in bdo
    assert _landing_keys(s3) == []

    # output_core: one row per conversation, partitioned by client/operation, then processing date.
    core_prefix = (
        "transacciones/parquet/transcripciones_core/cliente_prefijo=BDO/"
        f"operacion_prefijo=SAC/{_date_path()}/"
    )
    core_rows = []
    for key in bdo["output_core_keys"]:
        assert key.startswith(core_prefix)
        assert key.rsplit("/", 1)[-1].startswith("transcripciones_")
        core_rows += _read_parquet_rows(s3, REFINED_BUCKET, key)
    assert len(core_rows) == 2
    row = next(r for r in core_rows if r["interaccion_id"] == "5624561b59ae99a0fae1e65cc206e4a1")
    assert row["consumidor_id"] == "14984986"
    assert json.loads(row["mensajes"])[0]["role"] == "assistant"
    assert row["ejecucion_id"] == "req-contracts"
    assert "registro_hash" in row

    # output_atts: one EAV row per business column per conversation (18 columns x 2).
    atts_rows = []
    for key in bdo["output_atts_keys"]:
        assert "/cliente_prefijo=BDO/operacion_prefijo=SAC/" in key
        atts_rows += _read_parquet_rows(s3, REFINED_BUCKET, key)
    assert len(atts_rows) == 18 * 2

    # The ids feed the surveys payload for the contract's organization.
    assert list(_by_org(result)) == ["org-3"]
    entry = _by_org(result)["org-3"]
    assert entry["ids"] == sorted(BDO_IDS)
    assert entry["request_context"]["url"] == "/api/v2/quality/surveys/{surveyId}"
    assert entry["request_context"]["headers"]["Authorization"] == "Bearer token-for-org-3"
    assert entry["request_context"]["base_path"] == GENESYS_BASE_PATH


def test_each_contract_contributes_ids_to_its_own_organization(aws, seeded_second_provider, genesys_api):
    result = run(EVENT, _context())

    assert result["contracts"]["contracts_processed"] == 2
    assert result["contracts"]["processed_files"] == 3

    organizations = _by_org(result)
    assert organizations["org-2"]["ids"] == [PEL_CONVERSATION_ID]
    assert organizations["org-2"]["request_context"]["headers"]["Authorization"] == "Bearer token-for-org-2"
    assert organizations["org-3"]["ids"] == sorted(BDO_IDS)
    assert sorted(genesys_api["requested_orgs"]) == ["org-2", "org-3"]

    written = {r["contract_key"]: r["output_core_keys"] for r in result["contracts"]["results"]}
    assert all("cliente_prefijo=PEL/" in key for key in written[PEL_CONTRACT_KEY])
    assert all("cliente_prefijo=BDO/" in key for key in written[CONTRACT_KEY])


def test_contracts_sharing_an_organization_share_one_group_and_token(
    aws, seeded_second_provider, genesys_api
):
    s3 = aws["s3"]
    bpp = json.loads(s3.get_object(Bucket=RESOURCES_BUCKET, Key=PEL_CONTRACT_KEY)["Body"].read())
    bpp["client_prefix"] = "BPP"
    bpp["source"]["prefix_pattern"] = "external/datanexa/transacciones/empatia/transcripciones/BPP"
    s3.put_object(
        Bucket=RESOURCES_BUCKET, Key=f"{CONTRACTS_PREFIX}bpp/cob/transcripcion.json", Body=json.dumps(bpp)
    )
    record = load_fixture("sample_transcription_1.json")
    record["genesys_cloud_id"] = "bpp-conversation"
    s3.put_object(
        Bucket=PROVIDERS_LANDING_BUCKET,
        Key="external/datanexa/transacciones/empatia/transcripciones/BPP/sample_bpp.json",
        Body=json.dumps(record),
    )

    result = run(EVENT, _context())

    assert result["contracts"]["contracts_processed"] == 3
    # pel and bpp are both org-2: one organization entry, one token.
    assert _by_org(result)["org-2"]["ids"] == sorted(["bpp-conversation", PEL_CONVERSATION_ID])
    assert sorted(genesys_api["requested_orgs"]) == ["org-2", "org-3"]


def test_a_failing_contract_does_not_stop_the_others(aws, seeded_second_provider):
    s3 = aws["s3"]
    broken_key = f"{CONTRACTS_PREFIX}bpp/cob/transcripcion.json"
    s3.put_object(Bucket=RESOURCES_BUCKET, Key=broken_key, Body=b"{}")

    result = run(EVENT, _context())

    assert [failed["contract_key"] for failed in result["contracts"]["failed_contracts"]] == [broken_key]
    assert result["contracts"]["contracts_processed"] == 2
    assert set(_by_org(result)) == {"org-2", "org-3"}


def test_source_files_are_kept_when_a_contract_fails_before_deletion(aws, seeded_source_files, monkeypatch):
    def refined_unavailable(*args, **kwargs):
        raise RuntimeError("refined bucket unavailable")

    monkeypatch.setattr("src.contracts_process.write_hive_parquet", refined_unavailable)

    result = run(EVENT, _context())

    assert result["contracts"]["failed_contracts"] == [
        {"contract_key": CONTRACT_KEY, "error": "refined bucket unavailable"}
    ]
    assert _landing_keys(aws["s3"]) == sorted(seeded_source_files)
    assert result["responses"] == []


def test_unreadable_source_files_are_skipped_and_left_in_place(aws, seeded_source_files):
    s3 = aws["s3"]
    bad_key = f"{SOURCE_PREFIX}/corrupted.json"
    s3.put_object(Bucket=PROVIDERS_LANDING_BUCKET, Key=bad_key, Body=b"")

    result = run(EVENT, _context())

    bdo = result["contracts"]["results"][0]
    assert bdo["processed_files"] == 2
    assert bdo["skipped_files"] == [bad_key]
    assert bdo["deleted_source_files"] == 2
    assert _landing_keys(s3) == [bad_key]


def test_contracts_without_source_files_produce_an_empty_payload(aws, genesys_api):
    result = run(EVENT, _context())

    assert result["contracts"]["contracts_processed"] == 1
    assert result["contracts"]["results"][0]["processed_files"] == 0
    assert result["responses"] == []
    assert genesys_api["load_config"] == []


def test_a_token_failure_reports_the_ids_that_can_no_longer_be_reread(aws, seeded_source_files, monkeypatch):
    def oauth_down(*args, **kwargs):
        raise RuntimeError("oauth down")

    monkeypatch.setattr("src.main.get_token", oauth_down)

    result = run(EVENT, _context())

    # The source files are already deleted, so the ids must survive somewhere:
    # since org-3 is the only organization here and it failed, no payload file
    # is written at all -- the ids are only in the run's own failed_organizations.
    assert _landing_keys(aws["s3"]) == []
    assert result["responses"] == []
    assert result["failed_organizations"] == [
        {"organization_id": "org-3", "id_count": 2, "error": "oauth down"}
    ]


def test_an_unknown_ids_source_fails_before_touching_any_files(aws, seeded_source_files):
    with pytest.raises(EventError, match="Unknown ids_source 'contract'"):
        run({"tag": "surveys", "ids_source": "contract"}, _context())

    assert _landing_keys(aws["s3"]) == sorted(seeded_source_files)


def test_an_unknown_tag_fails_before_the_contracts_process_deletes_anything(aws, seeded_source_files):
    with pytest.raises(ValueError, match="not declared in dispatcher.json"):
        run({"tag": "nope", "ids_source": "contracts"}, _context())

    assert _landing_keys(aws["s3"]) == sorted(seeded_source_files)


def test_runs_without_ids_source_do_not_report_a_contracts_summary(aws):
    result = run({"tag": "surveys", "organizations": []}, _context())

    assert "contracts" not in result
