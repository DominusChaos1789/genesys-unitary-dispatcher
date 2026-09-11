"""The contracts process: transcription files -> parquet -> conversation ids.

Every contract under CONTRACTS_PREFIX (or only CONTRACT_KEY, when set)
describes one provider/operation pair: where its transcription files land,
how to rename and type their columns, which technical columns to compute,
where the two parquet outputs go, and which Genesys organization it belongs
to. Each contract runs independently:

1. List and read the JSON files under its source prefix. Unreadable files
   are skipped and left in place.
2. Rename, cast and transform the columns, then deduplicate.
3. Compute the technical columns and split the rows into output_core and the
   output_atts EAV rows.
4. Write both as Hive-partitioned parquet.
5. Delete the source files that were processed.

The conversation ids found are grouped by each contract's
genesys_cloud_organization, which is what the dispatcher needs to build the
surveys payload. A contract that fails is recorded and the rest still run;
its source files stay in place, because deletion is the last step.

This is the only module that needs polars (through parquet_io), so the
dispatcher imports it only when a run asks for `"ids_source": "contracts"`.
"""

import logging
from datetime import datetime, timezone

import src.s3_utils as s3_utils
from src.config import Settings
from src.contract import list_contract_keys, load_contract
from src.parquet_io import write_hive_parquet
from src.technical_columns import build_output_rows
from src.transform import build_rows, dedup_rows

logger = logging.getLogger(__name__)


def _empty_result(contract_key: str, organization_id: str) -> dict:
    return {
        "contract_key": contract_key,
        "organization_id": organization_id,
        "processed_files": 0,
        "skipped_files": [],
        "deleted_source_files": 0,
        "output_core_keys": [],
        "output_atts_keys": [],
        "conversation_ids": [],
    }


def process_contract(s3_client, settings: Settings, contract_key: str, execution_id: str) -> dict:
    """Runs one contract end to end and returns its summary, including the
    conversation ids it found and the organization they belong to."""
    contract = load_contract(s3_client, settings, contract_key)

    source_bucket = settings.resolve_bucket(contract.source_bucket_logical)
    source_keys = s3_utils.list_json_keys(s3_client, source_bucket, contract.source_prefix)
    logger.info(
        "[%s] Found %d json files under s3://%s/%s",
        contract_key,
        len(source_keys),
        source_bucket,
        contract.source_prefix,
    )
    if not source_keys:
        return _empty_result(contract_key, contract.gne_organization_id)

    records, successful_keys, skipped_keys = s3_utils.read_json_files(s3_client, source_bucket, source_keys)
    if skipped_keys:
        logger.warning("[%s] Skipped %d unreadable source file(s)", contract_key, len(skipped_keys))

    rows, conversation_ids = build_rows(records, contract)
    for row, key in zip(rows, successful_keys):
        row["_source_key"] = key
    rows = dedup_rows(rows, contract.deduplication)

    core_rows, atts_rows = build_output_rows(rows, contract, datetime.now(timezone.utc), execution_id)

    core_bucket = settings.resolve_bucket(contract.output_core_bucket_logical)
    core_keys = write_hive_parquet(
        s3_client,
        core_rows,
        core_bucket,
        contract.output_core_prefix,
        partition_cols=contract.partition_by,
        column_types={**contract.column_types, **contract.technical_column_types},
        filename_prefix=contract.output_core_filename,
    )

    atts_bucket = settings.resolve_bucket(contract.output_atts_bucket_logical)
    atts_keys = write_hive_parquet(
        s3_client,
        atts_rows,
        atts_bucket,
        contract.output_atts_prefix,
        partition_cols=contract.partition_by,
        column_types=contract.technical_column_types,
        filename_prefix=contract.output_atts_filename,
    )
    logger.info(
        "[%s] Wrote %d output_core and %d output_atts parquet object(s)",
        contract_key,
        len(core_keys),
        len(atts_keys),
    )

    # Last step, so a failure anywhere above leaves the source files in place
    # to re-run. Only files that were read are deleted; unreadable ones stay
    # for investigation.
    deleted_keys = s3_utils.delete_objects(s3_client, source_bucket, successful_keys)
    logger.info("[%s] Deleted %d source object(s)", contract_key, len(deleted_keys))

    return {
        "contract_key": contract_key,
        "organization_id": contract.gne_organization_id,
        "processed_files": len(successful_keys),
        "skipped_files": skipped_keys,
        "deleted_source_files": len(deleted_keys),
        "output_core_bucket": core_bucket,
        "output_core_keys": core_keys,
        "output_atts_bucket": atts_bucket,
        "output_atts_keys": atts_keys,
        "conversation_ids": conversation_ids,
    }


def _ids_by_organization(results: list[dict]) -> dict[str, list[str]]:
    """Contracts sharing an organization (e.g. pel and bpp on org-2) merge into
    one group -- and later, one token."""
    grouped: dict[str, set[str]] = {}
    for result in results:
        if result["conversation_ids"]:
            grouped.setdefault(result["organization_id"], set()).update(result["conversation_ids"])
    return {organization_id: sorted(ids) for organization_id, ids in grouped.items()}


def _result_summary(result: dict) -> dict:
    """A contract's result for the Lambda response: its conversation ids are
    replaced by their count, since the response has to stay small."""
    summary = {key: value for key, value in result.items() if key != "conversation_ids"}
    summary["conversation_count"] = len(result["conversation_ids"])
    return summary


def run_contracts(s3_client, settings: Settings, execution_id: str) -> dict:
    """Processes every contract. Returns the conversation ids grouped by
    organization, and a summary for the Lambda's response."""
    contract_keys = list_contract_keys(s3_client, settings)
    logger.info("Processing %d contract(s): %s", len(contract_keys), contract_keys)

    results: list[dict] = []
    failed: list[dict] = []
    for contract_key in contract_keys:
        try:
            results.append(process_contract(s3_client, settings, contract_key, execution_id))
        except Exception as exc:  # noqa: BLE001 -- one bad contract must not stop the rest
            logger.exception("Contract %s failed", contract_key)
            failed.append({"contract_key": contract_key, "error": str(exc)})

    return {
        "ids_by_organization": _ids_by_organization(results),
        "summary": {
            "contracts_processed": len(results),
            "failed_contracts": failed,
            "processed_files": sum(result["processed_files"] for result in results),
            "results": [_result_summary(result) for result in results],
        },
    }
