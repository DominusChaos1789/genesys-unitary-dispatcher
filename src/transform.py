"""Turns raw landing-bucket transcription JSON records into rows matching
the contract's target schema: rename via `columns`, stamp constants, apply
`transformations`, and hand back the parquet-ready rows plus the list of
conversation ids seen in this batch. Also dedups rows per the contract's
`deduplication` block.

Deliberately pandas-free: rows are plain dicts all the way through.
"""

import json
import unicodedata
from datetime import datetime, timezone
from typing import Any, Optional

from src.contract import Contract

CONVERSATION_ID_COLUMN = "conversacion_id"


def _cast_value(raw_value: Any, col_type: str) -> Any:
    if raw_value is None:
        return None
    if col_type == "string":
        return str(raw_value)
    if col_type == "integer":
        return int(raw_value)
    if col_type == "timestamp":
        # Normalize to a UTC ISO-8601 string; parquet_io.py casts this back
        # to a real TIMESTAMP column when it writes the parquet file.
        parsed = datetime.fromisoformat(raw_value)
        return parsed.astimezone(timezone.utc).isoformat()
    if col_type == "json":
        return raw_value
    return raw_value


def _remove_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(c for c in normalized if not unicodedata.combining(c))


def _map_record(record: dict, contract: Contract) -> dict:
    row: dict[str, Any] = {}
    for col_spec in contract.columns:
        name = col_spec["name"]
        if "source_column" in col_spec:
            raw_value = record.get(col_spec["source_column"])
            row[name] = _cast_value(raw_value, col_spec["type"])
        else:
            row[name] = col_spec.get("default_value")
    return row


def _apply_transformations(row: dict, contract: Contract) -> dict:
    for rule in contract.transformations:
        rule_type = rule["type"]
        if rule.get("selector") == "all-string":
            target_columns = [k for k, v in row.items() if isinstance(v, str)]
        else:
            target_columns = rule.get("columns", [])

        for col in target_columns:
            value = row.get(col)
            if not isinstance(value, str):
                continue
            if rule_type == "trim":
                row[col] = value.strip()
            elif rule_type == "uppercase":
                row[col] = value.upper()
            elif rule_type == "remove_accents":
                row[col] = _remove_accents(value)
    return row


def _serialize_json_columns(row: dict, contract: Contract) -> dict:
    for col_spec in contract.columns:
        if col_spec["type"] == "json":
            name = col_spec["name"]
            if row.get(name) is not None:
                row[name] = json.dumps(row[name], ensure_ascii=False)
    return row


def build_rows(records: list[dict], contract: Contract) -> tuple[list[dict], list[str]]:
    """Maps+cleans every record, returning the resulting rows and the
    deduplicated list of conversacion_id values seen. Row-level dedup
    itself happens later, via dedup_rows() in this module."""
    rows = []
    conversation_ids: list[str] = []

    for record in records:
        row = _map_record(record, contract)
        row = _apply_transformations(row, contract)
        conversation_id = row.get(CONVERSATION_ID_COLUMN)
        if conversation_id:
            conversation_ids.append(conversation_id)
        row = _serialize_json_columns(row, contract)
        rows.append(row)

    unique_conversation_ids = sorted(set(conversation_ids))
    return rows, unique_conversation_ids


def _dedup_comparator(order_by: list[str], reverse_flags: list[bool]):
    def compare(row_a: dict, row_b: dict) -> int:
        for col, reverse in zip(order_by, reverse_flags):
            a, b = row_a.get(col), row_b.get(col)
            if a == b:
                continue
            if a is None:
                result = 1
            elif b is None:
                result = -1
            else:
                result = -1 if a < b else 1
            return -result if reverse else result
        return 0

    return compare


def dedup_rows(rows: list[dict], dedup: Optional[dict]) -> list[dict]:
    """Keeps one row per `record_key` group, per `order_by`/`order_type`
    (higher-priority columns first; "desc" keeps the largest value).
    `order_type` may be a single string or a list, one per order_by column."""
    if not dedup or not dedup.get("enabled"):
        return rows

    record_key = dedup.get("record_key", [])
    if not record_key:
        return rows

    order_by = dedup.get("order_by", [])
    order_type = dedup.get("order_type", [])
    if isinstance(order_type, str):
        order_type = [order_type]
    reverse_flags = [
        order_type[i].lower() == "desc" if i < len(order_type) else False for i in range(len(order_by))
    ]
    is_better = _dedup_comparator(order_by, reverse_flags)

    best_by_key: dict[tuple, dict] = {}
    for row in rows:
        key = tuple(row.get(c) for c in record_key)
        current_best = best_by_key.get(key)
        if current_best is None or is_better(row, current_best) < 0:
            best_by_key[key] = row
    return list(best_by_key.values())
