"""Recorded conversations of a day, from the curated sessions table in Athena.

recordings can be fed by date instead of by a file:

    {"tag": "recordings", "ids_source": "unitary_conversations", "date": "2026-11-19"}

optionally narrowed with any of

    "cliente_prefijo": "BDO", "operacion_prefijo": "SAC", "organization_id": "org-3"

This runs one Athena query over the curated sessions table (database
augusta-nexa-<env>-transacciones-curated, table
genesys-conversaciones-sesiones-core, over s3://augusta-nexa-<env>-refined/
transacciones/curated/genesys-conversaciones-sesiones/core/data/): every
recorded conversation (`interaccion_grabada = true`) that started that day,
with its organization worked out from `cliente_prefijo` (ORGANIZATION_RULES).
The table's year=/month=/day= partitions are the day the data was processed,
not the day the conversation happened, so the filter is on
`conversacion_fecha_inicio`, not on the partitions.

The Lambda's role needs athena:StartQueryExecution/GetQueryExecution/
GetQueryResults/StopQueryExecution, glue:GetTable/GetPartitions on that
table, read access to the refined bucket and write access to the
workgroup's (or ATHENA_OUTPUT_LOCATION's) query results location.
"""

import logging
import re
import time
from datetime import date

import boto3

from src.config import Settings
from src.sources import EventError

logger = logging.getLogger(__name__)

# cliente_prefijo (lowercased, LIKE pattern) -> organization, first match wins;
# anything else is DEFAULT_ORGANIZATION. Same rules as the CSV export query.
ORGANIZATION_RULES = (
    ("%bdb%", "org-2"),
    ("%bpp%", "org-3"),
    ("%bdo%", "org-3"),
)
DEFAULT_ORGANIZATION = "org-3"
# Optional event filters -> the column each one narrows.
FILTERS = {"cliente_prefijo": "cliente_prefijo", "operacion_prefijo": "operacion_prefijo"}
_SAFE_VALUE = re.compile(r"^[A-Za-z0-9_-]+$")
_ORGANIZATION = re.compile(r"^org-\d+$")
_RUNNING_STATES = ("QUEUED", "RUNNING")


def _athena_client(settings: Settings):
    return boto3.client("athena", region_name=settings.region)


def _filters(event: dict) -> dict[str, str]:
    """The event's optional filters, validated -- they go into the SQL text."""
    filters = {}
    for key in (*FILTERS, "organization_id"):
        value = event.get(key)
        if value is None:
            continue
        pattern = _ORGANIZATION if key == "organization_id" else _SAFE_VALUE
        if not isinstance(value, str) or not pattern.match(value):
            raise EventError(f"{key!r} must match {pattern.pattern}, got {value!r}")
        filters[key] = value
    return filters


def build_query(settings: Settings, day: date, filters: dict[str, str]) -> str:
    cases = "\n".join(
        f"            WHEN lower(cliente_prefijo) LIKE '{pattern}' THEN '{organization}'"
        for pattern, organization in ORGANIZATION_RULES
    )
    conditions = [
        "interaccion_grabada = true",
        f"CAST(conversacion_fecha_inicio AS DATE) = DATE '{day.isoformat()}'",
        *(f"{FILTERS[key]} = '{value}'" for key, value in filters.items() if key in FILTERS),
    ]
    where = "\n      AND ".join(conditions)
    query = f"""SELECT DISTINCT conversacion_id, organizacion FROM (
    SELECT
        lower(conversacion_id) AS conversacion_id,
        CASE
{cases}
            ELSE '{DEFAULT_ORGANIZATION}'
        END AS organizacion
    FROM "{settings.athena_database}"."{settings.athena_table}"
    WHERE {where}
)"""
    if "organization_id" in filters:
        query += f"\nWHERE organizacion = '{filters['organization_id']}'"
    return query


def _wait(client, query_id: str, timeout_seconds: float, poll_seconds: float, sleep) -> None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        status = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return
        if state not in _RUNNING_STATES:
            raise RuntimeError(
                f"Athena query {query_id} {state}: {status.get('StateChangeReason', 'no reason given')}"
            )
        if time.monotonic() >= deadline:
            client.stop_query_execution(QueryExecutionId=query_id)
            raise RuntimeError(f"Athena query {query_id} still {state} after {timeout_seconds}s; stopped")
        sleep(poll_seconds)


def _rows(client, query_id: str) -> list[tuple[str, str]]:
    rows = []
    header_skipped = False
    for page in client.get_paginator("get_query_results").paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"]["Rows"]:
            values = [cell.get("VarCharValue") for cell in row["Data"]]
            if not header_skipped:  # Athena returns the column names as the first row
                header_skipped = True
                continue
            rows.append((values[0], values[1]))
    return rows


def collect_recorded_conversations(
    settings: Settings,
    day: date,
    event: dict,
    *,
    client=None,
    timeout_seconds: float = 600,
    poll_seconds: float = 1,
    sleep=time.sleep,
) -> dict:
    """{"ids_by_organization": {organization: [conversation id, ...]}, "summary": ...}."""
    filters = _filters(event)
    client = client or _athena_client(settings)
    query = build_query(settings, day, filters)
    request = {
        "QueryString": query,
        "QueryExecutionContext": {"Database": settings.athena_database},
        "WorkGroup": settings.athena_workgroup,
    }
    if settings.athena_output_location:
        request["ResultConfiguration"] = {"OutputLocation": settings.athena_output_location}
    query_id = client.start_query_execution(**request)["QueryExecutionId"]
    logger.info("Athena query %s for recorded conversations of %s %s", query_id, day, filters)
    _wait(client, query_id, timeout_seconds, poll_seconds, sleep)

    grouped: dict[str, set[str]] = {}
    for conversation_id, organization_id in _rows(client, query_id):
        if conversation_id and organization_id:
            grouped.setdefault(organization_id, set()).add(conversation_id)
    ids_by_organization = {org: sorted(ids) for org, ids in sorted(grouped.items())}
    return {
        "ids_by_organization": ids_by_organization,
        "summary": {
            "query_execution_id": query_id,
            "date": day.isoformat(),
            "filters": filters,
            "conversations": {org: len(ids) for org, ids in ids_by_organization.items()},
        },
    }
