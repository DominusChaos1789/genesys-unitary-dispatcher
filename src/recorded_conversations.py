"""Recorded conversations of a day, from the curated tables in Athena.

recordings can be fed by date instead of by a file:

    {"tag": "recordings", "ids_source": "unitary_conversations", "date": "2026-11-19"}

optionally narrowed with any of

    "cliente_prefijo": "BDO", "operacion_prefijo": "SAC", "organization_id": "org-3"

This runs one Athena query that returns the same columns as the recordings
CSV export -- conversacion_id, cola_nombre, bucket_nombre, prefix_nombre,
cliente_prefijo, caso_uso_nombre, Organizacion -- so its rows go through the
very same code as that CSV (conversations_details.collect_from_csv): one
payload per (caso_uso_nombre, organization), each carrying its bucket_nombre
and prefix_nombre.

The query joins

- the sessions table (augusta-nexa-<env>-transacciones-curated.
  genesys-conversaciones-sesiones-core): recorded conversations
  (`interaccion_grabada = true`) that started that day
  (`conversacion_fecha_inicio`);
- the segments table (same database, genesys-conversaciones-sesiones-
  segmentos-core): each conversation's queues (`cola_atencion_id`);
- the parametric catalog (augusta-nexa-<env>-catalogos-curated.
  parametrica-core): per queue with `grabacion_estado = '1'`, its caso de
  uso, destination bucket/prefix and organization
  (`organizacion_id` "ORG_03" -> "org-3").

So only conversations through a queue the catalog has recordings turned on
for come back. A conversation that went through several such queues keeps
only its **first** one (earliest segment, by ATHENA_SEGMENT_ORDER_COLUMN), so
it belongs to a single caso de uso: one payload, one download.

The sessions table's year=/month=/day= partitions are the day the data was
processed, not the day the conversation happened, so the filter is on
`conversacion_fecha_inicio`, not on the partitions.

The Lambda's role needs athena:StartQueryExecution/GetQueryExecution/
GetQueryResults/StopQueryExecution, glue:GetTable/GetPartitions on the three
tables, read access to their buckets and write access to the workgroup's (or
ATHENA_OUTPUT_LOCATION's) query results location.
"""

import logging
import re
import time
from datetime import date

import boto3

from src.config import Settings
from src.conversations_details import collect_from_csv
from src.dispatcher_config import RECORDING_ID_KIND
from src.sources import EventError

logger = logging.getLogger(__name__)

# Optional event filters -> the sessions table column each one narrows.
FILTERS = {"cliente_prefijo": "a.cliente_prefijo", "operacion_prefijo": "a.operacion_prefijo"}
# The catalog's organizacion_id ("ORG_03") as this Lambda's organization ("org-3").
ORGANIZATION_EXPRESSION = "replace(lower(b.organizacion_id), '_0', '-')"
_SAFE_VALUE = re.compile(r"^[A-Za-z0-9_-]+$")
_SAFE_COLUMN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ORGANIZATION = re.compile(r"^org-\d+$")
_RUNNING_STATES = ("QUEUED", "RUNNING")
# Athena lower-cases column aliases; the CSV export's column is "Organizacion".
_COLUMN_NAMES = {"organizacion": "Organizacion"}


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
    conditions = [
        "a.interaccion_grabada = true",
        f"CAST(a.conversacion_fecha_inicio AS DATE) = DATE '{day.isoformat()}'",
        *(f"{FILTERS[key]} = '{value}'" for key, value in filters.items() if key in FILTERS),
    ]
    if "organization_id" in filters:
        conditions.append(f"{ORGANIZATION_EXPRESSION} = '{filters['organization_id']}'")
    where = "\n  AND ".join(conditions)
    sessions = f'"{settings.athena_database}"."{settings.athena_table}"'
    segments = f'"{settings.athena_database}"."{settings.athena_segments_table}"'
    catalog = f'"{settings.athena_catalog_database}"."{settings.athena_catalog_table}"'
    order_column = settings.athena_segment_order_column
    if not _SAFE_COLUMN.match(order_column):
        raise ValueError(f"ATHENA_SEGMENT_ORDER_COLUMN must be a plain column name, got {order_column!r}")
    return f"""SELECT DISTINCT
    lower(a.conversacion_id) AS conversacion_id,
    b.cola_nombre,
    b.bucket_nombre,
    b.prefix_nombre,
    a.cliente_prefijo,
    b.caso_uso_nombre,
    {ORGANIZATION_EXPRESSION} AS Organizacion
FROM {sessions} a
INNER JOIN (
    -- Each conversation's FIRST queue with recordings on, so it belongs to
    -- one caso de uso only (one payload, one download).
    SELECT conversacion_id, bucket_nombre, prefix_nombre, cola_nombre, organizacion_id, caso_uso_nombre
    FROM (
        SELECT
            s.conversacion_id,
            p.bucket_nombre,
            p.prefix_nombre,
            p.cola_nombre,
            p.organizacion_id,
            p.caso_uso_nombre,
            row_number() OVER (
                PARTITION BY lower(s.conversacion_id)
                ORDER BY s.{order_column} ASC, p.caso_uso_nombre ASC, p.cola_nombre ASC
            ) AS queue_order
        FROM {segments} s
        INNER JOIN (
            SELECT * FROM {catalog} WHERE grabacion_estado = '1'
        ) AS p ON lower(s.cola_atencion_id) = lower(p.cola_id)
    )
    WHERE queue_order = 1
) AS b ON lower(a.conversacion_id) = lower(b.conversacion_id)
WHERE {where}"""


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


def _rows(client, query_id: str) -> list[dict]:
    """The result as one dict per row, keyed by column name -- the first row
    Athena returns is the header."""
    header = None
    rows = []
    for page in client.get_paginator("get_query_results").paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"]["Rows"]:
            values = [cell.get("VarCharValue") for cell in row["Data"]]
            if header is None:
                header = [_COLUMN_NAMES.get(name, name) for name in values]
                continue
            rows.append(dict(zip(header, values)))
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
    """Like a recordings CSV: {"ids_by_organization": ..., "groups": ...
    (per caso_uso_nombre), "summary": ...}."""
    filters = _filters(event)
    client = client or _athena_client(settings)
    request = {
        "QueryString": build_query(settings, day, filters),
        "QueryExecutionContext": {"Database": settings.athena_database},
        "WorkGroup": settings.athena_workgroup,
    }
    if settings.athena_output_location:
        request["ResultConfiguration"] = {"OutputLocation": settings.athena_output_location}
    query_id = client.start_query_execution(**request)["QueryExecutionId"]
    logger.info("Athena query %s for recorded conversations of %s %s", query_id, day, filters)
    _wait(client, query_id, timeout_seconds, poll_seconds, sleep)

    outcome = collect_from_csv(RECORDING_ID_KIND, _rows(client, query_id), f"athena:{query_id}")
    summary = outcome["summary"]
    summary.pop("file", None)
    outcome["summary"] = {
        "query_execution_id": query_id,
        "date": day.isoformat(),
        "filters": filters,
        **summary,
    }
    return outcome
