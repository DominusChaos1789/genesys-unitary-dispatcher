"""Management unit ids from the Genesys management units download.

A separate process downloads the management unit list into the landing
bucket, one folder per organization and day -- the same layout
conversations_details.py reads (see landing_partitions.py):

    augusta-nexa-<env>-landing/funcionarios/genesys/api/management_unit_list/
        org_id=<N>/year=YYYY/month=MM/day=DD/*.json

Each file is {"endpoint": [{"id": ..., "name": ..., "businessUnit": {"id":
..., ...}, "division": {...}, "selfUri": ...}, ...], "status": "success"}.
For a run with `"ids_source": "management_unit_list"` and
`"date": "YYYY-MM-DD"`, this reads that day's files for every organization
and collects ids -- which part depends on the tag's id_kind:

- "management_unit" (funcionarios_adherencia): each record's own `id`.
- "management_unit_schedule" (funcionarios_programaciones): one
  {managementUnitId, businessUnitId} pair per record -- its schedule
  endpoints need the management unit's business unit too.

Unlike the contracts process there is nothing to transform or write: the ids
only feed the payload, and the files are never modified or deleted here.
"""

from datetime import date

from src.config import Settings
from src.landing_partitions import collect_ids

ENDPOINT_KEY = "endpoint"


def _units(document) -> list[dict]:
    units = document.get(ENDPOINT_KEY) if isinstance(document, dict) else None
    if not isinstance(units, list):
        raise ValueError(f"expected an object with an {ENDPOINT_KEY!r} list")
    return units


def _management_unit_ids(document) -> list[str]:
    return [unit["id"] for unit in _units(document) if isinstance(unit, dict) and unit.get("id")]


def _management_unit_business_unit_pairs(document) -> list[tuple[str, str]]:
    pairs = []
    for unit in _units(document):
        if not isinstance(unit, dict) or not unit.get("id"):
            continue
        business_unit = unit.get("businessUnit")
        if isinstance(business_unit, dict) and business_unit.get("id"):
            pairs.append((unit["id"], business_unit["id"]))
    return pairs


def collect_management_unit_ids(s3_client, settings: Settings, day: date) -> dict:
    """The day's management unit ids grouped by organization -- for
    "management_unit" id_kind flows (funcionarios_adherencia)."""
    return collect_ids(
        s3_client,
        settings.management_unit_list_bucket,
        settings.management_unit_list_prefix,
        day,
        _management_unit_ids,
        "managementUnitId",
        counts_key="management_units",
    )


def collect_management_unit_business_unit_pairs(s3_client, settings: Settings, day: date) -> dict:
    """The day's {managementUnitId, businessUnitId} pairs grouped by
    organization -- for "management_unit_schedule" id_kind flows
    (funcionarios_programaciones)."""
    return collect_ids(
        s3_client,
        settings.management_unit_list_bucket,
        settings.management_unit_list_prefix,
        day,
        _management_unit_business_unit_pairs,
        "managementUnitId+businessUnitId",
        render=lambda pair: {"managementUnitId": pair[0], "businessUnitId": pair[1]},
        counts_key="management_units",
    )
