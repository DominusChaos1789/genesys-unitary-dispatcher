"""Management unit ids from the Genesys management units download.

A separate process downloads the management unit list into the landing
bucket, one folder per organization and day -- the same layout
conversations_details.py reads (see landing_partitions.py):

    augusta-nexa-<env>-landing/funcionarios/genesys/api/management_unit_list/
        org_id=<N>/year=YYYY/month=MM/day=DD/*.json

Each file is {"endpoint": [{"id": ..., "name": ..., "businessUnit": {...},
"division": {...}, "selfUri": ...}, ...], "status": "success"}. For a run
with `"ids_source": "management_unit_list"` and `"date": "YYYY-MM-DD"`, this
reads that day's files for every organization and collects each management
unit's `id`. Unlike the contracts process there is nothing to transform or
write: the ids only feed the payload, and the files are never modified or
deleted here.
"""

from datetime import date

from src.config import Settings
from src.landing_partitions import collect_ids

ENDPOINT_KEY = "endpoint"


def _management_unit_ids(document) -> list[str]:
    units = document.get(ENDPOINT_KEY) if isinstance(document, dict) else None
    if not isinstance(units, list):
        raise ValueError(f"expected an object with an {ENDPOINT_KEY!r} list")
    return [unit["id"] for unit in units if isinstance(unit, dict) and unit.get("id")]


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
