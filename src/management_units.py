"""Management unit / business unit ids from the Genesys management units download.

A separate process downloads the management unit list into the landing
bucket, one folder per organization and day -- the same layout
conversations_details.py reads (see landing_partitions.py):

    augusta-nexa-<env>-landing/funcionarios/genesys/api/management_unit_list/
        org_id=<N>/year=YYYY/month=MM/day=DD/*.json

Each file is {"endpoint": [{"id": ..., "name": ..., "businessUnit": {"id":
..., ...}, "division": {...}, "selfUri": ...}, ...], "status": "success"}.
A run with `"ids_source": "management_unit_list"` reads either

- `"date": "YYYY-MM-DD"`: that day's files for every organization, or
- one file (`ids_location`, or the S3 "Object Created" event EventBridge
  sends when the download writes it): just that file, for the organization
  of its org_id= folder,

and collects ids -- which part depends on the tag's id_kind:

- "management_unit" (funcionarios_adherencia): each record's own `id`.
- "management_unit_schedule" (funcionarios_programaciones): one
  {managementUnitId, businessUnitId} pair per record -- its schedule
  endpoints need the management unit's business unit too.
- "business_unit" (transacciones_pronosticos, funcionarios_grupos_planificacion):
  each record's `businessUnit.id`, once per business unit -- several
  management units can share one, and forecasts and planning groups are per
  business unit.

Unlike the contracts process there is nothing to transform or write: the ids
only feed the payload, and the files are never modified or deleted here.
"""

import re
from datetime import date

import src.s3_utils as s3_utils
from src.config import Settings
from src.dispatcher_config import (
    BUSINESS_UNIT_ID_KIND,
    MANAGEMENT_UNIT_ID_KIND,
    MANAGEMENT_UNIT_SCHEDULE_ID_KIND,
)
from src.landing_partitions import collect_ids
from src.sources import EventError

ENDPOINT_KEY = "endpoint"
_ORG_IN_KEY = re.compile(r"(?:^|/)org_id=([^/]+)/")


def _units(document) -> list[dict]:
    units = document.get(ENDPOINT_KEY) if isinstance(document, dict) else None
    if not isinstance(units, list):
        raise ValueError(f"expected an object with an {ENDPOINT_KEY!r} list")
    return units


def _business_unit_id(unit) -> str | None:
    business_unit = unit.get("businessUnit") if isinstance(unit, dict) else None
    return business_unit.get("id") if isinstance(business_unit, dict) else None


def _management_unit_ids(document) -> list[str]:
    return [unit["id"] for unit in _units(document) if isinstance(unit, dict) and unit.get("id")]


def _management_unit_business_unit_pairs(document) -> list[tuple[str, str]]:
    return [
        (unit["id"], _business_unit_id(unit))
        for unit in _units(document)
        if isinstance(unit, dict) and unit.get("id") and _business_unit_id(unit)
    ]


def _business_unit_ids(document) -> list[str]:
    return [business_unit for business_unit in map(_business_unit_id, _units(document)) if business_unit]


# id_kind -> (extractor, label, render).
_SPECS = {
    MANAGEMENT_UNIT_ID_KIND: (_management_unit_ids, "managementUnitId", lambda item: item),
    MANAGEMENT_UNIT_SCHEDULE_ID_KIND: (
        _management_unit_business_unit_pairs,
        "managementUnitId+businessUnitId",
        lambda pair: {"managementUnitId": pair[0], "businessUnitId": pair[1]},
    ),
    BUSINESS_UNIT_ID_KIND: (_business_unit_ids, "businessUnitId", lambda item: item),
}


def _spec(id_kind: str):
    if id_kind not in _SPECS:
        raise EventError(
            f'ids_source "management_unit_list" cannot feed id_kind {id_kind!r}; '
            f"expected one of {sorted(_SPECS)}"
        )
    return _SPECS[id_kind]


def collect_for_id_kind(s3_client, settings: Settings, day: date, id_kind: str) -> dict:
    """The day's ids for `id_kind`, grouped by organization."""
    extractor, label, render = _spec(id_kind)
    return collect_ids(
        s3_client,
        settings.management_unit_list_bucket,
        settings.management_unit_list_prefix,
        day,
        extractor,
        label,
        render=render,
        counts_key="management_units",
    )


def organization_from_key(key: str) -> str | None:
    """ ".../org_id=3/year=2026/.../file.json" -> "org-3"."""
    match = _ORG_IN_KEY.search(key)
    return f"org-{match.group(1)}" if match else None


def collect_from_file(s3_client, settings: Settings, bucket: str, key: str, id_kind: str) -> dict:
    """One management units file -- the one an S3 event or `ids_location`
    names -- as ids for `id_kind`, for the organization of its org_id= folder."""
    extractor, _label, render = _spec(id_kind)
    organization_id = organization_from_key(key)
    if organization_id is None:
        raise EventError(f"management_unit_list file {key!r} is not under an org_id= folder")
    bucket = settings.resolve_bucket(bucket)
    try:
        items = extractor(s3_utils.read_json(s3_client, bucket, key))
    except ValueError as exc:  # also covers JSONDecodeError
        raise EventError(f"management_unit_list file s3://{bucket}/{key} is unreadable: {exc}") from None
    ids = [render(item) for item in sorted(set(items))]
    ids_by_organization = {organization_id: ids} if ids else {}
    return {
        "ids_by_organization": ids_by_organization,
        "summary": {
            "bucket": bucket,
            "file": key,
            "management_units": {org: len(found) for org, found in ids_by_organization.items()},
        },
    }
