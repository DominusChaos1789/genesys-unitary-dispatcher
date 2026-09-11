"""Endpoint catalog: which Genesys calls make up each tag's flow.

core.json maps group names to endpoint-definition files, e.g.
"unitary": ["params/genesys/api/unitary.json"]. An endpoint in those files
may carry a `tag`; the endpoints sharing the requested tag are that flow's
stages, keyed by their `type`:

    unitary -> request_context   the direct call, or the initial listing
    init    -> request_init      starts an async job and returns a jobId
    status  -> request_status    polled until the job completes

So "surveys" is a single request_context, while "funcionarios_adherencia"
is request_context + request_init + request_status. A new flow is added by
tagging its endpoints -- no code change.
"""

from typing import Any

import src.s3_utils as s3_utils
from src.config import Settings

STAGE_BY_TYPE = {"unitary": "request_context", "init": "request_init", "status": "request_status"}
STAGE_ORDER = ("request_context", "request_init", "request_status")


def _references(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    return list(value or [])


def _merge(catalog: dict[str, dict], definitions: dict[str, dict], reference: str) -> None:
    for name, spec in definitions.items():
        if name in catalog and catalog[name] != spec:
            raise ValueError(f"Endpoint {name!r} is defined differently in more than one file ({reference})")
        catalog[name] = spec


def load_endpoint_catalog(s3_client, settings: Settings) -> dict[str, dict]:
    """Every endpoint definition from the configured core.json groups, by name.
    Groups that aren't configured are never read."""
    core = s3_utils.read_json(s3_client, settings.resources_bucket, settings.core_config_key)
    if "endpoints" in core:
        core = core["endpoints"]

    catalog: dict[str, dict] = {}
    for group in settings.endpoint_groups:
        if group not in core:
            raise ValueError(f"core config {settings.core_config_key} has no {group!r} group")
        for reference in _references(core[group]):
            definitions = s3_utils.read_json(s3_client, settings.resources_bucket, reference)
            _merge(catalog, definitions, reference)
    return catalog


def select_stages(catalog: dict[str, dict], tag: str) -> dict[str, tuple[str, dict]]:
    """The tag's stages as {stage: (endpoint name, definition)}, in flow order."""
    stages: dict[str, tuple[str, dict]] = {}
    for name, spec in catalog.items():
        if spec.get("tag") != tag:
            continue
        stage = STAGE_BY_TYPE.get(spec.get("type"))
        if stage is None:
            raise ValueError(
                f"Endpoint {name!r} (tag {tag!r}) has type {spec.get('type')!r}, which maps to no stage; "
                f"expected one of {sorted(STAGE_BY_TYPE)}"
            )
        if stage in stages:
            raise ValueError(f"Tag {tag!r} has two {stage} endpoints: {stages[stage][0]!r} and {name!r}")
        stages[stage] = (name, spec)

    if not stages:
        raise ValueError(f"No endpoints are tagged {tag!r}")
    return {stage: stages[stage] for stage in STAGE_ORDER if stage in stages}
