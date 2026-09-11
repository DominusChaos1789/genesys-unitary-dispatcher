"""Builds one tag's payload: an entry per Genesys organization, each carrying
that organization's ids and a request template for every stage of the flow.

Only the organization-specific parts are rendered -- the regional base_url
and the bearer token. Per-id placeholders ({conversationId}, {mu_id},
{user_id}, {jobId}, dates) stay in place for Unitary Status and Unitary
Download to fill when they execute each call.
"""

from typing import Any

from src.templates import render_template

_COPIED_FIELDS = ("type", "path", "result_data")


def build_stage(
    spec: dict[str, Any],
    *,
    token: dict[str, Any],
    connection: dict[str, Any],
    server: dict[str, Any],
    base_path: str,
) -> dict[str, Any]:
    stage: dict[str, Any] = {
        "base_url": render_template(connection["base_url"], region_id=server["region_id"]),
        "url": spec["url"],
        "method": spec.get("method"),
        "headers": render_template(connection["header_template"], access_token=token["access_token"]),
    }

    # The endpoint files spell this key "body_templante". Accept the correct
    # spelling too, so fixing those files later doesn't silently drop bodies.
    body = spec.get("body_templante", spec.get("body_template"))
    if body is not None:
        stage["payload"] = body
    if spec.get("params_template") is not None:
        stage["params"] = spec["params_template"]

    for field in _COPIED_FIELDS:
        stage[field] = spec.get(field)
    stage["base_path"] = base_path
    stage["server_path"] = server["relative_path"]
    return stage


def build_organization_entry(
    organization_id: str,
    ids: list[str],
    stages: dict[str, tuple[str, dict]],
    *,
    token: dict[str, Any],
    connection: dict[str, Any],
    server: dict[str, Any],
    base_path: str,
) -> dict[str, Any]:
    entry: dict[str, Any] = {"organization_id": organization_id, "ids": ids}
    for stage_key, (_, spec) in stages.items():
        entry[stage_key] = build_stage(
            spec, token=token, connection=connection, server=server, base_path=base_path
        )
    return entry


def build_payload(tag: str, organizations: list[dict[str, Any]]) -> dict[str, Any]:
    return {"tag": tag, "organization": organizations}
