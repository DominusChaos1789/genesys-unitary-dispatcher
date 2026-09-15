"""Builds one organization's payload: its ids and a request template for
every stage of the flow.

Only the organization-specific parts are rendered -- the regional base_url
and the bearer token. Per-id placeholders ({conversationId}, {mu_id},
{jobId}, {communicationId}, dates) stay in place for Unitary Status and
Unitary Download to fill when they execute each call.

Each organization gets its own flat payload file (see main._write_payloads):
no "organization" array to unpack, so a Step Function can read one file
straight into a Map state.
"""

from typing import Any

from src.templates import render_template

_COPIED_FIELDS = ("type", "path", "result_data")


def output_base_path(output_config: dict[str, Any], key: str) -> str:
    """The prefix a flow's downloaded JSON is saved under, from `config.output`
    (SSM) -- `key` comes from dispatcher_config.output_base_path_key(tag)."""
    if key not in output_config:
        raise ValueError(f"config.output.{key} isn't set")
    return output_config[key]


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


def build_organization_payload(
    tag: str,
    entry: dict[str, Any],
    failed_organizations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """The file Unitary Status and Unitary Download read for one organization:
    a flat object, no "organization" array. Organizations that couldn't be
    served in this run are listed with their ids under `failed_organizations`
    (the same list in every organization's file for this tag), so a re-run
    has them even though this file's own organization is a successful one."""
    return {"tag": tag, **entry, "failed_organizations": failed_organizations or []}
