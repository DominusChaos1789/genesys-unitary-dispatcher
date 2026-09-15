"""dispatcher.json: the master config for which flows this Lambda runs.

`<resources bucket>/params/genesys/api/dispatcher.json` declares, per tag,
whether it's enabled, which output *domain* it belongs to (which SSM
`config.output` key its downloads are saved under), and its `id_kind` (what
kind of id feeds it -- "conversation", "management_unit", ...). `"tags":
"all"` expands to every enabled flow whose `id_kind` is "conversation".

    {
      "version": 1,
      "domains": {
        "transacciones": {"output_base_path_key": "base_path"},
        "funcionarios":  {"output_base_path_key": "base_path_wfm"}
      },
      "flows": {
        "surveys":                  {"enabled": true, "domain": "transacciones", "id_kind": "conversation"},
        "transcripts":               {"enabled": true, "domain": "transacciones", "id_kind": "conversation"},
        "funcionarios_adherencia":  {"enabled": true, "domain": "funcionarios",  "id_kind": "management_unit"}
      }
    }

This is config, not code: turning a flow on or off, moving it to a different
output domain, or adding a new domain's output path is an edit to this one S3
file, not a deploy. Adding a genuinely new *kind* of flow (a new stage type,
like the `request_url` stage transcripts introduced) still needs a code
change in endpoints.py/payload.py -- dispatcher.json only configures flows
built from stage types the code already understands.

Endpoint definitions (unitary.json/status.json) stay the single source of
truth for the actual URLs/methods/bodies; dispatcher.json never duplicates
them, only which tags may run and where their output goes.
"""

import src.s3_utils as s3_utils
from src.config import Settings

CONVERSATION_ID_KIND = "conversation"


class DispatcherConfigError(ValueError):
    """dispatcher.json is missing, malformed, or references something that
    doesn't exist -- or the requested tag isn't declared or is disabled."""


def load_dispatcher_config(s3_client, settings: Settings) -> dict:
    config = s3_utils.read_json(s3_client, settings.resources_bucket, settings.dispatcher_config_key)
    _validate(config)
    return config


def _validate(config) -> None:
    if not isinstance(config, dict) or not isinstance(config.get("domains"), dict):
        raise DispatcherConfigError("dispatcher.json needs a top-level 'domains' object")
    if not isinstance(config.get("flows"), dict):
        raise DispatcherConfigError("dispatcher.json needs a top-level 'flows' object")

    domains = config["domains"]
    for name, domain in domains.items():
        if not isinstance(domain, dict) or not domain.get("output_base_path_key"):
            raise DispatcherConfigError(f"domains.{name!r} needs a non-empty 'output_base_path_key'")

    for tag, flow in config["flows"].items():
        if not isinstance(flow, dict) or "enabled" not in flow:
            raise DispatcherConfigError(f"flows.{tag!r} needs an 'enabled' boolean")
        if flow.get("domain") not in domains:
            raise DispatcherConfigError(f"flows.{tag!r}.domain {flow.get('domain')!r} is not in 'domains'")


def flow_config(config: dict, tag: str) -> dict:
    """The tag's entry in dispatcher.json. Raises if the tag isn't declared,
    or is declared but disabled -- callers should check this before doing
    anything else with the tag (reading ids, deleting source files, ...)."""
    flow = config["flows"].get(tag)
    if flow is None:
        raise DispatcherConfigError(f"Tag {tag!r} is not declared in dispatcher.json's 'flows'")
    if not flow["enabled"]:
        raise DispatcherConfigError(f"Tag {tag!r} is disabled in dispatcher.json")
    return flow


def output_base_path_key(config: dict, tag: str) -> str:
    """Which `config.output` key (from SSM) this tag's downloads are saved
    under, resolved through the tag's domain."""
    flow = flow_config(config, tag)
    return config["domains"][flow["domain"]]["output_base_path_key"]


def conversation_tags(config: dict) -> list[str]:
    """Every enabled flow whose id_kind is "conversation" -- what `"tags":
    "all"` expands to. Flows keyed by other ids (e.g. adherence's management
    units) are never included."""
    return sorted(
        tag
        for tag, flow in config["flows"].items()
        if flow.get("enabled") and flow.get("id_kind") == CONVERSATION_ID_KIND
    )
