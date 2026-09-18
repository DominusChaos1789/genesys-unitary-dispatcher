"""dispatcher.json: the master config for which flows this Lambda runs.

`<resources bucket>/params/genesys/api/dispatcher.json` declares, per tag,
whether it's enabled, which output *domain* it belongs to (which SSM
`config.output` key its downloads are saved under), and its `id_kind` (what
kind of id feeds it):

- "conversation": the ids are conversation ids themselves.
- "survey": the ids are Finished surveyIds read off those same conversations'
  `surveys[]` (surveys) -- Unitary consumes them by surveyId, not by
  conversation.
- "transcript_session": the ids are (conversationId, communicationId) pairs,
  one per participant session on those same conversations (transcripts) --
  its endpoint needs both ids, and a conversation can have several sessions.
- "transcript_event": also (conversationId, communicationId) pairs, but read
  one event id at a time from the real-time conversation-event process
  instead of a whole day's conversations_details download
  (transcript_events).
- "management_unit": a wholly different source (funcionarios_adherencia).

"conversation", "survey" and "transcript_session" all come from the same
conversations_details download, just a different part of the same records,
so `"tags": "all"` expands to every enabled flow of any of those three kinds.
"transcript_event" and "management_unit" flows are never included -- they
need `tags`/`tag` explicitly, since neither is driven by a `date`.

    {
      "version": 1,
      "domains": {
        "transacciones": {"output_base_path_key": "base_path"},
        "funcionarios":  {"output_base_path_key": "base_path_wfm"}
      },
      "flows": {
        "surveys":     {"enabled": true, "domain": "transacciones", "id_kind": "survey"},
        "transcripts": {"enabled": true, "domain": "transacciones", "id_kind": "transcript_session"},
        "funcionarios_adherencia": {"enabled": true, "domain": "funcionarios", "id_kind": "management_unit"}
      }
    }

This is config, not code: turning a flow on or off, moving it to a different
output domain, or adding a new domain's output path is an edit to this one S3
file, not a deploy. Adding a genuinely new *kind* of flow (a new stage type,
like the `request_url` stage transcripts introduced, or a new `id_kind`) still
needs a code change in endpoints.py/payload.py/conversations_details.py --
dispatcher.json only configures flows built from kinds the code already
understands.

Endpoint definitions (unitary.json/status.json) stay the single source of
truth for the actual URLs/methods/bodies; dispatcher.json never duplicates
them, only which tags may run and where their output goes.
"""

import src.s3_utils as s3_utils
from src.config import Settings

CONVERSATION_ID_KIND = "conversation"
SURVEY_ID_KIND = "survey"
TRANSCRIPT_SESSION_ID_KIND = "transcript_session"
TRANSCRIPT_EVENT_ID_KIND = "transcript_event"
CONVERSATION_DETAILS_ID_KINDS = (CONVERSATION_ID_KIND, SURVEY_ID_KIND, TRANSCRIPT_SESSION_ID_KIND)


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


def flow_id_kind(config: dict, tag: str) -> str:
    """The tag's id_kind, defaulting to "conversation" for flows that predate
    this field."""
    return flow_config(config, tag).get("id_kind", CONVERSATION_ID_KIND)


def conversation_tags(config: dict) -> list[str]:
    """Every enabled flow whose id_kind is "conversation", "survey" or
    "transcript_session" -- what `"tags": "all"` expands to. Flows keyed by
    other ids (e.g. adherence's management units) are never included."""
    return sorted(
        tag
        for tag, flow in config["flows"].items()
        if flow.get("enabled") and flow.get("id_kind") in CONVERSATION_DETAILS_ID_KINDS
    )
