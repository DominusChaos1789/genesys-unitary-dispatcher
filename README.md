# genesys-unitary-dispatcher

**Request Unitary** — the multi-tag dispatcher Lambda for Genesys Cloud. Each
invocation runs one or more **tags** (workflows) that [dispatcher.json](#dispatcherjson)
allows, writes one flat payload file **per organization per tag** to the logs
bucket, and returns a list of where each one went, for the Step Function.

**How it works** — step-by-step logic, diagrams, token handling and failure
behavior: [docs/how-it-works.md](docs/how-it-works.md).

This repo contains Request Unitary and the **contracts process** it can run as
an ids source: transcription files → parquet (core + EAV attributes, with
technical columns) → conversation ids per Genesys organization. **Unitary
Status** (polls a jobId until the job completes) and **Unitary Download**
(fetches the result into `landing`) read the payload this Lambda writes, and
live elsewhere.

```
EventBridge (event / scheduled) ──► Step Function ──► Request Unitary ──► logs bucket
                                                          │                   │
                                  token_manager ◄─────────┘      Unitary Status / Download
                                  (SSM + Secrets Manager + DynamoDB cache)
```

## Flows

A flow is defined by tagging endpoints in the endpoint files that `core.json`
references. An endpoint's `type` decides which stage it fills:

| type | stage | meaning |
|---|---|---|
| `unitary` | `request_context` | the direct call, or the initial listing |
| `init` | `request_init` | starts an async job, returns a `jobId` |
| `status` | `request_status` | polled until the job completes |
| `url` | `request_url` | a call whose ids come from `entry["ids"]` itself rather than a preceding stage (e.g. transcripts: `{conversationId}`/`{communicationId}` are already known from the conversations download) |

Current tags:

| tag | stages | endpoints |
|---|---|---|
| `surveys` | `request_context` | `conversations_surveys_result` (GET `/quality/surveys/{surveyId}`, one call per Finished surveyId, no polling) |
| `transcripts` | `request_url` | `transcripts_url` (GET `/speechandtextanalytics/conversations/{conversationId}/communications/{communicationId}/transcripturl`, one call per (conversationId, communicationId) pair — see [Payload](#payload)) |
| `transcript_events` | `request_url` | `transcript_events_url` (the same call as `transcripts_url`; only its ids source differs — real-time events instead of a daily download) |
| `funcionarios_adherencia` | `request_init` → `request_status` | `adherence_historical_init` (POST, one bulk job per management unit; no `userIds`, so it covers every user in the unit), `adherence_agent_status` |

Adding a flow needs two things: tag its endpoints here (no code change, as
long as it's built from stage types already in the table above), and add it
to [dispatcher.json](#dispatcherjson) (enabled, its domain, its `id_kind`) —
otherwise the tag exists in the catalog but the run refuses it. A tagged
endpoint whose `type` maps to no stage (e.g. `result`) is rejected rather than
silently dropped.

## dispatcher.json

`<resources bucket>/params/genesys/api/dispatcher.json` is the master switch
for which tags may run and where their output goes — an S3 edit, not a
deploy:

```json
{
  "domains": {
    "transacciones": {"output_base_path_key": "base_path"},
    "funcionarios":  {"output_base_path_key": "base_path_wfm"}
  },
  "flows": {
    "surveys":                 {"enabled": true, "domain": "transacciones", "id_kind": "survey"},
    "transcripts":              {"enabled": true, "domain": "transacciones", "id_kind": "transcript_session"},
    "transcript_events":        {"enabled": true, "domain": "transacciones", "id_kind": "transcript_event"},
    "funcionarios_adherencia": {"enabled": true, "domain": "funcionarios",  "id_kind": "management_unit"}
  }
}
```

- **`enabled`** — a kill switch. A disabled or undeclared tag fails the run
  before any ids are read, even if its endpoints are still tagged.
- **`domain`** → **`output_base_path_key`** — which `config.output` key (SSM)
  this tag's downloads are saved under; see [Payload](#payload).
- **`id_kind`** — `"tags": "all"` expands to every *enabled* flow whose
  `id_kind` is `"conversation"`, `"survey"` or `"transcript_session"`. All
  three come from the same conversations_details download, just a different
  part of the same records (see [Ids from the Genesys conversations
  download](#ids-from-the-genesys-conversations-download)); `"transcript_event"`
  and adherence's `"management_unit"` keep it out of `"all"` since neither is
  driven by a `date`. With `ids_source: "conversations_details"`, ids are
  resolved once per distinct `id_kind` among the tags run — a `"survey"` tag
  and a `"transcript_session"` tag in the same run never share an ids list,
  even though both come from that day's files. An organization with ids for
  one kind but none for another (e.g. no Finished surveys that day) simply gets
  no file for that tag — it's not a failure.

This is config, not code: turning a flow on/off, moving it to a different
domain, or adding a domain's output path is a `dispatcher.json` edit. Adding a
genuinely new stage type (a new row in the Flows table above) still needs a
code change in `endpoints.py`/`payload.py`; `dispatcher.json` only configures
flows built from stage types the code already understands. Endpoint
definitions (`unitary.json`/`status.json`) stay the only source of truth for
URLs/methods/bodies — `dispatcher.json` never duplicates them.

## Event

The event names the tag and the ids per organization, inline or in S3:

```json
{"tag": "surveys",
 "organizations": [{"organization_id": "org-3", "ids": ["06dc7cc5-..."]}]}
```

```json
{"tag": "funcionarios_adherencia",
 "ids_location": {"bucket": "landing", "key": "funcionarios/.../management_units.json"}}
```

An S3 **Object Created** event through EventBridge works too: the bucket and
key come from `detail`, the tag from `tag` or `detail.tag` (set it with the
rule's input transformer). The ids file holds the same organizations list,
either bare or as `{"organizations": [...]}`. Bucket names may be logical
(`landing`) or full (`augusta-nexa-dev-landing`).

Organizations repeated across entries are merged, ids are de-duplicated and
sorted, and organizations with no ids are dropped. An entry missing
`organization_id` or `ids` is an error, so a typo like `"id"` can't silently
drop an organization.

### Several flows in one run

```json
{"tags": ["surveys", "recordings"], "ids_source": "conversations_details", "date": "2026-08-13"}
{"tags": "all", "ids_source": "conversations_details", "date": "2026-08-13"}
```

`tags` runs several flows: ids are resolved once per distinct `id_kind`
among them (not once overall — see [dispatcher.json](#dispatcherjson)), each
organization's token is requested once for the whole run, and each
(tag, organization) pair gets its own payload file. `"all"` means every
**enabled** flow in dispatcher.json whose `id_kind` is `"conversation"`,
`"survey"` or `"transcript_session"` — adherence's `"management_unit"` and
transcript_events' `"transcript_event"` keep those two out, and a new
conversations_details-sourced flow joins `"all"` as soon as it's declared
there. Every tag is validated (declared, enabled, has endpoints) before any
ids are read. Send either `tag` or `tags`, not both.

### Ids from the contracts process

```json
{"tag": "surveys", "ids_source": "contracts"}
```

Typically sent by the hourly schedule. The run first executes the
[contracts process](#contracts-process) and uses the conversation ids it
finds, grouped by each contract's organization. The process is imported only
for these runs, so other flows don't load polars.

### Ids from the Genesys conversations download

```json
{"tag": "surveys", "ids_source": "conversations_details", "date": "2026-08-13"}
```

A separate process downloads conversation details into
`augusta-nexa-<env>-landing/transacciones/genesys/api/conversations_details/org_id=<N>/year=YYYY/month=MM/day=DD/`.
The run reads that `date`'s files for every `org_id=` folder (`org_id=1` →
`org-1`) and collects ids from each file's `endpoint[]` records — which part
depends on the tag's `id_kind`:

- `"conversation"` — the conversation's own `conversationId`.
- `"survey"` (surveys) — each conversation's `surveys[]` entries with
  `surveyStatus == "Finished"`, by their `surveyId`. Unitary consumes
  `conversations_surveys_result` (`/quality/surveys/{surveyId}`), not the
  conversation itself, so Expired/other-status surveys and conversations with
  none are skipped.
- `"transcript_session"` (transcripts) — one `{conversationId,
  communicationId}` pair per **recorded voice** participant session on the
  conversation (`participants[].sessions[].sessionId` → `communicationId`,
  kept only when that session's `recording` is `true` and `mediaType` is
  `"voice"`). A conversation with several qualifying sessions yields several
  pairs; its other sessions (ivr, acd routing, ...) have no transcript to
  fetch and are skipped, so they don't multiply the volume the downstream
  Lambdas have to process.

Nothing is transformed or written, and the files are never modified or
deleted: they belong to the download process. Files that can't be read, or
that have no `endpoint` list, are skipped and listed in
`conversations_details.skipped_files`. `date` is required, as `YYYY-MM-DD`.

### Ids from the Genesys management units download

```json
{"tag": "funcionarios_adherencia", "ids_source": "management_unit_list", "date": "2026-08-13"}
```

An alternative to `ids_location`/an S3 event for `funcionarios_adherencia`,
when the management units aren't already grouped by organization in one
file. A separate process downloads the management unit list into
`augusta-nexa-<env>-landing/funcionarios/genesys/api/management_unit_list/org_id=<N>/year=YYYY/month=MM/day=DD/`
-- the same layout as the conversations download. The run reads that
`date`'s files for every `org_id=` folder and collects each `endpoint[]`
record's `id` (`management_units.py`). Files that can't be read, or that
have no `endpoint` list, are skipped and listed in
`management_unit_list.skipped_files`. `date` is required, as `YYYY-MM-DD`.

An unknown `ids_source` value fails the run before any file is touched.

### Ids from real-time conversation events (`transcript_events`)

```json
{"tag": "transcript_events",
 "organizations": [{"organization_id": "org-1", "ids": ["<event_id>", "..."]}]}
```

`transcript_events`' `id_kind` is `"transcript_event"`, not one of the three
above — its ids aren't read from a whole day's conversations_details
download at all. A separate real-time process writes each Genesys Cloud
conversation event into
`augusta-nexa-<env>-landing/transacciones/genesys/events/org_id=<N>/<event_id>.json`
as it happens (no date partitioning), and an SQS-fed Step Function hands this
Lambda the event ids to read for each organization, the same way any other
tag's ids are supplied. For each one, the run reads that file — shaped like
`{"detail": {"eventBody": {"conversationId": ..., "sessionId": ..., ...}}}` —
and resolves it into the same `{conversationId, communicationId}` pair shape
`"transcript_session"` builds from the batch download, feeding the same
`transcript_events_url` endpoint transcripts uses. An event id whose file
can't be read, or that's missing `conversationId`/`sessionId`, is skipped and
listed in `transcript_events.skipped_files`; nothing is deleted.

## Payload

Each organization gets its own **flat** file — no `"organization"` array to
unpack, so a Step Function can read one file straight into a Map state:

```json
{
  "tag": "funcionarios_adherencia",
  "date": "2026-08-13",
  "organization_id": "org-1",
  "ids": ["<management unit id>", "..."],
  "request_init": {
    "base_url": "https://api.sae1.pure.cloud",
    "url": "/api/v2/workforcemanagement/adherence/historical/bulk",
    "method": "POST",
    "headers": {"Authorization": "Bearer <org-1 token>", "Content-Type": "application/json"},
    "payload": {
      "items": [{"managementUnitId": "{mu_id}", "startDate": "{star_date}", "endDate": "{end_date}",
                 "includeExceptions": true, "includeActuals": true}],
      "timeZone": "America/Bogota"
    },
    "type": "init", "path": "adherence_details", "result_data": "jobId",
    "base_path": "funcionarios/genesys/api", "server_path": "org_id=1/"
  },
  "request_status": {"...": "...", "url": ".../bulk/jobs/{jobId}", "result_data": "status"},
  "failed_organizations": []
}
```

- Only the organization-specific parts are rendered: `base_url` (region) and
  the `Authorization` header (token). Per-id placeholders — `{surveyId}`,
  `{conversationId}`, `{communicationId}`, `{mu_id}`, `{jobId}`,
  `{star_date}`, `{end_date}` — are left for Status/Download to fill per call.
- `date` is `event["date"]` for a run sourced by `conversations_details` or
  `management_unit_list`, otherwise the day the run happened (UTC) — every
  tag's payload carries it, not just the date-driven ones.

transcripts' `entry["ids"]` are `{conversationId, communicationId}` objects
(not plain strings), and its one stage renders straight from them — there's
no preceding call to fill `{communicationId}` from:

```json
{
  "tag": "transcripts",
  "date": "2026-09-21",
  "organization_id": "org-1",
  "ids": [{"conversationId": "79b342f9-...", "communicationId": "5363b9e1-..."}],
  "request_url": {
    "base_url": "https://api.usw2.pure.cloud",
    "url": "/api/v2/speechandtextanalytics/conversations/{conversationId}/communications/{communicationId}/transcripturl",
    "method": "GET",
    "type": "url", "path": "transcripts_url", "result_data": "state"
  },
  "failed_organizations": []
}
```

- Every stage of an organization uses **that organization's** token and region.
- `method`, `type`, `path`, `result_data` and the body come from the endpoint
  definition, so `request_init` is `POST` as `adherence_historical_init` says.
- `body_templante` (the spelling used in the endpoint files) becomes
  `payload`; the correct `body_template` spelling is accepted too.
  `params_template` becomes `params`.
- `server_path` is the server's `relative_path` (`org_id=1/`).
- `base_path` is the prefix the downloaded JSON is saved under, resolved
  through the tag's [dispatcher.json](#dispatcherjson) domain (`base_path` for
  `transacciones`, `base_path_wfm` for `funcionarios`) — a missing config key
  fails the run.
- `failed_organizations` lists, with their ids, every organization that
  couldn't be served in this run for this tag — see [Failures](#failures).

### Response

The handler always returns a **list**, one entry per (tag, organization) pair
that got a file — even a single-tag single-organization run — so a Step
Function Map state can iterate it the same way regardless of how many tags or
organizations were involved. It's where the payload was written, never the
payload itself (a day of conversations can exceed the Step Functions 256 KB
state limit):

```json
[
  {
    "bucket": "augusta-nexa-dev-logs",
    "payload_location": "transacciones/genesys/api/payload_request_unitary/surveys/org-1.json",
    "organization_id": "org-1",
    "stages": ["request_context"],
    "failed_organizations": [{"organization_id": "org-9", "id_count": 12, "error": "no servers entry for org_9"}],
    "tag": "surveys"
  }
]
```

- `payload_location` is the object key (prefix and file name) inside `bucket`.
- **No execution id anywhere.** The key is fixed per (tag, organization), so
  Status/Download always read the same, latest location, and each run
  **overwrites** the previous payload for that pair — there's no history to
  clean up. The Lambda request id still exists (`_context.aws_request_id`)
  but only for the logged run summary, not the file path.
- `failed_organizations` here gives an id **count**; the payload file (above)
  lists the actual ids.
- The detailed run summary (execution id, ids per organization, the contracts
  or conversations-download counts) is logged as `Run summary: {...}` in
  CloudWatch.

### Where it's written

`s3://augusta-nexa-<env>-logs/transacciones/genesys/api/payload_request_unitary/<tag>/<organization_id>.json`

Fixed per (tag, organization) — not per execution — so Status/Download always
know where to look without being told an execution id, and a run for the same
tag and organization simply replaces the previous payload. Pass each
response's `bucket` + `payload_location` to the next states instead of
rebuilding the key.

### Failures

- An unknown or disabled tag (not in `dispatcher.json`, or `enabled: false`),
  a tag with no endpoints, both `tag` and `tags`, `tags` that isn't `"all"` or
  a list of names, or `"all"` with no enabled conversation flow → the
  invocation fails **before** any ids are read (or, for the contracts process,
  before any source file is deleted).
- An organization with no `servers` entry, or whose token can't be obtained →
  gets **no payload file at all**, for any tag. Every other organization's
  file for that tag lists it under `failed_organizations` **with its ids**,
  and the response gives its `id_count`. With the contracts process the
  source files are already deleted at that point, so the payload file is
  where those ids survive.
- No ids at all → no payload files are written, and Genesys isn't called.

## Contracts process

Every `.json` under `CONTRACTS_PREFIX` is a contract, one per provider and
operation (`bdo/sac`, `pel/sac`, `bpp/cob`); `CONTRACT_KEY` pins a run to one.
Each contract runs independently:

1. Read the JSON files under its `source.prefix_pattern` in
   `augusta-nexa-<env>-providers-landing`. Unreadable files are skipped and
   left in place.
2. Rename, cast and transform the columns per the contract, then deduplicate.
3. Compute the `technical_columns` and split the rows into `output_core` (one
   row per conversation) and `output_atts` (one row per business column).
4. Write both as Hive-partitioned parquet to `augusta-nexa-<env>-refined`:
   `<prefix>/cliente_prefijo=<X>/operacion_prefijo=<Y>/year=YYYY/month=MM/day=DD/<output_name_file>_<timestamp>.parquet`.
5. Delete the source files that were processed.

The conversation ids are grouped by each contract's
`genesys_cloud_organization`, so contracts sharing an organization share one
entry and one token. A contract that fails is listed in
`contracts.failed_contracts` and keeps its source files, because deletion is
the last step; the other contracts still run. The response includes a
`contracts` summary with each contract's file counts and parquet keys.

Known gaps in the contract itself:

- `archivo_fecha_id` is listed in `output_atts.technical_col_keep` but has no
  `technical_columns` entry in the original contract. The test fixture adds one
  (`calculus_type: file_landing_date`); the contract in S3 needs the same.
- `gestion_tipo` / `gestion_canal` are kept in `output_core` without a
  `technical_columns` entry, so they're written as empty string columns.

## Tokens

`token_manager.py` resolves one token per organization: `servers[org_N]` gives
the region, `relative_path` and the `oauth` secret name; Secrets Manager gives
`client_id`/`client_secret`. Tokens are cached in DynamoDB through the
**runtime-control layer** (`runtime_control`) under the dataset
`<base_path>/<relative_path>`, and re-minted only within 15 minutes of expiry.
That key always uses `config.output.base_path`, even for `funcionarios` flows
whose output goes under `base_path_wfm`: tokens are per organization, so a
per-domain key would miss the cached token and mint a second one.
`runtime_control` is imported lazily, so the code imports and tests run
without the layer.

## Environment variables

All optional.

| Variable | Default | Purpose |
|---|---|---|
| `ENV_PREFIX` → `ENVIRONMENT` → `PROFILE` → `STACK_ID` | `dev` | Environment token (`dev`/`stg`/`pro`), first one set wins. The pipeline sets `ENVIRONMENT`/`STACK_ID`, not `ENV_PREFIX`. |
| `RESOURCES_BUCKET` | `augusta-nexa-<env>-resources` | Holds `core.json` and the endpoint files. Logical or full name. |
| `CORE_CONFIG_KEY` | `params/genesys/api/core.json` | Endpoint catalog. |
| `DISPATCHER_CONFIG_KEY` | `params/genesys/api/dispatcher.json` | The [flow allowlist/domain config](#dispatcherjson). |
| `ENDPOINT_GROUPS` | `unitary,status` | Which `core.json` groups to load. Others (daily/ondemand/actions) are never read. |
| `PAYLOAD_LOG_BUCKET` | `augusta-nexa-<env>-logs` | Logical or full name. |
| `PAYLOAD_LOG_KEY_TEMPLATE` | `transacciones/genesys/api/payload_request_unitary/{tag}/{organization_id}.json` | Fixed per (tag, organization) — no execution id — so each run overwrites the previous payload for that pair. Keep `{organization_id}` in any override, or different organizations will overwrite each other's file. |
| `API_GENESYS_PARAMS` | `/augusta-nexa-<env>/genesys/api` | SSM path for `connection`/`servers`/`config`; OAuth secrets share the prefix. |
| `REGION` | `us-east-2` | SSM / Secrets Manager region. |
| `RESOURCE_NAME` | `augusta-nexa-<env>-genesys-api-unitary-request` | Name in the runtime-control log (token cache). |
| `CONTRACTS_PREFIX` | `contracts/entrada/transacciones/empatia/transcripciones/` | Contracts process: every `.json` under it is a contract. |
| `CONTRACT_KEY` | *(unset)* | Contracts process: pin the run to this one contract. |
| `CONVERSATIONS_DETAILS_BUCKET` | `augusta-nexa-<env>-landing` | Conversations-download source: the bucket it writes to. Logical or full name. |
| `CONVERSATIONS_DETAILS_PREFIX` | `transacciones/genesys/api/conversations_details/` | The folder holding the `org_id=<N>/year=/month=/day=` partitions. |
| `CONVERSATIONS_EVENTS_BUCKET` | `augusta-nexa-<env>-landing` | `transcript_events` source: the bucket the real-time event process writes to. Logical or full name. |
| `CONVERSATIONS_EVENTS_PREFIX` | `transacciones/genesys/events/` | The folder holding the `org_id=<N>/<event_id>.json` files (no date partitioning). |
| `MANAGEMENT_UNIT_LIST_BUCKET` | `augusta-nexa-<env>-landing` | `management_unit_list` source: the bucket the Genesys management units download writes to. Logical or full name. |
| `MANAGEMENT_UNIT_LIST_PREFIX` | `funcionarios/genesys/api/management_unit_list/` | The folder holding the `org_id=<N>/year=/month=/day=` partitions. |

## Deployment

- Handler: `src.main.handler`, Python 3.13.
- Package from [requirements-lambda.txt](requirements-lambda.txt) (`boto3`,
  `requests`).
- Layers: `augusta-nexa-<env>-runtime-control`, plus the polars layer
  ([layers/polars](layers/polars/requirements.txt), `polars-lts-cpu`) for runs
  with `"ids_source": "contracts"`. The layer has to be built for Lambda's
  Linux x86_64 / Python 3.13, not the machine running pip:

  ```bash
  pip install -r layers/polars/requirements.txt --platform manylinux2014_x86_64 --python-version 3.13 --implementation cp --abi cp313 --only-binary=:all: -t layers/polars/build/python
  ```
- IAM:
  - `s3:GetObject` on the resources bucket and on wherever ids files land
  - `s3:PutObject` on `augusta-nexa-<env>-logs/*`
  - contracts process: `s3:ListBucket` on the resources bucket (contract
    discovery); `s3:ListBucket`, `s3:GetObject` and `s3:DeleteObject` on
    `augusta-nexa-<env>-providers-landing`; `s3:PutObject` on
    `augusta-nexa-<env>-refined/*`
  - conversations-download source: `s3:ListBucket` and `s3:GetObject` on
    `augusta-nexa-<env>-landing` (read-only)
  - `ssm:GetParametersByPath` on `/augusta-nexa-<env>/genesys/api/*`
  - `secretsmanager:ListSecrets` (`*`) and `secretsmanager:GetSecretValue` on that prefix
  - whatever `runtime_control` needs on `augusta-nexa-<env>-runtime-data`

## Testing in the AWS console

[events/](events/README.md) has one test event per use case (surveys, conversations
download, several tags, contracts, adherence, EventBridge S3, and the error
cases), with what each one touches and what it should return.

## Development

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements-dev.txt
.venv/Scripts/python -m pytest --cov=src --cov-report=term-missing
.venv/Scripts/python -m black src/ test/
.venv/Scripts/python -m flake8 src/ test/
```

Tests use moto for S3/SSM/Secrets Manager and stub the token lookup — no AWS
credentials or network needed. `test/fixtures/` holds the endpoint files
(`unitary.json` with the per-management-unit adherence config, `status.json`,
`jobs.json`), the BDO `transcripcion.json` contract and two sample
transcriptions.

## Open items

- **Who sends the adherence init.** Something has to send the init `POST`,
  filling `{mu_id}`, `{star_date}` and `{end_date}`, before Status can poll the
  `jobId`. This Lambda only builds the templates.
- **Bulk job limits.** Adherence runs one bulk job per management unit.
  `userIds` is left out, which per the Genesys SDK model
  `WfmHistoricalAdherenceBulkItem` queries every user in the unit. Not yet
  confirmed: the maximum `items` per job, the maximum date range per item, and
  how many jobs can run at once per organization. Check them before long
  backfills or grouping several units into one job. The body matches the
  model: required `items` (each with required `managementUnitId`, `startDate`,
  `endDate` in ISO-8601, and optional `includeExceptions`, `includeActuals`)
  and a required olson `timeZone`. Results come back as UTC timestamps.
- **Status polling identity.** Genesys documents the bulk job status endpoint
  as *"only the user who started the operation can query the status"*, so
  Unitary Status must poll with the same organization's OAuth client that
  sent the init. The per-organization tokens in the payload do that.
- **Tokens at rest.** The payload embeds bearer tokens, so they're stored in the
  logs bucket and in Step Functions execution history for ~24h.
- **OAuth credentials in the query string.** The token exchange sends
  `client_id`/`client_secret` as query parameters (`params=`) rather than a
  form body, carried over from the original client. Query strings tend to end
  up in proxy and access logs.
