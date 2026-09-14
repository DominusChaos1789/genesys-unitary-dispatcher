# genesys-unitary-dispatcher

**Request Unitary** — the multi-tag dispatcher Lambda for Genesys Cloud. Each
invocation runs one **tag** (one workflow), builds a payload with an entry per
Genesys organization, writes it to the logs bucket and returns it for the Step
Function.

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

A flow is defined entirely by tagging endpoints in the endpoint files that
`core.json` references. An endpoint's `type` decides which stage it fills:

| type | stage | meaning |
|---|---|---|
| `unitary` | `request_context` | the direct call, or the initial listing |
| `init` | `request_init` | starts an async job, returns a `jobId` |
| `status` | `request_status` | polled until the job completes |

Current tags:

| tag | stages | endpoints |
|---|---|---|
| `surveys` | `request_context` | `conversations_surveys` (GET, one call per conversation id, no polling) |
| `funcionarios_adherencia` | `request_init` → `request_status` | `adherence_historical_init` (POST, one bulk job per management unit; no `userIds`, so it covers every user in the unit), `adherence_agent_status` |

Adding a flow (e.g. the generic conversations extraction) means tagging its
endpoints — no code change, as long as it has at most one endpoint per stage.
A tagged endpoint whose `type` maps to no stage (e.g. `result`) is rejected
rather than silently dropped.

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

`tags` runs several flows over the same ids: the ids are read once, each
organization's token is requested once, and each tag gets its own payload file.
`"all"` means every conversation flow, i.e. every tag with an endpoint whose URL
contains `{conversationId}`. Flows keyed by other ids, like adherence
(`{mu_id}`), are never included, and a new conversation flow joins `"all"` as
soon as its endpoint is tagged. Every tag is validated before any ids are read.
Send either `tag` or `tags`, not both.

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
`org-1`) and collects each file's `endpoint[].conversationId`. Nothing is
transformed or written, and the files are never modified or deleted: they
belong to the download process. Files that can't be read, or that have no
`endpoint` list, are skipped and listed in `conversations_details.skipped_files`.
`date` is required, as `YYYY-MM-DD`.

An unknown `ids_source` value fails the run before any file is touched.

## Payload

```json
{
  "tag": "funcionarios_adherencia",
  "organization": [
    {
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
      "request_status": {"...": "...", "url": ".../bulk/jobs/{jobId}", "result_data": "status"}
    }
  ],
  "failed_organizations": []
}
```

- Only the organization-specific parts are rendered: `base_url` (region) and
  the `Authorization` header (token). Per-id placeholders — `{conversationId}`,
  `{mu_id}`, `{jobId}`, `{star_date}`, `{end_date}` — are left for
  Status/Download to fill per call.
- Every stage of an organization uses **that organization's** token and region.
- `method`, `type`, `path`, `result_data` and the body come from the endpoint
  definition, so `request_init` is `POST` as `adherence_historical_init` says.
- `body_templante` (the spelling used in the endpoint files) becomes
  `payload`; the correct `body_template` spelling is accepted too.
  `params_template` becomes `params`.
- `server_path` is the server's `relative_path` (`org_id=1/`).
- `base_path` is the prefix the downloaded JSON is saved under, and it follows
  the tag's **domain** (the tag's first segment):

  | domain | config key | prefix |
  |---|---|---|
  | `funcionarios_*` (workforce management, e.g. adherence) | `config.output.base_path_wfm` | `funcionarios/genesys/api` |
  | anything else — transacciones (surveys, conversations, ...) | `config.output.base_path` | `transacciones/genesys/api` |

  A flow in a new domain needs an entry in `OUTPUT_BASE_PATH_KEY_BY_DOMAIN`
  ([src/payload.py](src/payload.py)); a missing config key fails the run.

### Response

The response says where each payload is and how many ids it holds, never the
payload itself: a day of conversations can exceed the Step Functions 256 KB
state limit.

```json
{
  "execution_id": "...",
  "tags": ["surveys"],
  "payloads": [
    {
      "tag": "surveys",
      "payload_location": "s3://augusta-nexa-dev-logs/transacciones/genesys/api/payload_request_unitary/surveys/<execution_id>.json",
      "stages": ["request_context"],
      "organizations": {"org-1": 5100, "org-3": 820}
    }
  ],
  "failed_organizations": [{"organization_id": "org-9", "id_count": 12, "error": "no servers entry for org_9"}]
}
```

With an ids source, the response also carries that source's summary under its
name (`contracts`, `conversations_details`), with counts rather than ids.

### Where it's written

`s3://augusta-nexa-<env>-logs/transacciones/genesys/api/payload_request_unitary/<tag>/<execution_id>.json`

The key includes the tag and the Lambda request id because Status and Download
read the payload back: with one fixed key, a surveys run could overwrite an
adherence payload that's still being processed. Pass each
`payloads[].payload_location` to the next states (for example a Map state over
`$.payloads`) instead of rebuilding the key.

### Failures

- Unknown tag, or a misconfigured catalog → the invocation fails, **before**
  any ids are read.
- An organization with no `servers` entry, or whose token can't be obtained →
  left out of every payload. Each payload file lists it under
  `failed_organizations` **with its ids**, and the response gives its
  `id_count`; the other organizations still get entries. With the contracts
  process the source files are already deleted at that point, so the payload
  file is where those ids survive.
- Both `tag` and `tags`, `tags` that isn't `"all"` or a list of names, or
  `"all"` with no conversation flow in the catalog → the invocation fails.
- No ids at all → an empty payload is still written, and Genesys isn't called.

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
| `ENDPOINT_GROUPS` | `unitary,status` | Which `core.json` groups to load. Others (daily/ondemand/actions) are never read. |
| `PAYLOAD_LOG_BUCKET` | `augusta-nexa-<env>-logs` | Logical or full name. |
| `PAYLOAD_LOG_KEY_TEMPLATE` | `transacciones/genesys/api/payload_request_unitary/{tag}/{execution_id}.json` | |
| `API_GENESYS_PARAMS` | `/augusta-nexa-<env>/genesys/api` | SSM path for `connection`/`servers`/`config`; OAuth secrets share the prefix. |
| `REGION` | `us-east-2` | SSM / Secrets Manager region. |
| `RESOURCE_NAME` | `augusta-nexa-<env>-genesys-api-unitary-request` | Name in the runtime-control log (token cache). |
| `CONTRACTS_PREFIX` | `contracts/entrada/transacciones/empatia/transcripciones/` | Contracts process: every `.json` under it is a contract. |
| `CONTRACT_KEY` | *(unset)* | Contracts process: pin the run to this one contract. |
| `CONVERSATIONS_DETAILS_BUCKET` | `augusta-nexa-<env>-landing` | Conversations-download source: the bucket it writes to. Logical or full name. |
| `CONVERSATIONS_DETAILS_PREFIX` | `transacciones/genesys/api/conversations_details/` | The folder holding the `org_id=<N>/year=/month=/day=` partitions. |

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
