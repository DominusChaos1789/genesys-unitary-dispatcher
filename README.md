# genesys-unitary-dispatcher

**Request Unitary** — the multi-tag dispatcher Lambda for Genesys Cloud. Each
invocation runs one **tag** (one workflow), builds a payload with an entry per
Genesys organization, writes it to the logs bucket and returns it for the Step
Function.

This repo contains only Request Unitary. **Unitary Status** (polls a jobId
until the job completes) and **Unitary Download** (fetches the result into
`landing`) read the payload this Lambda writes, and live elsewhere. The hourly
contracts ETL (transcriptions → parquet with technical columns) is also a
separate pipeline; it's one of the sources that can feed ids into a tag here.

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
  ]
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

The handler returns the payload plus `execution_id`, `payload_location`,
`stages` and `failed_organizations`.

### Where it's written

`s3://augusta-nexa-<env>-logs/transacciones/genesys/api/payload_request_unitary/<tag>/<execution_id>.json`

The key includes the tag and the Lambda request id because Status and Download
read the payload back: with one fixed key, a surveys run could overwrite an
adherence payload that's still being processed. Pass `payload_location` to the
next states instead of rebuilding the key.

### Failures

- Unknown tag, or a misconfigured catalog → the invocation fails, **before**
  any ids are read.
- An organization with no `servers` entry, or whose token can't be obtained →
  listed in `failed_organizations`; the other organizations still get entries.
- No ids at all → an empty payload is still written, and Genesys isn't called.

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

## Deployment

- Handler: `src.main.handler`, Python 3.13.
- Package from [requirements-lambda.txt](requirements-lambda.txt) (`boto3`,
  `requests`). No polars/parquet here, so no extra data layer is needed.
- Layer: `augusta-nexa-<env>-runtime-control`.
- IAM:
  - `s3:GetObject` on the resources bucket and on wherever ids files land
  - `s3:PutObject` on `augusta-nexa-<env>-logs/*`
  - `ssm:GetParametersByPath` on `/augusta-nexa-<env>/genesys/api/*`
  - `secretsmanager:ListSecrets` (`*`) and `secretsmanager:GetSecretValue` on that prefix
  - whatever `runtime_control` needs on `augusta-nexa-<env>-runtime-data`

## Development

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements-dev.txt
.venv/Scripts/python -m pytest --cov=src --cov-report=term-missing
.venv/Scripts/python -m black src/ test/
.venv/Scripts/python -m flake8 src/ test/
```

Tests use moto for S3/SSM/Secrets Manager and stub the token lookup — no AWS
credentials or network needed. The endpoint files in `test/fixtures/` are
copies of the real `unitary.json`, `status.json` and `jobs.json`.

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
