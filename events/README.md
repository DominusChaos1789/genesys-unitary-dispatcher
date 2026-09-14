# Console test events

One event per use case, ready to paste into the Lambda console. Every file here
is run through the handler by [test/test_console_events.py](../test/test_console_events.py),
so they stay in sync with the code.

## Creating them in the console

1. Open the function (dev: `augusta-nexa-dev-genesys-api-unitary-request`) and go to the **Test** tab.
2. **Create new event**, use the file name without `.json` as the event name, paste the file's content, and **Save**.
   Mark it **Shareable** if the rest of the team should see it.
3. Click **Test**. The response and the logs appear below.

The same event from the CLI:

```bash
aws lambda invoke --function-name augusta-nexa-dev-genesys-api-unitary-request --cli-binary-format raw-in-base64-out --payload fileb://events/02-surveys-inline.json response.json
```

## The events

Run them roughly in this order: the first ones need the fewest permissions and
touch nothing, and `06` deletes files.

| Event | What it does | Side effects | Expected response |
|---|---|---|---|
| `01-smoke-empty` | surveys with no ids | writes an empty payload to logs; **no Genesys or SSM calls** | `payloads[0].organizations` is `{}` |
| `10`–`14` `err-*` | invalid events (see below) | none | the run fails with the error below |
| `02-surveys-inline` | surveys for one conversation id sent in the event | reads SSM/Secrets, gets org-1's token, writes the payload | `organizations: {"org-1": 1}` |
| `03-surveys-conv-details` | surveys for every conversation downloaded on 2026-08-13 | reads the landing files (never deletes them) | one count per `org_id=` folder with files that day |
| `04-tags-list` | same, with `tags` as a list | same as 03 | `tags: ["surveys"]` |
| `05-tags-all` | every conversation flow for 2026-08-13 | same as 03, one payload per flow | `tags` lists every conversation flow (today only `surveys`) |
| `07-adherence-inline` | adherence for one management unit sent in the event | token + payload | `organizations: {"org-1": 1}`, `stages: ["request_init", "request_status"]` |
| `08-adherence-s3-file` | adherence for the units listed in an S3 file | reads the file | same as 07 |
| `09-eventbridge-s3` | the S3 "Object Created" event EventBridge would send for that file | reads the file | same as 07 |
| `06-surveys-contracts` | the hourly contracts process, then surveys | ⚠️ **writes parquet to refined and deletes the processed transcription files in providers-landing** | `contracts.contracts_processed`, one count per contract organization |

Every successful run writes one file per tag to
`s3://augusta-nexa-dev-logs/transacciones/genesys/api/payload_request_unitary/<tag>/<request id>.json`;
open the `payload_location` from the response to see the full payload.

### Before running

- **`02`**: `00000000-0000-4000-8000-000000000001` is a placeholder. Replace it
  with a real `conversationId` from a landing file if Status/Download will act on
  the payload.
- **`03`–`05`**: change `date` to a day that has files under
  `augusta-nexa-dev-landing/transacciones/genesys/api/conversations_details/org_id=<N>/year=/month=/day=/`.
  A full day is dozens of files per organization; if the run times out, raise the
  function timeout (dev is 60 s) before reading anything into the result.
- **`07`**: replace `REPLACE_WITH_MU_ID` with a real management unit id.
- **`08`, `09`**: upload the units file first (edit its id too). The key is an
  example; use wherever the 2 a.m. process actually writes it:

  ```bash
  aws s3 cp events/s3-objects/management_units_2026-08-13.json s3://augusta-nexa-dev-landing/funcionarios/genesys/api/management_units/2026-08-13.json
  ```

- **`09`**: a real S3 event has no `tag`. The EventBridge rule's input
  transformer has to add `detail.tag` (or a top-level `tag`); the file shows
  the result.
- **`06`**: only run it when there are transcription files you're happy to have
  processed and removed from providers-landing.

### Error events

| Event | Fails with |
|---|---|
| `10-err-unknown-tag` | `No endpoints are tagged 'survey'` (checked before any ids are read) |
| `11-err-unknown-source` | `Unknown ids_source 'contract'; expected one of [...]` |
| `12-err-tag-and-tags` | `Event has both 'tag' and 'tags'; send one of them` |
| `13-err-missing-date` | `ids_source "conversations_details" needs "date" as YYYY-MM-DD` |
| `14-err-no-ids` | `Event carries no ids: expected 'organizations', 'ids_location', or an S3 event 'detail'` |
