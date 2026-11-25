# EventBridge rule: management units download → forecasts and planning groups

When the Genesys management units download writes a file such as

```
s3://augusta-nexa-dev-landing/funcionarios/genesys/api/management_unit_list/org_id=3/year=2026/month=08/day=13/management_unit_list_2026_08_13.json
```

this rule starts the Step Function that calls Request Unitary with

```json
{
  "tags": ["transacciones_pronosticos", "funcionarios_grupos_planificacion"],
  "ids_source": "management_unit_list",
  "ids_location": {
    "bucket": "augusta-nexa-dev-landing",
    "key": "funcionarios/genesys/api/management_unit_list/org_id=3/year=2026/month=08/day=13/management_unit_list_2026_08_13.json"
  }
}
```

Request Unitary reads **only that file**. The organization comes from its
`org_id=` folder (`org_id=3` → `org-3`) and the payload `date` comes from its
`year=/month=/day=` folders. Each distinct `businessUnit.id` in the file
becomes one id. Both flows get their own payload file per organization.

## 1. Turn on EventBridge notifications for the landing bucket

One time, per environment (S3 → bucket → Properties → Amazon EventBridge → On):

```bash
aws s3api put-bucket-notification-configuration \
  --bucket augusta-nexa-dev-landing \
  --notification-configuration '{"EventBridgeConfiguration": {}}'
```

⚠️ `put-bucket-notification-configuration` **replaces** the bucket's whole
notification configuration. If the bucket already has SNS/SQS/Lambda
notifications, read them first (`get-bucket-notification-configuration`) and
send them back together with `EventBridgeConfiguration`, or use the console.

## 2. The rule

Event pattern ([management-unit-list-pattern.json](management-unit-list-pattern.json)):
only `.json` objects inside an `org_id=` folder under the download's prefix.

```bash
aws events put-rule \
  --name augusta-nexa-dev-genesys-management-unit-list-created \
  --event-pattern file://docs/eventbridge/management-unit-list-pattern.json
```

## 3. The target, with its input transformer

The target is the Step Function that invokes Request Unitary (the same one the
other events use), with
[management-unit-list-input-transformer.json](management-unit-list-input-transformer.json)
turning the S3 event into the event above:

```bash
aws events put-targets \
  --rule augusta-nexa-dev-genesys-management-unit-list-created \
  --targets '[{
    "Id": "request-unitary",
    "Arn": "arn:aws:states:us-east-2:<account>:stateMachine:<request unitary state machine>",
    "RoleArn": "arn:aws:iam::<account>:role/<role allowed to states:StartExecution>",
    "InputTransformer": '"$(cat docs/eventbridge/management-unit-list-input-transformer.json)"'
  }]'
```

To have the same file also feed adherence and schedules, add
`"funcionarios_adherencia"` and `"funcionarios_programaciones"` to `tags` in the
template. Both already accept a file named this way.

The rule fires for every organization's file, so one file means one run for
that organization. For other environments, swap `dev` in the bucket name and
the rule name.

## Without the transformer

The raw S3 event works as well, as long as `tags`/`tag` and
`"ids_source": "management_unit_list"` are added somewhere the Lambda reads
them. The tags go top level or under `detail`; `ids_source` goes top level.
The transformer above is the simplest way to do that.
