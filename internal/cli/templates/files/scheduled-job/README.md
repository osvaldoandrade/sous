Scheduled job invoked by `cs-scheduler` on a fixed interval.

## Trigger

Bind the function to a schedule:

```
cs schedule create {{.Name}}_30s --every 30 --fn {{.Name}}@prod
```

`cs.timer` ticks deliver the schedule payload to the handler. `ctx.trigger.type`
will be `"schedule"` in production and `"api"` when running locally with
`cs fn test`.

## Event shape

`event` is the JSON payload configured on the schedule (or `{}` if none).
`ctx.deadline_ms` carries the timer fire deadline.

## Capabilities

- `kv.prefixes` — `job:` for idempotency anchors via `cs.kv.{get,set,del}`.
- `codeq.publishTopics` — fan-out under `jobs.scheduled.*` via
  `cs.codeq.publish`.
- `http.allowHosts` — empty by default; widen when the job needs outbound
  HTTP.

## Response

Return a JSON envelope; only `statusCode` is required for non-HTTP triggers.
