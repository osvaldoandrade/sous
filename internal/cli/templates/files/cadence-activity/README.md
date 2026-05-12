Cadence activity handler dispatched by `cs-cadence-poller`.

## Trigger

Bind the function to an activity via a WorkerBinding (see
`docs/12-cadence-integration.md`):

```
cs cadence worker create {{.Name}}-worker \
  --domain payments --tasklist payments-activities \
  --worker-id cs-payments-01 \
  --activity SousInvokeActivity={{.Name}}@prod
```

## Event shape (activity payload contract)

```json
{
  "domain": "payments",
  "tasklist": "payments-activities",
  "workflowId": "wf_...",
  "runId": "run_...",
  "activityType": "SousInvokeActivity",
  "input": { /* user payload */ }
}
```

`ctx.trigger.type` is `"cadence"` in production. `cs.cadence.heartbeat()` is
only allowed under that trigger; the template guards the call so `cs fn test`
remains usable.

## Capabilities

- `kv.prefixes` — `act:` for activity-scoped state.
- `codeq.publishTopics` — `cadence.activity.*` for side effects.
- `http.allowHosts` — outbound calls limited to `api.example.com` with a
  5000ms timeout.

## Response

Return a JSON envelope. Non-success statuses are reported back to Cadence as
an activity failure with retry semantics governed by the workflow.
