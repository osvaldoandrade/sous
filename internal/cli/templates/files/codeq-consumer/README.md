codeQ consumer that processes subscribed messages with idempotent state.

## Subscribe stub

Wire the function to a topic on the control plane (subscriber API or
`cs codeq subscribe` once available):

```
# pseudo: subscribe {{.Name}}@prod to topic "orders.created"
```

## Event shape

`event` is the codeQ message payload. The template uses `event.id` as the
idempotency key, falling back to `ctx.activation_id` when absent.

## Capabilities

- `kv.prefixes` — `idem:` is the typical anchor for idempotent consumers via
  `cs.kv.{get,set,del}`.
- `codeq.publishTopics` — `consumer.*` for downstream fan-out.
- `http.allowHosts` — empty by default.

## Response

Return `200` for successfully processed (or deduped) messages. Non-2xx
statuses signal a retryable failure to the dispatcher.
