HTTP handler invoked via `cs http invoke` or `cs fn invoke` with a JSON event.

## Event shape

The `event` argument is the JSON body posted to the gateway, e.g.

```json
{ "method": "POST", "path": "/orders", "body": { "id": "o_1" } }
```

## Context

`ctx` exposes `activation_id`, `deadline_ms`, `tenant`, `namespace`,
`function`, `ref`, `trigger`, and `principal`. See `docs/06-runtime-js.md`.

## Capabilities

- `kv.prefixes` — keys under `ctr:` available via `cs.kv.{get,set,del}`.
- `codeq.publishTopics` — topics `jobs.*` available via `cs.codeq.publish`.
- `http.allowHosts` — outbound calls limited to `api.example.com` with a
  1500ms timeout via `cs.http.fetch`.

## Response

Return an object with `statusCode`, `headers`, `body` and optional
`isBase64Encoded`. The gateway forwards it verbatim.
