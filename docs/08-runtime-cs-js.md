# Runtime: cs-js

This document defines the JavaScript runtime contract.

The runtime exists to execute code that an agent can produce and publish as text.

## Runtime shape

A published function version contains two UTF-8 files:

- `function.js`
- `manifest.json`

The system stores these files as a canonical tar bundle.
The system addresses the tar bundle by `sha256`.

The runtime loads the bundle from KVRocks and executes it in an isolate.

## Why an isolate exists

User code runs in the same cluster as platform services.
The platform must treat user code as untrusted.

An isolate lets the invoker:

- cap memory
- cap wall time
- remove access to host filesystem
- remove access to process spawn
- gate side effects behind explicit host APIs

## Handler contract

`function.js` exports a default async function.

```js
export default async function handle(event, ctx) {
  return { statusCode: 200, body: "ok" }
}
```

The invoker passes:

- `event`: JSON value
- `ctx`: execution context

## Context contract

`ctx` is a JSON object with stable fields.

```json
{
  "activation_id": "uuid",
  "deadline_ms": 1730000003000,
  "tenant": "t_abc123",
  "namespace": "payments",
  "function": "reconcile",
  "version": 17,
  "ref": { "alias": "prod" },
  "trigger": { "type": "http" },
  "principal": { "sub": "user:123", "roles": ["role:app"] }
}
```

The invoker may add fields.
The invoker must not remove fields in v0.1.

## Manifest schema

`manifest.json` declares limits and capabilities.

```json
{
  "schema": "cs.function.script.v1",
  "runtime": "cs-js",
  "entry": "function.js",
  "handler": "default",
  "limits": { "timeoutMs": 3000, "memoryMb": 64, "maxConcurrency": 1 },
  "capabilities": {
    "kv": { "prefixes": ["ctr:"], "ops": ["get","set"] },
    "codeq": { "publishTopics": ["jobs.*"] },
    "http": { "allowHosts": ["api.example.com"], "timeoutMs": 1500 }
  }
}
```

The control plane validates this manifest at publish time.
The invoker validates it again at execution time.

## Host API (`cs`)

User code calls host APIs through a single object: `cs`.

### Logging

- `cs.log.info(value)`
- `cs.log.warn(value)`
- `cs.log.error(value)`

The invoker serializes `value` as JSON if possible.
The invoker writes log chunks to KVRocks.

### Key-value access

- `cs.kv.get(key)`
- `cs.kv.set(key, value, { ttlSeconds })`
- `cs.kv.del(key)`

The runtime enforces:

- `key` prefix allowlist
- operation allowlist

The runtime serializes `value` as JSON.

### codeQ publish

- `cs.codeq.publish(topic, payload)`

The runtime enforces a topic prefix allowlist.

### HTTP fetch

- `cs.http.fetch(url, { method, headers, body, timeoutMs })`

The runtime enforces:

- hostname allowlist
- private IP block
- timeout cap from manifest

The runtime returns:

```json
{
  "status": 200,
  "headers": { "content-type": "application/json" },
  "body": "base64",
  "isBase64Encoded": true
}
```

## Limits

### Wall time

The invoker enforces `timeoutMs`.

The invoker sets `deadline_ms = now_ms + timeoutMs`.
The invoker cancels the isolate at `deadline_ms`.

### Memory

The invoker configures the isolate memory budget from `memoryMb`.
The invoker terminates the activation if the isolate exceeds this budget.

### Concurrency

The invoker enforces `maxConcurrency` per function version per replica.

The invoker uses a semaphore keyed by:

- tenant
- namespace
- function
- version

## Result contract

The handler returns an object.

`cs-invoker-pool` accepts these fields:

- `statusCode` integer
- `headers` map[string]string
- `body` string
- `isBase64Encoded` boolean

If the handler returns a non-object, the invoker returns:

- `statusCode = 200`
- `body = JSON.stringify(returnValue)`

## Local runner parity

The `cs` CLI embeds the same runtime and host API definitions.

The CLI implements `cs.kv` as:

- an in-memory provider by default
- an optional remote provider when `--kv-endpoint` exists

The CLI blocks network egress unless `http.allowHosts` exists in the manifest.
