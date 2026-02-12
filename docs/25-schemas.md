# Schemas

This document defines JSON Schema for public and internal objects.

The platform uses these schemas in three places:

- control plane validation at publish time
- data plane validation at invocation time
- contract validation in integration tests

All schemas use draft 2020-12.

## 1. Function manifest (`cs.function.script.v1`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cs.function.script.v1",
  "type": "object",
  "required": ["schema", "runtime", "entry", "handler", "limits", "capabilities"],
  "properties": {
    "schema": { "const": "cs.function.script.v1" },
    "runtime": { "const": "cs-js" },
    "entry": { "type": "string", "pattern": "^[a-zA-Z0-9._/-]+$" },
    "handler": { "type": "string", "const": "default" },
    "limits": {
      "type": "object",
      "required": ["timeoutMs", "memoryMb", "maxConcurrency"],
      "properties": {
        "timeoutMs": { "type": "integer", "minimum": 1, "maximum": 900000 },
        "memoryMb": { "type": "integer", "minimum": 16, "maximum": 4096 },
        "maxConcurrency": { "type": "integer", "minimum": 1, "maximum": 100 }
      },
      "additionalProperties": false
    },
    "capabilities": {
      "type": "object",
      "required": ["kv", "codeq", "http"],
      "properties": {
        "kv": {
          "type": "object",
          "required": ["prefixes", "ops"],
          "properties": {
            "prefixes": {
              "type": "array",
              "items": { "type": "string", "minLength": 1, "maxLength": 256 },
              "maxItems": 64
            },
            "ops": {
              "type": "array",
              "items": { "enum": ["get", "set", "del"] },
              "maxItems": 3
            }
          },
          "additionalProperties": false
        },
        "codeq": {
          "type": "object",
          "required": ["publishTopics"],
          "properties": {
            "publishTopics": {
              "type": "array",
              "items": { "type": "string", "minLength": 1, "maxLength": 256 },
              "maxItems": 64
            }
          },
          "additionalProperties": false
        },
        "http": {
          "type": "object",
          "required": ["allowHosts", "timeoutMs"],
          "properties": {
            "allowHosts": {
              "type": "array",
              "items": { "type": "string", "minLength": 1, "maxLength": 253 },
              "maxItems": 128
            },
            "timeoutMs": { "type": "integer", "minimum": 1, "maximum": 30000 }
          },
          "additionalProperties": false
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

## 2. InvocationRequest (`cs.invoke.v1`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cs.invoke.v1",
  "type": "object",
  "required": [
    "activation_id",
    "request_id",
    "tenant",
    "namespace",
    "ref",
    "trigger",
    "principal",
    "deadline_ms",
    "event"
  ],
  "properties": {
    "activation_id": { "type": "string", "format": "uuid" },
    "request_id": { "type": "string", "minLength": 8, "maxLength": 64 },
    "tenant": { "type": "string", "minLength": 3, "maxLength": 64 },
    "namespace": { "type": "string", "minLength": 1, "maxLength": 64 },
    "ref": {
      "type": "object",
      "required": ["function"],
      "properties": {
        "function": { "type": "string", "minLength": 1, "maxLength": 64 },
        "alias": { "type": "string", "minLength": 1, "maxLength": 32 },
        "version": { "type": "integer", "minimum": 1 }
      },
      "additionalProperties": false
    },
    "trigger": {
      "type": "object",
      "required": ["type", "source"],
      "properties": {
        "type": { "enum": ["http", "schedule", "cadence", "api"] },
        "source": { "type": "object" }
      },
      "additionalProperties": false
    },
    "principal": {
      "type": "object",
      "required": ["sub", "roles"],
      "properties": {
        "sub": { "type": "string", "minLength": 1, "maxLength": 256 },
        "roles": {
          "type": "array",
          "items": { "type": "string", "minLength": 1, "maxLength": 256 },
          "maxItems": 256
        }
      },
      "additionalProperties": false
    },
    "deadline_ms": { "type": "integer", "minimum": 1 },
    "event": {}
  },
  "additionalProperties": false
}
```

## 3. InvocationResult (`cs.results.v1`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cs.results.v1",
  "type": "object",
  "required": ["activation_id", "request_id", "status", "duration_ms"],
  "properties": {
    "activation_id": { "type": "string", "format": "uuid" },
    "request_id": { "type": "string", "minLength": 8, "maxLength": 64 },
    "status": { "enum": ["success", "error", "timeout"] },
    "duration_ms": { "type": "integer", "minimum": 0 },
    "result": {
      "type": "object",
      "properties": {
        "statusCode": { "type": "integer", "minimum": 100, "maximum": 599 },
        "headers": { "type": "object", "additionalProperties": { "type": "string" } },
        "body": { "type": "string" },
        "isBase64Encoded": { "type": "boolean" }
      },
      "additionalProperties": false
    },
    "error": {
      "type": "object",
      "properties": {
        "type": { "type": "string", "minLength": 1, "maxLength": 128 },
        "message": { "type": "string", "maxLength": 65536 },
        "stack": { "type": "string", "maxLength": 8192 }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

## 4. Schedule create request

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cs.api.schedule.create.v1",
  "type": "object",
  "required": ["name", "every_seconds", "overlap_policy", "ref"],
  "properties": {
    "name": { "type": "string", "minLength": 3, "maxLength": 64 },
    "every_seconds": { "type": "integer", "minimum": 1, "maximum": 86400 },
    "overlap_policy": { "enum": ["skip", "queue", "parallel"] },
    "ref": {
      "type": "object",
      "required": ["function"],
      "properties": {
        "function": { "type": "string", "minLength": 1, "maxLength": 64 },
        "alias": { "type": "string", "minLength": 1, "maxLength": 32 },
        "version": { "type": "integer", "minimum": 1 }
      },
      "additionalProperties": false
    },
    "payload": {}
  },
  "additionalProperties": false
}
```

## 5. WorkerBinding create request

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cs.api.cadence.worker.create.v1",
  "type": "object",
  "required": ["name", "domain", "tasklist", "worker_id", "activity_map"],
  "properties": {
    "name": { "type": "string", "minLength": 3, "maxLength": 64 },
    "domain": { "type": "string", "minLength": 1, "maxLength": 128 },
    "tasklist": { "type": "string", "minLength": 1, "maxLength": 128 },
    "worker_id": { "type": "string", "minLength": 1, "maxLength": 128 },
    "activity_map": {
      "type": "object",
      "additionalProperties": {
        "type": "object",
        "required": ["function"],
        "properties": {
          "function": { "type": "string", "minLength": 1, "maxLength": 64 },
          "alias": { "type": "string", "minLength": 1, "maxLength": 32 },
          "version": { "type": "integer", "minimum": 1 }
        },
        "additionalProperties": false
      }
    },
    "pollers": {
      "type": "object",
      "properties": {
        "activity": { "type": "integer", "minimum": 1, "maximum": 256 }
      },
      "additionalProperties": false
    },
    "limits": {
      "type": "object",
      "properties": {
        "max_inflight_tasks": { "type": "integer", "minimum": 1, "maximum": 100000 }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```
