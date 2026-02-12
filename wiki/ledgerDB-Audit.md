# ledgerDB Audit

ledgerDB stores an append-only record of changes.

In v0.1, audit is optional.
If enabled, `cs-control` emits an event for each mutation.

## Event envelope

```json
{
  "schema": "cs.audit.v1",
  "ts_ms": 1730000000000,
  "tenant": "t_abc123",
  "actor": { "sub": "user:123", "roles": ["role:admin"] },
  "type": "FunctionPublished",
  "data": { }
}
```

## Event types

### FunctionCreated

Data:

- `namespace`
- `function`
- `runtime`

### DraftUploaded

Data:

- `namespace`
- `function`
- `draft_id`
- `sha256`
- `size_bytes`

### FunctionPublished

Data:

- `namespace`
- `function`
- `version`
- `sha256`
- `config_hash`

### AliasUpdated

Data:

- `namespace`
- `function`
- `alias`
- `version`

### ScheduleCreated

Data:

- `namespace`
- `schedule`
- `every_seconds`
- `ref`

### ScheduleDeleted

Data:

- `namespace`
- `schedule`

### CadenceWorkerCreated

Data:

- `namespace`
- `worker`
- `domain`
- `tasklist`
- `worker_id`

### CadenceWorkerDeleted

Data:

- `namespace`
- `worker`
