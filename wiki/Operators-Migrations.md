# Operators: Migrations

Sous follows a strict versioning discipline so an operator can roll a cluster forward, roll it back, or run a mixed-version fleet during a release without losing data or breaking tenants. The discipline rests on three commitments: every public surface carries a version field, every breaking change ships behind an additive transition, and every release is testable against the parity harness before it goes out. This page covers what each surface guarantees, how additive changes coexist with the prior shape, what is reversible in a rollback, and what is not.

## Release versioning

Sous follows semantic versioning at the release level. The version applies to the platform binaries jointly — `cs-control`, `cs-http-gateway`, `cs-invoker-pool`, `cs-scheduler`, and `cs-cadence-poller` ship from the same monorepo and carry the same tag. Operators pin all five binaries to the same release; running mixed versions is supported only for the rollout window of a single minor release.

- **Major** (`v2.0.0`) — a breaking change is removed (a deprecated field disappears, a schema-version bump becomes mandatory, an error code is renamed). Operators upgrade only after every tenant has migrated off the deprecated surface.
- **Minor** (`v1.4.0`) — additive features land. New configuration knobs are introduced with safe defaults; new error codes appear; new metric series are emitted. Existing tenants observe no change unless they opt in.
- **Patch** (`v1.4.3`) — bug fixes only. The patch level never changes a schema, a wire format, or a metric label.

Deprecated surfaces remain functional for at least one minor release after the deprecation note lands in the release notes. The release notes name the replacement and the target removal release. The pre-deprecation cycle gives tenants and operators a window long enough to plan a migration without scheduling pressure.

## KVRocks schema migrations

The persistence layer stores JSON records under typed keys. Every record carries a `schema` (or `meta.schema`) field that names the wire shape and a `schema_version` integer. The control plane writes only the latest version; the readers across the platform handle every shape the prior minor release wrote.

Additions roll out without a migration. A new optional field appears on a record the next time `cs-control` writes it; readers that do not know the field ignore it under the JSON additive-decoding rule. Operators do not run a job to populate the new field on existing records — the next mutation does it implicitly, and read paths tolerate the legacy shape until then.

Removals stage through two releases. Release N+1 stops writing the field while still tolerating it on read. Release N+2 stops reading the field entirely. Operators run a one-shot `cs-migrate` job between the two releases to scrub the field from every record; the job is idempotent and resumable, and the release notes document the exact key prefixes it touches.

Renames stage through three releases. Release N+1 writes both the old and the new field. Release N+2 reads only the new field. Release N+3 stops writing the old field. The double-write window guarantees that a downgrade inside the rollout never loses data.

The migration job lives under `cmd/cs-migrate/` and is shipped as a Kubernetes `Job` template in `deploy/helm/code-sous`. Operators run it during a maintenance window, not in production traffic, because it iterates every key under a prefix and can saturate KVRocks read capacity if the prefix is large.

## codeQ message schema

Message envelopes carry a `schema_version` field. The producer writes the version it ships with; the consumer accepts every version up to and including its own. The schema is additive — fields can appear, never disappear within a major.

Two patterns coexist during a rollout:

- **Old producer / new consumer.** The new consumer tolerates the old shape because every field added in the newer release is optional. Operators upgrade consumers (`cs-invoker-pool`, `cs-scheduler` results consumer, `cs-cadence-poller`) before producers (`cs-http-gateway`, `cs-control`).
- **New producer / old consumer.** The old consumer ignores unknown fields. Operators who must run a new producer against an old consumer (an unsupported configuration in production, but inevitable during a partial rollback) verify that no required field was added; release notes flag the rare cases where a new producer cannot talk to the previous minor's consumer.

Two messages of different schema versions can sit on the same codeQ topic during the rollout window. The platform never relies on a global flag-day cutover; consumers handle the version difference transparently. See [codeQ Protocol](codeQ-Protocol) for the envelope shape.

## Manifest and trigger schemas

Function manifests carry `manifest_schema` strings like `cs.function.script.v1`. The control plane writes only the latest schema; the invoker executes bundles whose manifest schema is listed in its configuration. Unknown schemas refuse to execute with `CS_VALIDATION_FAILED` at publish time and `CS_RUNTIME_VALIDATION_FAILED` at invoke time. The schema list is forwarded-compatible: a `v1` invoker accepts a `v1`-marked manifest produced by a future control plane only when every required field at `v1` is still present.

Trigger records (`http`, `schedule`, `cadence`, `codeq`) carry their own `schema_version`. The most recent additive change introduced the `sampling` block (E7.02). Old triggers without `sampling` continue to record every activation; new triggers with a `sampling` block flow through the per-mode `Decider`. Operators do not need to rewrite existing triggers when the platform adds a sampling field; the missing block is treated as `mode: always`.

## Rollback rules

Sous separates reversible operations from irreversible ones explicitly.

**Always reversible.**

- **Alias updates.** `PUT /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/aliases/{alias}` swaps the version an alias points at atomically. A rollback is a second `PUT` against the previous version. Old versions are immutable and remain executable.
- **Deployment image rollback.** Operators pin the previous image digest in `deploy/helm/code-sous/values.yaml` and re-apply. The KVRocks records the previous image wrote are still readable by the previous image; double-write transitions guarantee data compatibility across one minor release.
- **Configuration rollback.** A YAML change rolls back by reverting the ConfigMap and restarting the affected deployment. No persisted state remembers the previous configuration.
- **Trigger updates.** A trigger update is itself a control-plane mutation; the prior state is in the audit ring buffer and can be replayed manually.

**Irreversible (or hard to reverse).**

- **Schema deletions.** Once a `cs-migrate` job scrubs a deprecated field, the field is gone. Operators run the job only after they have confirmed every consumer is on the release that no longer reads the field.
- **Signing-key rotation.** Rotating a tenant's signing key invalidates every signature produced by the old private key. Old signed versions verify against the new key only if it happens to produce them (a freshly generated keypair never does). Operators re-publish affected versions under the new key; the previous key cannot be recovered.
- **Bundle deletion.** Deleting a function or a version removes the bundle bytes from KVRocks. Recovery requires a backup restore.
- **DLQ drain.** Envelopes drained from a DLQ topic without acknowledgement are gone. Operators that need a re-attempt copy the envelopes off the DLQ before draining.

A rollback that crosses a major release is unsupported. Operators that need to roll back across a major restore from a backup taken before the major upgrade ran.

## Pre-release testing

Every release passes through three gates before it cuts.

The **parity harness** under `internal/runtime/parity/` runs every public runtime contract against `cs-js`, `cs-wasm`, and `cs-python`. A new release lands only when the parity matrix is green: the same input on the same manifest produces the same output and the same observable side effects across every runtime. The harness covers `cs.kv.*`, `cs.codeq.publish`, `cs.http.fetch`, `cs.env.get`, and the trigger envelopes.

The **contract tests** (introduced by E1.01) pin the wire shape of every cross-service exchange: REST API request/response pairs, codeQ envelopes, KVRocks record layouts. The tests refuse to compile when the shape changes without an accompanying schema bump.

The **integration tests** under `test/` exercise the full control-plane plus data-plane path against an in-process KVRocks and an in-process codeQ broker. The tests include a publish-invoke-rollback cycle that catches alias-swap regressions, a scheduler tick-over-leader-change cycle that catches lease-handling regressions, and a Cadence end-to-end cycle that catches poller regressions.

Operators preparing a custom build run the three suites in CI before any cluster picks up the new image:

```bash
go test ./internal/runtime/parity/...
go test ./internal/api/...
go test ./test/...
```

A clean run is a necessary condition for a deploy, not a sufficient one — the burn-rate alerts in production are the ultimate gate.

## Cross-references

- [Operators Configuration Reference](Operators-Configuration-Reference) — the YAML knobs whose defaults change between releases.
- [Operators Runbooks](Operators-Runbooks) — rollback procedures keyed off symptoms.
- [Operators Observability](Operators-Observability) — the signals that confirm a rollout did not regress an SLO.
- [Signing and SBOM](Managing-Functions-Signing-and-SBOM) — the canonical payload and key lifecycle behind irreversible key rotation.
- [codeQ Protocol](codeQ-Protocol) — the envelope schema versioning.
- `cmd/cs-migrate/` — the migration job that scrubs deprecated fields.
- `internal/runtime/parity/` — the parity harness.
