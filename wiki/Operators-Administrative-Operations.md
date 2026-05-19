# Operators: Administrative Operations

This page documents the routine operations that Sous operators run during the lifetime of a cluster. The procedures here are scheduled, planned changes — key rotations, credential rotations, sink migrations, topic rebalances, and backup/restore drills. Incident-specific procedures (a service is down, a queue is overflowing, a tenant is rate-limited unexpectedly) live in [Operators: Runbooks](Operators-Runbooks).

The operations below are written as runnable procedures with a short rationale, the steps in order, and the verification step that confirms success. Each procedure assumes the operator has admin-level access to the cluster, kubectl/helm tooling, and the credentials for any external system it touches.

## Signing key rotation

Tenant signing keys (Ed25519) sign published bundle versions. The publish handler in cs-control verifies the signature on every publish against the tenant's active public key (cited at cmd/cs-control/signing_keys.go). The private half exists only on the publisher's machine; the public half lives in KVRocks under `cs:tenant:{tenant}:signing-keys`. Rotation creates a new key pair, returns the new private key once, and overwrites the active slot.

When to rotate: scheduled rotation per the operator's key-lifecycle policy (typically annually), suspected private-key compromise, or before a publisher's machine changes hands.

**Procedure**:

1. Notify the publisher (the agent or human that signs bundles) that a rotation is imminent. They will need to switch to the new private key before their next publish.
2. Call the rotation endpoint as the tenant admin: `POST /v1/tenants/{tenant}/signing-keys/rotate`. The response body contains `kid`, `algorithm`, `public_key`, `private_key`, and `created_at_ms`. The `private_key` field is the only place the private bytes are ever exposed — capture it immediately.
3. Hand the new private key to the publisher via the operator's secret-distribution channel. Confirm the publisher persisted it.
4. Verify the active key has changed: `GET /v1/tenants/{tenant}/signing-keys/active`. The response should show the new `kid` and `created_at_ms`.
5. Re-sign existing published versions if your policy requires it. Sous's publish handler verifies the signature at publish time, not at invoke time, so previously-published versions remain invocable even after rotation. If your policy requires every active version to be signed by the current key, run the publisher's re-publish flow against each version (this allocates new version numbers and updates aliases accordingly).
6. Audit verification: `GET /v1/tenants/{tenant}/audit?action=tenant.signing_key.rotate` confirms a single `tenant.signing_key.rotate` event with the new `kid` in the detail map.

The rotation overwrites the active slot — there is no key-history surface in v0.1. If a publisher loses the new private key before persisting it, the operator must rotate again. The previously-rotated public key is overwritten and cannot be recovered.

## Tikti token rotation

Sous calls Tikti's introspection endpoint to authenticate every authenticated REST request. The service-account credential Sous uses is configured via `plugins.authn.tikti.api_key` in the YAML config. This credential is long-lived and rotated per the operator's identity-credential policy.

When to rotate: scheduled per Tikti policy, suspected compromise, or when the Tikti service account is reissued.

**Procedure**:

1. In Tikti, provision a new API key for the Sous service account. Capture the new key value.
2. Update `plugins.authn.tikti.api_key` in the deployment config. For Kubernetes, this means updating the `cs-config` ConfigMap (or the upstream Secret if the operator has refactored credential injection) and triggering a rollout of cs-control and cs-http-gateway: `kubectl rollout restart deployment/cs-control deployment/cs-http-gateway -n code-sous-system`.
3. Wait for the rollout to complete: `kubectl rollout status deployment/cs-control deployment/cs-http-gateway -n code-sous-system`.
4. Verify introspection succeeds: send a test request through cs-http-gateway with a valid bearer token and confirm it authenticates. The introspection cache may hold stale entries for up to `plugins.authn.tikti.cache_ttl_seconds` (default 60 seconds); cache entries are keyed by the user token, not the service key, so existing valid tokens continue to authenticate during the transition.
5. In Tikti, revoke the old API key. Sous should now reject any attempt to use the old key.

The cs-scheduler and cs-cadence-poller do not call Tikti — they stamp synthetic principals — so they do not need restart for this rotation. The cs-invoker-pool does not call Tikti either (the principal is delivered in the `InvocationRequest` envelope).

If your Sous deployment does not use Tikti (the `plugins.authn.driver` is set to `memory` or another driver), this procedure does not apply.

## Vault token / AppRole rotation

cs-invoker-pool resolves secrets from Vault at activation start. The credential Sous uses is either a long-lived token (`plugins.secrets.vault.token`) or an AppRole (`plugins.secrets.vault.role_id` + `secret_id`). Rotation depends on which credential type your Vault deployment requires.

When to rotate: scheduled per Vault token TTL or AppRole policy, suspected compromise, or before any change to the Vault auth method.

**Procedure (token)**:

1. In Vault, mint a new token with the required policy attached. Confirm the new token can read the paths Sous needs: `vault kv get secret/{tenant}/some-key`.
2. Update `plugins.secrets.vault.token` in the deployment config and roll out cs-invoker-pool: `kubectl rollout restart deployment/cs-invoker-pool -n code-sous-system`.
3. Wait for rollout: `kubectl rollout status deployment/cs-invoker-pool -n code-sous-system`.
4. Verify by invoking a function that resolves a secret. Confirm the function reads the expected value and that no `CS_SECRETS_*` errors appear in the invoker logs.
5. In Vault, revoke the old token: `vault token revoke <old-token>`.

**Procedure (AppRole)**:

1. In Vault, regenerate the secret_id for the existing role_id: `vault write -force auth/approle/role/sous/secret-id`. Capture the new secret_id.
2. Update `plugins.secrets.vault.secret_id` in the deployment config (the role_id remains stable). Roll out cs-invoker-pool.
3. Verify and revoke the previous secret_id: `vault write auth/approle/role/sous/secret-id-accessor/destroy accessor=<old-accessor>`.

AppRole is the recommended pattern for long-running services; token rotation is acceptable when the operator's tooling is centered on Vault tokens.

## Audit sink switching

The audit recorder writes every successful control-plane mutation to a configured sink (stdout, codeq, or webhook). Switching sinks is a common operation when migrating from a dev-grade stdout setup to a production codeq or webhook pipeline. The switch must be coordinated so no events are lost in flight.

When to switch: production cutover, sink-provider change (new ledgerDB endpoint, new SIEM), or migration between sink types.

**Procedure**:

1. Provision the destination sink. For codeq, ensure the `cs.audit.{tenant}` topics exist on the broker with appropriate retention. For webhook, provision the receiver and confirm it accepts and verifies the HMAC signature.
2. Drain in-flight audit events from the current sink. The recorder is asynchronous; before switching, pause control-plane mutations for a brief window. The recorder's history limit in KVRocks acts as a buffer — events written there are durable through the switch.
3. Update `plugins.audit.sink` in the config. Set `plugins.audit.topic_prefix` (codeq), `plugins.audit.webhook_url`, and `plugins.audit.hmac_secret` (webhook) as appropriate.
4. Roll out cs-control: `kubectl rollout restart deployment/cs-control -n code-sous-system`.
5. Resume control-plane mutations. Verify the new sink is receiving events: for codeq, consume from `cs.audit.{tenant}` and confirm events arrive; for webhook, check the receiver's logs.
6. If migrating from one durable sink to another, replay the events the old sink received during the drain window. The audit ring buffer in KVRocks holds the last `plugins.audit.history_limit` events per tenant and is reachable via `GET /v1/tenants/{tenant}/audit?since=<ms>`; export the events and post them to the new sink out-of-band.

The drain window depends on traffic. For most operators, a 30-second pause is sufficient; the recorder flushes to the sink within milliseconds of a mutation. The audit ring buffer in KVRocks is the safety net — even if the sink is unreachable during the switch, the events are durably stored and recoverable via the replay endpoint.

## Alias snapshots

Aliases (`prod`, `staging`, `canary`) point at concrete version numbers. Operators occasionally take a point-in-time snapshot of alias state to support rollback rehearsals or to capture a known-good configuration before a major change.

When to snapshot: before a major migration, before a deployment that touches many functions, or as part of a rollback rehearsal.

**Procedure**:

1. Enumerate the aliases for the target tenant and namespace: `GET /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/aliases` for each function. Capture the response.
2. Persist the captured state. A simple `aliases-{timestamp}.json` file per tenant suffices; some operators commit the snapshot to a version-controlled "alias state" repository.
3. To restore from snapshot: for each alias in the snapshot, call `PUT /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/aliases/{alias}` with the captured version number. Restoring is a per-alias write; the order does not matter because each alias is independent.

Sous does not provide a bulk alias-snapshot endpoint in v0.1. The procedure above is operator-side scripting against the existing REST surface. The audit trail records every alias change with `function.alias.set` events; the snapshot is a deliberate operator artifact independent of the audit history.

## KVRocks backup and restore

KVRocks is the source of truth. Backup is therefore the most consequential routine operation Sous operators run. KVRocks uses RocksDB under the hood; backups are RocksDB snapshots taken with `BACKUP` or via the underlying RocksDB `CreateBackup` API.

When to back up: on a schedule (hourly snapshots, daily exports), before major migrations, and before any cluster-wide upgrade.

**Procedure (snapshot)**:

1. Issue a snapshot command to the KVRocks instance: from a Redis CLI client, `BGSAVE` triggers an asynchronous background save to the configured backup directory. KVRocks acknowledges immediately and the snapshot completes in the background.
2. Verify the snapshot completed: `LASTSAVE` returns the Unix timestamp of the most recent successful save. The timestamp should be after the BGSAVE command.
3. Copy the snapshot files from the KVRocks data directory to durable backup storage (S3, GCS, on-prem object store). KVRocks writes snapshots to a configured backup path; consult the KVRocks documentation for the exact path on your image.
4. Verify the backup is readable: in a non-production sandbox, restore the snapshot and run a Sous service against it; confirm a handful of tenant reads return the expected data.

**Procedure (restore)**:

1. Stop all Sous services to prevent writes during the restore: `kubectl scale deployment/cs-control deployment/cs-http-gateway deployment/cs-invoker-pool deployment/cs-scheduler deployment/cs-cadence-poller --replicas=0 -n code-sous-system`.
2. Stop KVRocks: `kubectl scale statefulset/kvrocks --replicas=0 -n kvrocks-system` (or equivalent for your KVRocks deployment).
3. Replace the KVRocks data directory with the backup snapshot. The exact mechanism depends on how the data directory is mounted — typically a PVC, which the operator overwrites by mounting the backup to a one-shot job that copies it into place.
4. Restart KVRocks: scale the StatefulSet back to its original replica count and wait for it to report Ready.
5. Restart Sous services: scale each Deployment back to its original replica count and wait for `/readyz` to return 200.
6. Verify by listing functions and activations for a few tenants. Recent activations may be missing if the snapshot is older than the failure event.

The restore is destructive — every key written after the snapshot is lost. Operators should always document the snapshot timestamp and communicate the recovery point objective to tenants before performing a restore in production.

## codeQ topic rebalancing

codeQ topics (`cs.invoke`, `cs.results`, `cs.dlq.invoke`, `cs.dlq.results`, audit topics, subscription topics) are partitioned for parallel consumption. As tenant volume grows, the original partition count may become a bottleneck. Adding partitions is a routine scale-up operation; it is also the trickiest of the routine operations because Kafka's partition semantics constrain what is safe to do online.

When to rebalance: sustained consumer lag on `cs.invoke` or `cs.results` that does not respond to scaling cs-invoker-pool, observed partition skew where one partition is consistently hotter than others, or planned capacity expansion.

**Procedure (add partitions)**:

1. Confirm the new partition count is greater than the existing count. Kafka does not support reducing partition count online; reductions require a topic recreation, which is a separate, more invasive procedure.
2. Issue the partition increase: `rpk topic alter-config cs.invoke --set partitions=<N>` (Redpanda) or `kafka-topics.sh --alter --topic cs.invoke --partitions <N>` (Apache Kafka).
3. Restart cs-invoker-pool to trigger a consumer-group rebalance: `kubectl rollout restart deployment/cs-invoker-pool -n code-sous-system`. The new partitions become assigned to consumer instances as they reconnect.
4. Verify the rebalance: `rpk group describe cs-invoker-pool-{hostname}` should show the consumer group with assignments across all partitions including the new ones.

**Procedure (drain old topic, switch to new)**:

For changes that cannot be done as a partition-count alter — partition-count reduction, topic-key change, retention-policy change that requires recreation — the safe pattern is:

1. Create a new topic with the desired configuration: e.g., `cs.invoke.v2`.
2. Deploy a temporary consumer that reads from the old topic and republishes to the new topic. This preserves the in-flight backlog.
3. Update Sous's config to use the new topic: `plugins.messaging.codeq.topics.invoke=cs.invoke.v2`. Roll out cs-http-gateway, cs-scheduler, cs-cadence-poller (the publishers) first, so they start publishing to the new topic.
4. Wait for the old topic to drain: when `rpk group describe cs-invoker-pool-{hostname}` shows zero lag on the old topic, the drain is complete.
5. Roll out cs-invoker-pool to consume from the new topic. The temporary bridge consumer can be retired.
6. Delete the old topic.

The drain pattern is more involved but applies to any change that cannot be expressed as an online alter. For most rebalances, the partition-count alter is sufficient.

For incident-specific procedures (a consumer-group is stuck, a partition is hot, a tenant's traffic is spiking) see [Operators: Runbooks](Operators-Runbooks).
