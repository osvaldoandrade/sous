# Enabled Services: codeQ

This page documents codeQ as an operational dependency of Sous: the broker
that carries InvocationRequest and InvocationResult envelopes between the
gateway, the invoker pool, and the auxiliary pollers. Where the companion
page [Event Sources: codeQ Topics](Event-Sources-codeQ-Topics) describes the
on-the-wire protocol — which topics exist, what messages flow on them, and
how `request_id` and `activation_id` correlate one end to the other — this
page treats codeQ as a deployable component. It answers the operator's
questions: what binary actually runs, how to configure Sous to reach it,
how to size its topics and consumer groups, what to watch, and what to do
when it breaks.

In v0.1 codeQ is realized by Redpanda. Redpanda speaks the Kafka wire
protocol but ships as a single binary with no ZooKeeper or Kraft quorum
service to operate separately, which keeps the local-dev story to a single
container and keeps the production story to a single managed or
self-hosted broker fleet. Sous never imports a Redpanda-specific client;
the driver in `internal/codeq/codeq.go` uses `segmentio/kafka-go` against
the broker's Kafka listener, so every claim on this page applies equally
to a Kafka cluster pretending to be codeQ.

The contract that the rest of Sous depends on is narrow. Producers call
`Publish`, `PublishInvocation`, `PublishResult`, and the DLQ variants;
consumers join a group and call `ConsumeInvocations`, `ConsumeResults`, or
`ConsumeTopic`; the synchronous HTTP gateway uses `WaitForResult`, which
mounts a private consumer group on `cs.results` for the lifetime of one
request. Anything codeQ-compatible that exposes those primitives is a
valid backing service.

## Deployment options

Sous supports three deployment shapes for codeQ.

**Local development.** The repository's `docker-compose.yml` provisions
Redpanda alongside KVRocks. The service starts in single-node mode with
`--smp=1 --memory=1G --overprovisioned`, advertises the Kafka listener at
`localhost:9092`, and exposes the admin API on `9644`. This is the
canonical setup for `make e2e` and for hand-running services with the
defaults in `config.example.yaml`. Topics auto-create on first publish in
this mode, which keeps the developer onboarding flow short but produces
single-partition topics that are useless for performance work.

**Self-hosted Redpanda or Kafka.** Operators who want full control run
their own broker cluster — typically a Redpanda Helm release on Kubernetes
or a Kafka StatefulSet wired through Strimzi. Sous treats the two
interchangeably as long as the Kafka protocol level matches what
`segmentio/kafka-go` advertises (see `go.mod` for the exact version).

**Managed Kafka-compatible service.** In hosted environments, operators
point Sous at a managed broker — AWS MSK, Confluent Cloud, Aiven for
Kafka, Redpanda Cloud, or equivalent. The Sous side is identical; only
the connection string and credential plumbing change. Managed services
are the recommended production posture for v0.1 because they remove the
broker-operations burden from the Sous SRE team.

## Connection configuration

Every Sous service that talks to codeQ reads its connection settings from
the `plugins.messaging.codeq` block of `config.example.yaml`. The driver
is selected by `plugins.messaging.driver`; when set to `codeq` and
`brokers` is populated, the driver in
`internal/plugins/messaging/codeq/codeq.go` constructs a Kafka client. An
alternate `base_url` setting selects the HTTP transport
(`http_provider.go`); operators who want raw Kafka leave `base_url`
empty.

```yaml
plugins:
  messaging:
    driver: codeq
    codeq:
      # Leave base_url empty to use the Kafka-compatible transport.
      base_url: ""
      brokers:
        - redpanda-0.broker.svc:9092
        - redpanda-1.broker.svc:9092
        - redpanda-2.broker.svc:9092
      topics:
        invoke: cs.invoke
        results: cs.results
        dlq_invoke: cs.dlq.invoke
        dlq_results: cs.dlq.results
```

The `brokers` list is the standard Kafka bootstrap-server list. Sous
deliberately keeps the schema small in v0.1: SASL credentials, TLS
material, and explicit `client_id` overrides are not yet exposed in
`internal/config/config.go`. Operators who deploy against a managed
service that requires those features set them through the environment of
the broker library — see [Deployment: Kubernetes](Deployment-Kubernetes)
for the recommended Secret-and-env approach — until a dedicated YAML
block lands. The intent is that the schema grows toward an explicit
`sasl`, `tls`, and `client_id` section without breaking the
`brokers` / `topics` shape already in place.

The four topic names are operator-visible knobs. The defaults
(`cs.invoke`, `cs.results`, `cs.dlq.invoke`, `cs.dlq.results`) match
every code path in the repository; renaming them is supported but
unusual. Setting `dlq_invoke` or `dlq_results` to the empty string
disables that DLQ surface — `PublishDLQInvoke` and `PublishDLQResult`
return `nil` without writing anything when the topic is unset, which is
the right behaviour for tests but not for production.

## Topic creation

In development, Redpanda auto-creates topics with one partition and the
broker default retention on first publish. This is sufficient for `make
e2e` and for the worked examples in [Get Started](Get-Started), and it is
what lets a fresh developer clone the repo and see green tests without
touching the broker.

In production, operators pre-create topics with explicit partition counts
and retention. The recommended posture:

- `cs.invoke` — partitions sized for peak invocation RPS divided by the
  per-replica throughput target; replication factor at least 3 in
  multi-broker clusters.
- `cs.results` — partitions sized like `cs.invoke`; result throughput
  matches invocation throughput one-for-one because the invoker writes
  exactly one result per activation.
- `cs.dlq.invoke` and `cs.dlq.results` — one partition each, replication
  factor at least 3, retention measured in weeks. DLQ topics are
  low-volume in steady state and are read by humans, not by hot
  consumers, so partition parallelism is wasted.

Creating these topics through `rpk topic create` or the equivalent Kafka
admin tool is part of the cluster-bring-up runbook; Sous does not
implement an "ensure topics" step inside the services because doing so
would require admin credentials on the producer path.

## Partitioning strategy

The producer in `internal/codeq/codeq.go` keys every message by the
tenant id (`kafka.Message{Key: []byte(tenant), ...}`). This sends all
traffic for a single tenant to the same partition and therefore preserves
per-tenant ordering on each topic. It does not preserve per-activation
ordering across producers, because the only per-activation guarantee
needed by the protocol is that the invoker writes its single
InvocationResult after it consumes the InvocationRequest — a guarantee
the activation lifecycle provides on its own.

The trade-off is the standard Kafka one: a single hot tenant cannot
parallelise across partitions of `cs.invoke`. Operators who run with a
small number of very large tenants should monitor per-partition lag (see
"Observability" below) and either increase the partition count or move
the hot tenant onto a dedicated topic via the YAML override.

Producers across all Sous services use the same partitioner — the
`kafka.LeastBytes` balancer wrapped around the tenant key — so messages
written by `cs-http-gateway`, `cs-scheduler`, `cs-cadence-poller`, and
`cs-control` (subscription consumer republish) land on the same partition
for the same tenant. Operators who add a non-Sous producer to the same
topics must match the keying scheme to keep ordering intact.

## Retention

`cs.invoke` and `cs.results` are working-set topics. A message on
`cs.invoke` is consumed within milliseconds of being written under
healthy conditions, and a message on `cs.results` is consumed by the
gateway's `WaitForResult` group within the request timeout. Recommended
retention is short — hours to one day — which keeps disk usage bounded
and limits the blast radius of a replay during incident response.

`cs.dlq.invoke` and `cs.dlq.results` are forensic topics. Messages land
there only when the invoker has exhausted its retry policy or when a
result fails correlation. Recommended retention is long — weeks to
months — because the value of a DLQ message is precisely that an
operator can read it days after the fact and reconstruct what happened.
The audit and incident-response procedures in [Runbooks](Runbooks)
assume DLQ messages survive at least the post-incident review window.

Retention is set at topic-creation time and changed in place via
`rpk topic alter-config`. Sous does not read or enforce a retention value
itself.

## Consumer groups

Sous uses several stable consumer-group names. The mapping today, as
written in the source:

- `cs-invoker-pool-<hostname>` — one group per invoker replica, set in
  `cmd/cs-invoker-pool/main.go`. Each replica's threads share the
  group and divide `cs.invoke` partitions among themselves. Distinct
  replicas, by contrast, hold distinct groups and therefore each receive
  the full stream; the v0.1 invoker pool relies on this to keep work
  visible to every replica until the dispatch layer applies its own
  inflight and tenant caps. This is a deliberate simplification that
  will tighten in a future epic.
- `cs-cadence-poller-results` and `cs-cadence-poller-heartbeats` — the
  Cadence poller's two stable groups on `cs.results`, set in
  `cmd/cs-cadence-poller/main.go`. They consume independently of the
  invoker.
- `cs-http-gateway-wait-<uuid>` — an ephemeral group created per
  synchronous HTTP request in `WaitForResult`. Each request mounts its
  own group, scans for the matching `request_id`, and tears the group
  down on completion. This wastes broker bookkeeping at scale and is one
  of the known follow-ups for hardening the HTTP path.
- `cs-sub-<tenant>-<namespace>-<name>` — derived by cs-control for
  subscription bindings when the operator omits an explicit `GroupID`
  on the binding (see `internal/codeq/subscription.go` and
  `cmd/cs-control/subscriptions.go`). Each binding gets a distinct
  group so two bindings on the same source topic do not steal messages
  from each other.

Each group commits offsets through the standard Kafka group protocol;
Sous does not maintain its own offset store.

## Scaling and rebalancing

Within a single invoker-pool replica, scaling is a configuration knob:
`cs_invoker_pool.workers.threads` in `config.example.yaml` controls the
number of consumer goroutines that share the per-host group. Increasing
threads beyond the partition count of `cs.invoke` adds idle consumers but
does not increase throughput, because Kafka assigns at most one
consumer per partition per group.

Across replicas, the v0.1 group-naming choice (one group per hostname)
means horizontal scaling does not partition the work; each replica sees
the full stream and is gated by its own inflight caps. The practical
implication for operators is that adding replicas increases total
capacity but not pure parallelism on a single hot tenant — the tenant's
messages all land on one partition because of the tenant-keyed
partitioner, and every replica reads that partition. Capacity planning
should treat the invoker pool as a fleet whose ceiling is the product of
per-replica `max_inflight` and replica count, with partition count of
`cs.invoke` chosen to keep per-partition lag bounded under that fleet's
target load.

Rebalances happen on the standard Kafka triggers: a thread joining or
leaving its group, a broker becoming the new group coordinator, or a
session timing out. During a rebalance, `FetchMessage` blocks; Sous
tolerates the pause by virtue of the at-least-once delivery and dedup
machinery in `internal/codeq/dedup.go`, which collapses any messages the
broker redelivers across the rebalance boundary.

## Observability

The metrics emitted by Sous (see [Observability](Observability)) cover
the producer and consumer paths but not the broker itself. Operators
should layer broker-side metrics onto the same dashboards. The minimum
set:

- per-topic produce and consume rate, segmented at least by `cs.invoke`
  and `cs.results`;
- consumer-group lag for every group named in the previous section,
  with `cs-invoker-pool-*` lag treated as the leading indicator of
  invoker-pool saturation;
- in-sync replica (ISR) shrinks and under-replicated partitions on
  multi-broker clusters — these correlate with broker-host issues that
  precede an outage;
- broker availability and disk usage on self-hosted clusters; managed
  services expose equivalent metrics through their own dashboards.

Sous-side counterparts that pair with the broker view:
`cs_invoker_queue_lag_ms{topic="cs.invoke"}` (Sous's own lag estimate
from envelope `ts_ms` to consumption time) and the runtime metrics on
`cs-invocations-total`. When Sous's lag metric and the broker's
consumer-group lag disagree, one of them is broken; both pointing at the
same lag is the expected steady state. See
[Operators: Observability](Operators-Observability) for the full
dashboard recipe.

## Operational runbooks

Two failure modes are common enough to call out here; the full set lives
in [Operators: Runbooks](Operators-Runbooks).

**Broker outage.** When the broker is unreachable, every Sous producer
fails fast: the publish path wraps the broker error as
`CS_CODEQ_PUBLISH_FAILED` (see `internal/errors/errors.go`). The HTTP
gateway surfaces this as a 5xx response on synchronous invokes; the
scheduler and Cadence poller surface it through their own retry paths.
Sous does not buffer unsent messages, which makes the outage symptom
loud and short rather than quiet and long. Recovery is to restore
broker quorum and let the publishers retry naturally — no manual replay
step is required because the protocol assumes nothing was written.

**DLQ accumulation.** A non-zero, growing depth on `cs.dlq.invoke` or
`cs.dlq.results` means the invoker is exhausting its retry policy on
some class of messages. Alert at a configurable depth threshold; the
investigation procedure is to drain a sample of the DLQ topic with `rpk
topic consume cs.dlq.invoke --num 50` (or the Kafka equivalent),
classify the failures by `error.type` in the payload, and either patch
the offending function version, raise the retry policy, or accept the
drop. The DLQ topic is never auto-drained by Sous; operators reset
offsets explicitly when they have decided the backlog is processed.

The other runbooks for codeQ — topic rebalancing, broker version
upgrades, credential rotation — follow the broker product's own
documentation. Sous adds no Sous-specific procedure to those flows.

## Alternative drivers

The `messaging.Provider` interface in
`internal/plugins/messaging/messaging.go` is the seam where alternate
transports plug in. Today the registry in
`internal/plugins/registry` knows about a single driver, `codeq`,
implemented both as a Kafka client and as an HTTP shim
(`http_provider.go`) that targets a hosted codeQ control plane. The
HTTP shim is the path operators take when they want to outsource the
broker entirely; it keeps the same `Publish` / `Consume` surface but
moves the message store behind an HTTPS endpoint.

A future in-memory driver is the obvious next addition. It would
register under a new name (`memory` or `inproc`), share no code with
the Kafka path, and exist primarily to remove the Redpanda container
from single-node test deployments and to speed up the unit-test suite.
Because every Sous service depends on `messaging.Provider` rather than
on `*codeq.Kafka`, adding such a driver is a single-package change with
no fan-out into the rest of the codebase.
