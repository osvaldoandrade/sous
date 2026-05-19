# Operators: Deployment (Kubernetes)

The canonical Kubernetes packaging for Sous is the Helm chart at deploy/helm/code-sous. The chart renders one Deployment per Sous service, a single ConfigMap holding the YAML config, a per-service Service for in-cluster routing, PodDisruptionBudgets to protect availability during rolling updates, HorizontalPodAutoscalers for the two services that benefit from autoscaling, a NetworkPolicy scaffold, and a Namespace. The chart is intentionally minimal — it covers what every Sous deployment needs and leaves operator-specific concerns (Ingress controller choice, secret injection, image registry, persistent storage for KVRocks) to overlays or upstream charts.

This page walks through each rendered resource, the replica strategy and resource sizing per service, the probe and rolling-update configuration, and the operational concerns that ship outside the chart (image pinning, registry credentials, KVRocks and codeQ provisioning). It assumes the operator is familiar with Kubernetes objects at the level of `kubectl explain Deployment` and is comfortable rendering Helm charts with `helm template`.

The chart's metadata lives at deploy/helm/code-sous/Chart.yaml; the values surface at deploy/helm/code-sous/values.yaml; the templates render from deploy/helm/code-sous/templates/. The chart version (`0.1.0`) and the appVersion (`0.1.0`) are pinned in Chart.yaml and should be bumped on each operator release.

## Namespace

The chart creates one namespace (`code-sous-system` by default; configurable via `.Values.namespace`) and runs every platform pod in it. Tenants do not map to namespaces — tenancy is enforced inside the application by URL paths, principal checks, and KVRocks key prefixes (see [Operators: Architecture](Operators-Architecture)). The namespace exists for operational isolation: separate RBAC, separate quotas, separate NetworkPolicy.

The namespace resource is rendered by deploy/helm/code-sous/templates/namespace.yaml. Operators who manage namespaces through an external mechanism (cluster-admin tooling, GitOps namespace controller) can disable the chart's namespace creation by templating with `--namespace ... --create-namespace=false` and removing the namespace.yaml template, or by skipping the chart's namespace and applying their own.

## Deployments

The chart renders five Deployments, one per Sous service. The template at deploy/helm/code-sous/templates/deployments.yaml iterates over a list of `{name, port, replicas}` tuples and produces a Deployment + Service pair for each. All five share the same image repository (`.Values.image.repository`), the same tag (`.Values.image.tag`), the same pull policy (`.Values.image.pullPolicy`), and the same configuration mount (a ConfigMap named `cs-config` mounted at `/etc/code-sous`).

### cs-control

The control plane runs at 1-2 replicas. The recommendation is 2 for production (one warm follower so a rolling update or a single-pod failure does not interrupt the API surface) and 1 for dev/CI. The control plane's leader-election is not currently surfaced in the chart — cs-control's writes are coordinated by the underlying atomic operations in KVRocks, and the audit recorder runs in every replica. Operators who require strict single-writer semantics for the subscription runner (cmd/cs-control/subscription_runner.go) should run a single replica.

The default port is 8080 (`.Values.service.control`). The Deployment exposes a readiness probe at `GET /readyz` and a liveness probe at `GET /healthz`. Both probes are configured with default timing in the chart — operators can override via a strategic merge patch if they need tighter or looser bounds.

Resource sizing is the operator's responsibility. The chart's `.Values.resources` is empty by default. A reasonable starting point for cs-control is 250m CPU request, 256Mi memory request, 1 CPU limit, 512Mi memory limit. The control plane is low-throughput; this sizing should comfortably handle thousands of mutations per minute.

### cs-http-gateway

The gateway runs at 3 replicas by default and scales horizontally. It is the public-facing service: external load balancers point at the `cs-http-gateway` Service in the cluster. The default port is 8081.

The gateway's HPA (deploy/helm/code-sous/templates/hpa.yaml) targets 70% CPU utilization with min replicas equal to `.Values.replicas.httpGateway` (3) and max replicas of 30. CPU-only autoscaling is the v0.1 default; operators should layer a request-rate or latency-percentile metric for production workloads, since the gateway is often I/O bound on its codeQ wait loop. The chart does not configure custom metrics adapters; operators using KEDA or Prometheus Adapter wire those separately and patch the HPA.

Resource sizing for the gateway should account for the wait-for-result loop. Each in-flight invocation holds a goroutine and a small amount of memory; sizing is bounded by the product of `tenant_rps` and the function timeout. A reasonable starting point is 500m CPU request, 512Mi memory request, 2 CPU limit, 1Gi memory limit per replica, sustaining around 200 concurrent waits.

### cs-invoker-pool

The invoker runs at 10 replicas by default and is the workhorse of the data plane. It is the only service that runs user code, so its sizing dominates capacity planning. The default port is 8082 (used for `/healthz`, `/readyz`, and `/metrics`; the invoker does not serve external traffic).

The invoker's HPA targets 70% CPU utilization with min replicas equal to `.Values.replicas.invokerPool` (10) and max replicas of 50. CPU-only autoscaling is a reasonable v0.1 default for CPU-bound workloads; for I/O-bound functions, operators should add the secondary metric `cs_invoker_queue_lag_ms` exposed by the invoker's Prometheus endpoint. The chart leaves this metric unwired; operators add it via an HPA strategic merge patch once the metrics pipeline is in place.

Resource sizing for the invoker is workload-dependent. The invoker runs `cs_invoker_pool.workers.threads` goroutines per replica (default 32) and a per-replica max-inflight cap (default 2048). For cs-js workloads, a reasonable starting point is 1 CPU request, 1Gi memory request, 4 CPU limit, 4Gi memory limit per replica. For cs-python and cs-wasm workloads, memory limits should be at least doubled to accommodate the language runtime.

### cs-scheduler

The scheduler runs at 1-2 replicas. The chart's default is 2; with `cs_scheduler.leader_election.enabled=true` in the rendered config, one is the leader and the other is hot standby. The default port is 8083.

The scheduler is small, low-CPU, and low-memory. A starting point of 100m CPU request, 128Mi memory request, 500m CPU limit, 256Mi memory limit is sufficient for most fleets. The scheduler has no HPA — leader election makes scaling beyond 2 replicas useless, since followers do not contribute to throughput.

The scheduler's lease is held in KVRocks at key `cs:scheduler:leader:lease` with a 5-second TTL renewed every 2 seconds. On leader failure, the standby acquires the lease within 5 seconds and resumes ticking. The catch-up cap (`cs_scheduler.max_catchup_ticks`, default 60) bounds how many missed ticks fire per schedule on resumption.

### cs-cadence-poller

The poller runs at 2 replicas by default and scales N per binding. The default port is 8084.

Sizing the poller is per-binding rather than per-cluster. Each binding declares `pollers.activity` (the number of long-poll goroutines per replica) and `limits.max_inflight_tasks` (the in-flight cap per binding). Across N replicas, the effective concurrency is `replicas * pollers.activity` for long-polls and `replicas * max_inflight_tasks` for in-flight work. Operators with many bindings should plan accordingly; the poller's goroutine count grows linearly with both binding count and per-binding poller count.

Resource sizing for an activity-kind poller is light: 250m CPU request, 256Mi memory request, 1 CPU limit, 512Mi memory limit. Workflow-kind bindings (E8.01) execute workflow code inline on the decision loop and benefit from more CPU headroom; operators running workflow-heavy fleets should size for 1 CPU request and 4 CPU limit per replica.

The poller has no HPA in the chart. Cadence's long-poll model makes CPU a poor autoscaling signal; operators typically scale manually based on binding count and observed poll latency.

## Services

Every Deployment is paired with a ClusterIP Service of the same name (deploy/helm/code-sous/templates/deployments.yaml renders the Service inline). The Service exposes the same port the container listens on, making cross-service in-cluster traffic addressable by service name (e.g., `cs-control:8080`, `cs-invoker-pool:8082`). External traffic to cs-http-gateway flows through whatever Ingress or external LoadBalancer the operator wires in front of the `cs-http-gateway` Service; the chart itself does not render an Ingress object.

The Services are typed ClusterIP, which is the right choice for in-cluster traffic. Operators exposing cs-http-gateway externally either patch the Service to LoadBalancer or NodePort in their overlay or — more commonly — front the ClusterIP Service with an Ingress controller (nginx, Traefik, GCE, ALB) configured separately. Sous does not ship a default Ingress; the choice of controller and TLS termination strategy is operator-specific.

## ConfigMap and configuration

A single ConfigMap named `cs-config` (deploy/helm/code-sous/templates/configmap.yaml) holds the rendered `config.yaml` for every service. The content of this config is the `.Values.config` block in values.yaml — a YAML document that all five services parse at startup. Each service reads the keys it cares about (`cs_control`, `cs_http_gateway`, `cs_invoker_pool`, `cs_scheduler`, `cs_cadence_poller`) and ignores the rest, plus the shared `plugins` block that wires authentication, persistence, messaging, audit, and secrets.

The single-config-for-all-services pattern is intentional. It guarantees that every service reads the same `plugins.persistence.kvrocks.addr`, the same Tikti introspection URL, and the same codeQ topic names. Drift between services is the most common cause of opaque misconfiguration bugs in distributed platforms; co-locating the config eliminates this class of bug at the source.

Operators wanting per-service overrides can layer them by templating multiple ConfigMaps and mounting them at different paths. The chart in its current form does not support this; a fork or a patch overlay is required.

## Secrets

The chart does not currently render Kubernetes Secrets. Sensitive material — the Tikti API key, Vault token or AppRole, KVRocks auth password, codeQ producer/worker tokens — is injected by the operator through one of:

- **External secret manager**: External Secrets Operator, Sealed Secrets, or HashiCorp Vault Agent sidecar fetches the secret and presents it as a file or environment variable.
- **Static Secret object**: applied out-of-band and mounted as a volume or referenced via `envFrom` in a Deployment patch.

Sous reads secrets from the configuration YAML, which is mounted from the `cs-config` ConfigMap. Operators following the External Secrets pattern typically render the config with placeholders and use an init container or a templating sidecar to substitute the real values at boot. A future revision of the chart will support sourcing sensitive fields directly from a Secret reference; the v0.1 surface keeps it simple.

## PodDisruptionBudgets

Every service has a PodDisruptionBudget with `minAvailable: 1`, rendered by deploy/helm/code-sous/templates/pdb.yaml. This protects each service against simultaneous voluntary disruption (node drain, rolling update, eviction). The `minAvailable: 1` floor is the right default for production: it permits a single replica to be unavailable at a time, which suffices for rolling updates as long as `maxUnavailable` on the Deployment is also 1.

Operators running larger replica counts (e.g., 20 invoker replicas) can raise `minAvailable` to maintain higher availability during drains — for example, `minAvailable: "75%"` keeps at least 15 of 20 invokers available during a node drain. The chart's current default trades availability headroom for simplicity; operator overlays should tune per their reliability budget.

## HorizontalPodAutoscalers

The chart renders two HPAs (deploy/helm/code-sous/templates/hpa.yaml): one for cs-invoker-pool and one for cs-http-gateway. Both target 70% CPU utilization with the min replicas equal to the configured replica count and max replicas of 50 (invoker) and 30 (gateway). The remaining three services (cs-control, cs-scheduler, cs-cadence-poller) have no HPA because their workloads do not benefit from CPU-driven autoscaling — they are either coordination-bound (control, scheduler with leader election) or I/O-bound on long-poll RPC (cadence-poller).

Operators wanting custom metrics (queue lag, request rate, p99 latency) layer them through HPA v2's `external` or `pods` metric types, which requires a metrics adapter wired into the cluster (Prometheus Adapter, KEDA, or a cloud-provider equivalent). The chart's HPAs are a starting point; production tuning is operator-specific.

## NetworkPolicy

The chart renders a single NetworkPolicy (deploy/helm/code-sous/templates/networkpolicy.yaml) named `code-sous-default` with empty ingress and egress rules. In Kubernetes semantics, an empty rule (`- {}`) permits all traffic in that direction — so the chart's default is permissive by intent. This default keeps the chart self-contained for clusters without a NetworkPolicy provider; operators with Calico, Cilium, or equivalent should replace this template with tenant-appropriate restrictions.

A production-grade NetworkPolicy for Sous should:

- Allow ingress to cs-http-gateway from the Ingress controller's namespace only.
- Deny ingress to cs-control, cs-scheduler, cs-cadence-poller, and cs-invoker-pool from outside the platform namespace.
- Allow egress from every service to KVRocks (`kvrocks:6666` or its external equivalent) and codeQ (`codeq:80` or the broker's port).
- Allow egress from cs-control and cs-http-gateway to Tikti's introspection endpoint.
- Allow egress from cs-invoker-pool to Vault.
- Allow egress from cs-cadence-poller to the Cadence frontend.

The chart's empty NetworkPolicy is a placeholder; operators with security requirements layer a real policy on top.

## Image pinning and rollout

The image reference is composed at template time as `{repository}/{service-name}:{tag}` (see deploy/helm/code-sous/templates/deployments.yaml). The default repository is `ghcr.io/osvaldoandrade/sous`, and the default tag is `latest`. Production deployments must pin to a specific tag (a SemVer release, a commit SHA, or a content-addressable digest) rather than `latest`; `latest` defeats the immutability guarantees of Kubernetes' rolling update and makes rollback fragile.

The Deployment's update strategy defaults to RollingUpdate (Kubernetes default) with `maxSurge: 25%` and `maxUnavailable: 25%`. For cs-control with 2 replicas, this rounds to `maxSurge: 1, maxUnavailable: 1`, which permits one replacement pod to start before the old pod terminates. For cs-invoker-pool with 10 replicas, the defaults permit 3 unavailable and 3 surge during rollouts. Operators wanting tighter or looser bounds patch the Deployment's `spec.strategy.rollingUpdate` block.

Image pull credentials are injected via `imagePullSecrets` on the Deployment's spec, which the chart does not currently template. Operators using a private registry add a `imagePullSecrets` entry to the Deployments via overlay, or rely on a service account with `imagePullSecrets` attached.

## Probes

Every service exposes `/healthz` (liveness) and `/readyz` (readiness) on its main HTTP port. The chart configures both probes via HTTP GET against the container's port. The probes share the same path conventions across services:

- `/healthz` returns 200 OK if the process is alive. It does not check dependencies.
- `/readyz` returns 200 OK if the process can serve traffic — specifically, if the KVRocks Ping succeeds. The invoker's readyz also implicitly verifies the codeQ subscription by virtue of running its consumer goroutines.

Probe timing is left at Kubernetes defaults (initial delay 0, period 10s, timeout 1s, failure threshold 3, success threshold 1 for readiness). Operators tuning for faster failover should reduce the period and failure threshold; operators tuning for slower-starting workloads (large bundle caches at startup) should increase the initial delay.

## KVRocks and codeQ provisioning

The chart does not ship KVRocks or codeQ. These are platform dependencies that the operator provisions separately. Recommended approaches:

- **KVRocks**: deploy as a StatefulSet via the upstream Apache KVRocks chart or a hand-rolled manifest. KVRocks needs persistent storage (PVC), a single-leader replication topology, and a backup pipeline (RocksDB snapshots). See [KVRocks](Enabled-Services-KVRocks) for sizing and operational guidance.
- **codeQ**: deploy as a Strimzi-managed Kafka cluster, a Confluent Platform cluster, or a Redpanda Operator deployment. Sous uses only standard Kafka semantics; any Kafka-compatible broker works. See [codeQ Operations](Enabled-Services-codeQ) for topic provisioning and broker sizing.

Both dependencies should live in their own namespace (e.g., `kvrocks-system`, `codeq-system`) and expose Services that the Sous pods reach by DNS. The `.Values.config` block references them by hostname (`kvrocks:6666`, `codeq.default.svc.cluster.local:80` in the default values); operators adjust these to match their actual cluster topology.

## Observability

Every service exposes Prometheus metrics at `/metrics` on its main port. The chart does not ship a ServiceMonitor or PodMonitor — operators using the kube-prometheus stack layer those separately. The metric prefix is `cs_` for application metrics (e.g., `cs_invocations_total`, `cs_invoker_queue_lag_ms`, `cs_publish_total`); standard Go runtime metrics use the default `go_` and `process_` prefixes.

Structured logs are emitted to stdout in JSON form by every service. Kubernetes' default log collection (kubectl logs, fluentd/fluent-bit sidecars, cloud-provider log aggregators) captures these without further configuration.

Distributed traces are exposed via OpenTelemetry when the operator wires an exporter; Sous emits spans for HTTP requests, codeQ publishes/consumes, KVRocks operations, and user-code execution. The trace propagation header is the W3C `traceparent`, which the gateway forwards into the trigger envelope and the invoker continues onto outbound calls via the egress shim.

## Verifying a deployment

After `helm install` (or `helm upgrade`) succeeds, the operator should verify:

1. All five Deployments report Ready replicas equal to the configured count.
2. All five Services have endpoints (`kubectl get endpoints -n code-sous-system`).
3. `/readyz` returns 200 for every pod (`kubectl exec` or a probe-driven Ready status).
4. cs-control responds to a tenant-scoped read (`curl http://cs-control:8080/v1/tenants/{tenant}/...` from inside the cluster, with a valid bearer token).
5. cs-http-gateway responds to an invoke request for a published function.
6. cs-scheduler logs `tick` events at the configured cadence.
7. cs-cadence-poller logs `binding refresh` events.

The fastest end-to-end check is to publish a small function via cs-control and invoke it via cs-http-gateway; that single test exercises Tikti introspection, KVRocks read/write, codeQ publish/consume, and the invoker's full execution path.

## Configuration overlays

The chart in deploy/helm/code-sous accepts a single `--values` override file per `helm install` invocation. For more elaborate environment-specific customization, operators typically maintain one base values file (committed in version control alongside the chart) plus one per-environment overlay (dev, staging, prod). The overlay overrides only the fields that differ: replica counts, image tag, KVRocks address, codeQ broker list, Tikti URL, and any sensitive credential references.

A common pattern is to keep the cluster-level config in the overlay's `.Values.config` block and let the chart render the ConfigMap. Operators with sophisticated config management (Kustomize, ArgoCD with Helm post-renderer) layer additional transformations after Helm renders the chart — for example, substituting secret references into the ConfigMap, adding sidecar containers for secret-fetching, or patching the Deployment for cloud-specific node selectors.

## Image building

The Sous binaries are produced by `go build ./cmd/<service>` from the repository root. Each service is a single static binary with no native dependencies, which makes containerization straightforward — a multi-stage Dockerfile copies the binary into a minimal base image (`gcr.io/distroless/static-debian12` is the recommended base for the smallest attack surface). The chart references one image per service, all sharing the same `.Values.image.repository` prefix; the convention is `{repository}/cs-control:{tag}`, `{repository}/cs-invoker-pool:{tag}`, and so on.

A reproducible build pipeline pins the Go toolchain version, builds with `CGO_ENABLED=0`, and tags the resulting image with the commit SHA. The chart's `.Values.image.tag` should reference this SHA (or a SemVer that maps deterministically to a SHA via a release manifest). The chart does not currently support per-service tag overrides — all five services use the same tag — which simplifies the rollout story at the cost of forcing all-or-nothing version bumps.

## Storage requirements

The Sous services themselves are stateless and require no PersistentVolumes. Their state lives in KVRocks (see [KVRocks](Enabled-Services-KVRocks) for the StatefulSet pattern, PVC sizing, and snapshot procedure) and in codeQ topics (see [codeQ Operations](Enabled-Services-codeQ) for broker storage). The five Sous pods can be scheduled on any node and rescheduled freely; pod restarts and node drains are lossless as long as KVRocks and codeQ remain available.

The ConfigMap holding `cs-config` is read-only mounted; the chart does not write to the filesystem at runtime. The recommended `readOnlyRootFilesystem: true` security-context flag works for all five services without modification.

## Upgrading

Sous follows SemVer for the appVersion. A patch bump (0.1.0 → 0.1.1) is always backwards compatible: it does not change the KVRocks key schema, does not change the codeQ envelope shape, and does not change the REST API. Operators upgrade patch versions by bumping `.Values.image.tag` and running `helm upgrade`. Rolling update completes without service interruption thanks to the per-service PDB.

A minor bump (0.1.0 → 0.2.0) may add new fields to envelopes or new endpoints to the REST API but does not break existing clients. The release notes document any additive change. Upgrade procedure is the same as for patch versions.

A major bump (0.x → 1.0, or 1.x → 2.0) may include breaking changes. Read the release notes carefully, run the migration procedure (if one is documented), and stage the upgrade in non-production before applying to production. The migration may include a KVRocks key-schema change, a codeQ topic recreation, or an API surface change.

Downgrading from a patch or minor version is generally safe and follows the same procedure with an older tag. Downgrading across a major version is not supported without explicit operator action (e.g., restoring an older KVRocks snapshot).

## Tearing down

To tear down a Sous deployment, run `helm uninstall code-sous -n code-sous-system`. This removes all Sous-managed resources (Deployments, Services, ConfigMap, PDBs, HPAs, NetworkPolicy). The Namespace itself is removed only if the chart rendered it; if the operator created the namespace externally, the chart's uninstall leaves the namespace intact.

KVRocks and codeQ are not affected by the chart's uninstall — they are provisioned separately. To fully wipe a Sous deployment including state, the operator must also delete the KVRocks PVCs and codeQ topics. Production teardowns are rare; the more common case is a chart upgrade in place or a `helm uninstall && helm install` cycle preserving the underlying state.
