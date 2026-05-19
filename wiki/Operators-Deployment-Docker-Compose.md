# Operators: Deployment (Docker Compose)

The repository ships a docker-compose.yml at the root that brings up the local development dependency stack — KVRocks and a Kafka-compatible broker (Redpanda). The Sous services themselves (cs-control, cs-http-gateway, cs-scheduler, cs-cadence-poller, cs-invoker-pool) are not included in compose. Local development runs them as host processes built from `cmd/`, pointing at the compose-managed dependencies through `localhost`. This split keeps the dependency stack cheap to bring up while letting developers rebuild and restart Sous services with sub-second feedback.

The compose file is intentionally small. It exists to remove the friction of running KVRocks and Redpanda on a workstation — both are non-trivial to install natively, and both have well-curated container images. Everything else (Vault, Tikti, ledgerDB, Cadence) is mocked or omitted in dev; the unit tests stub these dependencies, and the Sous services run with the in-memory drivers (`plugins.persistence.driver=kvrocks` against the compose KVRocks, `plugins.messaging.driver=codeq` against Redpanda, `plugins.authn.driver=memory` for local auth, `plugins.secrets.driver=memory` for local secrets).

This page documents the compose file's services, how Sous host processes connect to them, and the operational troubleshooting checklist for the common failures: port conflicts, image pull failures, and stale-state symptoms.

## Services

The compose file defines two services. Cited at docker-compose.yml.

### kvrocks

The KVRocks service runs the official `apache/kvrocks:2.11.0` image and exposes port 6666 to the host. The container starts KVRocks with the bundled default configuration at `/etc/kvrocks/kvrocks.conf`, which enables the Redis wire protocol on 6666 and writes data to a container-local data directory.

Sous services configured for local dev point at `kvrocks://localhost:6666` (or `localhost:6666` depending on the driver). The KVRocks image accepts the standard Redis CLI as well, so a developer can debug state directly with `redis-cli -p 6666` once the container is up.

No volume mount is declared in the compose file. This means KVRocks state lives inside the container's writable layer and is destroyed when the container is removed (`docker compose down`). For most dev workflows this is correct — a fresh KVRocks per session is desirable. Developers who want persistence across sessions add a named volume:

```yaml
services:
  kvrocks:
    volumes:
      - kvrocks-data:/var/lib/kvrocks
volumes:
  kvrocks-data:
```

The path inside the container varies with the KVRocks image's default config; check `/etc/kvrocks/kvrocks.conf` in the running container if the data path matters.

### redpanda

The Redpanda service runs the `docker.redpanda.com/redpandadata/redpanda:v24.3.10` image with arguments that configure a single-broker cluster suitable for dev. Cited at docker-compose.yml. The key flags:

- `--smp=1` and `--memory=1G`: single CPU shard, 1 GiB heap. Sufficient for dev throughput; production clusters use Redpanda Operator or Strimzi Kafka with proper sizing.
- `--overprovisioned`: tells Redpanda to assume it shares the host with other processes, which disables some hot-loop optimizations and behaves better on a laptop.
- `--check=false`: skips Redpanda's startup self-check, which would otherwise fail on systems without huge pages or other production-only kernel features.
- `--kafka-addr=PLAINTEXT://0.0.0.0:9092`: listens on all interfaces on 9092.
- `--advertise-kafka-addr=PLAINTEXT://localhost:9092`: advertises localhost to clients, which is correct when Sous services run as host processes connecting via the published port.

Ports 9092 (Kafka) and 9644 (Redpanda admin) are published to the host. Sous services use the Kafka API on 9092; `9644` is exposed for the Redpanda admin CLI (`rpk cluster info`, `rpk topic list`, etc.) — useful for debugging.

Redpanda's Kafka API is wire-compatible with Kafka, so Sous's codeQ driver works against Redpanda unchanged. The driver reads the brokers list from `plugins.messaging.codeq.brokers` in the Sous config and connects directly; no separate codeQ service is required in dev.

## Running the Sous services on the host

Sous services are typically started as host processes during development:

```sh
./bin/cs-control --config config.yaml
./bin/cs-http-gateway --config config.yaml
./bin/cs-invoker-pool --config config.yaml
./bin/cs-scheduler --config config.yaml
./bin/cs-cadence-poller --config config.yaml
```

Each binary is produced by `go build ./cmd/<name>` against the repository root. The `--config` flag points at a YAML file that wires the services together. For a compose-based local stack, the config sets:

- `plugins.persistence.kvrocks.addr: localhost:6666`
- `plugins.messaging.codeq.brokers: [localhost:9092]` (or `codeq.base_url` for HTTP-mode codeQ)
- `plugins.authn.driver: memory` (no Tikti in dev)
- `plugins.secrets.driver: memory` (no Vault in dev)
- `plugins.audit.sink: stdout`

The repository's config.example.yaml is the starting point; copy it to config.yaml and adjust the addresses. A dev-friendly default points everything at `localhost` since the compose services publish ports to the host.

Why host processes rather than compose? Two reasons. First, iteration: changing a line in `cmd/cs-control/main.go`, running `go build`, and restarting the binary is faster than rebuilding a container image and recreating the compose service. Second, debugging: attaching a debugger or strace to a host process is straightforward; attaching to a container service requires extra setup. Production runs the services as container images (see [Operators: Deployment (Kubernetes)](Operators-Deployment-Kubernetes)); dev runs them as host processes against compose-managed dependencies.

## Health checks

The compose file does not declare healthchecks. Both KVRocks and Redpanda start in under a couple of seconds on modern hardware; the typical dev workflow is to run `docker compose up -d`, wait briefly, and start the Sous services. For automated workflows (CI, scripted bootstraps), operators wrap the compose-up with a wait loop:

```sh
docker compose up -d
until redis-cli -p 6666 ping >/dev/null 2>&1; do sleep 0.5; done
until nc -z localhost 9092; do sleep 0.5; done
```

The first loop waits for KVRocks to accept the Redis wire protocol. The second waits for Redpanda's Kafka listener to bind. Once both succeed, the Sous services can start safely.

A future revision of the compose file will declare healthchecks for both services so `docker compose up --wait` blocks until they are ready. For now, the wait-loop pattern above is the recommended approach.

## Persistence and volumes

The compose file declares no named volumes. KVRocks state and Redpanda state live in the containers' writable layers and disappear on `docker compose down`. This is correct for most dev workflows — a fresh state per session matches the unit-test contract and prevents accumulation of stale data.

Developers who need cross-session persistence (e.g., to demo a long-running scenario) declare named volumes:

```yaml
services:
  kvrocks:
    volumes:
      - kvrocks-data:/var/lib/kvrocks
  redpanda:
    volumes:
      - redpanda-data:/var/lib/redpanda/data
volumes:
  kvrocks-data:
  redpanda-data:
```

After adding these, `docker compose down` preserves the volumes and `docker compose up -d` reuses them. `docker compose down -v` deletes the volumes for a clean reset.

## Troubleshooting

Three classes of problems are common in the local compose stack.

**Port conflicts**: if another process on the host is using 6666, 9092, or 9644, compose fails to bind. The fix is either to stop the conflicting process or to remap the port:

```yaml
services:
  kvrocks:
    ports:
      - "16666:6666"
```

After remapping, update the Sous config to point at the new host port (e.g., `localhost:16666`). The most common conflict is a previous compose run that did not clean up: `docker ps` reveals stale containers, and `docker compose down` cleans them.

**Image pull failures**: both images are pulled from public registries, but air-gapped or restricted-network workstations may need a proxy or a mirror. Setting `HTTP_PROXY` and `HTTPS_PROXY` in the Docker daemon configuration is the usual fix; alternatively, push the images to an internal registry and patch the compose file to reference the internal tags. The KVRocks image tag is pinned at `2.11.0` and the Redpanda image tag is pinned at `v24.3.10`; both should be fetched once and cached locally.

**Fresh-state reset**: when development state gets confusing (corrupted activations, stale schedules, leftover idempotency reservations), the cleanest fix is to reset both dependencies. `docker compose down` (without `-v` if volumes are declared, or by default if not) tears down the containers; `docker compose up -d` brings them back fresh. KVRocks reads as empty, Redpanda's topics are recreated by Sous on first publish, and the next test run starts from zero.

**Stale Redpanda topics**: if a Sous service crashed while consuming and left a consumer-group offset behind, Redpanda will continue serving from that offset on the next run. The symptom is "I published a message but the invoker never sees it." The fix is `rpk group delete <group-id>` against `localhost:9644`, or — more bluntly — `docker compose down -v && docker compose up -d` to wipe everything.

**KVRocks connection refused**: if the Sous service logs `connection refused` against `localhost:6666`, KVRocks did not bind. Check `docker compose logs kvrocks` for a startup error; the most common cause is a corrupted data directory after a hard kill. `docker compose down -v && docker compose up -d` resets it.

For most dev sessions, the workflow is: `docker compose up -d`, run the Sous services, develop, `docker compose down` when finished. The compose stack is small enough that bringing it up and down has negligible cost.
