# Deployment on Kubernetes

This file defines the Kubernetes objects for `code-sous`.

## Namespaces

The platform uses:

- `code-sous-system` for platform pods

Tenants map to logical isolation in Tikti and in KVRocks keys.

## Deployments

### cs-control

- replicas: 2
- service: ClusterIP
- ports: 8080

### cs-http-gateway

- replicas: 3
- service: ClusterIP
- ports: 8081

### cs-invoker-pool

- replicas: 10
- service: ClusterIP
- ports: 8082

### cs-scheduler

- replicas: 2
- leader election: enabled

### cs-cadence-poller

- replicas: 2
- per WorkerBinding pollers: configured in binding

## ConfigMaps and Secrets

ConfigMap:

- `cs-config` stores YAML config for services.

Secret:

- `cs-secrets` stores Tikti service tokens.
- `cs-secrets` stores KVRocks auth if enabled.
- `cs-secrets` stores codeQ auth if enabled.

## Probes

All services expose:

- `/healthz`
- `/readyz`

## Autoscaling

HPA targets:

- `cs-invoker-pool` scales on:
  - CPU
  - `cs_invoker_queue_lag_ms`

- `cs-http-gateway` scales on:
  - CPU
  - request rate

## Pod disruption

PDB:

- minAvailable: 1 for each service

## Network policy

The cluster applies policies:

- only gateways accept ingress from outside
- invokers can reach KVRocks and codeQ only
- pollers can reach code-flow, KVRocks, codeQ
