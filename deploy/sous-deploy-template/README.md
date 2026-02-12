# `sous-deploy` template (build from source dependency)

This folder contains a reference for `github.com/osvaldoandrade/sous-deploy`.

## Expected flow

1. Checkout `sous-deploy`.
2. Checkout `github.com/osvaldoandrade/sous` at `sous_ref`.
3. Build container images for each service from source.
4. Push images to your registry.
5. Run `helm upgrade --install` using the chart in `sous/deploy/helm/code-sous` and environment values from `sous-deploy`.

## Inputs and secrets

Workflow inputs:

- `sous_ref` (tag/branch/sha)
- `environment`

Required secrets:

- `REGISTRY_USERNAME`
- `REGISTRY_PASSWORD`
- `KUBECONFIG_B64`
