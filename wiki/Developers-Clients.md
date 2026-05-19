# Developers: Clients

SOUS does not ship language-specific SDKs in v0.1. A search of the repository under `cmd/`, `internal/`, and the root tree turns up no `sdk/` or `clients/` directory, and the only client surface that lives outside the Go monorepo is the npm wrapper at `npm/cs/`, which is not a programmatic SDK at all but a thin installer for the prebuilt Go CLI binary. There is therefore no `@sous/client`, no `sous-python`, and no `sous-go` library — and there is no plan to publish one until tenants demonstrate concrete demand.

The two supported integration paths are deliberately narrow. The first is the `cs` CLI binary, written in Go (see `cmd/cs-cli`), distributed either by the shell installer at `install.sh` (`curl … | sh`) or by `npm install -g @osvaldoandrade/cs`. The CLI handles authentication against Tikti, local development, draft upload, publish, alias management, and invoke. The second path is raw HTTP against the documented REST surface in [REST API](REST-API), authenticated with a Tikti bearer token. Every operation the CLI performs is a thin wrapper over those HTTP calls, which means any HTTP client in any language is sufficient.

The decision to skip SDKs in v0.1 is intentional. The control-plane surface is small and stable — fewer than twenty endpoints, all JSON over HTTPS — and the synchronous invoke envelope is fixed in `spec/cs.invoke.v1.json` and `spec/cs.results.v1.json`. The CLI is the ergonomic path for humans and agents; the REST API is the integration path for services. Generating idiomatic SDKs in three or four languages and keeping them in sync with the control plane is not a fight worth picking before there is a steady stream of clients asking for one. When that demand materializes, the schemas under `spec/` are designed to drive code generation rather than hand-written wrappers.

## Calling SOUS from any language

Every endpoint accepts `application/json` and returns `application/json`. Authentication is `Authorization: Bearer <tikti_token>` on every call (see [IAM with Tikti](IAM-with-Tikti)). The base URL is the host running `cmd/cs-control` for lifecycle operations and the host running `cmd/cs-http-gateway` for HTTP invokes; in many deployments both ride on the same fronting LB under different path prefixes.

Publish a new version of a previously-uploaded draft:

```bash
curl -sS -X POST "https://control.example.com/v1/tenants/t_abc123/namespaces/payments/functions/reconcile/versions" \
  -H "Authorization: Bearer $TIKTI_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "draft_id": "drf_01H...",
    "config": {
      "timeout_ms": 3000,
      "memory_mb": 64,
      "max_concurrency": 1,
      "authz": { "invoke_http_roles": ["role:app"] }
    }
  }'
```

Invoke a function synchronously through the API path:

```bash
curl -sS -X POST "https://control.example.com/v1/tenants/t_abc123/namespaces/payments/functions/reconcile:invoke" \
  -H "Authorization: Bearer $TIKTI_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "ref":  { "alias": "prod" },
    "mode": "sync",
    "event": { "order_id": "o_42" }
  }'
```

Invoke through the HTTP gateway, where the URL path itself routes to the function and the request body becomes the event payload:

```bash
curl -sS -X POST "https://gateway.example.com/v1/web/t_abc123/payments/reconcile/prod" \
  -H "Authorization: Bearer $TIKTI_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{ "order_id": "o_42" }'
```

Read an activation record by id:

```bash
curl -sS "https://control.example.com/v1/tenants/t_abc123/activations/$ACTIVATION_ID" \
  -H "Authorization: Bearer $TIKTI_TOKEN"
```

These four shapes — publish, sync invoke (API), HTTP invoke (gateway), and activation read — are the working set for almost every integration.

## Node.js

Node 18+ ships a global `fetch`, so no dependency is required. The example below publishes a draft and then invokes the resulting version through the HTTP gateway.

```javascript
const CONTROL = process.env.SOUS_CONTROL_URL;
const GATEWAY = process.env.SOUS_GATEWAY_URL;
const TOKEN   = process.env.TIKTI_TOKEN;

const headers = {
  "Authorization": `Bearer ${TOKEN}`,
  "Content-Type":  "application/json",
};

async function publish(tenant, ns, fn, draftId) {
  const res = await fetch(
    `${CONTROL}/v1/tenants/${tenant}/namespaces/${ns}/functions/${fn}/versions`,
    {
      method: "POST",
      headers,
      body: JSON.stringify({
        draft_id: draftId,
        config: { timeout_ms: 3000, memory_mb: 64 },
      }),
    },
  );
  if (!res.ok) throw new Error(`publish failed: ${res.status} ${await res.text()}`);
  return res.json();
}

async function invokeHTTP(tenant, ns, fn, ref, payload) {
  const res = await fetch(
    `${GATEWAY}/v1/web/${tenant}/${ns}/${fn}/${ref}`,
    { method: "POST", headers, body: JSON.stringify(payload) },
  );
  // A 5xx is infrastructure; a 2xx with an error envelope is function-returned.
  if (res.status >= 500) throw new Error(`gateway error: ${res.status}`);
  return res.json();
}

const { version } = await publish("t_abc123", "payments", "reconcile", "drf_01H...");
const result = await invokeHTTP("t_abc123", "payments", "reconcile", version, { order_id: "o_42" });
console.log(result);
```

## Python

Python uses `requests`. The same two operations look like this:

```python
import os
import requests

CONTROL = os.environ["SOUS_CONTROL_URL"]
GATEWAY = os.environ["SOUS_GATEWAY_URL"]
TOKEN   = os.environ["TIKTI_TOKEN"]

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type":  "application/json",
}

def publish(tenant: str, ns: str, fn: str, draft_id: str) -> dict:
    url = f"{CONTROL}/v1/tenants/{tenant}/namespaces/{ns}/functions/{fn}/versions"
    body = {
        "draft_id": draft_id,
        "config":   {"timeout_ms": 3000, "memory_mb": 64},
    }
    r = requests.post(url, json=body, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()

def invoke_http(tenant: str, ns: str, fn: str, ref: str, event: dict) -> dict:
    url = f"{GATEWAY}/v1/web/{tenant}/{ns}/{fn}/{ref}"
    r = requests.post(url, json=event, headers=HEADERS, timeout=10)
    # 5xx is infrastructure; 2xx may still encode a function-returned error.
    if r.status_code >= 500:
        r.raise_for_status()
    return r.json()

if __name__ == "__main__":
    v = publish("t_abc123", "payments", "reconcile", "drf_01H...")
    out = invoke_http("t_abc123", "payments", "reconcile", str(v["version"]), {"order_id": "o_42"})
    print(out)
```

## Go

Go services typically already speak `net/http`; the call pattern is the same.

```go
package soushttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	Control string
	Gateway string
	Token   string
	HTTP    *http.Client
}

func New(control, gateway, token string) *Client {
	return &Client{
		Control: control,
		Gateway: gateway,
		Token:   token,
		HTTP:    &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *Client) do(ctx context.Context, method, url string, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")

	res, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode >= 500 {
		raw, _ := io.ReadAll(res.Body)
		return fmt.Errorf("sous infra error: %d %s", res.StatusCode, string(raw))
	}
	if out != nil {
		return json.NewDecoder(res.Body).Decode(out)
	}
	return nil
}

func (c *Client) Publish(ctx context.Context, tenant, ns, fn, draftID string) (map[string]any, error) {
	url := fmt.Sprintf("%s/v1/tenants/%s/namespaces/%s/functions/%s/versions", c.Control, tenant, ns, fn)
	in := map[string]any{
		"draft_id": draftID,
		"config":   map[string]any{"timeout_ms": 3000, "memory_mb": 64},
	}
	out := map[string]any{}
	if err := c.do(ctx, http.MethodPost, url, in, &out); err != nil {
		return nil, err
	}
	return out, nil
}
```

The 5xx-versus-2xx distinction is load-bearing here; see "Result-failure vs internal-error" below.

## The npm wrapper

The package `@osvaldoandrade/cs` (source at `npm/cs/`) is not an SDK. It is a 200-line postinstall script (`npm/cs/scripts/postinstall.js`) that downloads the prebuilt Go CLI binary from the matching GitHub Release and drops it under `bin/cs-bin` in the package directory. The `bin` entry in `npm/cs/package.json` then makes `cs` resolvable on the user's `PATH`.

Install globally:

```bash
npm install -g @osvaldoandrade/cs
cs --help
```

Pin to a specific version (recommended for CI):

```bash
npm install -g @osvaldoandrade/cs@0.3.1
```

The version in `npm/cs/package.json` is the source of truth: the postinstall script reads it, builds the GitHub Release tag as `v<version>`, and downloads `cs-<goos>-<goarch>[.exe]` along with `SHA256SUMS.txt`. The wrapper supports `linux`, `darwin`, and `windows` on `amd64` and `arm64`; any other host fails the postinstall step with a clear error. Two environment variables override the download source for forks or mirrors:

- `SOUS_GH_OWNER` (default `osvaldoandrade`)
- `SOUS_GH_REPO`  (default `sous`)

If the release repository is private, set `GITHUB_TOKEN` or `GH_TOKEN` so the postinstall download can authenticate against the GitHub API.

Common troubleshooting:

- `unsupported platform` or `unsupported arch` — the host is not in the matrix; fall back to building from source with `make build` in the monorepo, or use the shell installer `curl … | sh` from `install.sh`.
- `HTTP 404 for …/cs-<goos>-<goarch>` — the version pinned in `package.json` does not have a published release asset for the host's OS/arch combination. Pin a known-good version.
- `sha256 mismatch` — the download stream was corrupted or the release was retagged. Reinstall; if the error persists, the release is compromised and should be reported.

Installs that do not need the binary at all (for example, npm projects that pull `@osvaldoandrade/cs` only into a dev dependency tree on CI runners where the CLI will not actually run) can set `npm_config_ignore_scripts=true` to skip the postinstall.

## Authentication

Every HTTP call carries a Tikti bearer token in the `Authorization` header. The CLI obtains and stores its token via `cs auth login --tikti-url … --tenant t_…`, writing the result to `$HOME/.config/code-sous/auth.json`; programmatic clients should obtain a token from Tikti directly with the same OIDC flow they would use for any other Tikti-protected service. Tokens are short-lived; long-running clients refresh on `CS_AUTHN_EXPIRED_TOKEN`.

The gateway validates the token on every request and forwards `X-Tikti-Subject`, `X-Tikti-Tenant`, and `X-Tikti-Roles` into the control plane. Authorization is enforced both at ingress (action allowlist) and again at the per-function level (`authz.invoke_http_roles` and siblings), so a client with a valid token for tenant A cannot invoke functions belonging to tenant B even if the URL targets them.

The full token lifecycle, action vocabulary, and resource model live in [IAM with Tikti](IAM-with-Tikti). Programmatic clients should read that page before integrating; the action names (`cs:function:invoke:http`, `cs:function:invoke:api`, `cs:activation:read`, etc.) drive every 403 the gateway emits.

## Idempotency and retries

The HTTP gateway accepts an `Idempotency-Key` request header for client-supplied idempotency keys. The key must match `[A-Za-z0-9_-]{8,128}` (enforced in `cmd/cs-http-gateway/idempotency_mw.go`). When present, the gateway:

1. derives a deterministic `activation_id` from `(tenant, function-ref, key)`,
2. fingerprints the request body with SHA-256, and
3. consults the dedup store keyed by `(tenant, function-ref, key)` plus that fingerprint.

If the client retries with the same key and the same body, the gateway replays the cached response verbatim and stamps `X-CS-Idempotency-Replay: 1` so the client can distinguish a replay from a fresh execution. If the client retries with the same key but a different body, the gateway returns `409 CS_IDEMPOTENCY_CONFLICT` without re-invoking — the protocol assumes the key uniquely identifies the request, not just the operation.

A retry strategy on top of that looks like exponential backoff with full jitter:

```python
import random, time

def with_retry(call, *, attempts=5, base=0.2, cap=5.0):
    key = generate_idempotency_key()  # stable across all retries of one logical op
    for i in range(attempts):
        try:
            return call(idempotency_key=key)
        except TransientError:
            if i == attempts - 1:
                raise
            sleep = min(cap, base * (2 ** i)) * random.random()
            time.sleep(sleep)
```

Two rules that prevent the common bugs:

- Generate the idempotency key once per logical operation, before the first attempt. Reusing a fresh key on every retry defeats the entire mechanism.
- Retry on `5xx` and on connection-level failures. Do not retry on `4xx`; those are client errors and a retry will produce the same response.

Idempotency reservations have a TTL — the default in `cmd/cs-http-gateway/idempotency_mw.go` is one hour, with deployments encouraged to set `function_timeout + 3600s` per [HTTP Invoke Path](HTTP-Invoke-Path). After that TTL, the key is reusable for a new request.

## Result-failure vs internal-error

A clean separation between platform failures and function failures is the most important contract a client must internalize, and it does not match the HTTP status code one-for-one.

A `5xx` response is an infrastructure failure. The gateway, control plane, or downstream messaging layer could not complete the request: KVRocks unreachable, codeQ correlation timeout, queue publish failed. These are safe to retry (with idempotency) because the function was either never invoked or never produced a terminal result.

A `2xx` response is the gateway's report that *something* terminal happened. The body may still describe an error, in one of two shapes:

- An `InvocationResult` with `result: null` and `error: { type, message, stack }` — the function ran and threw, hit its timeout, or exceeded its memory limit. The shape is defined in `spec/cs.results.v1.json`. Do not retry blindly: the failure is deterministic with respect to the input.
- An HTTP-gateway invoke that returned an HTTP-shaped error from the function itself — a `200` from the gateway carrying the function's chosen `statusCode` (which might itself be `400` or `500`). The gateway transcribes whatever the function returned. The error model in [Error Model](Error-Model) puts it this way: "runtime errors inside function → 200 with function-defined statusCode, when the function returns a valid response object."

Concretely, a robust client distinguishes three buckets:

| Symptom                                  | Bucket          | Action                                  |
| ---------------------------------------- | --------------- | --------------------------------------- |
| Transport error, `5xx` from gateway      | Infrastructure  | Retry with idempotency key + backoff    |
| `2xx` with `error` field populated       | Function failure| Surface to caller; do not retry blindly |
| `2xx` with `result.statusCode >= 400`    | Function HTTP   | Surface to caller; treat like upstream  |
| `2xx` with `result.statusCode < 400`     | Success         | Use the response                        |

The same bucketing applies to the API invoke path (`POST …/functions/{name}:invoke`): a `200` may carry `status: "error"` or `status: "timeout"` per `spec/cs.results.v1.json`. Treating any non-2xx status as failure is wrong; treating any 2xx as success is also wrong. Clients must inspect the envelope.
