#!/usr/bin/env python3
"""
End-to-end scenario runner for code-sous via cs-cli.

What it does:
- logs in with `cs auth login`
- creates/publishes temporary JS functions
- invokes functions through `cs fn invoke` and `cs http invoke`
- validates expected outputs for success and failure scenarios
- optionally deletes created functions at the end

Auth options:
- preferred: provide --token (or SOUS_TOKEN / CS_TOKEN)
- fallback: login at Identity signInWithPassword using --email/--password
"""

from __future__ import annotations

import argparse
import http.client
import json
import math
import os
import re
import shutil
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


# Keep a sensible default email for convenience, but never hardcode real credentials.
DEFAULT_EMAIL = "admin@codecompany.com.br"
DEFAULT_PASSWORD = ""
# Production identity base used for signInWithPassword in our current environments.
DEFAULT_IDENTITY_URL = "https://api.storifly.ai/v1/accounts"


@dataclass
class CmdResult:
    argv: list[str]
    returncode: int
    stdout: str
    stderr: str


class TestFailure(Exception):
    pass


def now_ms() -> int:
    return int(time.time() * 1000)


def random_name(prefix: str) -> str:
    suffix = uuid.uuid4().hex[:10]
    return f"{prefix}{suffix}"


def shlex_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def redact_argv(argv: list[str]) -> list[str]:
    """Redact secrets from argv for logging only."""
    out: list[str] = []
    redact_next = False
    for arg in argv:
        if redact_next:
            out.append("<redacted>")
            redact_next = False
            continue
        if arg in ("--token", "--password", "--identity-api-key", "--api-key"):
            out.append(arg)
            redact_next = True
            continue
        out.append(arg)
    return out


def parse_json_from_text(text: str) -> Any:
    text = text.strip()
    if not text:
        raise TestFailure("empty output, expected JSON")

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    for idx, ch in enumerate(text):
        if ch not in "[{":
            continue
        try:
            obj, _end = decoder.raw_decode(text[idx:])
            return obj
        except json.JSONDecodeError:
            continue

    raise TestFailure(f"could not parse JSON from output: {text[:300]}")


def add_query_param(url: str, key: str, value: str) -> str:
    parsed = urllib.parse.urlparse(url)
    query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    if key not in query:
        query[key] = value
    new_query = urllib.parse.urlencode(query)
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def fetch_identity_token(identity_url: str, api_key: str, email: str, password: str, timeout_seconds: int) -> str:
    base = identity_url.rstrip("/")
    if "signInWithPassword" not in base:
        base = f"{base}/signInWithPassword"
    if api_key:
        base = add_query_param(base, "key", api_key)

    payload = {
        "email": email,
        "password": password,
        "returnSecureToken": True,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(base, data=data, headers={"Content-Type": "application/json"}, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace") if exc.fp is not None else ""
        raise TestFailure(f"identity login failed ({exc.code}): {body}") from exc
    except urllib.error.URLError as exc:
        raise TestFailure(f"identity login request failed: {exc}") from exc

    try:
        obj = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise TestFailure(f"identity login returned non-JSON payload: {raw[:200]!r}") from exc

    for key in ("idToken", "accessToken", "access_token", "token"):
        token = obj.get(key)
        if isinstance(token, str) and token.strip():
            return token.strip()

    raise TestFailure(f"identity login response has no token field: keys={list(obj.keys())}")


def default_gateway_url_for_control_url(control_url: str) -> str:
    parsed = urllib.parse.urlparse(control_url)
    host = parsed.hostname or ""
    port = parsed.port
    if host in ("localhost", "127.0.0.1") and port == 8080:
        # Matches config.example.yaml: cs_control :8080, cs_http_gateway :8081
        netloc = f"{host}:8081"
        return urllib.parse.urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    return control_url


class HTTPClient:
    def __init__(self, base_url: str, token: str, *, timeout_seconds: int = 20) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds

        parsed = urllib.parse.urlparse(self.base_url)
        self.scheme = parsed.scheme or "http"
        self.host = parsed.hostname or ""
        if not self.host:
            raise TestFailure(f"invalid base url (missing host): {base_url}")
        self.port = parsed.port or (443 if self.scheme == "https" else 80)
        self.base_path = parsed.path.rstrip("/")

        self._ssl_context = ssl.create_default_context() if self.scheme == "https" else None
        self._conn: http.client.HTTPConnection | None = None

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def _ensure_conn(self) -> http.client.HTTPConnection:
        if self._conn is None:
            if self.scheme == "https":
                self._conn = http.client.HTTPSConnection(
                    self.host,
                    self.port,
                    timeout=self.timeout_seconds,
                    context=self._ssl_context,
                )
            else:
                self._conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout_seconds)
        return self._conn

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
        retry_once: bool = True,
    ) -> tuple[int, dict[str, str], bytes]:
        if not path.startswith("/"):
            path = "/" + path
        full_path = self.base_path + path

        hdrs = {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
        if headers:
            hdrs.update(headers)
        if body is not None and "Content-Type" not in hdrs:
            hdrs["Content-Type"] = "application/json"

        conn = self._ensure_conn()
        try:
            conn.request(method.upper(), full_path, body=body, headers=hdrs)
            resp = conn.getresponse()
            data = resp.read()
            hdr_map = {k.lower(): v for (k, v) in resp.getheaders()}
            return resp.status, hdr_map, data
        except Exception as exc:  # noqa: BLE001
            self.close()
            if retry_once:
                return self.request(method, path, headers=headers, body=body, retry_once=False)
            raise TestFailure(f"http request failed: {method} {self.base_url}{full_path}: {exc}") from exc


def preview_bytes(raw: bytes, *, limit: int = 240) -> str:
    if not raw:
        return ""
    text = raw[:limit].decode("utf-8", "replace").strip()
    text = re.sub(r"\\s+", " ", text)
    if len(raw) > limit:
        text += " ..."
    return text


def percentile_ms(samples_ms: list[float], p: float) -> float:
    if not samples_ms:
        return 0.0
    if p <= 0:
        return min(samples_ms)
    if p >= 100:
        return max(samples_ms)
    ordered = sorted(samples_ms)
    idx = int(math.ceil((p / 100.0) * len(ordered))) - 1
    idx = max(0, min(idx, len(ordered) - 1))
    return ordered[idx]


def summarize_latencies_ms(samples_ms: list[float]) -> dict[str, float]:
    if not samples_ms:
        return {
            "count": 0,
            "total_ms": 0.0,
            "avg_ms": 0.0,
            "min_ms": 0.0,
            "max_ms": 0.0,
            "p50_ms": 0.0,
            "p90_ms": 0.0,
            "p99_ms": 0.0,
        }
    total = sum(samples_ms)
    return {
        "count": float(len(samples_ms)),
        "total_ms": total,
        "avg_ms": total / float(len(samples_ms)),
        "min_ms": min(samples_ms),
        "max_ms": max(samples_ms),
        "p50_ms": percentile_ms(samples_ms, 50),
        "p90_ms": percentile_ms(samples_ms, 90),
        "p99_ms": percentile_ms(samples_ms, 99),
    }


class SousScenarioRunner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.tmp = tempfile.TemporaryDirectory(prefix="sous-e2e-")
        self.base = Path(self.tmp.name)
        self.work = self.base / "work"
        self.work.mkdir(parents=True, exist_ok=True)

        self.created_functions: list[str] = []
        self.failures: list[str] = []
        self.passes: list[str] = []

        self.env = os.environ.copy()
        self.env["XDG_CONFIG_HOME"] = str(self.base / "xdg")
        self._auth_token: str = ""
        self._api_url: str = ""
        self._http_clients: dict[str, HTTPClient] = {}

    def close(self) -> None:
        for c in self._http_clients.values():
            c.close()
        self.tmp.cleanup()

    def log(self, message: str) -> None:
        print(f"[test_sous] {message}", flush=True)

    def run_cmd(
        self,
        argv: list[str],
        *,
        timeout_seconds: int = 40,
        expected_codes: tuple[int, ...] = (0,),
    ) -> CmdResult:
        pretty = " ".join(shlex_quote(a) for a in redact_argv(argv))
        self.log(f"$ {pretty}")
        try:
            cp = subprocess.run(
                argv,
                cwd=str(self.work),
                env=self.env,
                text=True,
                capture_output=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise TestFailure(f"command timed out after {timeout_seconds}s: {pretty}") from exc

        stdout = (cp.stdout or "").strip()
        stderr = (cp.stderr or "").strip()

        if stdout:
            self.log(f"stdout: {stdout[:1000]}")
        if stderr:
            self.log(f"stderr: {stderr[:1000]}")

        result = CmdResult(argv=argv, returncode=cp.returncode, stdout=stdout, stderr=stderr)
        if cp.returncode not in expected_codes:
            raise TestFailure(
                "unexpected return code "
                f"{cp.returncode} (expected {expected_codes}) for: {pretty}\n"
                f"stdout={stdout}\n"
                f"stderr={stderr}"
            )
        return result

    def run_cs(self, *parts: str, timeout_seconds: int = 40, expected_codes: tuple[int, ...] = (0,)) -> CmdResult:
        return self.run_cmd([self.args.cs_bin, *parts], timeout_seconds=timeout_seconds, expected_codes=expected_codes)

    def run_cs_json(self, *parts: str, timeout_seconds: int = 40) -> Any:
        out = self.run_cs(*parts, timeout_seconds=timeout_seconds)
        return parse_json_from_text(out.stdout)

    def ensure_api_url(self, url: str) -> None:
        url = url.strip().rstrip("/")
        if not url:
            raise TestFailure("missing api url")
        if self._api_url == url:
            return
        if not self._auth_token:
            raise TestFailure("missing auth token (did you call auth()?)")
        self.run_cs(
            "auth",
            "login",
            "--tenant",
            self.args.tenant,
            "--token",
            self._auth_token,
            "--api-url",
            url,
        )
        self._api_url = url

    def auth(self) -> None:
        token = self.args.token.strip() if self.args.token else ""
        if not token:
            env_token = os.environ.get("SOUS_TOKEN") or os.environ.get("CS_TOKEN")
            if env_token:
                token = env_token.strip()

        if not token:
            email = self.args.email.strip()
            password = self.args.password.strip()
            if not email or not password:
                raise TestFailure("missing token and missing email/password for identity login")
            self.log("No token provided, requesting token from identity signInWithPassword")
            token = fetch_identity_token(
                identity_url=self.args.identity_url,
                api_key=self.args.identity_api_key,
                email=email,
                password=password,
                timeout_seconds=15,
            )

        self._auth_token = token

        self.ensure_api_url(self.args.control_url)
        whoami = self.run_cs("auth", "whoami")
        if self.args.tenant not in whoami.stdout:
            raise TestFailure("whoami output does not include tenant")
        if self.args.control_url.rstrip("/") not in whoami.stdout:
            raise TestFailure("whoami output does not include control_url")

    def http_client(self, base_url: str) -> HTTPClient:
        base_url = base_url.strip().rstrip("/")
        if not base_url:
            raise TestFailure("missing base url for http client")
        if not self._auth_token:
            raise TestFailure("missing auth token (did you call auth()?)")
        c = self._http_clients.get(base_url)
        if c is None:
            c = HTTPClient(base_url, self._auth_token, timeout_seconds=30)
            self._http_clients[base_url] = c
        return c

    def write_bundle(self, fn_dir: Path, function_js: str, *, timeout_ms: int = 3000) -> None:
        manifest = {
            "schema": "cs.function.script.v1",
            "runtime": "cs-js",
            "entry": "function.js",
            "handler": "default",
            "limits": {
                "timeoutMs": timeout_ms,
                "memoryMb": 64,
                "maxConcurrency": 1,
            },
            "capabilities": {
                "kv": {"prefixes": ["ctr:"], "ops": ["get", "set", "del"]},
                "codeq": {"publishTopics": ["jobs.*"]},
                "http": {"allowHosts": ["api.example.com"], "timeoutMs": 1500},
            },
        }
        fn_dir.mkdir(parents=True, exist_ok=True)
        (fn_dir / "function.js").write_text(function_js, encoding="utf-8")
        (fn_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    def create_upload_publish(
        self,
        *,
        name: str,
        fn_dir: Path,
        invoke_http_roles: str,
        timeout_ms: int = 3000,
        alias: str = "prod",
    ) -> dict[str, Any]:
        self.ensure_api_url(self.args.control_url)
        self.run_cs("fn", "create", "--namespace", self.args.namespace, name)

        draft_resp = self.run_cs_json(
            "fn",
            "draft",
            "upload",
            "--namespace",
            self.args.namespace,
            "--path",
            str(fn_dir),
            name,
        )
        draft_id = draft_resp.get("draft_id")
        if not isinstance(draft_id, str) or not draft_id.startswith("drf_"):
            raise TestFailure(f"invalid draft_id from draft upload: {draft_resp}")

        publish_resp = self.run_cs_json(
            "fn",
            "publish",
            "--namespace",
            self.args.namespace,
            "--draft",
            draft_id,
            "--alias",
            alias,
            "--timeout-ms",
            str(timeout_ms),
            "--memory-mb",
            "64",
            "--invoke-http-roles",
            invoke_http_roles,
            "--invoke-schedule-roles",
            "admin",
            "--invoke-cadence-roles",
            "admin",
            name,
        )

        version = publish_resp.get("version")
        if not isinstance(version, int) or version <= 0:
            raise TestFailure(f"invalid publish response version: {publish_resp}")

        self.created_functions.append(name)
        return {
            "name": name,
            "alias": alias,
            "version": version,
            "draft_id": draft_id,
            "publish": publish_resp,
        }

    def invoke_api(self, fn_ref: str, event: Any) -> dict[str, Any]:
        self.ensure_api_url(self.args.control_url)
        event_path = self.work / f"event-{uuid.uuid4().hex}.json"
        event_path.write_text(json.dumps(event), encoding="utf-8")
        out = self.run_cs_json(
            "fn",
            "invoke",
            "--namespace",
            self.args.namespace,
            "--event",
            str(event_path),
            fn_ref,
            timeout_seconds=50,
        )
        if not isinstance(out, dict):
            raise TestFailure(f"invoke output is not an object: {out!r}")
        return out

    def invoke_http(self, path: str, body: Any) -> tuple[int, str]:
        self.ensure_api_url(self.args.gateway_url)
        body_path = self.work / f"http-{uuid.uuid4().hex}.json"
        body_path.write_text(json.dumps(body), encoding="utf-8")
        result = self.run_cs("http", "invoke", "-X", "POST", "-d", f"@{body_path}", path, timeout_seconds=50)

        match = re.match(r"^status=(\d+)\n(.*)$", result.stdout, re.DOTALL)
        if not match:
            raise TestFailure(f"unexpected cs http invoke output: {result.stdout}")
        status = int(match.group(1))
        payload = match.group(2).strip()
        return status, payload

    def cleanup_functions(self) -> None:
        if self.args.no_cleanup:
            self.log("Skipping cleanup (--no-cleanup)")
            return

        self.ensure_api_url(self.args.control_url)
        for name in self.created_functions:
            path = f"/v1/tenants/{self.args.tenant}/namespaces/{self.args.namespace}/functions/{name}"
            try:
                self.run_cs("http", "invoke", "-X", "DELETE", path, expected_codes=(0, 2))
            except TestFailure as exc:
                self.log(f"WARN cleanup failed for {name}: {exc}")

    def scenario_happy_path_api_and_http(self) -> None:
        name = random_name("fnok")
        fn_dir = self.work / name
        fn_js = """
export default async function handle(event, ctx) {
  const name = event && event.name ? String(event.name) : "world";
  return {
    statusCode: 200,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ message: "hello " + name, function: ctx.function }),
    isBase64Encoded: false
  };
}
""".strip()
        self.write_bundle(fn_dir, fn_js)

        meta = self.create_upload_publish(
            name=name,
            fn_dir=fn_dir,
            invoke_http_roles=self.args.allowlist_roles,
        )

        alias_ref = f"{name}@{meta['alias']}"
        version_ref = f"{name}@{meta['version']}"

        resp_alias = self.invoke_api(alias_ref, {"name": "sous"})
        if resp_alias.get("status") != "success":
            raise TestFailure(f"alias invoke expected success, got: {resp_alias}")
        result_alias = resp_alias.get("result")
        if not isinstance(result_alias, dict):
            raise TestFailure(f"alias invoke missing result object: {resp_alias}")
        if result_alias.get("statusCode") != 200:
            raise TestFailure(f"alias invoke expected statusCode=200, got: {result_alias}")
        alias_body = parse_json_from_text(str(result_alias.get("body", "")))
        if alias_body.get("message") != "hello sous":
            raise TestFailure(f"alias invoke body mismatch: {alias_body}")

        resp_version = self.invoke_api(version_ref, {"name": "version"})
        if resp_version.get("status") != "success":
            raise TestFailure(f"version invoke expected success, got: {resp_version}")
        result_version = resp_version.get("result")
        if not isinstance(result_version, dict) or result_version.get("statusCode") != 200:
            raise TestFailure(f"version invoke unexpected result: {resp_version}")
        version_body = parse_json_from_text(str(result_version.get("body", "")))
        if version_body.get("message") != "hello version":
            raise TestFailure(f"version invoke body mismatch: {version_body}")

        status, http_body = self.invoke_http(
            f"/v1/web/{self.args.tenant}/{self.args.namespace}/{name}/{meta['alias']}",
            {"name": "ignored-in-http"},
        )
        if status != 200:
            raise TestFailure(f"http invoke expected status 200, got {status}: {http_body}")
        parsed_http = parse_json_from_text(http_body)
        if parsed_http.get("message") != "hello world":
            raise TestFailure(f"http invoke body mismatch, expected hello world: {parsed_http}")

        async_resp = self.run_cs_json(
            "fn",
            "invoke",
            "--namespace",
            self.args.namespace,
            "--mode",
            "async",
            alias_ref,
        )
        if async_resp.get("status") != "queued":
            raise TestFailure(f"async invoke expected queued status, got: {async_resp}")
        if not async_resp.get("activation_id"):
            raise TestFailure(f"async invoke expected activation_id, got: {async_resp}")

        perf_http = int(getattr(self.args, "perf_iterations", 0) or 0)
        perf_api = int(getattr(self.args, "perf_api_iterations", 0) or 0)
        if perf_http > 0 or perf_api > 0:
            self.log(f"=== perf: http={perf_http} api={perf_api} iterations ===")
            if perf_api > 0:
                self.perf_invoke_api(name=name, alias=meta["alias"], iterations=perf_api)
            if perf_http > 0:
                self.perf_invoke_http(
                    name=name,
                    ref=meta["alias"],
                    iterations=perf_http,
                )

    def perf_invoke_api(self, *, name: str, alias: str, iterations: int) -> None:
        if iterations <= 0:
            return

        client = self.http_client(self.args.control_url)
        path = f"/v1/tenants/{self.args.tenant}/namespaces/{self.args.namespace}/functions/{name}:invoke"
        payload = json.dumps(
            {
                "ref": {"alias": alias},
                "mode": "sync",
                "event": {"name": "perf"},
            }
        ).encode("utf-8")

        samples: list[float] = []
        failures = 0
        status_counts: dict[int, int] = {}
        decode_failures = 0
        failure_log_limit = int(getattr(self.args, "perf_failure_log_limit", 0) or 0)
        failure_logs = 0
        progress_every = int(getattr(self.args, "perf_progress_every", 0) or 0)
        t_phase0 = time.perf_counter()
        for _i in range(iterations):
            t0 = time.perf_counter()
            status, hdrs, raw = client.request("POST", path, body=payload)
            dt_ms = (time.perf_counter() - t0) * 1000.0
            status_counts[status] = status_counts.get(status, 0) + 1
            if status != 200:
                failures += 1
                if failure_log_limit > 0 and failure_logs < failure_log_limit:
                    failure_logs += 1
                    req_id = hdrs.get("x-request-id", "")
                    self.log(f"PERF API POST fail i={_i+1} status={status} request_id={req_id} body={preview_bytes(raw)}")
            else:
                try:
                    obj = json.loads(raw.decode("utf-8", "replace"))
                except json.JSONDecodeError:
                    failures += 1
                    decode_failures += 1
                    if failure_log_limit > 0 and failure_logs < failure_log_limit:
                        failure_logs += 1
                        req_id = hdrs.get("x-request-id", "")
                        self.log(
                            f"PERF API POST fail i={_i+1} status=200 decode_error request_id={req_id} body={preview_bytes(raw)}"
                        )
                else:
                    if obj.get("status") != "success":
                        failures += 1
                        if failure_log_limit > 0 and failure_logs < failure_log_limit:
                            failure_logs += 1
                            req_id = hdrs.get("x-request-id", "")
                            self.log(
                                f"PERF API POST fail i={_i+1} status=200 app_status={obj.get('status')} request_id={req_id}"
                            )
            samples.append(dt_ms)
            if progress_every > 0 and (_i+1) % progress_every == 0:
                elapsed_s = max(0.000001, time.perf_counter()-t_phase0)
                avg_ms = (sum(samples) / float(len(samples))) if samples else 0.0
                rps = float(_i+1) / elapsed_s
                self.log(
                    f"PERF API POST progress i={_i+1}/{iterations} failures={failures} avg_ms={avg_ms:.3f} rps={rps:.2f}"
                )

        s = summarize_latencies_ms(samples)
        wall_ms = (time.perf_counter() - t_phase0) * 1000.0
        self.log(
            "PERF API POST "
            f"count={int(s['count'])} failures={failures} decode_failures={decode_failures} "
            f"wall_ms={wall_ms:.1f} total_ms={s['total_ms']:.1f} avg_ms={s['avg_ms']:.3f} "
            f"p50_ms={s['p50_ms']:.3f} p90_ms={s['p90_ms']:.3f} p99_ms={s['p99_ms']:.3f} "
            f"status_counts={status_counts}"
        )
        if failures > 0 and bool(getattr(self.args, "perf_fail_on_errors", False)):
            raise TestFailure(f"perf api post had failures={failures} status_counts={status_counts} decode_failures={decode_failures}")

    def perf_invoke_http(self, *, name: str, ref: str, iterations: int) -> None:
        if iterations <= 0:
            return

        client = self.http_client(self.args.gateway_url)
        path = f"/v1/web/{self.args.tenant}/{self.args.namespace}/{name}/{ref}"
        progress_every = int(getattr(self.args, "perf_progress_every", 0) or 0)
        failure_log_limit = int(getattr(self.args, "perf_failure_log_limit", 0) or 0)

        def run(method: str, *, body: bytes | None) -> tuple[list[float], int, float, dict[int, int], int]:
            samples_ms: list[float] = []
            failures = 0
            status_counts: dict[int, int] = {}
            decode_failures = 0
            failure_logs = 0
            t_phase0 = time.perf_counter()
            for _i in range(iterations):
                t0 = time.perf_counter()
                status, hdrs, raw = client.request(method, path, body=body)
                dt_ms = (time.perf_counter() - t0) * 1000.0
                status_counts[status] = status_counts.get(status, 0) + 1
                if status != 200:
                    failures += 1
                    if failure_log_limit > 0 and failure_logs < failure_log_limit:
                        failure_logs += 1
                        req_id = hdrs.get("x-request-id", "")
                        self.log(
                            f"PERF HTTP {method.upper()} fail i={_i+1} status={status} request_id={req_id} body={preview_bytes(raw)}"
                        )
                else:
                    # Light validation: payload should be JSON.
                    try:
                        json.loads(raw.decode("utf-8", "replace"))
                    except json.JSONDecodeError:
                        failures += 1
                        decode_failures += 1
                        if failure_log_limit > 0 and failure_logs < failure_log_limit:
                            failure_logs += 1
                            req_id = hdrs.get("x-request-id", "")
                            self.log(
                                f"PERF HTTP {method.upper()} fail i={_i+1} status=200 decode_error request_id={req_id} body={preview_bytes(raw)}"
                            )
                samples_ms.append(dt_ms)
                if progress_every > 0 and (_i+1) % progress_every == 0:
                    elapsed_s = max(0.000001, time.perf_counter()-t_phase0)
                    avg_ms = (sum(samples_ms) / float(len(samples_ms))) if samples_ms else 0.0
                    rps = float(_i+1) / elapsed_s
                    self.log(
                        f"PERF HTTP {method.upper()} progress i={_i+1}/{iterations} failures={failures} avg_ms={avg_ms:.3f} rps={rps:.2f}"
                    )
            wall_ms = (time.perf_counter() - t_phase0) * 1000.0
            return samples_ms, failures, wall_ms, status_counts, decode_failures

        get_samples, get_fail, get_wall_ms, get_status_counts, get_decode_fail = run("GET", body=None)
        post_body = json.dumps({"name": "perf"}).encode("utf-8")
        post_samples, post_fail, post_wall_ms, post_status_counts, post_decode_fail = run("POST", body=post_body)

        s_get = summarize_latencies_ms(get_samples)
        s_post = summarize_latencies_ms(post_samples)
        self.log(
            "PERF HTTP GET "
            f"count={int(s_get['count'])} failures={get_fail} decode_failures={get_decode_fail} "
            f"wall_ms={get_wall_ms:.1f} total_ms={s_get['total_ms']:.1f} avg_ms={s_get['avg_ms']:.3f} "
            f"p50_ms={s_get['p50_ms']:.3f} p90_ms={s_get['p90_ms']:.3f} p99_ms={s_get['p99_ms']:.3f} "
            f"status_counts={get_status_counts}"
        )
        self.log(
            "PERF HTTP POST "
            f"count={int(s_post['count'])} failures={post_fail} decode_failures={post_decode_fail} "
            f"wall_ms={post_wall_ms:.1f} total_ms={s_post['total_ms']:.1f} avg_ms={s_post['avg_ms']:.3f} "
            f"p50_ms={s_post['p50_ms']:.3f} p90_ms={s_post['p90_ms']:.3f} p99_ms={s_post['p99_ms']:.3f} "
            f"status_counts={post_status_counts}"
        )
        total_failures = get_fail + post_fail
        if total_failures > 0 and bool(getattr(self.args, "perf_fail_on_errors", False)):
            raise TestFailure(
                "perf http had failures="
                f"{total_failures} get_status_counts={get_status_counts} post_status_counts={post_status_counts} "
                f"get_decode_failures={get_decode_fail} post_decode_failures={post_decode_fail}"
            )

    def scenario_runtime_exception(self) -> None:
        name = random_name("fnerr")
        fn_dir = self.work / name
        fn_js = """
export default async function handle() {
  throw new Error("boom_sous_exception");
}
""".strip()
        self.write_bundle(fn_dir, fn_js)

        meta = self.create_upload_publish(
            name=name,
            fn_dir=fn_dir,
            invoke_http_roles=self.args.allowlist_roles,
        )
        resp = self.invoke_api(f"{name}@{meta['alias']}", {"k": "v"})

        if resp.get("status") not in ("error", "timeout"):
            raise TestFailure(f"exception invoke expected status error/timeout, got: {resp}")
        err = resp.get("error")
        if not isinstance(err, dict):
            raise TestFailure(f"exception invoke expected error object, got: {resp}")
        text = json.dumps(err)
        if "boom_sous_exception" not in text and "Exception" not in text:
            raise TestFailure(f"exception invoke error does not mention expected failure: {err}")

    def scenario_syntax_error(self) -> None:
        name = random_name("fnsyn")
        fn_dir = self.work / name
        fn_js = """
export default async function handle(event, ctx) {
  const x = ;
  return { statusCode: 200, body: "ok" };
}
""".strip()
        self.write_bundle(fn_dir, fn_js)

        meta = self.create_upload_publish(
            name=name,
            fn_dir=fn_dir,
            invoke_http_roles=self.args.allowlist_roles,
        )
        resp = self.invoke_api(f"{name}@{meta['alias']}", {"k": 1})
        if resp.get("status") != "error":
            raise TestFailure(f"syntax error invoke expected status=error, got: {resp}")
        err = resp.get("error")
        if not isinstance(err, dict):
            raise TestFailure(f"syntax error invoke expected error object, got: {resp}")

    def scenario_timeout_loop(self) -> None:
        name = random_name("fnto")
        fn_dir = self.work / name
        fn_js = """
export default async function handle() {
  while (true) {}
}
""".strip()
        self.write_bundle(fn_dir, fn_js, timeout_ms=1000)

        meta = self.create_upload_publish(
            name=name,
            fn_dir=fn_dir,
            invoke_http_roles=self.args.allowlist_roles,
            timeout_ms=1000,
        )
        resp = self.invoke_api(f"{name}@{meta['alias']}", {"kind": "timeout"})

        status = resp.get("status")
        if status not in ("timeout", "error"):
            raise TestFailure(f"timeout scenario expected timeout/error status, got: {resp}")
        err = resp.get("error")
        if isinstance(err, dict):
            err_type = str(err.get("type", ""))
            err_msg = str(err.get("message", ""))
            if status == "timeout" and "Timeout" not in err_type and "timeout" not in err_msg.lower():
                raise TestFailure(f"timeout scenario expected timeout semantics in error, got: {err}")

    def scenario_authz_denied(self) -> None:
        name = random_name("fnauth")
        fn_dir = self.work / name
        fn_js = """
export default async function handle() {
  return { statusCode: 200, body: "ok" };
}
""".strip()
        self.write_bundle(fn_dir, fn_js)

        meta = self.create_upload_publish(
            name=name,
            fn_dir=fn_dir,
            invoke_http_roles="role:__never_match_test_sous__",
        )

        result = self.run_cs(
            "fn",
            "invoke",
            "--namespace",
            self.args.namespace,
            f"{name}@{meta['alias']}",
            expected_codes=(1, 2, 3),
        )
        if result.returncode == 0:
            raise TestFailure("authz deny scenario expected non-zero exit code")
        combined = f"{result.stdout}\n{result.stderr}"
        if "CS_AUTHZ_ROLE_MISSING" not in combined and "role missing" not in combined.lower():
            raise TestFailure(f"authz deny scenario expected role-missing error, got: {combined}")

    def run_all(self) -> int:
        start = now_ms()
        scenario_map = {
            "happy": self.scenario_happy_path_api_and_http,
            "exception": self.scenario_runtime_exception,
            "syntax": self.scenario_syntax_error,
            "timeout": self.scenario_timeout_loop,
            "authz": self.scenario_authz_denied,
        }
        selected = self.args.only if self.args.only else list(scenario_map.keys())

        self.auth()

        for scenario_name in selected:
            fn = scenario_map.get(scenario_name)
            if fn is None:
                self.failures.append(f"unknown scenario: {scenario_name}")
                continue
            self.log(f"=== scenario: {scenario_name} ===")
            try:
                fn()
            except Exception as exc:  # noqa: BLE001
                self.failures.append(f"{scenario_name}: {exc}")
                self.log(f"FAIL {scenario_name}: {exc}")
            else:
                self.passes.append(scenario_name)
                self.log(f"PASS {scenario_name}")

        self.cleanup_functions()

        elapsed = now_ms() - start
        print("\n========== SUMMARY ==========")
        print(f"Passed: {len(self.passes)} -> {self.passes}")
        print(f"Failed: {len(self.failures)}")
        for failure in self.failures:
            print(f"- {failure}")
        print(f"Elapsed: {elapsed} ms")

        return 0 if not self.failures else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run code-sous E2E scenarios through cs-cli")
    parser.add_argument("--cs-bin", default=os.environ.get("SOUS_CS_BIN", "./bin/cs"), help="Path to cs-cli binary")
    # Back-compat: a single ingress may route both control (/v1/tenants/...) and gateway (/v1/web/...) paths.
    # Prefer --control-url/--gateway-url for clarity.
    parser.add_argument(
        "--api-url",
        default=os.environ.get("SOUS_API_URL", "http://localhost:8080"),
        help="(deprecated) Base URL for both control plane and HTTP gateway (single-ingress deployments)",
    )
    parser.add_argument(
        "--control-url",
        default=os.environ.get("SOUS_CONTROL_URL", ""),
        help="Base URL for cs-control (function lifecycle + API invoke)",
    )
    parser.add_argument(
        "--gateway-url",
        default=os.environ.get("SOUS_GATEWAY_URL", ""),
        help="Base URL for cs-http-gateway (HTTP invoke /v1/web/...)",
    )
    parser.add_argument("--tenant", default=os.environ.get("SOUS_TENANT", "t_abc123"), help="Tenant id")
    parser.add_argument("--namespace", default=os.environ.get("SOUS_NAMESPACE", "default"), help="Namespace for tests")

    parser.add_argument("--token", default=os.environ.get("SOUS_TOKEN") or os.environ.get("CS_TOKEN", ""), help="Bearer token")
    parser.add_argument("--email", default=os.environ.get("TEST_ADMIN_EMAIL", DEFAULT_EMAIL), help="Identity login email")
    parser.add_argument("--password", default=os.environ.get("TEST_ADMIN_PASSWORD", DEFAULT_PASSWORD), help="Identity login password")
    parser.add_argument("--identity-url", default=os.environ.get("IDENTITY_URL", DEFAULT_IDENTITY_URL), help="Identity base URL")
    parser.add_argument("--identity-api-key", default=os.environ.get("API_KEY", ""), help="Identity api key (optional)")

    parser.add_argument(
        "--allowlist-roles",
        default=os.environ.get(
            "SOUS_INVOKE_ALLOWLIST_ROLES",
            "admin,role:admin,action:cs:function:invoke:api,cs:function:invoke:api,action:cs:function:invoke:http,cs:function:invoke:http",
        ),
        help="CSV roles used at publish for invoke_http_roles",
    )
    parser.add_argument("--no-cleanup", action="store_true", help="Do not delete created functions at the end")
    parser.add_argument(
        "--perf-iterations",
        type=int,
        default=int(os.environ.get("SOUS_PERF_ITERATIONS", "0") or "0"),
        help="If >0, run N performance iterations for HTTP GET/POST in the happy scenario",
    )
    parser.add_argument(
        "--perf-api-iterations",
        type=int,
        default=int(os.environ.get("SOUS_PERF_API_ITERATIONS", "0") or "0"),
        help="If >0, run N performance iterations for API POST invoke in the happy scenario",
    )
    parser.add_argument(
        "--perf-progress-every",
        type=int,
        default=int(os.environ.get("SOUS_PERF_PROGRESS_EVERY", "100") or "100"),
        help="Print perf progress every N iterations (0 disables)",
    )
    parser.add_argument(
        "--perf-failure-log-limit",
        type=int,
        default=int(os.environ.get("SOUS_PERF_FAILURE_LOG_LIMIT", "10") or "10"),
        help="Max number of failure samples to print per perf phase (0 disables)",
    )
    parser.add_argument(
        "--perf-fail-on-errors",
        action="store_true",
        help="Fail the scenario if perf loops see non-200 or invalid JSON responses",
    )
    parser.add_argument(
        "--only",
        action="append",
        choices=["happy", "exception", "syntax", "timeout", "authz"],
        help="Run only selected scenario(s). Can be repeated.",
    )

    return parser


def main() -> int:
    args = build_parser().parse_args()

    # Normalize URLs:
    # - if control/gateway are not set, fall back to --api-url
    if not getattr(args, "control_url", "").strip():
        args.control_url = args.api_url
    if not getattr(args, "gateway_url", "").strip():
        args.gateway_url = default_gateway_url_for_control_url(args.control_url)

    if "/" in args.cs_bin:
        cs_path = Path(args.cs_bin).expanduser()
        if not cs_path.is_absolute():
            cs_path = (Path.cwd() / cs_path).resolve()
        if not cs_path.exists():
            print(f"error: cs binary not found: {cs_path}", file=sys.stderr)
            return 2
        args.cs_bin = str(cs_path)
    else:
        resolved = shutil.which(args.cs_bin)
        if resolved is None:
            print(f"error: cs binary not found in PATH: {args.cs_bin}", file=sys.stderr)
            return 2
        args.cs_bin = resolved

    runner = SousScenarioRunner(args)
    try:
        return runner.run_all()
    finally:
        runner.close()


if __name__ == "__main__":
    sys.exit(main())
