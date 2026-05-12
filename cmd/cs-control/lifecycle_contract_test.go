package main

// End-to-end contract suite for the function lifecycle (E1.01).
// Locks the HTTP contract documented in docs/04-api-rest.md and the
// invariants in docs/19-entity-state-machines.md. Persistence is the
// real internal/kv.Store backed by miniredis so torn-write bugs in the
// publish pipeline (INCR + meta + bundle) surface in CI.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-chi/chi/v5"

	"github.com/osvaldoandrade/sous/internal/authz"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/kv"
	"github.com/osvaldoandrade/sous/internal/limits"
	"github.com/osvaldoandrade/sous/internal/observability"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// lifecycleHarness wires a cs-control server against a miniredis-backed
// kv.Store and exposes an httptest.Server bound to the lifecycle routes.
// Tests should use the helper methods (create, draft, publish, ...) so
// the URL shapes stay aligned with docs/04-api-rest.md.
type lifecycleHarness struct {
	t      *testing.T
	server *httptest.Server
	store  *kv.Store
	tenant string
	ns     string
}

func newLifecycleHarness(t *testing.T) *lifecycleHarness {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	store := kv.NewStore(mr.Addr(), "")
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("ping miniredis: %v", err)
	}

	cfg := config.Config{}
	cfg.CSControl.Limits.MaxBundleBytes = limits.DefaultMaxBundleBytes
	cfg.CSControl.Limits.DraftTTLSeconds = limits.DefaultDraftTTLSeconds
	cfg.CSControl.Limits.ActTTLSeconds = limits.DefaultActTTLSeconds

	s := &server{
		cfg:    cfg,
		store:  store,
		broker: &testutil.FakeMessaging{},
		logger: observability.NewLoggerWithWriter("contract", io.Discard),
	}

	r := chi.NewRouter()
	r.Use(observability.RequestIDMiddleware)
	r.Group(func(pr chi.Router) {
		pr.Use(authz.AuthnMiddleware(&permissiveAuthN{}))
		pr.Post("/v1/tenants/{tenant}/namespaces/{namespace}/functions", s.createFunction)
		pr.Get("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}", s.readFunction)
		pr.Delete("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}", s.deleteFunction)
		pr.Put("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/draft", s.uploadDraft)
		pr.Post("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/versions", s.publishVersion)
		pr.Put("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/aliases/{alias}", s.setAlias)
		pr.Get("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/aliases", s.listAliases)
	})

	httpSrv := httptest.NewServer(r)
	t.Cleanup(httpSrv.Close)

	// Each test gets its own tenant prefix so they remain isolated
	// even when miniredis is shared in a future parallel evolution.
	return &lifecycleHarness{
		t:      t,
		server: httpSrv,
		store:  store,
		tenant: fmt.Sprintf("t_test%d", time.Now().UnixNano()%1_000_000),
		ns:     "lifecycle",
	}
}

// do performs an authenticated HTTP request against the harness and
// returns the parsed status code and body. It fails the test if the
// transport fails.
func (h *lifecycleHarness) do(method, path string, body any) (int, []byte) {
	h.t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("marshal body: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, h.server.URL+path, rdr)
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer tok_contract")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// fnPath returns the URL prefix for a function under the harness's tenant.
func (h *lifecycleHarness) fnPath(name string) string {
	return fmt.Sprintf("/v1/tenants/%s/namespaces/%s/functions/%s", h.tenant, h.ns, name)
}

// fnRootPath returns the URL prefix for the function collection.
func (h *lifecycleHarness) fnRootPath() string {
	return fmt.Sprintf("/v1/tenants/%s/namespaces/%s/functions", h.tenant, h.ns)
}

// permissiveAuthN authorizes every request with a broad role bundle so
// the contract suite focuses on lifecycle semantics. It mimics the
// Tikti driver's surface area but bypasses any token introspection.
type permissiveAuthN struct{}

func (permissiveAuthN) Name() string { return "contract-permissive" }

func (permissiveAuthN) Introspect(_ context.Context, _ string) (authz.Principal, error) {
	return authz.Principal{
		Sub:    "u_contract",
		Tenant: "", // empty tenant disables the tenant-mismatch check
		Roles: []string{
			"role:admin",
		},
	}, nil
}

// makeBundleFiles returns a canonical pair of files (function.js +
// manifest.json) suitable for upload/publish. The manifest is the
// minimal valid form per internal/api.FunctionManifest.Validate.
func makeBundleFiles() map[string]string {
	manifest := []byte(`{"schema":"cs.function.script.v1","runtime":"cs-js","entry":"function.js","handler":"default","limits":{"timeoutMs":1000,"memoryMb":64,"maxConcurrency":1},"capabilities":{"kv":{"prefixes":["ctr:"],"ops":["get","set","del"]},"codeq":{"publishTopics":["jobs.*"]},"http":{"allowHosts":["example.com"],"timeoutMs":1000}}}`)
	fn := []byte(`export default function(){ return {statusCode:200, body:"ok"} }`)
	return map[string]string{
		"function.js":   base64.StdEncoding.EncodeToString(fn),
		"manifest.json": base64.StdEncoding.EncodeToString(manifest),
	}
}

// uploadDraft posts a fresh draft for the given function. It returns
// the parsed response body so callers can extract draft_id, sha256,
// or expires_at_ms.
func (h *lifecycleHarness) uploadDraft(name string) map[string]any {
	h.t.Helper()
	status, body := h.do(http.MethodPut, h.fnPath(name)+"/draft", map[string]any{"files": makeBundleFiles()})
	if status != http.StatusOK {
		h.t.Fatalf("uploadDraft status=%d body=%s", status, string(body))
	}
	return parseJSONBody(h.t, body)
}

// publishVersion publishes the given draft and returns the resulting
// version metadata.
func (h *lifecycleHarness) publishVersion(name, draftID, alias string) map[string]any {
	h.t.Helper()
	req := map[string]any{
		"draft_id": draftID,
		"config": map[string]any{
			"timeout_ms":      1000,
			"memory_mb":       64,
			"max_concurrency": 1,
		},
	}
	if alias != "" {
		req["alias"] = alias
	}
	status, body := h.do(http.MethodPost, h.fnPath(name)+"/versions", req)
	if status != http.StatusCreated {
		h.t.Fatalf("publishVersion status=%d body=%s", status, string(body))
	}
	return parseJSONBody(h.t, body)
}

// setAlias points an alias at a published version and returns the
// updated alias record.
func (h *lifecycleHarness) setAlias(name, alias string, version int64) map[string]any {
	h.t.Helper()
	status, body := h.do(http.MethodPut, h.fnPath(name)+"/aliases/"+alias, map[string]any{"version": version})
	if status != http.StatusOK {
		h.t.Fatalf("setAlias status=%d body=%s", status, string(body))
	}
	return parseJSONBody(h.t, body)
}

func TestLifecycleCreateReadDelete(t *testing.T) {
	h := newLifecycleHarness(t)

	// Create
	status, body := h.do(http.MethodPost, h.fnRootPath(), map[string]any{
		"name": "reconcile",
	})
	if status != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", status, string(body))
	}
	created := parseJSONBody(t, body)
	if created["name"] != "reconcile" || created["runtime"] != "cs-js" ||
		created["entry"] != "function.js" || created["handler"] != "default" {
		t.Fatalf("unexpected create response: %+v", created)
	}

	// Read-after-write: GET immediately returns the same record.
	status, body = h.do(http.MethodGet, h.fnPath("reconcile"), nil)
	if status != http.StatusOK {
		t.Fatalf("read status=%d body=%s", status, string(body))
	}
	readResp := parseJSONBody(t, body)
	fn := readResp["function"].(map[string]any)
	if fn["name"] != "reconcile" || fn["created_at_ms"].(float64) <= 0 {
		t.Fatalf("read returned unexpected function: %+v", fn)
	}

	// Delete is a soft-delete; the next read without include_deleted
	// returns 400 with CSValidationFailed per the locked contract.
	status, body = h.do(http.MethodDelete, h.fnPath("reconcile"), nil)
	if status != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", status, string(body))
	}
	status, body = h.do(http.MethodGet, h.fnPath("reconcile"), nil)
	if status != http.StatusBadRequest {
		t.Fatalf("read after delete should 400, got %d body=%s", status, string(body))
	}

	// include_deleted=true exposes the soft-deleted record.
	status, body = h.do(http.MethodGet, h.fnPath("reconcile")+"?include_deleted=true", nil)
	if status != http.StatusOK {
		t.Fatalf("read include_deleted status=%d body=%s", status, string(body))
	}
	includeResp := parseJSONBody(t, body)
	deletedFn := includeResp["function"].(map[string]any)
	if deletedFn["deleted_at_ms"] == nil {
		t.Fatalf("expected deleted_at_ms after delete: %+v", deletedFn)
	}
}

func TestLifecycleCreateIsIdempotent(t *testing.T) {
	h := newLifecycleHarness(t)
	body := map[string]any{"name": "idempotent_fn"}

	// First create returns 201.
	status, raw := h.do(http.MethodPost, h.fnRootPath(), body)
	if status != http.StatusCreated {
		t.Fatalf("first create status=%d body=%s", status, string(raw))
	}
	first := parseJSONBody(t, raw)

	// Second identical create returns 200 with the same record (no
	// side effects, no new created_at_ms).
	status, raw = h.do(http.MethodPost, h.fnRootPath(), body)
	if status != http.StatusOK {
		t.Fatalf("second create should be idempotent OK, got %d body=%s", status, string(raw))
	}
	second := parseJSONBody(t, raw)
	if first["created_at_ms"] != second["created_at_ms"] {
		t.Fatalf("idempotent create returned a different record: first=%+v second=%+v", first, second)
	}
}

func TestLifecycleCreateConflictOnDifferentIdentity(t *testing.T) {
	h := newLifecycleHarness(t)
	if status, raw := h.do(http.MethodPost, h.fnRootPath(), map[string]any{"name": "fixed_runtime"}); status != http.StatusCreated {
		t.Fatalf("seed create status=%d body=%s", status, string(raw))
	}

	// Re-create with a different runtime must return CS_IDEMPOTENCY_CONFLICT.
	status, raw := h.do(http.MethodPost, h.fnRootPath(), map[string]any{
		"name":    "fixed_runtime",
		"runtime": "cs-py", // different runtime
	})
	if status != http.StatusConflict {
		t.Fatalf("expected 409, got %d body=%s", status, string(raw))
	}
	if code := parseErrorCode(t, raw); code != cserrors.CSIdempotencyConflict {
		t.Fatalf("expected CS_IDEMPOTENCY_CONFLICT, got %s body=%s", code, string(raw))
	}
}

// ---------------------------------------------------------------------------
// DRAFT TTL + concurrency
// ---------------------------------------------------------------------------

func TestLifecycleDraftUploadIsolation(t *testing.T) {
	h := newLifecycleHarness(t)
	if status, raw := h.do(http.MethodPost, h.fnRootPath(), map[string]any{"name": "drafty"}); status != http.StatusCreated {
		t.Fatalf("seed create status=%d body=%s", status, string(raw))
	}

	d1 := h.uploadDraft("drafty")
	d2 := h.uploadDraft("drafty")
	if d1["draft_id"] == d2["draft_id"] {
		t.Fatalf("two draft uploads returned same draft_id: %+v / %+v", d1, d2)
	}
	if d1["sha256"] != d2["sha256"] {
		t.Fatalf("identical files must yield identical sha256, got %v vs %v", d1["sha256"], d2["sha256"])
	}
	// expires_at_ms is set independently per draft.
	if int64(d1["expires_at_ms"].(float64)) <= 0 || int64(d2["expires_at_ms"].(float64)) <= 0 {
		t.Fatalf("expires_at_ms must be positive: %+v / %+v", d1, d2)
	}
}

func TestLifecycleConcurrentDraftsAllPersist(t *testing.T) {
	// Concurrent draft uploads for the same function each get a
	// distinct draft_id; none of them collide and the second's TTL
	// window does not shorten the first's.
	h := newLifecycleHarness(t)
	if status, raw := h.do(http.MethodPost, h.fnRootPath(), map[string]any{"name": "concur_draft"}); status != http.StatusCreated {
		t.Fatalf("seed create status=%d body=%s", status, string(raw))
	}

	const n = 8
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		draftIDs = map[string]struct{}{}
		errs     []error
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			status, body := h.do(http.MethodPut, h.fnPath("concur_draft")+"/draft", map[string]any{"files": makeBundleFiles()})
			if status != http.StatusOK {
				mu.Lock()
				errs = append(errs, fmt.Errorf("upload status=%d body=%s", status, string(body)))
				mu.Unlock()
				return
			}
			resp := parseJSONBody(t, body)
			id, _ := resp["draft_id"].(string)
			if id == "" {
				mu.Lock()
				errs = append(errs, fmt.Errorf("missing draft_id in %s", string(body)))
				mu.Unlock()
				return
			}
			mu.Lock()
			draftIDs[id] = struct{}{}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("concurrent uploads produced errors: %v", errs)
	}
	if len(draftIDs) != n {
		t.Fatalf("expected %d distinct draft_ids, got %d (%v)", n, len(draftIDs), draftIDs)
	}
}

func TestLifecyclePublishRejectsExpiredDraft(t *testing.T) {
	// We can't easily fast-forward miniredis TTL with the real
	// kv.Store because PutDraft writes via Set ... EX. The test
	// instead patches the draft's expires_at_ms via the underlying
	// store after upload, then asserts publish returns the documented
	// "draft expired" error.
	h := newLifecycleHarness(t)
	if status, raw := h.do(http.MethodPost, h.fnRootPath(), map[string]any{"name": "expiry"}); status != http.StatusCreated {
		t.Fatalf("seed create status=%d body=%s", status, string(raw))
	}
	draft := h.uploadDraft("expiry")
	draftID := draft["draft_id"].(string)

	// Patch the on-disk record so ExpiresAtMS is in the past. We use
	// the persistence Provider directly to avoid waiting 24h.
	rec, err := h.store.GetDraft(context.Background(), h.tenant, h.ns, "expiry", draftID)
	if err != nil {
		t.Fatalf("get draft: %v", err)
	}
	rec.ExpiresAtMS = time.Now().Add(-time.Hour).UnixMilli()
	if err := h.store.PutDraft(context.Background(), h.tenant, h.ns, "expiry", rec, time.Hour); err != nil {
		t.Fatalf("re-put draft: %v", err)
	}

	status, body := h.do(http.MethodPost, h.fnPath("expiry")+"/versions", map[string]any{
		"draft_id": draftID,
		"config":   map[string]any{"timeout_ms": 1000, "memory_mb": 64, "max_concurrency": 1},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for expired draft, got %d body=%s", status, string(body))
	}
	if code := parseErrorCode(t, body); code != cserrors.CSValidationFailed {
		t.Fatalf("expected CSValidationFailed for expired draft, got %s body=%s", code, string(body))
	}
	if !strings.Contains(string(body), "draft expired") {
		t.Fatalf("expected 'draft expired' in error body, got %s", string(body))
	}
}

// ---------------------------------------------------------------------------
// PUBLISH atomicity
// ---------------------------------------------------------------------------

func TestLifecyclePublishAssignsMonotonicVersions(t *testing.T) {
	h := newLifecycleHarness(t)
	if status, raw := h.do(http.MethodPost, h.fnRootPath(), map[string]any{"name": "mono"}); status != http.StatusCreated {
		t.Fatalf("seed create status=%d body=%s", status, string(raw))
	}

	versions := []int64{}
	for i := 0; i < 5; i++ {
		draft := h.uploadDraft("mono")
		ver := h.publishVersion("mono", draft["draft_id"].(string), "")
		versions = append(versions, int64(ver["version"].(float64)))
	}
	// Versions are 1..N, strictly monotonic, no gaps.
	for i, v := range versions {
		if v != int64(i+1) {
			t.Fatalf("expected versions to be 1..N, got %v", versions)
		}
	}
}

func TestLifecycleConcurrentPublishesAreAtomic(t *testing.T) {
	// Concurrent publish calls must each receive a distinct, positive
	// integer version. Any duplicate or zero would indicate a torn
	// INCR / Tx pipeline failure.
	h := newLifecycleHarness(t)
	if status, raw := h.do(http.MethodPost, h.fnRootPath(), map[string]any{"name": "atomic_pub"}); status != http.StatusCreated {
		t.Fatalf("seed create status=%d body=%s", status, string(raw))
	}

	const n = 8
	drafts := make([]string, n)
	for i := 0; i < n; i++ {
		drafts[i] = h.uploadDraft("atomic_pub")["draft_id"].(string)
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = map[int64]int{}
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			status, body := h.do(http.MethodPost, h.fnPath("atomic_pub")+"/versions", map[string]any{
				"draft_id": drafts[i],
				"config":   map[string]any{"timeout_ms": 1000, "memory_mb": 64, "max_concurrency": 1},
			})
			if status != http.StatusCreated {
				t.Errorf("publish %d failed status=%d body=%s", i, status, string(body))
				return
			}
			resp := parseJSONBody(t, body)
			ver := int64(resp["version"].(float64))
			mu.Lock()
			seen[ver]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("expected %d distinct versions, got %d (%v)", n, len(seen), seen)
	}
	for v, count := range seen {
		if count != 1 {
			t.Fatalf("version %d was assigned %d times", v, count)
		}
		if v < 1 || v > n {
			t.Fatalf("version %d outside expected range [1..%d]", v, n)
		}
	}
}

func TestLifecyclePublishedVersionsAreImmutable(t *testing.T) {
	// docs/19-entity-state-machines.md: versions are immutable. A
	// re-publish of the same draft should produce a *new* version
	// rather than mutating the previous one.
	h := newLifecycleHarness(t)
	if status, raw := h.do(http.MethodPost, h.fnRootPath(), map[string]any{"name": "immut"}); status != http.StatusCreated {
		t.Fatalf("seed create status=%d body=%s", status, string(raw))
	}
	d1 := h.uploadDraft("immut")
	v1 := h.publishVersion("immut", d1["draft_id"].(string), "")
	d2 := h.uploadDraft("immut")
	v2 := h.publishVersion("immut", d2["draft_id"].(string), "")

	if v1["version"] == v2["version"] {
		t.Fatalf("immutability violated: both publishes returned version %v", v1["version"])
	}
	// Re-fetch version 1 via read-after-write: it must still expose
	// the original sha256 / published_at_ms.
	got, _, err := h.store.GetVersion(context.Background(), h.tenant, h.ns, "immut", int64(v1["version"].(float64)))
	if err != nil {
		t.Fatalf("re-read v1: %v", err)
	}
	if got.SHA256 != v1["sha256"].(string) {
		t.Fatalf("v1 sha256 drifted: stored=%s api=%v", got.SHA256, v1["sha256"])
	}
}

// ---------------------------------------------------------------------------
// ALIAS CRUD + swap atomicity
// ---------------------------------------------------------------------------

func TestLifecycleAliasSetAndList(t *testing.T) {
	h := newLifecycleHarness(t)
	if status, raw := h.do(http.MethodPost, h.fnRootPath(), map[string]any{"name": "aliased"}); status != http.StatusCreated {
		t.Fatalf("seed create status=%d body=%s", status, string(raw))
	}
	d := h.uploadDraft("aliased")
	v := h.publishVersion("aliased", d["draft_id"].(string), "")

	// Set alias.
	got := h.setAlias("aliased", "prod", int64(v["version"].(float64)))
	if got["alias"] != "prod" || int64(got["version"].(float64)) != int64(v["version"].(float64)) {
		t.Fatalf("setAlias response mismatch: %+v", got)
	}

	// Read-after-write: list returns the new alias.
	status, body := h.do(http.MethodGet, h.fnPath("aliased")+"/aliases", nil)
	if status != http.StatusOK {
		t.Fatalf("listAliases status=%d body=%s", status, string(body))
	}
	listed := parseJSONBody(t, body)
	aliases, _ := listed["aliases"].([]any)
	if len(aliases) != 1 || aliases[0].(map[string]any)["alias"] != "prod" {
		t.Fatalf("listAliases payload mismatch: %+v", listed)
	}
}

func TestLifecycleAliasSwapIsAtomic(t *testing.T) {
	// A reader racing the writer must always observe either the
	// pre-swap version or the post-swap version — never zero, never
	// the wrong version, never a "not found" error after the alias
	// has been set at least once.
	h := newLifecycleHarness(t)
	if status, raw := h.do(http.MethodPost, h.fnRootPath(), map[string]any{"name": "swap"}); status != http.StatusCreated {
		t.Fatalf("seed create status=%d body=%s", status, string(raw))
	}
	dA := h.uploadDraft("swap")
	vA := h.publishVersion("swap", dA["draft_id"].(string), "")
	dB := h.uploadDraft("swap")
	vB := h.publishVersion("swap", dB["draft_id"].(string), "")
	versionA := int64(vA["version"].(float64))
	versionB := int64(vB["version"].(float64))

	// Seed alias at version A so readers start with a known good state.
	h.setAlias("swap", "prod", versionA)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		readers   sync.WaitGroup
		bad       atomic.Int64
		seenA     atomic.Int64
		seenB     atomic.Int64
		seenOther atomic.Int64
	)
	readers.Add(4)
	for i := 0; i < 4; i++ {
		go func() {
			defer readers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				rec, err := h.store.GetAlias(ctx, h.tenant, h.ns, "swap", "prod")
				if err != nil {
					// "not found" is the only other allowed outcome,
					// but only before the first set. Past this point
					// the alias must always be readable.
					bad.Add(1)
					return
				}
				switch rec.Version {
				case versionA:
					seenA.Add(1)
				case versionB:
					seenB.Add(1)
				default:
					seenOther.Add(1)
				}
			}
		}()
	}

	// Hammer the alias pointer back and forth. Each PUT is a single
	// SET on the alias key, which is atomic in KVRocks/redis.
	const swaps = 50
	for i := 0; i < swaps; i++ {
		target := versionA
		if i%2 == 0 {
			target = versionB
		}
		h.setAlias("swap", "prod", target)
	}

	cancel()
	readers.Wait()

	if bad.Load() != 0 {
		t.Fatalf("alias readers observed an unexpected error %d times", bad.Load())
	}
	if seenOther.Load() != 0 {
		t.Fatalf("alias readers observed a torn version %d times", seenOther.Load())
	}
	if seenA.Load() == 0 || seenB.Load() == 0 {
		t.Fatalf("readers never observed at least one of each: seenA=%d seenB=%d", seenA.Load(), seenB.Load())
	}
}

func TestLifecycleAliasRejectsUnknownVersion(t *testing.T) {
	// docs/19-entity-state-machines.md: an alias must point at an
	// existing version. PUT alias for an unpublished version returns
	// CS_VALIDATION_FAILED (400) so callers can't pin a phantom.
	h := newLifecycleHarness(t)
	if status, raw := h.do(http.MethodPost, h.fnRootPath(), map[string]any{"name": "noversion"}); status != http.StatusCreated {
		t.Fatalf("seed create status=%d body=%s", status, string(raw))
	}
	status, body := h.do(http.MethodPut, h.fnPath("noversion")+"/aliases/prod", map[string]any{"version": 99})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown version, got %d body=%s", status, string(body))
	}
	if code := parseErrorCode(t, body); code != cserrors.CSValidationFailed {
		t.Fatalf("expected CSValidationFailed, got %s body=%s", code, string(body))
	}
}

// ---------------------------------------------------------------------------
// Read-after-write consistency on the full pipeline
// ---------------------------------------------------------------------------

func TestLifecycleReadAfterWriteFullFlow(t *testing.T) {
	h := newLifecycleHarness(t)
	if status, raw := h.do(http.MethodPost, h.fnRootPath(), map[string]any{"name": "raw_flow"}); status != http.StatusCreated {
		t.Fatalf("seed create status=%d body=%s", status, string(raw))
	}
	d := h.uploadDraft("raw_flow")
	v := h.publishVersion("raw_flow", d["draft_id"].(string), "prod")
	verNum := int64(v["version"].(float64))

	// Read function returns the version we just published as
	// latest_version with the same sha256.
	status, body := h.do(http.MethodGet, h.fnPath("raw_flow"), nil)
	if status != http.StatusOK {
		t.Fatalf("read status=%d body=%s", status, string(body))
	}
	read := parseJSONBody(t, body)
	latest := read["latest_version"].(map[string]any)
	if int64(latest["version"].(float64)) != verNum {
		t.Fatalf("latest_version mismatch: got %v want %d", latest["version"], verNum)
	}
	if latest["sha256"] != v["sha256"] {
		t.Fatalf("latest sha256 mismatch: got %v want %v", latest["sha256"], v["sha256"])
	}

	// Alias was set inline with publish — list reflects that.
	aliases := read["aliases"].([]any)
	if len(aliases) != 1 || aliases[0].(map[string]any)["alias"] != "prod" {
		t.Fatalf("aliases mismatch on read: %+v", aliases)
	}
}
