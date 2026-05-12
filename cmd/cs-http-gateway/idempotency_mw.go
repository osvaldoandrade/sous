// Idempotency middleware for cs-http-gateway.
//
// This middleware enforces the contract described in docs/10-http-invoke.md:
//
//   - When a request carries an `Idempotency-Key` header, the gateway derives
//     a deterministic activation_id from (tenant, function-ref, key) and
//     consults the dedup store keyed by the same tuple plus a SHA-256
//     fingerprint of the request body.
//   - On a cache hit with the same body fingerprint, the original cached
//     response is replayed verbatim and the response carries the header
//     `X-CS-Idempotency-Replay: 1`.
//   - On a key reuse with a different body fingerprint, the gateway responds
//     with `409 CS_IDEMPOTENCY_CONFLICT` per docs/21-errors.md without
//     re-executing the function.
//   - When no prior record exists, the middleware wraps the response writer,
//     forwards to the next handler, and commits the captured response
//     (status, headers, body) on success so a subsequent retry replays it.
//
// The store contract lives in internal/idempotency and is implementation
// agnostic; in production it is backed by KVRocks, while tests use the
// in-memory implementation returned by idempotency.NewMemoryStore.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/idempotency"
	"github.com/osvaldoandrade/sous/internal/observability"
)

// idempotencyHeader is the request header that opts a call into dedup.
const idempotencyHeader = "Idempotency-Key"

// idempotencyReplayHeader is the response header set on cache hits so clients
// can distinguish a fresh execution from a replayed cached result.
const idempotencyReplayHeader = "X-CS-Idempotency-Replay"

// defaultIdempotencyTTL is used when no per-function timeout is available.
// Production deployments should override this via configuration; the value
// here mirrors the docs/10-http-invoke.md default of (function_timeout +
// 3600s) for a 1h function timeout floor.
const defaultIdempotencyTTL = time.Hour

// idempotencyKeyPattern enforces the wire format documented in
// docs/10-http-invoke.md ("Idempotency"): `[A-Za-z0-9_-]{8,128}`.
var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{8,128}$`)

// cachedResponse is the persisted payload for replayed idempotent calls.
// It is JSON-encoded into the idempotency.Store record so the contract stays
// implementation-agnostic.
type cachedResponse struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    []byte            `json:"body,omitempty"`
}

// idempotencyMiddleware returns a chi middleware that enforces the
// Idempotency-Key contract using the supplied store. ttl is the wall-clock
// TTL for new reservations; pass <=0 to use defaultIdempotencyTTL.
func idempotencyMiddleware(store idempotency.Store, ttl time.Duration) func(http.Handler) http.Handler {
	if ttl <= 0 {
		ttl = defaultIdempotencyTTL
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := strings.TrimSpace(r.Header.Get(idempotencyHeader))
			if key == "" || store == nil {
				next.ServeHTTP(w, r)
				return
			}
			if !idempotencyKeyPattern.MatchString(key) {
				cserrors.WriteHTTP(w,
					cserrors.New(cserrors.CSValidationFailed,
						"Idempotency-Key must match [A-Za-z0-9_-]{8,128}"),
					observability.RequestIDFromContext(r.Context()))
				return
			}

			tenant := chi.URLParam(r, "tenant")
			namespace := chi.URLParam(r, "namespace")
			function := chi.URLParam(r, "function")
			ref := chi.URLParam(r, "ref")
			// Buffer the body so downstream handlers can still read it after
			// the middleware computes the fingerprint.
			body, err := io.ReadAll(r.Body)
			if err != nil {
				cserrors.WriteHTTP(w,
					cserrors.New(cserrors.CSValidationFailed, "failed to read request body"),
					observability.RequestIDFromContext(r.Context()))
				return
			}
			_ = r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))

			storeKey := buildStoreKey(tenant, namespace, function, ref, key)
			fp := idempotency.Fingerprint(body)
			activationID := deriveActivationID(tenant, function, ref, key)

			reserved, err := store.Reserve(r.Context(), storeKey, activationID, fp, ttl)
			if err != nil {
				if errors.Is(err, idempotency.ErrConflict) {
					cserrors.WriteHTTP(w,
						cserrors.New(cserrors.CSIdempotencyConflict,
							"Idempotency-Key reused with a different request body"),
						observability.RequestIDFromContext(r.Context()))
					return
				}
				cserrors.WriteHTTP(w, err, observability.RequestIDFromContext(r.Context()))
				return
			}

			if !reserved.Created && reserved.Record.Terminal {
				replayCachedResponse(w, reserved.Record.Result)
				return
			}

			// Wrap the response writer so we can capture the bytes the
			// downstream handler writes and persist them on success.
			cw := &capturingWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(cw, r)

			// Skip commit on framework error paths so a retried client can
			// still execute and produce a real terminal result.
			if cw.status >= 500 {
				return
			}

			cached := cachedResponse{
				Status:  cw.status,
				Headers: filteredHeaders(cw.Header()),
				Body:    cw.body.Bytes(),
			}
			payload, err := json.Marshal(cached)
			if err != nil {
				return
			}
			_ = store.Commit(r.Context(), storeKey, fp, payload, "")
		})
	}
}

// buildStoreKey is the canonical dedup key for the persistent store. It
// composes the full (tenant, function-ref, key) identity so two tenants or
// two functions cannot collide on the same user-supplied Idempotency-Key.
func buildStoreKey(tenant, namespace, function, ref, key string) string {
	return "idem:" + tenant + ":" + namespace + ":" + function + ":" + ref + ":" + key
}

// replayCachedResponse writes a previously captured response back to the
// client and tags it with the replay header so observers can distinguish
// fresh vs. replayed activations.
func replayCachedResponse(w http.ResponseWriter, payload []byte) {
	var cached cachedResponse
	if err := json.Unmarshal(payload, &cached); err != nil {
		// Fall through to a generic 500 rather than crash the gateway.
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSRuntimeException, "cached response corrupt"), "")
		return
	}
	for k, v := range cached.Headers {
		w.Header().Set(k, v)
	}
	w.Header().Set(idempotencyReplayHeader, "1")
	status := cached.Status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if len(cached.Body) > 0 {
		_, _ = w.Write(cached.Body)
	}
}

// filteredHeaders copies the captured response headers minus any that would
// be meaningless or harmful to replay (e.g. per-request correlation IDs).
func filteredHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, vs := range h {
		if strings.EqualFold(k, "X-Request-Id") || strings.EqualFold(k, idempotencyReplayHeader) {
			continue
		}
		if len(vs) > 0 {
			out[k] = strings.Join(vs, ",")
		}
	}
	return out
}

// capturingWriter buffers status + body so the middleware can persist the
// terminal response for later replay. It still flushes everything to the
// real ResponseWriter so the original caller observes a normal response.
type capturingWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
	body        bytes.Buffer
}

func (c *capturingWriter) WriteHeader(status int) {
	if c.wroteHeader {
		return
	}
	c.status = status
	c.wroteHeader = true
	c.ResponseWriter.WriteHeader(status)
}

func (c *capturingWriter) Write(b []byte) (int, error) {
	if !c.wroteHeader {
		c.WriteHeader(http.StatusOK)
	}
	c.body.Write(b)
	return c.ResponseWriter.Write(b)
}
