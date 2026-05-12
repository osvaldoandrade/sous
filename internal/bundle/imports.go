// Package bundle frozen-import-map support (roadmap task E5.01).
//
// The cs-js runtime has no build step and no network egress at invoke
// time. To support bare-specifier imports (e.g. `import { z } from "zod"`)
// the control plane resolves a publisher-declared dependency map at
// publish time and freezes the bytes into the canonical tar bundle under
// the `deps/` subtree. A generated `import-map.json` records the mapping
// from bare specifier to frozen path plus an SRI digest of the bytes the
// runtime can verify before loading.
//
// This file owns:
//
//   - Resolver: fetches remote deps from a curated allowlist or copies
//     local-path deps from the uploaded files; produces a sealed copy of
//     the file set the canonical bundle is built from.
//   - FrozenImportMap / FrozenImport: the on-disk shape written into
//     `import-map.json`.
//   - LoadImportMap: read-side helper used by the runtime.
//
// See docs/08-runtime-cs-js.md (Imports section) and docs/15-security.md
// (curated mirror allowlist).
package bundle

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
)

// ImportMapFileName is the canonical filename inside the bundle that
// stores the frozen specifier → path/integrity mapping.
const ImportMapFileName = "import-map.json"

// DepsPrefix is the tar subtree under which all frozen dependency bytes
// live. The runtime refuses to load anything else as an import target.
const DepsPrefix = "deps/"

// defaultPerImportMaxBytes is the safety cap applied to any single frozen
// dependency. It prevents a single huge URL from filling the bundle even
// before the 16 MiB total-bundle cap kicks in. Callers can override via
// ResolverOptions.MaxBytesPerImport.
const defaultPerImportMaxBytes = 4 * 1024 * 1024 // 4 MiB

// defaultResolveTimeout caps per-import HTTP fetches. It is small on
// purpose: publish is a control-plane operation and must not block on a
// slow mirror.
const defaultResolveTimeout = 10 * time.Second

// FrozenImport is the on-disk record for a single resolved dependency.
// Path is the in-bundle path (always prefixed with DepsPrefix). Integrity
// is the canonical SRI digest of the frozen bytes (sha256-<base64>) the
// runtime verifies before loading; SizeBytes is the resolved size, kept
// for observability and for cheap pre-flight checks.
type FrozenImport struct {
	Path      string `json:"path"`
	Integrity string `json:"integrity"`
	SizeBytes int    `json:"size_bytes"`
}

// FrozenImportMap is the on-disk shape of import-map.json. Schema is a
// fixed string so the runtime can refuse anything it does not understand.
type FrozenImportMap struct {
	Schema  string                  `json:"schema"`
	Imports map[string]FrozenImport `json:"imports"`
}

const importMapSchema = "cs.importmap.v1"

// ResolverOptions captures the publish-time policy the control plane
// applies when freezing imports. AllowedMirrors is the set of hostnames
// (case-insensitive, no port) the resolver may fetch from over HTTPS or
// HTTP. An empty AllowedMirrors disables remote fetching entirely; only
// `path:` imports work in that mode. MaxBytesPerImport defaults to 4 MiB
// when zero. Timeout defaults to 10s when zero.
type ResolverOptions struct {
	AllowedMirrors    []string
	MaxBytesPerImport int
	Timeout           time.Duration
	HTTPClient        *http.Client
}

// Resolver freezes a publisher-declared imports map into a set of files
// the canonical bundle builder can consume.
type Resolver struct {
	opts   ResolverOptions
	client *http.Client
	allow  map[string]struct{}
}

// NewResolver constructs a Resolver from options. AllowedMirrors is
// normalised to lowercase hostnames; the resolver consults this set on
// every URL fetch.
func NewResolver(opts ResolverOptions) *Resolver {
	if opts.MaxBytesPerImport <= 0 {
		opts.MaxBytesPerImport = defaultPerImportMaxBytes
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultResolveTimeout
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: opts.Timeout}
	}
	allow := make(map[string]struct{}, len(opts.AllowedMirrors))
	for _, host := range opts.AllowedMirrors {
		h := strings.ToLower(strings.TrimSpace(host))
		if h != "" {
			allow[h] = struct{}{}
		}
	}
	return &Resolver{opts: opts, client: client, allow: allow}
}

// ResolveResult is the output of Resolver.Resolve. Files contains the
// original uploaded bundle files plus the resolved dep bytes under
// `deps/` and the generated `import-map.json`. ImportMap is the same map
// serialised into `import-map.json`, returned for caller diagnostics.
type ResolveResult struct {
	Files     map[string][]byte
	ImportMap FrozenImportMap
}

// Resolve applies the manifest's Imports field to the uploaded files and
// returns a sealed copy of the file set with frozen deps and a generated
// import-map.json. When the manifest has no imports the input files are
// returned unchanged (with an empty FrozenImportMap), so the v0.1
// single-file path is preserved bit-for-bit.
//
// Errors are returned as descriptive Go errors so the caller (cs-control
// publishVersion) can wrap them with a typed CS_VALIDATION_* code. A
// disallowed mirror, an SRI mismatch, or a missing local path each
// produce a distinct error suitable for surfacing to the publishing
// agent.
func (r *Resolver) Resolve(ctx context.Context, manifest api.FunctionManifest, files map[string][]byte) (ResolveResult, error) {
	out := ResolveResult{
		Files:     copyFiles(files),
		ImportMap: FrozenImportMap{Schema: importMapSchema, Imports: map[string]FrozenImport{}},
	}
	if len(manifest.Imports) == 0 {
		return out, nil
	}

	// Stable order so canonical bundle sha256 is deterministic for the
	// same input regardless of map iteration order.
	specs := make([]string, 0, len(manifest.Imports))
	for k := range manifest.Imports {
		specs = append(specs, k)
	}
	sort.Strings(specs)

	for _, spec := range specs {
		imp := manifest.Imports[spec]
		data, err := r.fetchOne(ctx, spec, imp, files)
		if err != nil {
			return ResolveResult{}, err
		}
		integrity, err := canonicalSRI(imp.Integrity, data)
		if err != nil {
			return ResolveResult{}, fmt.Errorf("imports[%q]: %w", spec, err)
		}
		depPath := DepsPrefix + safeDepFileName(spec)
		// Refuse a path collision: the publisher cannot smuggle two
		// specifiers into the same in-bundle file.
		if _, exists := out.Files[depPath]; exists {
			return ResolveResult{}, fmt.Errorf("imports[%q]: dep path %q collides with an existing file", spec, depPath)
		}
		out.Files[depPath] = data
		out.ImportMap.Imports[spec] = FrozenImport{
			Path:      depPath,
			Integrity: integrity,
			SizeBytes: len(data),
		}
	}

	encoded, err := json.MarshalIndent(out.ImportMap, "", "  ")
	if err != nil {
		return ResolveResult{}, fmt.Errorf("encode import-map.json: %w", err)
	}
	out.Files[ImportMapFileName] = encoded
	return out, nil
}

// fetchOne resolves a single import entry into raw bytes. URL entries are
// fetched over HTTP from an allowlisted host; Path entries are pulled
// from the uploaded files.
func (r *Resolver) fetchOne(ctx context.Context, spec string, imp api.ManifestImport, files map[string][]byte) ([]byte, error) {
	hasURL := strings.TrimSpace(imp.URL) != ""
	hasPath := strings.TrimSpace(imp.Path) != ""
	if hasURL == hasPath {
		return nil, fmt.Errorf("imports[%q]: exactly one of url or path is required", spec)
	}
	if hasPath {
		data, ok := files[imp.Path]
		if !ok {
			return nil, fmt.Errorf("imports[%q]: path %q not found in uploaded files", spec, imp.Path)
		}
		if len(data) > r.opts.MaxBytesPerImport {
			return nil, fmt.Errorf("imports[%q]: dep exceeds per-import cap %d bytes", spec, r.opts.MaxBytesPerImport)
		}
		// Copy so the caller cannot mutate the original slice via the
		// returned map; this keeps the resolver pure.
		clone := make([]byte, len(data))
		copy(clone, data)
		return clone, nil
	}
	u, err := url.Parse(imp.URL)
	if err != nil {
		return nil, fmt.Errorf("imports[%q]: invalid url: %w", spec, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("imports[%q]: only http/https URLs are allowed", spec)
	}
	host := strings.ToLower(u.Hostname())
	if _, ok := r.allow[host]; !ok {
		return nil, fmt.Errorf("imports[%q]: mirror host %q not in publish.imports.allowed_mirrors", spec, host)
	}
	fetchCtx, cancel := context.WithTimeout(ctx, r.opts.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, imp.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("imports[%q]: build request: %w", spec, err)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("imports[%q]: fetch %s: %w", spec, imp.URL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("imports[%q]: mirror returned status %d", spec, resp.StatusCode)
	}
	// LimitReader is a hard cap; we add +1 so we can detect oversize
	// without buffering the full payload.
	limit := int64(r.opts.MaxBytesPerImport) + 1
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return nil, fmt.Errorf("imports[%q]: read body: %w", spec, err)
	}
	if int64(len(body)) > int64(r.opts.MaxBytesPerImport) {
		return nil, fmt.Errorf("imports[%q]: dep exceeds per-import cap %d bytes", spec, r.opts.MaxBytesPerImport)
	}
	return body, nil
}

// canonicalSRI returns the canonical SRI digest for the supplied bytes.
// If the publisher provided their own integrity hint, the hint is
// recomputed and compared; on mismatch a descriptive error is returned so
// the publishing agent can fix the manifest.
func canonicalSRI(claimed string, data []byte) (string, error) {
	digestSHA256 := sha256.Sum256(data)
	canonical := "sha256-" + base64.StdEncoding.EncodeToString(digestSHA256[:])
	if claimed == "" {
		return canonical, nil
	}
	switch {
	case strings.HasPrefix(claimed, "sha256-"):
		if claimed != canonical {
			return "", errors.New("integrity hash mismatch (sha256)")
		}
		return canonical, nil
	case strings.HasPrefix(claimed, "sha384-"):
		digest384 := sha512.Sum384(data)
		expected := "sha384-" + base64.StdEncoding.EncodeToString(digest384[:])
		if claimed != expected {
			return "", errors.New("integrity hash mismatch (sha384)")
		}
		// Always return the canonical sha256 digest as well so the
		// runtime has a single algorithm to verify against. We embed
		// the publisher's sha384 plus the canonical sha256 separated
		// by a space, matching the SRI spec for multi-algorithm
		// expressions.
		return expected + " " + canonical, nil
	default:
		return "", errors.New("unsupported integrity algorithm")
	}
}

// safeDepFileName turns a bare specifier into a stable, filesystem-safe
// path under DepsPrefix. We replace anything that is not [a-z0-9._-] with
// "_" and force a ".js" extension if the specifier does not already
// carry one — the cs-js runtime only loads JS.
func safeDepFileName(spec string) string {
	cleaned := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '.' || r == '-' || r == '_':
			return r
		default:
			return '_'
		}
	}, spec)
	if cleaned == "" {
		cleaned = "dep"
	}
	if path.Ext(cleaned) == "" {
		cleaned += ".js"
	}
	return cleaned
}

func copyFiles(in map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(in))
	for k, v := range in {
		clone := make([]byte, len(v))
		copy(clone, v)
		out[k] = clone
	}
	return out
}

// LoadImportMap extracts the frozen import map from a parsed bundle. It
// returns an empty map (and no error) when the bundle has no
// import-map.json — this is the v0.1 single-file path that must continue
// to work. The schema field is verified so the runtime fails loudly on
// an unexpected map shape.
func LoadImportMap(files map[string][]byte) (FrozenImportMap, error) {
	raw, ok := files[ImportMapFileName]
	if !ok {
		return FrozenImportMap{Schema: importMapSchema, Imports: map[string]FrozenImport{}}, nil
	}
	var m FrozenImportMap
	if err := json.Unmarshal(raw, &m); err != nil {
		return FrozenImportMap{}, fmt.Errorf("decode import-map.json: %w", err)
	}
	if m.Schema != importMapSchema {
		return FrozenImportMap{}, fmt.Errorf("unsupported import-map schema %q", m.Schema)
	}
	if m.Imports == nil {
		m.Imports = map[string]FrozenImport{}
	}
	return m, nil
}

// VerifyImportBytes returns nil when the bytes match the frozen
// integrity digest. Used by the runtime before handing dep bytes to the
// isolate. Always returns an explicit error so the caller can map it to
// CS_IMPORT_NOT_FOUND or a more specific runtime error.
func VerifyImportBytes(integrity string, data []byte) error {
	if integrity == "" {
		return errors.New("missing integrity")
	}
	for _, hash := range strings.Fields(integrity) {
		switch {
		case strings.HasPrefix(hash, "sha256-"):
			digest := sha256.Sum256(data)
			if "sha256-"+base64.StdEncoding.EncodeToString(digest[:]) == hash {
				return nil
			}
		case strings.HasPrefix(hash, "sha384-"):
			digest := sha512.Sum384(data)
			if "sha384-"+base64.StdEncoding.EncodeToString(digest[:]) == hash {
				return nil
			}
		}
	}
	return errors.New("integrity verification failed")
}
