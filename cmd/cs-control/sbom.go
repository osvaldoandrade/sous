package main

// SBOM handler for cs-control.
//
// E5.03 (#15) requires every published version to carry a CycloneDX 1.5
// Software Bill of Materials so regulated tenants can answer "what is
// inside this version, and who signed it?" The publish path writes the
// SBOM into KVRocks (see emitSBOMOnPublish below) and this file exposes
// the read endpoint:
//
//   GET /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/versions/{version}/sbom
//
// Authorization reuses the existing cs:function:read role check so the
// same callers that can introspect a version metadata record can also
// fetch its SBOM. Missing versions return 404 (CS_VALIDATION_FAILED)
// rather than a synthesized empty body so audit tools can distinguish
// "version exists but pre-SBOM" (re-publish to backfill) from "version
// does not exist".

import (
	"context"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
	"github.com/osvaldoandrade/sous/internal/sbom"
)

// getVersionSBOM serves the persisted CycloneDX 1.5 SBOM for a
// published version. The response is the raw JSON document with the
// IANA CycloneDX content type so downstream scanners can stream the
// body directly into their parsers.
func (s *server) getVersionSBOM(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, name, ok := s.authorize(w, r, "cs:function:read")
	if !ok {
		return
	}
	version, err := strconv.ParseInt(chi.URLParam(r, "version"), 10, 64)
	if err != nil || version <= 0 {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "invalid version"), requestID(r))
		return
	}
	raw, err := s.store.GetSBOM(r.Context(), tenant, namespace, name, version)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	w.Header().Set("Content-Type", sbom.ContentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

// emitSBOMOnPublish builds the CycloneDX SBOM for a freshly published
// version and persists it next to the version record. Errors are
// returned so the caller can fail the publish — an SBOM that cannot be
// produced means the supply-chain contract is silently broken, and
// regulated tenants would rather see a publish fail than discover the
// gap during an audit.
//
// files is the canonical, already-decoded file map the publish handler
// built from the draft; passing it directly avoids a redundant tar
// extraction on the publish hot path. E5.01 (#12) will extend the
// manifest with an import map — once that lands its entries should be
// translated into sbom.Input.Imports without changing this signature.
func emitSBOMOnPublish(
	ctx context.Context,
	store persistence.Provider,
	tenant, namespace, function string,
	version int64,
	manifest api.FunctionManifest,
	files map[string][]byte,
	bundleSize int,
	bundleSHA256 string,
) error {
	runtime, runtimeVersion := sbom.RuntimeFromManifest(manifest)
	doc, err := sbom.Build(sbom.Input{
		Tenant:         tenant,
		Namespace:      namespace,
		Function:       function,
		Version:        version,
		Runtime:        runtime,
		RuntimeVersion: runtimeVersion,
		BundleSHA256:   bundleSHA256,
		BundleSize:     bundleSize,
		Files:          files,
	})
	if err != nil {
		return cserrors.Wrap(cserrors.CSValidationFailed, "failed to build sbom", err)
	}
	return store.PutSBOM(ctx, tenant, namespace, function, version, doc)
}
