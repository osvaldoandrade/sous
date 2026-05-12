// Package sbom emits CycloneDX 1.5 JSON Software Bills of Materials for
// published function versions.
//
// The producer is intentionally hand-rolled on top of encoding/json so
// the platform takes on no third-party dependency for an output that
// belongs in regulated tenants' supply-chain audits. Determinism is the
// load-bearing property: every component slice is sorted by `bom-ref`
// before encoding, every map keyset is alphabetised, and the document
// `serialNumber` is derived from the bundle's `sha256` so replaying a
// publish for the same canonical bundle yields a byte-identical SBOM.
//
// The SBOM lists, at minimum:
//
//   - the `cs-js` runtime declared by the bundle manifest,
//   - every file that appears in the bundle (sha256, byte length),
//   - the signing identity recorded on the version, when present, and
//   - any import-map components recorded on the version metadata when
//     E5.01 ships them (see Input.Imports).
//
// See docs/15-security.md "Supply chain artifacts" and
// docs/04-api-rest.md "Functions" for the public contract.
package sbom

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/osvaldoandrade/sous/internal/api"
)

// SpecVersion is the CycloneDX specification version emitted by Build.
// We pin to 1.5 deliberately: the schema is widely supported by
// downstream scanners and is the version called out in the E5.03 issue.
const SpecVersion = "1.5"

// ContentType is the IANA-registered media type for CycloneDX JSON. It
// is returned by the HTTP SBOM endpoint and embedded in client tooling
// expectations.
const ContentType = "application/vnd.cyclonedx+json; version=1.5"

// Import describes a single dependency declared by the bundle manifest,
// typically populated from the import map shipped with E5.01. It is
// intentionally permissive: callers that have only a name and version
// can leave Integrity and Source empty.
type Import struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	Integrity string `json:"integrity,omitempty"`
	Source    string `json:"source,omitempty"`
}

// SigningIdentity records the bundle signer attestation captured by
// E5.02. All fields are optional so unsigned bundles still produce a
// valid SBOM.
type SigningIdentity struct {
	KID         string `json:"kid,omitempty"`
	Algorithm   string `json:"algorithm,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
}

// Input is the resolved view of a published version that the SBOM
// emitter needs. Callers construct it from the parsed bundle and the
// stored VersionRecord/manifest; the SBOM emitter never reaches back
// into KV.
type Input struct {
	Tenant    string
	Namespace string
	Function  string
	Version   int64

	// Runtime identifies the runtime the bundle targets (e.g. "cs-js").
	// Combined with RuntimeVersion it becomes the "application" or
	// "framework" component listed in the SBOM.
	Runtime        string
	RuntimeVersion string

	// BundleSHA256 is the canonical bundle digest written into version
	// metadata and serves as the SBOM's deterministic anchor.
	BundleSHA256 string
	// BundleSize is the byte size of the canonical bundle archive. It
	// is reported as a property so downstream tooling can pre-allocate
	// scanners without re-fetching the bundle.
	BundleSize int

	// Files are the named bytes that compose the bundle, after canonical
	// extraction. Each file becomes a CycloneDX `file` component with
	// its own sha256 hash.
	Files map[string][]byte

	// Imports are dep declarations harvested from the manifest's import
	// map (populated by E5.01). When empty the SBOM lists only the file
	// components plus the runtime.
	Imports []Import

	// Signer is the bundle signer attestation surfaced by E5.02. Nil
	// signals an unsigned bundle and is encoded as an empty
	// `properties` entry rather than an error.
	Signer *SigningIdentity
}

// Build returns the canonical CycloneDX 1.5 JSON document for the
// supplied input. The output is deterministic: the same input bytes
// always produce the same SBOM bytes.
func Build(in Input) ([]byte, error) {
	if in.BundleSHA256 == "" {
		return nil, fmt.Errorf("sbom: BundleSHA256 is required")
	}
	if in.Runtime == "" {
		in.Runtime = "cs-js"
	}

	doc := newDocument(in)

	// Encode with MarshalIndent so the document is human-inspectable
	// (compliance reviewers commonly diff SBOMs by eye). The 2-space
	// indent matches the convention used by ./docs/25-schemas.md
	// examples; encoding/json walks struct fields in declaration order
	// so determinism is preserved.
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("sbom: marshal: %w", err)
	}
	return out, nil
}

// RuntimeFromManifest is a small helper that pulls the runtime and
// declared limits off of a parsed manifest so callers don't have to
// reach into FunctionManifest themselves.
func RuntimeFromManifest(m api.FunctionManifest) (runtime, version string) {
	// The manifest schema does not yet carry a runtime version string;
	// we report the manifest schema as the version so the SBOM still
	// pins a concrete identifier. When E3.x exposes a runtime version
	// the schema fallback can be replaced without an SBOM consumer
	// migration because the field is a free-form string.
	if m.Runtime == "" {
		return "cs-js", m.Schema
	}
	return m.Runtime, m.Schema
}

// document is the CycloneDX 1.5 root encoded into JSON. Field names
// follow the schema literally; "json:omitempty" is avoided for
// load-bearing fields so the document validates against canned
// CycloneDX parsers.
type document struct {
	BOMFormat    string       `json:"bomFormat"`
	SpecVersion  string       `json:"specVersion"`
	SerialNumber string       `json:"serialNumber"`
	Version      int          `json:"version"`
	Metadata     metadata     `json:"metadata"`
	Components   []component  `json:"components"`
	Dependencies []dependency `json:"dependencies,omitempty"`
}

type metadata struct {
	Timestamp  string      `json:"timestamp,omitempty"`
	Tools      []tool      `json:"tools,omitempty"`
	Component  *component  `json:"component,omitempty"`
	Properties []propertyV `json:"properties,omitempty"`
}

type tool struct {
	Vendor  string `json:"vendor,omitempty"`
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

type component struct {
	Type        string      `json:"type"`
	BOMRef      string      `json:"bom-ref"`
	Name        string      `json:"name"`
	Version     string      `json:"version,omitempty"`
	Description string      `json:"description,omitempty"`
	PURL        string      `json:"purl,omitempty"`
	Hashes      []hash      `json:"hashes,omitempty"`
	ExternalRef []extRef    `json:"externalReferences,omitempty"`
	Properties  []propertyV `json:"properties,omitempty"`
}

type hash struct {
	Alg     string `json:"alg"`
	Content string `json:"content"`
}

type extRef struct {
	Type string `json:"type"`
	URL  string `json:"url"`
}

type propertyV struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type dependency struct {
	Ref       string   `json:"ref"`
	DependsOn []string `json:"dependsOn,omitempty"`
}

// newDocument assembles the CycloneDX document for an Input. Helpers
// below own the per-section work so this function stays readable.
func newDocument(in Input) document {
	rootRef := rootBOMRef(in)
	root := rootComponent(in, rootRef)

	components := []component{runtimeComponent(in)}
	components = append(components, fileComponents(in)...)
	components = append(components, importComponents(in)...)
	sort.Slice(components, func(i, j int) bool { return components[i].BOMRef < components[j].BOMRef })

	// Collect the bom-refs the root depends on so downstream tools can
	// render the dependency tree without re-parsing the component list.
	deps := []dependency{{Ref: rootRef, DependsOn: bomRefs(components)}}

	return document{
		BOMFormat:    "CycloneDX",
		SpecVersion:  SpecVersion,
		SerialNumber: serialNumber(in.BundleSHA256),
		Version:      1,
		Metadata: metadata{
			Tools: []tool{{
				Vendor: "sous",
				Name:   "cs-control",
			}},
			Component:  &root,
			Properties: rootProperties(in),
		},
		Components:   components,
		Dependencies: deps,
	}
}

// rootBOMRef is the bom-ref of the version-as-component (the "root"
// dependency in CycloneDX). Using a stable URN keeps the SBOM byte
// identical across replays.
func rootBOMRef(in Input) string {
	return fmt.Sprintf("urn:cs:fn:%s:%s:%s:v%d", in.Tenant, in.Namespace, in.Function, in.Version)
}

// rootComponent describes the published version itself. The bundle
// hash lives both on the component (as a CycloneDX hash) and in the
// document-level properties so downstream tooling can find it via
// either path.
func rootComponent(in Input, ref string) component {
	c := component{
		Type:    "application",
		BOMRef:  ref,
		Name:    fmt.Sprintf("%s/%s/%s", in.Tenant, in.Namespace, in.Function),
		Version: fmt.Sprintf("%d", in.Version),
		Hashes: []hash{
			{Alg: "SHA-256", Content: in.BundleSHA256},
		},
	}
	if in.BundleSize > 0 {
		c.Properties = append(c.Properties, propertyV{
			Name:  "cs:bundle.size_bytes",
			Value: fmt.Sprintf("%d", in.BundleSize),
		})
	}
	return c
}

// runtimeComponent reports the runtime declared by the bundle manifest.
// The CycloneDX type is "framework" because the runtime is shared
// infrastructure rather than a vendored library.
func runtimeComponent(in Input) component {
	return component{
		Type:    "framework",
		BOMRef:  "urn:cs:runtime:" + in.Runtime,
		Name:    in.Runtime,
		Version: in.RuntimeVersion,
	}
}

// fileComponents emits one CycloneDX `file` component per bundled
// artefact (function.js, manifest.json, plus any auxiliary files). The
// slice is sorted by file name so the resulting BOM is deterministic
// regardless of the iteration order of the input map.
func fileComponents(in Input) []component {
	names := make([]string, 0, len(in.Files))
	for name := range in.Files {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]component, 0, len(names))
	for _, name := range names {
		data := in.Files[name]
		sum := sha256.Sum256(data)
		out = append(out, component{
			Type:   "file",
			BOMRef: "urn:cs:file:" + name,
			Name:   name,
			Hashes: []hash{
				{Alg: "SHA-256", Content: hex.EncodeToString(sum[:])},
			},
			Properties: []propertyV{
				{Name: "cs:file.size_bytes", Value: fmt.Sprintf("%d", len(data))},
			},
		})
	}
	return out
}

// importComponents emits one CycloneDX `library` per import-map entry
// recorded on the version. Each declares its SRI hash (when present)
// and the registry URL it was fetched from so a downstream scanner can
// re-verify the artifact.
func importComponents(in Input) []component {
	if len(in.Imports) == 0 {
		return nil
	}
	imports := append([]Import(nil), in.Imports...)
	sort.Slice(imports, func(i, j int) bool {
		if imports[i].Name == imports[j].Name {
			return imports[i].Version < imports[j].Version
		}
		return imports[i].Name < imports[j].Name
	})
	out := make([]component, 0, len(imports))
	for _, imp := range imports {
		c := component{
			Type:    "library",
			BOMRef:  fmt.Sprintf("urn:cs:dep:%s@%s", imp.Name, imp.Version),
			Name:    imp.Name,
			Version: imp.Version,
		}
		if imp.Integrity != "" {
			c.Properties = append(c.Properties, propertyV{
				Name:  "cs:import.integrity",
				Value: imp.Integrity,
			})
		}
		if imp.Source != "" {
			c.ExternalRef = append(c.ExternalRef, extRef{Type: "distribution", URL: imp.Source})
		}
		out = append(out, c)
	}
	return out
}

// rootProperties surfaces the bundle digest and signing identity at
// the document metadata level so consumers do not have to scan the
// components list for the supply-chain facts the SBOM exists to
// publish. Property names are kebab-case under the `cs:` namespace to
// avoid collisions with other CycloneDX producers.
func rootProperties(in Input) []propertyV {
	props := []propertyV{
		{Name: "cs:bundle.sha256", Value: in.BundleSHA256},
	}
	if in.Signer != nil {
		if in.Signer.KID != "" {
			props = append(props, propertyV{Name: "cs:signing.kid", Value: in.Signer.KID})
		}
		if in.Signer.Algorithm != "" {
			props = append(props, propertyV{Name: "cs:signing.algorithm", Value: in.Signer.Algorithm})
		}
		if in.Signer.Fingerprint != "" {
			props = append(props, propertyV{Name: "cs:signing.fingerprint", Value: in.Signer.Fingerprint})
		}
	}
	// Properties are written in insertion order, which is already
	// deterministic because we always populate them in the same order.
	// Sorting defensively guards against future contributors changing
	// the order silently.
	sort.SliceStable(props, func(i, j int) bool { return props[i].Name < props[j].Name })
	return props
}

// bomRefs returns the bom-ref of each component, used to populate the
// root's dependsOn list. Components are already sorted by bom-ref so
// the slice is deterministic.
func bomRefs(components []component) []string {
	out := make([]string, 0, len(components))
	for _, c := range components {
		out = append(out, c.BOMRef)
	}
	return out
}

// serialNumber derives a stable URN UUID from the bundle digest. The
// CycloneDX spec calls for `urn:uuid:<v4>`; we deterministically
// synthesise a v4-shaped UUID from the first 16 bytes of the bundle
// sha256 so re-publishes produce byte-identical SBOMs. The bundle
// digest is itself unique per canonical content, so collision is
// equivalent to a sha256 collision.
func serialNumber(bundleSHA256 string) string {
	raw, err := hex.DecodeString(bundleSHA256)
	if err != nil || len(raw) < 16 {
		// Fall back to a static URN derived from the raw hex if we ever
		// receive a malformed digest; this keeps Build infallible from
		// the caller's perspective.
		return "urn:uuid:" + fallbackUUID(bundleSHA256)
	}
	var b [16]byte
	copy(b[:], raw[:16])
	// Set version (4) and variant (RFC 4122) bits so the bytes parse as
	// a canonical UUID.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("urn:uuid:%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// fallbackUUID synthesises a placeholder UUID-shaped string from any
// input. Used only when the bundle digest is unexpectedly malformed.
func fallbackUUID(s string) string {
	sum := sha256.Sum256([]byte(s))
	var b [16]byte
	copy(b[:], sum[:16])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
