// Package determinism implements the publish-time static linter that
// blocks nondeterministic API usage in Cadence workflow bundles.
//
// Workflow code is replayed against its history on every Decision, so
// any call that observes wall-clock time, system entropy, async timers
// or unmediated network IO will corrupt replay state and surface as a
// nondeterministic-history failure hours after the offending publish.
// cs-control runs ScanWorkflow against the JS source files of any
// bundle whose manifest declares cadence.kind == "workflow" and rejects
// the publish with CS_WORKFLOW_NON_DETERMINISTIC when violations are
// found. See docs/12-cadence-integration.md "Determinism rules" and
// roadmap task E8.03.
//
// v0.1 is a text-scan over function.js plus any deps/*.js files using
// pre-compiled regular expressions. A full JS AST walk (via goja) is
// intentionally out of scope here — operators on the publish hot path
// would pay parser cost for every publish, and the false-negative rate
// of a careful regex set is acceptable for the v0.1 surface. Authors
// can opt out of a single violation by suffixing the call site with a
// `// cs-determinism-allow` comment (operator review at code-review
// time is the override gate).
package determinism

import (
	"regexp"
	"sort"
	"strings"
)

// Violation describes one banned-API call site found by ScanWorkflow.
//
// File is the bundle-relative file name (e.g., "function.js" or
// "deps/foo.js"). Line and Column are 1-indexed, computed against the
// raw bytes of the file. Pattern is the human-readable name of the
// banned identifier (e.g., "Date.now", "Math.random"); Message is the
// remediation hint the publisher sees in the error envelope.
type Violation struct {
	File    string `json:"file"`
	Line    int    `json:"line"`
	Column  int    `json:"column"`
	Pattern string `json:"pattern"`
	Message string `json:"message"`
}

// bannedPattern is one entry in the banned-API table. The pre-compiled
// regexp is matched against every JS file in the bundle; matches that
// fall inside an opt-out comment are skipped. Keeping pattern + name +
// message together makes the table self-documenting and lets us extend
// the list (e.g., wire `crypto.randomUUID` once we agree on the
// remediation) without touching the scanner code.
type bannedPattern struct {
	name    string
	re      *regexp.Regexp
	message string
}

// bannedPatterns is the v0.1 banned-API table. The order is stable so
// violation reports are deterministic across runs (helpful when
// comparing publish-time output between CI and a local check).
//
// Notes on the patterns:
//   - `Date.now\b` and `Math.random\b` catch the canonical wall-clock /
//     entropy reads. `\b` anchors stop us flagging `Math.random123`.
//   - `\bsetTimeout\b` and friends catch direct calls. They intentionally
//     do not require an opening paren so authors get a hit on
//     `const t = setTimeout;` too — workflow code should not pin
//     references to those globals.
//   - `new Date\(\s*\)` matches only the no-arg constructor. Calls like
//     `new Date(history.startedAt)` are deterministic when the argument
//     comes from history.
//   - `\bfetch\b\s*\(` is the bare global; the runtime exposes
//     `cs.http.fetch` which is mediated and therefore allowed.
var bannedPatterns = []bannedPattern{
	{
		name:    "Date.now",
		re:      regexp.MustCompile(`Date\.now\b`),
		message: "Date.now() reads the wall clock; use cs.workflow.now() inside workflows.",
	},
	{
		name:    "Math.random",
		re:      regexp.MustCompile(`Math\.random\b`),
		message: "Math.random() is nondeterministic; use cs.workflow.sideEffect for entropy.",
	},
	{
		name:    "setTimeout",
		re:      regexp.MustCompile(`\bsetTimeout\b`),
		message: "setTimeout schedules real-time callbacks; use cs.workflow.sleep instead.",
	},
	{
		name:    "setInterval",
		re:      regexp.MustCompile(`\bsetInterval\b`),
		message: "setInterval schedules real-time callbacks; model recurrence via the workflow loop.",
	},
	{
		name:    "new Date()",
		re:      regexp.MustCompile(`new Date\(\s*\)`),
		message: "new Date() with no arguments reads the wall clock; use cs.workflow.now() instead.",
	},
	{
		name:    "fetch",
		re:      regexp.MustCompile(`\bfetch\b\s*\(`),
		message: "bare fetch() performs unmediated network IO; use cs.http.fetch via an Activity.",
	},
	{
		name:    "setImmediate",
		re:      regexp.MustCompile(`\bsetImmediate\b`),
		message: "setImmediate yields to the host event loop; workflows must be synchronous between awaits.",
	},
	{
		name:    "performance.now",
		re:      regexp.MustCompile(`\bperformance\.now\b`),
		message: "performance.now() reads the monotonic clock; use cs.workflow.now() instead.",
	},
	{
		name:    "crypto.getRandomValues",
		re:      regexp.MustCompile(`\bcrypto\.getRandomValues\b`),
		message: "crypto.getRandomValues is nondeterministic; use cs.workflow.sideEffect for entropy.",
	},
}

// allowMarker is the literal comment publishers add to opt out a single
// call site. Anything that contains this substring on the same line as
// the violation is silently skipped (the operator already reviewed it
// at PR time). Keeping it case-sensitive avoids accidental matches in
// unrelated comments.
const allowMarker = "cs-determinism-allow"

// jsFileExt is the suffix we scan. The v0.1 cs-js runtime only loads
// ".js" sources; ".mjs" and ".ts" are intentionally out of scope.
const jsFileExt = ".js"

// ScanWorkflow walks the bundle's file map and returns every banned-API
// call site it finds in JS sources. The returned slice is empty when
// the workflow is clean. The scan is intentionally cheap (regex over
// raw bytes) so the publish hot path stays under its CI latency budget.
//
// files is the canonical bundle map (the same shape bundle.ExtractTar
// returns). function.js is always scanned when present; everything
// under deps/ that ends in `.js` is scanned too. Other paths
// (manifest.json, lib/, .map files, etc.) are skipped — the linter is
// concerned only with code that will execute under the runtime.
func ScanWorkflow(files map[string][]byte) []Violation {
	if len(files) == 0 {
		return nil
	}
	names := make([]string, 0, len(files))
	for name := range files {
		if !shouldScan(name) {
			continue
		}
		names = append(names, name)
	}
	// Deterministic output: same input → same violation order. Tests
	// rely on this and so does the diff a publishing agent sees across
	// two retried publishes.
	sort.Strings(names)
	var out []Violation
	for _, name := range names {
		out = append(out, scanFile(name, files[name])...)
	}
	return out
}

// shouldScan reports whether the named bundle file should be subjected
// to the linter. function.js is the entrypoint contract; anything else
// must live under deps/ to be considered code that will execute under
// the runtime. We deliberately exclude paths that traverse into
// subdirectories of deps/ that aren't .js (e.g., deps/manifest.json
// fixtures) to keep the false-positive surface tight.
func shouldScan(name string) bool {
	if name == "function.js" {
		return true
	}
	if strings.HasPrefix(name, "deps/") && strings.HasSuffix(name, jsFileExt) {
		return true
	}
	return false
}

// scanFile applies every banned pattern to a single source buffer and
// returns the resulting violations. Line/column are computed once per
// file using a cached newline index so multi-pattern files don't pay
// O(patterns × bytes) to recompute positions.
func scanFile(name string, data []byte) []Violation {
	if len(data) == 0 {
		return nil
	}
	src := string(data)
	lineStarts := indexLineStarts(src)
	var out []Violation
	for _, bp := range bannedPatterns {
		for _, match := range bp.re.FindAllStringIndex(src, -1) {
			pos := match[0]
			if hasAllowMarker(src, pos) {
				continue
			}
			line, col := positionAt(lineStarts, pos)
			out = append(out, Violation{
				File:    name,
				Line:    line,
				Column:  col,
				Pattern: bp.name,
				Message: bp.message,
			})
		}
	}
	// Sort within a file by (line, column, pattern name) so two calls
	// on the same line still produce a stable order regardless of the
	// pattern iteration order above.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Line != out[j].Line {
			return out[i].Line < out[j].Line
		}
		if out[i].Column != out[j].Column {
			return out[i].Column < out[j].Column
		}
		return out[i].Pattern < out[j].Pattern
	})
	return out
}

// indexLineStarts returns the byte offset of the start of each line
// (the first line starts at 0). The slice is consulted by positionAt
// to translate a byte offset back into a 1-indexed (line, column)
// pair without re-scanning the source.
func indexLineStarts(src string) []int {
	starts := []int{0}
	for i := 0; i < len(src); i++ {
		if src[i] == '\n' {
			starts = append(starts, i+1)
		}
	}
	return starts
}

// positionAt converts a byte offset into the file into a 1-indexed
// (line, column) pair. Column is measured in bytes from the start of
// the line — the v0.1 scanner targets the ASCII subset of JS so
// rune-aware columns are an explicit non-goal.
func positionAt(lineStarts []int, pos int) (int, int) {
	// Binary search for the greatest line-start <= pos.
	lo, hi := 0, len(lineStarts)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if lineStarts[mid] <= pos {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo + 1, pos - lineStarts[lo] + 1
}

// hasAllowMarker reports whether the line containing pos carries the
// `cs-determinism-allow` opt-out comment. The check is intentionally
// line-scoped: a marker on a different line does not silence a
// violation, so reviewers can audit each escape hatch independently.
func hasAllowMarker(src string, pos int) bool {
	lineStart := strings.LastIndexByte(src[:pos], '\n') + 1
	lineEnd := strings.IndexByte(src[pos:], '\n')
	end := len(src)
	if lineEnd >= 0 {
		end = pos + lineEnd
	}
	return strings.Contains(src[lineStart:end], allowMarker)
}
