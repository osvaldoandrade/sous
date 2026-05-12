// Package runtime: frozen-import-map support (roadmap task E5.01).
//
// The cs-js runtime is a single goja isolate that runs publisher code.
// JavaScript bare specifiers (`import { z } from "zod"`) cannot resolve
// against the file system or the network — only against the frozen
// import map produced by cs-control at publish time. This file owns the
// publish-time output → runtime wiring:
//
//   - rewriteImportStatements turns ES `import` statements in the
//     publisher's function.js (and in each dep module) into calls to a
//     synchronous host helper `__cs_require(spec)`.
//   - rewriteExportStatements turns ES `export` statements in a dep
//     module into assignments on an injected `__cs_exports` object so
//     the host helper can return them.
//   - bindImports installs `__cs_require` on the isolate, lazily
//     evaluates each dep on first use, caches the result by specifier,
//     and verifies the SRI digest from the frozen import map before
//     letting any byte reach the isolate. Unknown specifiers raise
//     CS_IMPORT_NOT_FOUND (HTTP 422).
//
// See docs/08-runtime-cs-js.md "Imports" and docs/15-security.md.

package runtime

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/dop251/goja"

	"github.com/osvaldoandrade/sous/internal/bundle"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
)

// Import-statement regex set. Order matters: the wider "named exports"
// pattern must run before the side-effect-only "import \"spec\"" pattern
// so we do not strip semantic statements.
var (
	importDefaultRegex   = regexp.MustCompile(`(?m)^\s*import\s+([A-Za-z_$][\w$]*)\s+from\s+["']([^"']+)["']\s*;?\s*$`)
	importNamespaceRegex = regexp.MustCompile(`(?m)^\s*import\s+\*\s+as\s+([A-Za-z_$][\w$]*)\s+from\s+["']([^"']+)["']\s*;?\s*$`)
	importNamedRegex     = regexp.MustCompile(`(?m)^\s*import\s+\{([^}]+)\}\s+from\s+["']([^"']+)["']\s*;?\s*$`)
	importMixedRegex     = regexp.MustCompile(`(?m)^\s*import\s+([A-Za-z_$][\w$]*)\s*,\s*\{([^}]+)\}\s+from\s+["']([^"']+)["']\s*;?\s*$`)
	importSideEffect     = regexp.MustCompile(`(?m)^\s*import\s+["']([^"']+)["']\s*;?\s*$`)
)

// rewriteImportStatements converts publisher import statements into
// `const ... = __cs_require("spec")` form. The transform is line-oriented
// and intentionally conservative — anything that does not match the
// supported shapes is left untouched, so the runtime falls back to
// goja's parse error. Documented shapes:
//
//	import x from "spec"
//	import * as ns from "spec"
//	import { a, b as c } from "spec"
//	import x, { a } from "spec"
//	import "spec"
//
// All of these compile to references that, at execution time, call the
// host-bound __cs_require which looks the specifier up in the frozen
// import map. See docs/08-runtime-cs-js.md.
func rewriteImportStatements(src string) string {
	// Side-effect imports first so a bare `import "x"` does not get
	// mis-parsed by the more specific regexes below.
	out := importSideEffect.ReplaceAllString(src, `__cs_require("$1");`)
	out = importMixedRegex.ReplaceAllStringFunc(out, func(match string) string {
		m := importMixedRegex.FindStringSubmatch(match)
		def := m[1]
		named := normaliseNamedBindings(m[2])
		spec := m[3]
		return fmt.Sprintf(`const __cs_imp_%[1]s = __cs_require(%[2]q); const %[1]s = __cs_imp_%[1]s.default; const { %[3]s } = __cs_imp_%[1]s;`,
			def, spec, named)
	})
	out = importNamespaceRegex.ReplaceAllString(out, `const $1 = __cs_require("$2");`)
	out = importNamedRegex.ReplaceAllStringFunc(out, func(match string) string {
		m := importNamedRegex.FindStringSubmatch(match)
		named := normaliseNamedBindings(m[1])
		spec := m[2]
		return fmt.Sprintf(`const { %s } = __cs_require(%q);`, named, spec)
	})
	out = importDefaultRegex.ReplaceAllStringFunc(out, func(match string) string {
		m := importDefaultRegex.FindStringSubmatch(match)
		ident := m[1]
		spec := m[2]
		return fmt.Sprintf(`const %s = __cs_require(%q).default;`, ident, spec)
	})
	return out
}

// normaliseNamedBindings turns the contents of a `{ a, b as c }` clause
// into the destructuring form `a, b: c` used by `const { ... } =`.
func normaliseNamedBindings(group string) string {
	parts := strings.Split(group, ",")
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// `b as c` → `b: c`.
		if strings.Contains(p, " as ") {
			pieces := strings.SplitN(p, " as ", 2)
			parts[i] = strings.TrimSpace(pieces[0]) + ": " + strings.TrimSpace(pieces[1])
		} else {
			parts[i] = p
		}
	}
	return strings.Join(parts, ", ")
}

// rewriteExportStatements turns dep-module export statements into
// assignments on the `__cs_exports` object the runtime injects when it
// evaluates the dep. Supported shapes:
//
//	export default <expr>
//	export const NAME = <expr>
//	export let NAME = <expr>
//	export var NAME = <expr>
//	export function NAME(...) { ... }
//	export async function NAME(...) { ... }
//	export class NAME { ... }
//	export { a, b as c }
//
// The transform is line-oriented for simplicity. Anything else is left
// alone and will surface as a goja parse error — that is fine: deps that
// use exotic export forms (re-exports, namespace re-exports, etc.) are
// out of scope for v0.1 of cs-js per docs/08-runtime-cs-js.md.
var (
	exportDefaultExprRegex = regexp.MustCompile(`(?m)^\s*export\s+default\s+`)
	exportConstRegex       = regexp.MustCompile(`(?m)^\s*export\s+(const|let|var)\s+([A-Za-z_$][\w$]*)\s*=`)
	exportFuncRegex        = regexp.MustCompile(`(?m)^\s*export\s+(async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\(`)
	exportClassRegex       = regexp.MustCompile(`(?m)^\s*export\s+class\s+([A-Za-z_$][\w$]*)\s*`)
	exportListRegex        = regexp.MustCompile(`(?m)^\s*export\s+\{([^}]+)\}\s*;?\s*$`)
)

func rewriteExportStatements(src string) string {
	out := exportListRegex.ReplaceAllStringFunc(src, func(match string) string {
		m := exportListRegex.FindStringSubmatch(match)
		group := m[1]
		var lines []string
		for _, p := range strings.Split(group, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			name, alias := p, p
			if strings.Contains(p, " as ") {
				pieces := strings.SplitN(p, " as ", 2)
				name = strings.TrimSpace(pieces[0])
				alias = strings.TrimSpace(pieces[1])
			}
			lines = append(lines, fmt.Sprintf("__cs_exports.%s = %s;", alias, name))
		}
		return strings.Join(lines, "\n")
	})
	out = exportConstRegex.ReplaceAllStringFunc(out, func(match string) string {
		m := exportConstRegex.FindStringSubmatch(match)
		keyword := m[1]
		name := m[2]
		return fmt.Sprintf("%s %s =\n__cs_exports.%s =", keyword, name, name)
	})
	out = exportFuncRegex.ReplaceAllStringFunc(out, func(match string) string {
		m := exportFuncRegex.FindStringSubmatch(match)
		asyncPrefix := m[1]
		name := m[2]
		return fmt.Sprintf("__cs_exports.%s = %sfunction %s(", name, asyncPrefix, name)
	})
	out = exportClassRegex.ReplaceAllStringFunc(out, func(match string) string {
		m := exportClassRegex.FindStringSubmatch(match)
		name := m[1]
		return fmt.Sprintf("__cs_exports.%s = class %s ", name, name)
	})
	out = exportDefaultExprRegex.ReplaceAllString(out, `__cs_exports.default = `)
	return out
}

// bindImports installs `__cs_require` on the goja runtime. It uses the
// frozen import map and the dep bytes carried in the bundle file set; a
// missing specifier raises CS_IMPORT_NOT_FOUND. Each dep is evaluated at
// most once per activation and cached by specifier — deps are pure data
// at this point (bytes pinned by SRI), so caching is safe.
func bindImports(rt *goja.Runtime, importMap bundle.FrozenImportMap, files map[string][]byte) error {
	cache := make(map[string]goja.Value, len(importMap.Imports))
	resolve := func(spec string) (goja.Value, error) {
		if v, ok := cache[spec]; ok {
			return v, nil
		}
		entry, ok := importMap.Imports[spec]
		if !ok {
			return nil, cserrors.New(cserrors.CSImportNotFound, fmt.Sprintf("import %q not declared in manifest.imports", spec))
		}
		data, ok := files[entry.Path]
		if !ok {
			return nil, cserrors.New(cserrors.CSImportNotFound, fmt.Sprintf("import %q frozen path %q missing from bundle", spec, entry.Path))
		}
		if err := bundle.VerifyImportBytes(entry.Integrity, data); err != nil {
			return nil, cserrors.Wrap(cserrors.CSImportNotFound, fmt.Sprintf("import %q integrity verification failed", spec), err)
		}
		exports := rt.NewObject()
		// Seed cache *before* running dep code so circular imports
		// (a → b → a) see a partially-initialised exports object and
		// do not infinite-loop in __cs_require.
		cache[spec] = exports
		// Pre-install __cs_exports in the runtime; dep code reads/writes
		// it through the rewritten export statements. We restore the
		// previous value (if any) after running the dep so nested
		// resolves work correctly.
		prev := rt.Get("__cs_exports")
		if err := rt.Set("__cs_exports", exports); err != nil {
			return nil, err
		}
		defer func() {
			_ = rt.Set("__cs_exports", prev)
		}()
		transformed := rewriteImportStatements(rewriteExportStatements(string(data)))
		if _, err := rt.RunString(transformed); err != nil {
			delete(cache, spec)
			return nil, err
		}
		return exports, nil
	}
	return rt.Set("__cs_require", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) == 0 {
			panic(rt.NewGoError(cserrors.New(cserrors.CSImportNotFound, "__cs_require called without specifier")))
		}
		spec := call.Argument(0).String()
		v, err := resolve(spec)
		if err != nil {
			panic(rt.NewGoError(err))
		}
		return v
	})
}
