// Package templates packages the function scaffolds shipped by `cs fn init`.
//
// Each subdirectory under `files/` is one template. Templates contain at
// least `function.js` and `manifest.json`, plus a `README.md` whose first
// non-empty line is used as the one-line description shown by
// `cs fn init --list`.
//
// Template files may reference the placeholder `{{.Name}}` which the CLI
// substitutes with the function's logical name when scaffolding.
package templates

import (
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed all:files
var embedded embed.FS

const root = "files"

// Variables substituted into template files.
type Variables struct {
	Name string
}

// Names returns the available template names in deterministic order.
func Names() []string {
	entries, err := fs.ReadDir(embedded, root)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// Exists reports whether name is a known template.
func Exists(name string) bool {
	for _, n := range Names() {
		if n == name {
			return true
		}
	}
	return false
}

// Description returns the first non-empty line of the template's README.md,
// or an empty string when no README is present.
func Description(name string) string {
	raw, err := embedded.ReadFile(root + "/" + name + "/README.md")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}
	return ""
}

// Render returns the rendered files of the template, keyed by filename,
// with `{{.Name}}` (and other Variables fields) substituted.
// The README.md is included so that consumers may write it alongside the
// scaffold; callers that want only the runnable bundle can drop it.
func Render(name string, vars Variables) (map[string][]byte, error) {
	if !Exists(name) {
		return nil, fmt.Errorf("unknown template %q (available: %s)", name, strings.Join(Names(), ", "))
	}
	out := make(map[string][]byte)
	dir := root + "/" + name
	err := fs.WalkDir(embedded, dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		raw, err := embedded.ReadFile(path)
		if err != nil {
			return err
		}
		rel := strings.TrimPrefix(path, dir+"/")
		out[rel] = []byte(substitute(string(raw), vars))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func substitute(s string, vars Variables) string {
	return strings.ReplaceAll(s, "{{.Name}}", vars.Name)
}
