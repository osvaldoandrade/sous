package bundle

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
)

// entryFileForRuntime returns the canonical entry-file name expected
// inside a bundle for the given runtime identifier. An empty runtime
// resolves to cs-js for backward compatibility with v0.1 single-file
// manifests that pre-date the runtime discriminator. Unknown runtimes
// also fall back to cs-js so the bundle layer never blocks a publish
// the manifest validator already accepts — the runtime registry takes
// the final call on whether the runtime can actually execute.
func entryFileForRuntime(runtime string) string {
	switch runtime {
	case "cs-python":
		return "function.py"
	case "cs-wasm":
		return "module.wasm"
	default:
		return "function.js"
	}
}

// peekRuntime extracts the manifest.runtime field from the raw bundle
// map. It tolerates a missing or malformed manifest by returning the
// empty string; downstream callers either fail on the missing
// manifest.json check or fall back to the cs-js entry. Keeping the
// helper standalone means we never re-import internal/api here, which
// would create a cycle (api -> bundle -> api).
func peekRuntime(files map[string][]byte) string {
	raw, ok := files["manifest.json"]
	if !ok || len(raw) == 0 {
		return ""
	}
	var probe struct {
		Runtime string `json:"runtime"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return ""
	}
	return probe.Runtime
}

func BuildCanonical(files map[string][]byte) ([]byte, string, int, error) {
	if len(files) == 0 {
		return nil, "", 0, fmt.Errorf("files cannot be empty")
	}
	if _, ok := files["manifest.json"]; !ok {
		return nil, "", 0, fmt.Errorf("manifest.json is required")
	}
	entry := entryFileForRuntime(peekRuntime(files))
	if _, ok := files[entry]; !ok {
		return nil, "", 0, fmt.Errorf("%s is required", entry)
	}

	keys := make([]string, 0, len(files))
	for k := range files {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range keys {
		clean := filepath.Clean(name)
		if clean == "." || clean == ".." || clean != name {
			_ = tw.Close()
			return nil, "", 0, fmt.Errorf("invalid file path %q", name)
		}
		data := files[name]
		hdr := &tar.Header{
			Name: clean,
			Mode: 0o644,
			Size: int64(len(data)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			_ = tw.Close()
			return nil, "", 0, err
		}
		if _, err := tw.Write(data); err != nil {
			_ = tw.Close()
			return nil, "", 0, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, "", 0, err
	}
	out := buf.Bytes()
	sum := sha256.Sum256(out)
	return out, hex.EncodeToString(sum[:]), len(out), nil
}

func VerifySHA256(data []byte, expected string) bool {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]) == expected
}

func ExtractTar(data []byte) (map[string][]byte, error) {
	tr := tar.NewReader(bytes.NewReader(data))
	out := make(map[string][]byte)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		out[hdr.Name] = content
	}
	if _, ok := out["manifest.json"]; !ok {
		return nil, fmt.Errorf("bundle missing manifest.json")
	}
	entry := entryFileForRuntime(peekRuntime(out))
	if _, ok := out[entry]; !ok {
		return nil, fmt.Errorf("bundle missing %s", entry)
	}
	return out, nil
}
