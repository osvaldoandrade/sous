package bundle

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"sort"
)

func BuildCanonical(files map[string][]byte) ([]byte, string, int, error) {
	if len(files) == 0 {
		return nil, "", 0, fmt.Errorf("files cannot be empty")
	}
	if _, ok := files["function.js"]; !ok {
		return nil, "", 0, fmt.Errorf("function.js is required")
	}
	if _, ok := files["manifest.json"]; !ok {
		return nil, "", 0, fmt.Errorf("manifest.json is required")
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
	if _, ok := out["function.js"]; !ok {
		return nil, fmt.Errorf("bundle missing function.js")
	}
	if _, ok := out["manifest.json"]; !ok {
		return nil, fmt.Errorf("bundle missing manifest.json")
	}
	return out, nil
}
