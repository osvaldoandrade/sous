# cs-wasm test fixtures

This directory holds the hand-crafted WebAssembly binaries the cs-wasm
runner tests load via `//go:embed`. They are deliberately tiny (well
under 1 KiB each) so the repository stays vendor-light; the matching
WebAssembly Text (`.wat`) sources are inlined as comments next to the
embed directives in `runner_test.go`.

The binaries below were assembled by hand from the wasm core spec
(https://webassembly.github.io/spec/core/binary/modules.html). Keep
them in lockstep with the `.wat` comments in the test file when you
edit either side.

| File           | Purpose                                                |
| -------------- | ------------------------------------------------------ |
| `echo.wasm`    | Returns the request bytes verbatim from `handle`.      |
| `kv.wasm`      | Calls `cs_kv_set` then `cs_kv_get` to round-trip a key. |
| `fetch.wasm`   | Invokes `cs_http_fetch` for an allowed host.            |
| `forbidden.wasm` | Imports `wasi_snapshot_preview1.fd_write` (denied).  |
