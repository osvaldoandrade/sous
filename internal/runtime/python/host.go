package python

// auditHookSource is the Python bootstrap installed into every
// activation tempdir. It registers a sys.addaudithook that denies the
// most obvious capability-escape gadgets, then exposes a single
// helper, _cs_log(level, message), that writes a structured stderr
// line the Go side picks up as a log entry.
//
// IMPORTANT: this is a best-effort gate, not a hermetic sandbox. A
// determined adversary inside the Python interpreter can defeat an
// in-process audit hook through native-module loading or by
// rebinding sys.audit; the production capability model lives in the
// embedded CPython adapter follow-up where host bridges replace the
// stdout pipe. See the package doc in runner.go for the threat model
// caveats and docs/08c-runtime-cs-python.md "Security caveats".
//
// The script is kept as raw bytes (not //go:embed) to avoid a build-
// time file dependency and to make the gate easy to audit inline.
var auditHookSource = []byte(`# cs-python audit bootstrap (v0.1 subprocess MVP)
# Installed by internal/runtime/python.Runner before every activation.
# Blocks the most obvious capability-escape gadgets via sys.addaudithook.
# See internal/runtime/python/host.go for the threat model caveats.
import sys

_CS_BLOCKED_EVENTS = frozenset({
    # Process spawn / exec
    "os.system",
    "subprocess.Popen",
    "os.exec",
    "os.execv",
    "os.execve",
    "os.execvp",
    "os.execvpe",
    "os.fork",
    "os.forkpty",
    "os.posix_spawn",
    "os.posix_spawnp",
    # Raw sockets (Python emits "socket.__new__" for socket.socket() calls).
    "socket.__new__",
    "socket.bind",
    "socket.connect",
    # Dynamic native loading
    "ctypes.dlopen",
    "ctypes.LoadLibrary",
    "ctypes.cdll.LoadLibrary",
    # FFI / arbitrary memory access
    "ctypes.addressof",
})


def _cs_audit_hook(event, args):
    if event in _CS_BLOCKED_EVENTS:
        raise PermissionError(
            "CS_RUNTIME_CAPABILITY_DENIED: cs-python blocks event %r" % event
        )


sys.addaudithook(_cs_audit_hook)


def _cs_log(level, message):
    """Emit a structured log line on stderr.

    The Go side captures every stderr line as an activation log entry,
    mirroring cs.log.* in cs-js. User code may either call this helper
    explicitly or just print to stderr.
    """
    sys.stderr.write("[%s] %s\n" % (level, message))
    sys.stderr.flush()


# Expose the helper under the bare name "cs_log" so user code can do
# cs_log("info", "hello") without an import. The richer cs.* surface
# (cs.kv, cs.codeq, cs.http) is intentionally absent in v0.1 -- see the
# follow-up issue referenced in docs/08c-runtime-cs-python.md.
import builtins as _b
_b.cs_log = _cs_log
`)
