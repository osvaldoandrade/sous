#!/usr/bin/env sh
set -eu

REPO_DEFAULT="https://github.com/osvaldoandrade/sous.git"
REF_DEFAULT="main"
BIN_NAME="cs"

usage() {
  cat <<'EOF'
Usage:
  curl -fsSL https://raw.githubusercontent.com/osvaldoandrade/sous/main/install.sh | sh

Options (when using: sh -s -- ...):
  --repo <url>        Git repo URL (default: https://github.com/osvaldoandrade/sous.git)
  --ref <ref>         Git ref (branch/tag) (default: main)
  --dir <path>        Install dir (default: first writable dir in $PATH, else ~/.local/bin)
  -h, --help          Show help

Env vars (alternative to flags):
  SOUS_REPO, SOUS_REF, SOUS_INSTALL_DIR

Notes:
  - Requires: git, go
  - Windows support assumes a POSIX shell (Git Bash/MSYS2/Cygwin/WSL).
EOF
}

log() { printf '%s\n' "$*" >&2; }
die() { log "error: $*"; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing dependency: $1"
}

mktemp_dir() {
  if command -v mktemp >/dev/null 2>&1; then
    d="$(mktemp -d 2>/dev/null || mktemp -d -t sous-install 2>/dev/null)" || return 1
    printf '%s\n' "$d"
    return 0
  fi
  d="/tmp/sous-install.$$"
  (umask 077 && mkdir -p "$d") || return 1
  printf '%s\n' "$d"
}

first_writable_path_dir() {
  old_ifs=$IFS
  IFS=:
  for d in $PATH; do
    [ -n "$d" ] || continue
    [ "$d" = "." ] && continue
    if [ -d "$d" ] && [ -w "$d" ]; then
      printf '%s\n' "$d"
      IFS=$old_ifs
      return 0
    fi
  done
  IFS=$old_ifs
  return 1
}

path_contains() {
  case ":${PATH:-}:" in
    *":$1:"*) return 0 ;;
  esac
  return 1
}

add_path_hint() {
  install_dir=$1
  if path_contains "$install_dir"; then
    return 0
  fi

  log ""
  log "Add '$install_dir' to your PATH (example):"
  log "  export PATH=\"$install_dir:\$PATH\""
  log ""
  log "Common files to update:"
  log "  - ~/.profile"
  log "  - ~/.bashrc"
  log "  - ~/.zshrc"
}

REPO="${SOUS_REPO:-$REPO_DEFAULT}"
REF="${SOUS_REF:-$REF_DEFAULT}"
INSTALL_DIR="${SOUS_INSTALL_DIR:-}"

while [ $# -gt 0 ]; do
  case "$1" in
    --repo)
      [ $# -ge 2 ] || die "--repo requires a value"
      REPO=$2
      shift 2
      ;;
    --ref)
      [ $# -ge 2 ] || die "--ref requires a value"
      REF=$2
      shift 2
      ;;
    --dir)
      [ $# -ge 2 ] || die "--dir requires a value"
      INSTALL_DIR=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1 (use --help)"
      ;;
  esac
done

need_cmd git
need_cmd go

uname_s="$(uname -s 2>/dev/null || printf 'unknown')"
case "$uname_s" in
  Darwin) os=darwin ;;
  Linux) os=linux ;;
  MINGW*|MSYS*|CYGWIN*|Windows_NT) os=windows ;;
  *) os=unknown ;;
esac

bin="$BIN_NAME"
case "$os" in
  windows) bin="${BIN_NAME}.exe" ;;
esac

if [ -z "$INSTALL_DIR" ]; then
  home="${HOME:-}"

  # Prefer common bin dirs that are typically already in PATH.
  for d in "/opt/homebrew/bin" "/usr/local/bin" "${home:+$home/.local/bin}" "${home:+$home/bin}"; do
    [ -n "$d" ] || continue
    if path_contains "$d" && [ -d "$d" ] && [ -w "$d" ]; then
      INSTALL_DIR="$d"
      break
    fi
  done
fi

if [ -z "$INSTALL_DIR" ]; then
  if d="$(first_writable_path_dir)"; then
    INSTALL_DIR="$d"
  elif path_contains "/usr/local/bin"; then
    # We'll attempt sudo at install time if needed.
    INSTALL_DIR="/usr/local/bin"
  else
    home="${HOME:-}"
    [ -n "$home" ] || die "HOME is not set and no writable dir found in PATH; set SOUS_INSTALL_DIR"
    INSTALL_DIR="$home/.local/bin"
  fi
fi

tmp_root="$(mktemp_dir)" || die "failed to create temp dir"
cleanup() { rm -rf "$tmp_root"; }
trap cleanup EXIT INT TERM

src="$tmp_root/sous"
out="$tmp_root/$bin"

log "Cloning: $REPO ($REF)"
git clone --depth 1 --branch "$REF" "$REPO" "$src" >/dev/null 2>&1 || die "git clone failed (repo=$REPO ref=$REF)"

log "Building: $bin"
gomodcache="$tmp_root/gomodcache"
gocache="$tmp_root/gocache"
gopath="$tmp_root/gopath"
mkdir -p "$gomodcache" "$gocache" "$gopath" || die "failed to create go cache dirs"

(cd "$src" && \
  CGO_ENABLED=0 \
  GOPROXY="${GOPROXY:-https://proxy.golang.org|direct}" \
  GOPATH="$gopath" \
  GOMODCACHE="$gomodcache" \
  GOCACHE="$gocache" \
  go build -trimpath -ldflags "-s -w" -o "$out" ./cmd/cs-cli \
) || die "go build failed"

can_write_dir=0
if [ -d "$INSTALL_DIR" ]; then
  [ -w "$INSTALL_DIR" ] && can_write_dir=1
else
  if mkdir -p "$INSTALL_DIR" >/dev/null 2>&1; then
    [ -w "$INSTALL_DIR" ] && can_write_dir=1
  fi
fi

if [ "$can_write_dir" -eq 1 ]; then
  log "Installing to: $INSTALL_DIR/$bin"
  if command -v install >/dev/null 2>&1; then
    install -m 0755 "$out" "$INSTALL_DIR/$bin" || die "install failed"
  else
    cp "$out" "$INSTALL_DIR/$bin" || die "copy failed"
    chmod 0755 "$INSTALL_DIR/$bin" >/dev/null 2>&1 || true
  fi
else
  if [ "$os" != "windows" ] && command -v sudo >/dev/null 2>&1; then
    log "Installing with sudo to: $INSTALL_DIR/$bin"
    sudo mkdir -p "$INSTALL_DIR" || die "sudo mkdir failed"
    if command -v install >/dev/null 2>&1; then
      sudo install -m 0755 "$out" "$INSTALL_DIR/$bin" || die "sudo install failed"
    else
      sudo cp "$out" "$INSTALL_DIR/$bin" || die "sudo copy failed"
      sudo chmod 0755 "$INSTALL_DIR/$bin" >/dev/null 2>&1 || true
    fi
  else
    home="${HOME:-}"
    [ -n "$home" ] || die "install dir not writable and HOME is not set; set SOUS_INSTALL_DIR"
    INSTALL_DIR="$home/.local/bin"
    mkdir -p "$INSTALL_DIR" || die "failed to create install dir: $INSTALL_DIR"
    log "Install dir not writable; installed to: $INSTALL_DIR/$bin"
    if command -v install >/dev/null 2>&1; then
      install -m 0755 "$out" "$INSTALL_DIR/$bin" || die "install failed"
    else
      cp "$out" "$INSTALL_DIR/$bin" || die "copy failed"
      chmod 0755 "$INSTALL_DIR/$bin" >/dev/null 2>&1 || true
    fi
  fi
fi

log ""
log "Installed: $INSTALL_DIR/$bin"

add_path_hint "$INSTALL_DIR"
