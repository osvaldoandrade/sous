#!/bin/sh
set -eu

SOUS_REPO_URL="${SOUS_REPO_URL:-}"
SOUS_REF="${SOUS_REF:-main}" # branch, tag, or commit sha
SOUS_BIN_NAME="${SOUS_BIN_NAME:-}"
SOUS_PKG="${SOUS_PKG:-}"
SOUS_BIN_DIR="${SOUS_BIN_DIR:-}"

# Backward-compatible env vars (deprecated).
if [ -z "${SOUS_REPO_URL:-}" ] && [ -n "${SOUS_REPO:-}" ]; then
	SOUS_REPO_URL="$SOUS_REPO"
fi
if [ -z "${SOUS_BIN_DIR:-}" ] && [ -n "${SOUS_INSTALL_DIR:-}" ]; then
	SOUS_BIN_DIR="$SOUS_INSTALL_DIR"
fi

SOUS_REPO_URL="${SOUS_REPO_URL:-https://github.com/osvaldoandrade/sous.git}"

say() { printf '%s\n' "$*"; }
warn() { printf '%s\n' "warning: $*" >&2; }
die() { printf '%s\n' "error: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing dependency: $1"; }

uname_s="$(uname -s 2>/dev/null || printf unknown)"
os="unknown"
case "$uname_s" in
	Darwin*) os="darwin" ;;
	Linux*) os="linux" ;;
	MINGW* | MSYS* | CYGWIN*) os="windows" ;;
esac

exe=""
if [ "$os" = "windows" ]; then
	exe=".exe"
fi

find_writable_path_dir() {
	old_ifs="$IFS"
	IFS=":"
	for p in $PATH; do
		[ -n "${p:-}" ] || continue
		[ "$p" = "." ] && continue
		if [ -d "$p" ] && [ -w "$p" ]; then
			IFS="$old_ifs"
			printf '%s' "$p"
			return 0
		fi
	done
	IFS="$old_ifs"
	return 1
}

detect_cli_pkg_and_bin() {
	# Prefer a single ./cmd/*-cli directory as the "CLI".
	# Repo authors can override by setting SOUS_PKG and SOUS_BIN_NAME explicitly.
	candidate=""
	count=0
	for d in "$1"/cmd/*-cli; do
		[ -d "$d" ] || continue
		candidate="$d"
		count=$((count + 1))
	done
	if [ "$count" -eq 1 ]; then
		base="$(basename "$candidate")" # e.g. cs-cli
		if [ -z "${SOUS_PKG:-}" ]; then
			SOUS_PKG="./cmd/$base"
		fi
		if [ -z "${SOUS_BIN_NAME:-}" ]; then
			case "$base" in
				*-cli) SOUS_BIN_NAME="${base%-cli}" ;; # cs-cli -> cs
				*) SOUS_BIN_NAME="$base" ;;
			esac
		fi
		return 0
	fi
	return 1
}

bin_dir="$SOUS_BIN_DIR"
if [ -z "$bin_dir" ]; then
	if bin_dir="$(find_writable_path_dir 2>/dev/null)"; then
		:
	else
		if [ "$os" = "windows" ]; then
			bin_dir="${HOME}/bin"
		else
			bin_dir="${HOME}/.local/bin"
		fi
	fi
fi

mkdir -p "$bin_dir"

need go
need git

tmp_dir="$(mktemp -d 2>/dev/null || mktemp -d -t sous-install)"
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT INT TERM

src_dir="$tmp_dir/src"
mkdir -p "$src_dir"

repo_clone="$SOUS_REPO_URL"
repo_display="$SOUS_REPO_URL"
token="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
if [ -n "${token:-}" ]; then
	case "$SOUS_REPO_URL" in
		https://github.com/*)
			# x-access-token works for both classic and fine-grained tokens.
			repo_clone="https://x-access-token:${token}@${SOUS_REPO_URL#https://}"
			repo_display="$(printf '%s' "$SOUS_REPO_URL" | sed 's#^https://#https://***@#')"
			;;
	esac
fi

git init -q "$src_dir"
git -C "$src_dir" remote add origin "$repo_clone"
git -C "$src_dir" fetch -q --depth 1 origin "$SOUS_REF"
git -C "$src_dir" checkout -q FETCH_HEAD

if [ -z "${SOUS_PKG:-}" ] || [ -z "${SOUS_BIN_NAME:-}" ]; then
	detect_cli_pkg_and_bin "$src_dir" || true
fi

if [ -z "${SOUS_PKG:-}" ]; then
	die "could not detect CLI package; set SOUS_PKG (example: ./cmd/cs-cli)"
fi
if [ -z "${SOUS_BIN_NAME:-}" ]; then
	base="$(basename "$SOUS_PKG")"
	case "$base" in
		*-cli) SOUS_BIN_NAME="${base%-cli}" ;;
		*) SOUS_BIN_NAME="$base" ;;
	esac
fi

say "Installing ${SOUS_BIN_NAME}${exe} (${SOUS_REF}) from ${repo_display}"

out_path="$tmp_dir/${SOUS_BIN_NAME}${exe}"
(
	cd "$src_dir"
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o "$out_path" "$SOUS_PKG"
)

dest_path="${bin_dir%/}/${SOUS_BIN_NAME}${exe}"
if command -v install >/dev/null 2>&1; then
	install -m 0755 "$out_path" "$dest_path"
else
	cp "$out_path" "$dest_path"
	chmod 0755 "$dest_path" 2>/dev/null || true
fi

say "Installed to: $dest_path"
case ":$PATH:" in
	*":$bin_dir:"*) ;;
	*)
		warn "install dir is not on PATH: $bin_dir"
		if [ "$os" = "windows" ]; then
			warn "add it to PATH (Git Bash): export PATH=\"$bin_dir:\$PATH\" (persist in ~/.bashrc)"
		else
			warn "add it to PATH (bash/zsh): export PATH=\"$bin_dir:\$PATH\" (persist in ~/.profile or ~/.zshrc)"
		fi
		;;
esac
