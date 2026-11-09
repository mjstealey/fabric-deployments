#!/usr/bin/env bash
# Build and/or install Pegasus as a single pegasus-wms wheel, the way the
# pegasus repo's developer guide (CLAUDE.md "Build Environment" /
# "Installation of the wheel") prescribes:
#
#   python3 -m venv ~/pegasus-build-env; pip3 install build; make build
#   python3 -m venv <env>; pip3 install --force-reinstall dist/pegasus_wms-*.whl
#
# The wheel is self-contained: Python packages, the Java planner JARs, the C
# tools (kickstart, cluster, keg), the Go tools (pegasus-transfer & co) and a
# worker-package tarball for this platform. It is installed NEXT TO the apt
# Pegasus, never over it; PATH order decides which one a run uses.
#
# Two modes:
#   MODE=build    (submit node) toolchain + source + `make build` + install.
#                 Source is either a git ref cloned from PEGASUS_REPO, or a
#                 `git archive` tarball (STAGE_TARBALL) of a local checkout.
#   MODE=install  (any node) install an existing WHEEL into PEGASUS_ENV.
#
# Pool-specific choices (why this is not literally ~/pegasus-env):
#   * PEGASUS_ENV defaults to /opt/pegasus-env, world-readable. The pool's
#     UID_DOMAIN is per-host, so worker jobs run as `nobody`, which cannot
#     traverse /home/ubuntu (0750); PegasusLite would silently fall back to
#     the apt binaries in /usr/bin and reject their version.
#   * Go: the go.mod files need Go 1.24 (toolchain go1.24.4); noble ships
#     1.22, and GOTOOLCHAIN=auto fails on pegasus-globus-online's bare
#     "go 1.24" (no such release). So the official GO_VERSION tarball is
#     installed under /usr/local/go.
#
# Idempotent: MODE=install skips the pip install when the manifest already
# records the same wheel sha256. Every run rewrites the manifest at
#   $PEGASUS_ENV/.pegasus-wheel-manifest.json
#
# Inputs (env vars):
#   MODE           build | install                       (required)
#   PEGASUS_ENV    install prefix (venv)                 [/opt/pegasus-env]
#   PEGASUS_REPO   git URL for MODE=build without a tarball
#                                       [https://github.com/pegasus-isi/pegasus.git]
#   PEGASUS_REF    branch/tag to clone (MODE=build, no STAGE_TARBALL)
#   STAGE_TARBALL  git-archive tarball of a checkout (MODE=build, overrides REF)
#   PEGASUS_SRC_DESC  provenance note for the manifest (git describe)
#   SRC_DIR        build tree                            [/opt/src/pegasus]
#   WHEEL          wheel to install (MODE=install)
#   GO_VERSION     Go toolchain for the build            [go1.24.4]
#   FORCE=1        reinstall even when the manifest matches
set -euo pipefail

MODE=${MODE:?MODE=build or MODE=install}
PEGASUS_ENV=${PEGASUS_ENV:-/opt/pegasus-env}
PEGASUS_REPO=${PEGASUS_REPO:-https://github.com/pegasus-isi/pegasus.git}
PEGASUS_REF=${PEGASUS_REF:-}
STAGE_TARBALL=${STAGE_TARBALL:-}
PEGASUS_SRC_DESC=${PEGASUS_SRC_DESC:-}
SRC_DIR=${SRC_DIR:-/opt/src/pegasus}
GO_VERSION=${GO_VERSION:-go1.24.4}
FORCE=${FORCE:-0}
MANIFEST="$PEGASUS_ENV/.pegasus-wheel-manifest.json"

log() { echo "[pegasus-wheel $(date -u +%H:%M:%S)] $*"; }
apt_install() {
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q "$@" >/dev/null
}

install_wheel() {
  local wheel=$1 sha
  sha=$(sha256sum "$wheel" | cut -d' ' -f1)
  if [ "$FORCE" != 1 ] && [ -f "$MANIFEST" ] && grep -q "\"wheel_sha256\": \"$sha\"" "$MANIFEST" \
     && "$PEGASUS_ENV/bin/pegasus-version" >/dev/null 2>&1; then
    log "unchanged: $(basename "$wheel") already installed in $PEGASUS_ENV"
  else
    apt_install python3-venv
    log "installing $(basename "$wheel") into $PEGASUS_ENV"
    sudo rm -rf "$PEGASUS_ENV"
    sudo mkdir -p "$PEGASUS_ENV"
    sudo chown "$(id -u):$(id -g)" "$PEGASUS_ENV"
    python3 -m venv "$PEGASUS_ENV"
    "$PEGASUS_ENV/bin/pip3" install -q --force-reinstall "$wheel"
  fi
  chmod -R go+rX "$PEGASUS_ENV"
  local version
  version=$("$PEGASUS_ENV/bin/pegasus-version")
  # Worker jobs run as `nobody`; prove that identity can use the install.
  if ! sudo -u nobody "$PEGASUS_ENV/bin/pegasus-config" --version >/dev/null 2>&1; then
    log "ERROR: $PEGASUS_ENV is not usable by the 'nobody' job user"
    exit 1
  fi
  cat >"$MANIFEST" <<EOF
{
  "installed_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "host": "$(hostname)",
  "mode": "$MODE",
  "pegasus_version": "$version",
  "wheel": "$(basename "$wheel")",
  "wheel_sha256": "$sha",
  "source_ref": "${PEGASUS_SOURCE_COMMIT:-}",
  "source_desc": "${PEGASUS_SRC_DESC:-}"
}
EOF
  log "pegasus-version $version ($PEGASUS_ENV)"
  echo "WHEEL_INSTALL_COMPLETE $version"
}

if [ "$MODE" = install ]; then
  WHEEL=${WHEEL:?MODE=install needs WHEEL=<path to pegasus_wms-*.whl>}
  install_wheel "$WHEEL"
  exit 0
fi

[ "$MODE" = build ] || { echo "MODE must be build or install" >&2; exit 2; }

log "toolchain"
apt_install cmake openjdk-21-jdk-headless python3-venv python3-dev git curl
if [ "$(/usr/local/go/bin/go version 2>/dev/null | awk '{print $3}')" != "$GO_VERSION" ]; then
  log "installing $GO_VERSION"
  curl -fsSL "https://go.dev/dl/$GO_VERSION.linux-amd64.tar.gz" -o "/tmp/$GO_VERSION.tgz"
  sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf "/tmp/$GO_VERSION.tgz"
fi
export PATH=/usr/local/go/bin:$PATH GOTOOLCHAIN=local

log "source"
sudo mkdir -p "$(dirname "$SRC_DIR")"
sudo chown "$(id -u):$(id -g)" "$(dirname "$SRC_DIR")"
# A previous build leaves Go module caches (<build>/packages/*/go-path/pkg/mod)
# that Go writes read-only (files 0444, dirs 0555), so a plain rm -rf fails on
# every rebuild. Restore owner write first.
if [ -e "$SRC_DIR" ]; then
  chmod -R u+w "$SRC_DIR"
fi
rm -rf "$SRC_DIR"
if [ -n "$STAGE_TARBALL" ]; then
  mkdir -p "$SRC_DIR"
  tar -xzf "$STAGE_TARBALL" -C "$SRC_DIR"
  # make build needs a git tree for nothing but provenance; record it instead.
  PEGASUS_SOURCE_COMMIT=${PEGASUS_SOURCE_COMMIT:-tarball}
else
  [ -n "$PEGASUS_REF" ] || { echo "MODE=build needs PEGASUS_REF or STAGE_TARBALL" >&2; exit 2; }
  git clone -q --depth 1 --branch "$PEGASUS_REF" "$PEGASUS_REPO" "$SRC_DIR"
  PEGASUS_SOURCE_COMMIT=$(git -C "$SRC_DIR" rev-parse HEAD)
  PEGASUS_SRC_DESC=${PEGASUS_SRC_DESC:-"$PEGASUS_REPO@$PEGASUS_REF"}
fi
export PEGASUS_SOURCE_COMMIT PEGASUS_SRC_DESC
log "source commit ${PEGASUS_SOURCE_COMMIT} (${PEGASUS_SRC_DESC})"

log "make build (guide: venv ~/pegasus-build-env + pip install build)"
rm -rf ~/pegasus-build-env
python3 -m venv ~/pegasus-build-env
~/pegasus-build-env/bin/pip3 install -q build
( cd "$SRC_DIR" && PATH="$HOME/pegasus-build-env/bin:$PATH" make build PYTHON="$HOME/pegasus-build-env/bin/python3" )
WHEEL=$(ls -t "$SRC_DIR"/dist/pegasus_wms-*.whl | head -1)
log "built $(basename "$WHEEL") ($(du -h "$WHEEL" | cut -f1))"
echo "WHEEL_PATH $WHEEL"
install_wheel "$WHEEL"
