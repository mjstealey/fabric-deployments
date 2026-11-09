#!/usr/bin/env bash
# Overlay the `pegasus-monitor` CLI (pegasus-isi/pegasus branch
# feature/pegasus-monitor*) onto the apt-installed Pegasus.
#
# pegasus-monitor is pure Python and self-contained: a new package
# Pegasus/monitor/ plus a new CLI shim Pegasus/cli/pegasus-monitor.py under
# /usr/lib/pegasus/python/Pegasus/, and a /usr/bin/pegasus-monitor wrapper that
# is the stock pegasus-python-wrapper with %%BASE_NAME%% substituted (exactly
# what release-tools/update-python-tools does at build time). From the rest of
# the tree it only imports Pegasus.braindump and Pegasus.tools.properties,
# both present in the apt package, so the overlay is the whole install. It
# does NOT touch pegasus-monitord or the monitord plugin overlay
# (overlay_monitord_plugin.sh) -- the two retrofits are independent.
#
# Idempotent: every file is byte-compared and replaced only when it differs;
# stale modules in Pegasus/monitor/ that the new source no longer ships are
# removed (they would still be importable otherwise). None of the files exist
# in the apt package, so no .orig backups are taken. Every run rewrites the
# provenance manifest at
#   /usr/lib/pegasus/python/Pegasus/monitor/.pegasus-monitor-manifest.json
#
# Normally uploaded and run by provision_pegasus_slice.py
# --install-pegasus-monitor --pegasus-monitor-src <checkout>, but
# self-contained. Build the tarball from a clean checkout with
#
#   git archive --format=tar.gz -o pegasus-monitor-<sha>.tar.gz <sha> \
#     packages/pegasus-python/src/Pegasus/monitor \
#     packages/pegasus-python/src/Pegasus/cli/pegasus-monitor.py
#
# then on the node
#
#   STAGE_TARBALL=~/pegasus-htcondor/pegasus-monitor-<sha>.tar.gz \
#   PEGASUS_MONITOR_REF=<sha> bash overlay_pegasus_monitor.sh
#
# Inputs (env vars):
#   STAGE_TARBALL        REQUIRED -- the git-archive tarball above
#   PEGASUS_MONITOR_REF  full commit SHA the tarball was built from (manifest)
#   PEGASUS_SRC_DESC     provenance note (git describe --always --dirty)
#   STAGE_DIR            staging dir ($HOME/pegasus-htcondor/pegasus-monitor)
set -euo pipefail

STAGE_TARBALL="${STAGE_TARBALL:-}"
PEGASUS_MONITOR_REF="${PEGASUS_MONITOR_REF:-}"
PEGASUS_SRC_DESC="${PEGASUS_SRC_DESC:-}"
STAGE_DIR="${STAGE_DIR:-$HOME/pegasus-htcondor/pegasus-monitor}"

WRAPPER_TEMPLATE=/usr/bin/pegasus-python-wrapper
WRAPPER=/usr/bin/pegasus-monitor
# Two apt layouts. 5.x: /usr/lib/pegasus/python/Pegasus + the %%BASE_NAME%%
# pegasus-python-wrapper. 6.0: a venv (/usr/lib/pegasus/venv) whose bin/
# entry points dispatch through Pegasus.cli.main and are symlinked into
# /usr/bin; the deb's main.py may predate the monitor entry point, so the
# launcher runs cli/pegasus-monitor.py via runpy exactly as main._run_script does.
PEG_VENV=/usr/lib/pegasus/venv
if [ -d /usr/lib/pegasus/python/Pegasus/cli ]; then
  LAYOUT=legacy
  PEG_TREE=/usr/lib/pegasus/python/Pegasus
  PY=python3
else
  LAYOUT=venv
  PY="${PEG_VENV}/bin/python3"
  PEG_TREE="$("${PY}" -c 'import os, Pegasus; print(os.path.dirname(Pegasus.__file__))' 2>/dev/null || true)"
fi
MANIFEST="${PEG_TREE}/monitor/.pegasus-monitor-manifest.json"
# git archive paths start packages/pegasus-python/src/Pegasus/...; strip to Pegasus/
STRIP=3

log() { printf '\n=== %s ===\n' "$*"; }
sha() { [ -e "$1" ] && sha256sum "$1" | awk '{print $1}' || echo absent; }

[ -n "${STAGE_TARBALL}" ] && [ -s "${STAGE_TARBALL}" ] || {
  echo "error: STAGE_TARBALL must point at the git-archive tarball" >&2
  exit 1
}
[ -n "${PEG_TREE}" ] && [ -d "${PEG_TREE}/cli" ] || {
  echo "error: Pegasus/cli not found (${LAYOUT} layout, tree '${PEG_TREE}') -- is the pegasus apt package installed?" >&2
  exit 1
}
if [ "${LAYOUT}" = legacy ]; then
  [ -x "${WRAPPER_TEMPLATE}" ] && grep -q '%%BASE_NAME%%' "${WRAPPER_TEMPLATE}" || {
    echo "error: ${WRAPPER_TEMPLATE} missing or has no %%BASE_NAME%% placeholder" >&2
    exit 1
  }
fi
echo "  pegasus layout: ${LAYOUT} (${PEG_TREE})"

# --- 1. stage ---------------------------------------------------------------
log "staging $(basename "${STAGE_TARBALL}") into ${STAGE_DIR}/src (${PEGASUS_SRC_DESC:-no provenance note})"
rm -rf "${STAGE_DIR}/src"
mkdir -p "${STAGE_DIR}/src"
tar xzf "${STAGE_TARBALL}" -C "${STAGE_DIR}/src" --strip-components="${STRIP}"
SRC="${STAGE_DIR}/src/Pegasus"
[ -s "${SRC}/monitor/__init__.py" ] && [ -s "${SRC}/cli/pegasus-monitor.py" ] || {
  echo "error: tarball lacks Pegasus/monitor/__init__.py or Pegasus/cli/pegasus-monitor.py" >&2
  exit 1
}
mapfile -t MODULES < <(cd "${SRC}/monitor" && ls -1 *.py | sort)
echo "  ${#MODULES[@]} modules in Pegasus/monitor + cli/pegasus-monitor.py"

# --- 2. gates before touching the install -----------------------------------
log "py_compile gate"
for m in "${MODULES[@]}"; do "${PY}" -m py_compile "${SRC}/monitor/${m}"; done
"${PY}" -m py_compile "${SRC}/cli/pegasus-monitor.py"
echo "  all compile"

log "dependency gate (python3 = $(command -v "${PY}"))"
"${PY}" - <<'PYEOF'
import importlib.util, sys
missing = [m for m in ("rich", "yaml") if importlib.util.find_spec(m) is None]
if missing:
    sys.exit(
        f"error: {missing} not importable by {sys.executable}; "
        "pegasus-monitor needs them (pip install --user rich PyYAML, or apt "
        "python3-rich python3-yaml)"
    )
print("  rich + yaml importable")
PYEOF

# --- 3. install (byte-compare) ----------------------------------------------
log "overlaying into ${PEG_TREE}"
TSV="${STAGE_DIR}/.files.tsv"
: > "${TSV}"
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" >> "${TSV}"; }
put() {  # put <src> <target> <mode>
  local src="$1" target="$2" mode="$3" before after
  before="$(sha "${target}")"
  if cmp -s "${src}" "${target}" 2>/dev/null; then
    echo "  ${target#${PEG_TREE}/}: unchanged"
    record "${target}" "${before}" "${before}" no installed
    return
  fi
  sudo install -m "${mode}" -o root -g root "${src}" "${target}"
  after="$(sha "${target}")"
  echo "  ${target#${PEG_TREE}/}: ${before:0:12} -> ${after:0:12}"
  record "${target}" "${before}" "${after}" yes installed
}

sudo install -d -m 0755 -o root -g root "${PEG_TREE}/monitor"
for m in "${MODULES[@]}"; do
  put "${SRC}/monitor/${m}" "${PEG_TREE}/monitor/${m}" 0644
done
put "${SRC}/cli/pegasus-monitor.py" "${PEG_TREE}/cli/pegasus-monitor.py" 0644

# Modules a previous overlay installed that this source no longer ships.
for existing in "${PEG_TREE}"/monitor/*.py; do
  [ -e "${existing}" ] || continue
  name="$(basename "${existing}")"
  keep=no
  for m in "${MODULES[@]}"; do [ "${m}" = "${name}" ] && keep=yes && break; done
  if [ "${keep}" = no ]; then
    before="$(sha "${existing}")"
    sudo rm -f "${existing}"
    echo "  monitor/${name}: stale, removed"
    record "${existing}" "${before}" absent yes removed
  fi
done

# Stale bytecode from a previous overlay would shadow the new sources.
sudo rm -rf "${PEG_TREE}/monitor/__pycache__"
sudo rm -f "${PEG_TREE}/cli/__pycache__"/pegasus-monitor.*.pyc 2>/dev/null || true

# --- 4. /usr/bin wrapper ----------------------------------------------------
if [ "${LAYOUT}" = legacy ]; then
  log "wrapper ${WRAPPER} (from ${WRAPPER_TEMPLATE})"
  sed 's/%%BASE_NAME%%/pegasus-monitor/' "${WRAPPER_TEMPLATE}" > "${STAGE_DIR}/pegasus-monitor.wrapper"
  put "${STAGE_DIR}/pegasus-monitor.wrapper" "${WRAPPER}" 0755
else
  log "launcher ${PEG_VENV}/bin/pegasus-monitor (symlinked from ${WRAPPER})"
  cat > "${STAGE_DIR}/pegasus-monitor.wrapper" <<EOF
#!${PY}
import runpy
import sys
from pathlib import Path

import Pegasus.cli

if __name__ == '__main__':
    sys.argv[0] = 'pegasus-monitor'
    runpy.run_path(str(Path(Pegasus.cli.__file__).parent / 'pegasus-monitor.py'), run_name='__main__')
EOF
  put "${STAGE_DIR}/pegasus-monitor.wrapper" "${PEG_VENV}/bin/pegasus-monitor" 0755
  if [ "$(readlink "${WRAPPER}" 2>/dev/null)" != "${PEG_VENV}/bin/pegasus-monitor" ]; then
    sudo ln -sfn "${PEG_VENV}/bin/pegasus-monitor" "${WRAPPER}"
    echo "  ${WRAPPER} -> ${PEG_VENV}/bin/pegasus-monitor"
  fi
fi

# --- 5. provenance manifest --------------------------------------------------
log "writing manifest ${MANIFEST}"
PEGASUS_MONITOR_REF="${PEGASUS_MONITOR_REF}" PEGASUS_SRC_DESC="${PEGASUS_SRC_DESC}" \
  TARBALL_SHA="$(sha "${STAGE_TARBALL}")" TSV="${TSV}" \
  python3 - > "${STAGE_DIR}/manifest.json" <<'PYEOF'
import json, os, time

files = []
with open(os.environ["TSV"]) as fh:
    for line in fh:
        target, before, after, changed, state = line.rstrip("\n").split("\t")
        files.append({
            "target": target,
            "sha256_before": before,
            "sha256_after": after,
            "changed": changed == "yes",
            "state": state,
        })
print(json.dumps({
    "applied_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "pegasus_monitor_ref": os.environ["PEGASUS_MONITOR_REF"] or None,
    "pegasus_src_desc": os.environ["PEGASUS_SRC_DESC"] or None,
    "tarball_sha256": os.environ["TARBALL_SHA"],
    "files": files,
}, indent=2))
PYEOF
sudo install -m 0644 -o root -g root "${STAGE_DIR}/manifest.json" "${MANIFEST}"

# --- 6. does it run? ----------------------------------------------------------
log "verify"
hash -r
v="$(pegasus-monitor --version 2>&1)" || {
  echo "error: pegasus-monitor --version failed: ${v}" >&2
  exit 1
}
echo "  ${v} ($(command -v pegasus-monitor))"
pegasus-monitor --help > /dev/null || {
  echo "error: pegasus-monitor --help failed" >&2
  exit 1
}
echo "  --help ok"

echo
echo "OVERLAY_COMPLETE"
