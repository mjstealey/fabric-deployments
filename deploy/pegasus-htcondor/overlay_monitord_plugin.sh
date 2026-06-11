#!/usr/bin/env bash
# Overlay the pegasus-monitord plugin system onto the apt-installed Pegasus.
#
# The plugin system (pegasus-isi/pegasus branch monitord-plugin-system) is pure
# Python: one new file plus two modified ones under the installed tree at
# /usr/lib/pegasus/python/Pegasus/. Overlaying them is the whole "build" -- no
# compilation, no entry-point registration (plugins self-discover from installed
# packages via importlib.metadata). See deploy/MONITORD-PLUGIN.md.
#
# Idempotent: files are byte-compared first and replaced only when they differ.
# A <file>.orig backup is taken only when the current file still matches the
# md5 recorded by dpkg for the pegasus package (i.e. it is pristine apt
# content); plugin.py does not exist in the apt package and never gets one.
# Every run rewrites the provenance manifest at
#   /usr/lib/pegasus/python/Pegasus/monitoring/.monitord-plugin-manifest.json
#
# Normally uploaded and run by provision_pegasus_slice.py
# --install-monitord-plugin, but self-contained:
#
#   SOURCE_MODE=fetch PEGASUS_PLUGIN_REF=<sha> bash overlay_monitord_plugin.sh
#
# Inputs (env vars):
#   SOURCE_MODE         fetch    -- curl the 3 files from raw.githubusercontent
#                                   .com at PEGASUS_PLUGIN_REF (default)
#                       uploaded -- the 3 files are already in STAGE_DIR
#                                   (provisioner --pegasus-src upload path)
#   PEGASUS_PLUGIN_REF  REQUIRED for fetch -- full commit SHA (reproducibility);
#                                   recorded in the manifest either way
#   PEGASUS_SRC_DESC    provenance note for uploaded mode (git describe --dirty)
#   STAGE_DIR           staging dir ($HOME/pegasus-htcondor/monitord-plugin)
set -euo pipefail

SOURCE_MODE="${SOURCE_MODE:-fetch}"
PEGASUS_PLUGIN_REF="${PEGASUS_PLUGIN_REF:-}"
PEGASUS_SRC_DESC="${PEGASUS_SRC_DESC:-}"
STAGE_DIR="${STAGE_DIR:-$HOME/pegasus-htcondor/monitord-plugin}"

PEG_TREE=/usr/lib/pegasus/python/Pegasus
MANIFEST="${PEG_TREE}/monitoring/.monitord-plugin-manifest.json"
RAW_BASE="https://raw.githubusercontent.com/pegasus-isi/pegasus/${PEGASUS_PLUGIN_REF}"
SRC_PREFIX="packages/pegasus-python/src/Pegasus"

# name : path under the Pegasus tree (same shape in the repo under SRC_PREFIX)
FILES=(
  "plugin.py:monitoring/plugin.py"
  "event_output.py:monitoring/event_output.py"
  "pegasus-monitord.py:cli/pegasus-monitord.py"
)

log() { printf '\n=== %s ===\n' "$*"; }
sha() { [ -e "$1" ] && sha256sum "$1" | awk '{print $1}' || echo absent; }

[ -d "${PEG_TREE}/monitoring" ] || {
  echo "error: ${PEG_TREE}/monitoring not found -- is the pegasus apt package installed?" >&2
  exit 1
}

# --- 1. stage ---------------------------------------------------------------
mkdir -p "${STAGE_DIR}"
if [ "${SOURCE_MODE}" = "fetch" ]; then
  [ -n "${PEGASUS_PLUGIN_REF}" ] || {
    echo "error: SOURCE_MODE=fetch needs PEGASUS_PLUGIN_REF (full commit SHA)" >&2
    exit 1
  }
  log "fetching 3 files from pegasus-isi/pegasus @ ${PEGASUS_PLUGIN_REF}"
  for entry in "${FILES[@]}"; do
    name="${entry%%:*}"; rel="${entry#*:}"
    curl -fsSL "${RAW_BASE}/${SRC_PREFIX}/${rel}" -o "${STAGE_DIR}/${name}"
    echo "  staged ${name}"
  done
else
  log "using pre-uploaded files in ${STAGE_DIR} (${PEGASUS_SRC_DESC:-no provenance note})"
  for entry in "${FILES[@]}"; do
    name="${entry%%:*}"
    [ -s "${STAGE_DIR}/${name}" ] || {
      echo "error: expected ${STAGE_DIR}/${name} (uploaded by --pegasus-src)" >&2
      exit 1
    }
  done
fi

# --- 2. syntax gate before touching the install ------------------------------
log "py_compile gate"
for entry in "${FILES[@]}"; do
  name="${entry%%:*}"
  python3 -m py_compile "${STAGE_DIR}/${name}"
  echo "  ${name} compiles"
done

# --- 3. install (byte-compare; .orig only for pristine apt files) -----------
log "overlaying into ${PEG_TREE}"
TSV="${STAGE_DIR}/.files.tsv"
: > "${TSV}"
DPKG_MD5=/var/lib/dpkg/info/pegasus.md5sums
for entry in "${FILES[@]}"; do
  name="${entry%%:*}"; rel="${entry#*:}"
  target="${PEG_TREE}/${rel}"
  before="$(sha "${target}")"
  orig_created=no
  if cmp -s "${STAGE_DIR}/${name}" "${target}" 2>/dev/null; then
    echo "  ${rel}: unchanged"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${name}" "${target}" "${before}" "${before}" no "${orig_created}" >> "${TSV}"
    continue
  fi
  if [ -e "${target}" ] && [ ! -e "${target}.orig" ] && [ -r "${DPKG_MD5}" ]; then
    pkg_md5="$(grep "  *usr/lib/pegasus/python/Pegasus/${rel}\$" "${DPKG_MD5}" \
                 | awk '{print $1}' || true)"
    cur_md5="$(md5sum "${target}" | awk '{print $1}')"
    if [ -n "${pkg_md5}" ] && [ "${pkg_md5}" = "${cur_md5}" ]; then
      sudo cp -p "${target}" "${target}.orig"
      orig_created=yes
      echo "  ${rel}: pristine apt file -> backed up to $(basename "${target}").orig"
    fi
  fi
  sudo install -m 0644 -o root -g root "${STAGE_DIR}/${name}" "${target}"
  after="$(sha "${target}")"
  echo "  ${rel}: ${before:0:12} -> ${after:0:12}"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${name}" "${target}" "${before}" "${after}" yes "${orig_created}" >> "${TSV}"
done

# Stale bytecode from a previous overlay would shadow the new sources.
sudo rm -f "${PEG_TREE}/monitoring/__pycache__"/{plugin,event_output}.*.pyc \
           "${PEG_TREE}/cli/__pycache__"/pegasus-monitord*.pyc 2>/dev/null || true

# --- 4. provenance manifest ---------------------------------------------------
log "writing manifest ${MANIFEST}"
SOURCE_MODE="${SOURCE_MODE}" PEGASUS_PLUGIN_REF="${PEGASUS_PLUGIN_REF}" \
  PEGASUS_SRC_DESC="${PEGASUS_SRC_DESC}" TSV="${TSV}" \
  python3 - > "${STAGE_DIR}/manifest.json" <<'PYEOF'
import json, os, time

files = []
with open(os.environ["TSV"]) as fh:
    for line in fh:
        name, target, before, after, changed, orig = line.rstrip("\n").split("\t")
        files.append({
            "name": name,
            "target": target,
            "sha256_before": before,
            "sha256_after": after,
            "changed": changed == "yes",
            "orig_backup_created": orig == "yes",
        })
print(json.dumps({
    "applied_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "source_mode": os.environ["SOURCE_MODE"],
    "pegasus_plugin_ref": os.environ["PEGASUS_PLUGIN_REF"] or None,
    "pegasus_src_desc": os.environ["PEGASUS_SRC_DESC"] or None,
    "files": files,
}, indent=2))
PYEOF
sudo install -m 0644 -o root -g root "${STAGE_DIR}/manifest.json" "${MANIFEST}"
cat "${STAGE_DIR}/manifest.json"

echo
echo "OVERLAY_COMPLETE"
