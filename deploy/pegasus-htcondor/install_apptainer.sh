#!/usr/bin/env bash
# Install Apptainer (the container runtime) on one pool node. Idempotent and
# NON-fatal: a runtime hiccup must never abort a caller's bootstrap, and a node
# that already has apptainer is left alone.
#
# Single source of truth for the install, shared by:
#   - bootstrap_pegasus_node.sh (section 4b, on a fresh build)
#   - provision_pegasus_slice.py --install-apptainer (retrofit onto a running
#     slice that was bootstrapped before this runtime existed)
#
# Inputs (env vars, with defaults):
#   INSTALL_APPTAINER   1 to install, 0 to skip                       (1)
#   APPTAINER_DEB_URL   release .deb fallback if the PPA is unreachable (pinned)
#
# NOT `set -e`: the `||`/`if` fallbacks below intentionally tolerate failures.
set -uo pipefail

INSTALL_APPTAINER="${INSTALL_APPTAINER:-1}"
APPTAINER_DEB_URL="${APPTAINER_DEB_URL:-https://github.com/apptainer/apptainer/releases/download/v1.3.6/apptainer_1.3.6_amd64.deb}"

log() { printf '\n=== %s ===\n' "$*"; }
export DEBIAN_FRONTEND=noninteractive

if [ "${INSTALL_APPTAINER}" = "0" ]; then
  echo "INSTALL_APPTAINER=0 — skipping Apptainer install"
  exit 0
fi
if command -v apptainer >/dev/null 2>&1; then
  echo "apptainer already present: $(apptainer --version 2>/dev/null)"
  exit 0
fi

# Primary: the apptainer PPA (gives a current release + its deps). Fallback: the
# GitHub release .deb if the PPA is unreachable (apt resolves its deps).
log "installing Apptainer (container runtime)"
if sudo apt-get install -y software-properties-common \
   && sudo add-apt-repository -y ppa:apptainer/ppa \
   && sudo apt-get update -y \
   && sudo apt-get install -y apptainer; then :
else
  log "Apptainer PPA unavailable; falling back to the release .deb"
  if curl -fsSLo /tmp/apptainer.deb "${APPTAINER_DEB_URL}"; then
    sudo apt-get install -y /tmp/apptainer.deb || sudo dpkg -i /tmp/apptainer.deb || true
  fi
fi

# Pegasus 5.1 detects apptainer, but some code paths still probe for the
# `singularity` name; add a compat shim if only the apptainer binary exists.
if command -v apptainer >/dev/null 2>&1 && ! command -v singularity >/dev/null 2>&1; then
  sudo ln -sf "$(command -v apptainer)" /usr/local/bin/singularity
fi

if command -v apptainer >/dev/null 2>&1; then
  echo "  apptainer: $(apptainer --version 2>/dev/null)"
else
  echo "  ! Apptainer install FAILED (PPA + .deb both unreachable); containerized"
  echo "    jobs will not run on this node. The pool itself is unaffected."
fi
