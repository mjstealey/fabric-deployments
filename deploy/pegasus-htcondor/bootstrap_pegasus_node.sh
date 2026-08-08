#!/usr/bin/env bash
# Bring up one node of the pegasus-htcondor pool on a fresh FABRIC Ubuntu node.
#
# Role-aware:
#   ROLE=cm       submit/central-manager: condor CM+schedd, Pegasus,
#                 workflow-monitor, Vector (installed, left STOPPED until wire-es)
#   ROLE=execute  worker: condor startd + Pegasus (pegasus-keg / worker tools
#                 are needed on execute nodes for condorio 'installed' jobs)
#
# Normally uploaded + run by provision_pegasus_slice.py --bootstrap, but it is
# self-contained and can be run by hand on a node:
#
#   ROLE=cm CONDOR_HOST=10.128.5.2 NODE_IP=10.128.5.2 FABNET_CIDR=10.128.5.0/24 \
#     POOL_PASSWORD=secret bash bootstrap_pegasus_node.sh
#
# Inputs (env vars, with defaults):
#   ROLE                   cm | execute                               (cm)
#   CONDOR_HOST            submit/CM FABNet IP (pool central manager)  REQUIRED
#   NODE_IP                this node's own FABNet IP (daemon bind addr) REQUIRED
#   FABNET_CIDR            pool subnet for host authorization          (CONDOR_HOST/24)
#   POOL_PASSWORD          shared pool signing key                     REQUIRED
#   HTCONDOR_VERSION       exact HTCondor apt version, e.g. 25.12.2    (channel latest)
#   HTCONDOR_CHANNEL       wisc repo channel                           (<major>.x of
#                          HTCONDOR_VERSION, else 24.x)
#   PEGASUS_VERSION        Pegasus apt version (best-effort)           (5.1.2)
#   INSTALL_APPTAINER      install Apptainer container runtime         (1; 0 to skip)
#   PEG_DIR                uploaded asset dir                          ($HOME/pegasus-htcondor)
#   RUNS_DIR              (cm) absolute submit-dir root                (/opt/workflows)
#   ES_HOST              (cm) ES hostname baked into vector.toml       (workflow-monitor-es)
#   WORKFLOW_MONITOR_SPEC (cm) pip spec for workflow-monitor           (git URL)
#   INSTALL_VECTOR       (cm) non-empty to install Vector             (unset)
set -euo pipefail

ROLE="${ROLE:-cm}"
CONDOR_HOST="${CONDOR_HOST:?set CONDOR_HOST to the submit/CM FABNet IP}"
NODE_IP="${NODE_IP:?set NODE_IP to this node FABNet IP}"
FABNET_CIDR="${FABNET_CIDR:-${CONDOR_HOST%.*}.0/24}"
POOL_PASSWORD="${POOL_PASSWORD:?set POOL_PASSWORD to the shared pool signing key}"
HTCONDOR_VERSION="${HTCONDOR_VERSION:-}"
if [ -n "${HTCONDOR_VERSION}" ]; then
  HTCONDOR_CHANNEL="${HTCONDOR_CHANNEL:-${HTCONDOR_VERSION%%.*}.x}"
else
  HTCONDOR_CHANNEL="${HTCONDOR_CHANNEL:-24.x}"
fi
PEGASUS_VERSION="${PEGASUS_VERSION:-5.1.2}"
PEG_DIR="${PEG_DIR:-$HOME/pegasus-htcondor}"
RUNS_DIR="${RUNS_DIR:-/opt/workflows}"
ES_HOST="${ES_HOST:-workflow-monitor-es}"
WORKFLOW_MONITOR_SPEC="${WORKFLOW_MONITOR_SPEC:-git+https://github.com/pegasus-isi/workflow-monitor.git}"
INSTALL_VECTOR="${INSTALL_VECTOR:-}"
# Container runtime (every node): Pegasus runs containerized transformations via
# singularity/apptainer on the EXECUTE nodes, but its package pulls in none. The
# install itself lives in install_apptainer.sh (shared with --install-apptainer);
# INSTALL_APPTAINER=0 skips it, APPTAINER_DEB_URL overrides the .deb fallback.
INSTALL_APPTAINER="${INSTALL_APPTAINER:-1}"
# Vector apt setup script (Datadog-era; the old repositories.timber.io host is
# dead). Fallback: the GitHub release .deb if the repo is unreachable.
# VECTOR_VERSION is PINNED: 0.57.0 stopped interpolating ${VAR} env references
# in TOML configs, so auth.password = "${ELASTIC_INGEST_PASSWORD}" reaches ES
# verbatim and every sink healthcheck 401s. 0.56.0 is the last known-good.
VECTOR_VERSION="${VECTOR_VERSION:-0.56.0}"
VECTOR_SETUP_URL="${VECTOR_SETUP_URL:-https://setup.vector.dev}"
VECTOR_DEB_URL="${VECTOR_DEB_URL:-https://github.com/vectordotdev/vector/releases/download/v${VECTOR_VERSION}/vector_${VECTOR_VERSION}-1_amd64.deb}"

log() { printf '\n=== %s ===\n' "$*"; }
export DEBIAN_FRONTEND=noninteractive

# --- 1. base deps ----------------------------------------------------------
log "installing base packages"
sudo apt-get update -y
sudo apt-get install -y curl gnupg ca-certificates lsb-release python3 python3-pip unzip git
CODENAME="$(lsb_release -cs)"   # 'jammy' on default_ubuntu_22

# --- 2. HTCondor -----------------------------------------------------------
if ! command -v condor_version >/dev/null 2>&1; then
  log "installing HTCondor (research.cs.wisc.edu repo, ${HTCONDOR_CHANNEL}${HTCONDOR_VERSION:+, pinned ${HTCONDOR_VERSION}})"
  # research.cs.wisc.edu (and its htcss-downloads.chtc.wisc.edu redirect target)
  # serves a misordered TLS chain that OpenSSL tolerates but apt's GnuTLS
  # rejects ("certificate issuer is unknown"). Anchor the InCommon intermediate
  # the server itself presents so apt can verify the leaf directly; if wisc
  # fixes the chain the extraction finds nothing and apt works regardless.
  if [ ! -f /usr/local/share/ca-certificates/incommon-rsa-server-ca-2.crt ]; then
    echo | openssl s_client -connect research.cs.wisc.edu:443 \
        -servername research.cs.wisc.edu -showcerts 2>/dev/null \
      | awk '/BEGIN CERTIFICATE/{n++} n>0{print > ("/tmp/htcondor-chain" n ".pem")}'
    for f in /tmp/htcondor-chain*.pem; do
      [ -f "$f" ] || continue
      if openssl x509 -in "$f" -noout -subject 2>/dev/null | grep -q "InCommon RSA Server CA 2"; then
        sudo cp "$f" /usr/local/share/ca-certificates/incommon-rsa-server-ca-2.crt
        sudo update-ca-certificates >/dev/null
        break
      fi
    done
    rm -f /tmp/htcondor-chain*.pem
  fi
  curl -fsSL "https://research.cs.wisc.edu/htcondor/repo/keys/HTCondor-${HTCONDOR_CHANNEL}-Key" \
    | sudo gpg --batch --yes --dearmor -o /usr/share/keyrings/htcondor.gpg
  echo "deb [signed-by=/usr/share/keyrings/htcondor.gpg] https://research.cs.wisc.edu/htcondor/repo/ubuntu/${HTCONDOR_CHANNEL} ${CODENAME} main" \
    | sudo tee /etc/apt/sources.list.d/htcondor.list >/dev/null
  sudo apt-get update -y
  if [ -n "${HTCONDOR_VERSION}" ]; then
    # Resolve the full apt version string (e.g. 25.12.2 -> 25.12.2-1+ubu22); a
    # pin that is not in the channel must fail loudly, not fall back silently.
    # awk must read ALL input (no early exit): under pipefail an early exit
    # SIGPIPEs apt-cache and set -e kills the script with no message.
    CONDOR_APT_VER="$(apt-cache madison condor \
      | awk -v v="${HTCONDOR_VERSION}-" 'ver == "" && index($3, v) == 1 {ver=$3} END{print ver}')"
    [ -n "${CONDOR_APT_VER}" ] \
      || { echo "  ! condor ${HTCONDOR_VERSION} not found in the ${HTCONDOR_CHANNEL} repo"; exit 1; }
    echo "  resolved condor apt version: ${CONDOR_APT_VER}"
    sudo apt-get install -y "condor=${CONDOR_APT_VER}"
  else
    sudo apt-get install -y condor
  fi
fi

# --- 3. condor config drop-ins + shared pool signing key -------------------
log "configuring HTCondor (role=${ROLE}, CONDOR_HOST=${CONDOR_HOST})"
render() {  # substitute placeholders in an uploaded conf, emit to stdout
  sed -e "s|__CONDOR_HOST__|${CONDOR_HOST}|g" \
      -e "s|__NODE_IP__|${NODE_IP}|g" \
      -e "s|__FABNET_CIDR__|${FABNET_CIDR}|g" "$1"
}
render "${PEG_DIR}/condor/00-pool-common.conf" \
  | sudo tee /etc/condor/config.d/00-pool-common.conf >/dev/null
if [ "${ROLE}" = "cm" ]; then
  render "${PEG_DIR}/condor/10-central-manager.conf" \
    | sudo tee /etc/condor/config.d/10-central-manager.conf >/dev/null
else
  render "${PEG_DIR}/condor/20-execute.conf" \
    | sudo tee /etc/condor/config.d/20-execute.conf >/dev/null
fi

# Store the SAME pool signing key on every node so daemons trust each other
# under the PASSWORD method. Try the modern then legacy condor_store_cred forms.
sudo mkdir -p /etc/condor/passwords.d
if   printf '%s' "${POOL_PASSWORD}" | sudo condor_store_cred -c add 2>/dev/null; then :
elif printf '%s' "${POOL_PASSWORD}" | sudo condor_store_cred add -c -i - 2>/dev/null; then :
elif sudo condor_store_cred -c add -p "${POOL_PASSWORD}" 2>/dev/null; then :
else echo "  ! condor_store_cred failed — store the pool password by hand (see PEGASUS-HTCONDOR.md)"; fi

sudo systemctl enable condor
sudo systemctl restart condor

# --- 4. Pegasus (ALL nodes) ------------------------------------------------
# Submit node needs the planner; execute nodes need pegasus-keg + the worker
# tools (pegasus-kickstart/transfer) for condorio 'installed' transformations.
if ! command -v pegasus-version >/dev/null 2>&1; then
  log "installing Pegasus ${PEGASUS_VERSION} (download.pegasus.isi.edu repo)"
  curl -fsSL https://download.pegasus.isi.edu/pegasus/gpg.txt \
    | sudo gpg --batch --yes --dearmor -o /usr/share/keyrings/pegasus.gpg
  echo "deb [signed-by=/usr/share/keyrings/pegasus.gpg] https://download.pegasus.isi.edu/pegasus/ubuntu ${CODENAME} main" \
    | sudo tee /etc/apt/sources.list.d/pegasus.list >/dev/null
  sudo apt-get update -y
  sudo apt-get install -y "pegasus=${PEGASUS_VERSION}-1+${CODENAME}" \
    || sudo apt-get install -y pegasus \
    || echo "  ! Pegasus apt install failed; see PEGASUS-HTCONDOR.md for alternatives"
fi

# --- 4b. Apptainer (container runtime, ALL nodes) --------------------------
# Containerized Pegasus transformations (e.g. earthquake-workflow's
# docker://kthare10/earthquake-analysis image) run via singularity/apptainer on
# the EXECUTE nodes. The install logic lives in install_apptainer.sh so it has a
# single home, shared with `provision_pegasus_slice.py --install-apptainer`.
# Export the knobs so the child script (a separate process) sees them; an unset
# APPTAINER_DEB_URL is simply omitted (the child has its own default). Never let
# the install abort the bootstrap.
export INSTALL_APPTAINER APPTAINER_DEB_URL
bash "${PEG_DIR}/install_apptainer.sh" || true

if [ "${ROLE}" != "cm" ]; then
  log "execute node ready"
  condor_version || true
  exit 0
fi

# ===========================================================================
# submit/CM-only from here
# ===========================================================================

# --- 5. runs dir -----------------------------------------------------------
# Outside /home on purpose: the Vector systemd unit sets ProtectHome=true, so a
# home-relative submit dir would be invisible to Vector. /opt is on its
# ReadOnlyPaths list.
log "preparing runs dir ${RUNS_DIR}"
sudo mkdir -p "${RUNS_DIR}"
sudo chown "$(id -un):$(id -gn)" "${RUNS_DIR}"

# --- 6. workflow-monitor ---------------------------------------------------
# Turns pegasus-monitord's stampede DB into workflow-events.jsonl +
# diagnostics-events.jsonl (the streams the ES index templates expect).
log "installing workflow-monitor (${WORKFLOW_MONITOR_SPEC})"
python3 -m pip install --user --upgrade pip >/dev/null 2>&1 || true
python3 -m pip install --user "${WORKFLOW_MONITOR_SPEC}" \
  || echo "  ! workflow-monitor install failed; install it by hand (pip install <spec>)"
# Pegasus Python API for the example generator (so `python3 diamond.py` can
# `import Pegasus.api` regardless of how the apt package exposes it).
python3 -m pip install --user pegasus-wms.api >/dev/null 2>&1 \
  || echo "  ! pegasus-wms.api install failed; needed only for --run-example"
if ! grep -q '.local/bin' "${HOME}/.bashrc" 2>/dev/null; then
  echo 'export PATH="$HOME/.local/bin:$PATH"' >> "${HOME}/.bashrc"
fi

# --- 7. Vector (installed + configured, left STOPPED until --wire-es) ------
if [ -n "${INSTALL_VECTOR}" ]; then
  if ! command -v vector >/dev/null 2>&1; then
    log "installing Vector (apt repo ${VECTOR_SETUP_URL})"
    # Non-fatal: the pool + workflow-monitor must come up even if Vector
    # packaging hiccups; Vector can be (re)wired later with --wire-es. Using `if`
    # keeps these failures from tripping `set -e`.
    if curl -1sLf "${VECTOR_SETUP_URL}" | sudo -E bash; then
      sudo apt-get install -y "vector=${VECTOR_VERSION}-1" || true
    fi
    if ! command -v vector >/dev/null 2>&1; then
      log "apt repo unavailable (or lacks ${VECTOR_VERSION}); falling back to Vector release .deb"
      if curl -1sLfo /tmp/vector.deb "${VECTOR_DEB_URL}"; then
        sudo apt-get install -y /tmp/vector.deb || sudo dpkg -i /tmp/vector.deb || true
      fi
    fi
    sudo apt-mark hold vector >/dev/null 2>&1 || true
  fi
  if command -v vector >/dev/null 2>&1; then
    log "rendering Vector config (sink https://${ES_HOST}:9200, tailing ${RUNS_DIR})"
    id vector >/dev/null 2>&1 \
      || sudo useradd --system --shell /usr/sbin/nologin --home /var/lib/vector vector
    sudo install -d -o vector -g vector -m 0755 /var/lib/vector
    sudo install -d -o root -g vector -m 0750 /etc/vector
    sed -e "s|__RUNS_DIR__|${RUNS_DIR}|g" \
        -e "s|__ES_HOST__|${ES_HOST}|g" \
        -e "s|__CA_FILE__|/etc/vector/ca.crt|g" \
        "${PEG_DIR}/vector/vector.toml.tmpl" \
      | sudo tee /etc/vector/vector.toml >/dev/null
    sudo chown root:vector /etc/vector/vector.toml
    sudo chmod 0640 /etc/vector/vector.toml
    sudo install -o root -g root -m 0644 \
      "${PEG_DIR}/vector/vector.service" /etc/systemd/system/vector.service
    sudo systemctl daemon-reload
    echo "  Vector installed but NOT started (no CA / .env yet) — run provision_pegasus_slice.py --wire-es"
  else
    echo "  ! Vector install FAILED (repo + .deb both unreachable); the pool is unaffected."
    echo "    Install vector by hand on the submit node, then re-run --wire-es."
  fi
fi

log "submit/CM node ready"
condor_status -schedd 2>/dev/null || true
echo "pool central manager: ${CONDOR_HOST}"
echo "submit a workflow under: workflow-monitor ${RUNS_DIR}/submit/<run> --serve --diagnose --log"
