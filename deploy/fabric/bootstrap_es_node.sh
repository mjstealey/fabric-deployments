#!/usr/bin/env bash
# Bring up the workflow-monitor Elasticsearch on a fresh FABRIC node.
#
# Installs Docker, regenerates the node TLS cert WITH the slice's FABNet IP in
# its SAN, brings up single-node ES 8.x over the data plane (:9200), and applies
# the ILM policy, index templates, write-alias bootstrap, and the scoped
# vector_ingest role/user -- the same schema/security flow as deploy/README.md.
#
# Normally uploaded and run by provision_es_slice.py --bootstrap-es, but it is
# self-contained and can be run by hand on the node:
#
#   ES_IP=10.128.5.2 bash bootstrap_es_node.sh
#
# Inputs (env vars, with defaults):
#   ES_IP                    REQUIRED -- the node's FABNet data-plane IP (cert SAN)
#   ES_HOSTNAME              cert DNS SAN / Vector hostname   (workflow-monitor-es)
#   ES_VERSION               Elasticsearch image tag          (8.15.0)
#   ES_HTTP_PORT             published HTTP port              (9200)
#   ES_HEAP                  JVM heap, e.g. 16g               (1g)
#   ELASTIC_PASSWORD         superuser password               (changeme-elastic)
#   ELASTIC_INGEST_PASSWORD  vector_ingest password           (changeme-vector-ingest)
#   ES_DIR                   uploaded elastic-stack dir        ($HOME/elastic-stack)
set -euo pipefail

ES_IP="${ES_IP:?set ES_IP to the node FABNet data-plane IP}"
ES_HOSTNAME="${ES_HOSTNAME:-workflow-monitor-es}"
ES_VERSION="${ES_VERSION:-8.15.0}"
ES_HTTP_PORT="${ES_HTTP_PORT:-9200}"
ES_HEAP="${ES_HEAP:-1g}"
ELASTIC_PASSWORD="${ELASTIC_PASSWORD:-changeme-elastic}"
ELASTIC_INGEST_PASSWORD="${ELASTIC_INGEST_PASSWORD:-changeme-vector-ingest}"
ES_DIR="${ES_DIR:-$HOME/elastic-stack}"

IMAGE="docker.elastic.co/elasticsearch/elasticsearch:${ES_VERSION}"
log() { printf '\n=== %s ===\n' "$*"; }

# --- 1. Docker -------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  log "installing Docker (get.docker.com convenience script)"
  # Reaches IPv4-only hosts via FABRIC DNS64 on IPv6-management sites.
  curl -fsSL https://get.docker.com | sudo sh
fi
sudo systemctl enable --now docker
DOCKER="sudo docker"

# --- 2. TLS certs (regenerated WITH the FABNet IP in the SAN) --------------
# Vector verifies the cert against the address it connects to. Including ES_IP
# lets it connect by IP; ES_HOSTNAME keeps the by-name path working too.
log "generating CA + node cert (SAN: DNS ${ES_HOSTNAME},localhost / IP ${ES_IP},127.0.0.1)"
mkdir -p "${ES_DIR}/certs"
$DOCKER run --rm -v "${ES_DIR}/certs:/certs" -u 0 "${IMAGE}" bash -c "
  set -e
  cd /certs
  /usr/share/elasticsearch/bin/elasticsearch-certutil ca --silent --pem \
    --out /certs/ca.zip </dev/null
  unzip -o /certs/ca.zip
  /usr/share/elasticsearch/bin/elasticsearch-certutil cert --silent \
    --ca-cert /certs/ca/ca.crt --ca-key /certs/ca/ca.key \
    --pem --out /certs/node.zip --name node \
    --dns localhost,${ES_HOSTNAME} --ip 127.0.0.1,${ES_IP} </dev/null
  unzip -o /certs/node.zip
  chmod -R a+r /certs"

# --- 3. compose overrides (port + heap) ------------------------------------
# Edit the *uploaded* copy only -- the committed compose maps 9210 (laptop
# Roon collision) and pins 1g heap. On a clean VM we want :ES_HTTP_PORT and a
# heap sized to the node.
log "configuring compose (port ${ES_HTTP_PORT}, heap ${ES_HEAP})"
sed -i \
  -e "s|\"9210:9200\"|\"${ES_HTTP_PORT}:9200\"|" \
  -e "s|-Xms1g -Xmx1g|-Xms${ES_HEAP} -Xmx${ES_HEAP}|" \
  "${ES_DIR}/docker-compose.yml"

cat > "${ES_DIR}/.env" <<EOF
ELASTIC_PASSWORD=${ELASTIC_PASSWORD}
EOF
chmod 600 "${ES_DIR}/.env"

# --- 4. bring up + wait healthy --------------------------------------------
log "starting Elasticsearch"
( cd "${ES_DIR}" && $DOCKER compose up -d )
echo -n "waiting for healthy"
for _ in $(seq 1 60); do
  status="$($DOCKER inspect --format '{{.State.Health.Status}}' workflow-monitor-es 2>/dev/null || echo starting)"
  [ "${status}" = "healthy" ] && { echo " ok"; break; }
  echo -n "."; sleep 3
done

# --- 5. schema, retention, security ----------------------------------------
# Shared with the running-cluster retrofit path (provision_es_slice.py
# --apply-schema): ILM policy, one template per templates/*.json family,
# write-alias bootstrap, scoped vector_ingest role/user.
log "applying schema, retention, security (apply_es_schema.sh)"
ES_DIR="${ES_DIR}" ES_HTTP_PORT="${ES_HTTP_PORT}" \
  ELASTIC_PASSWORD="${ELASTIC_PASSWORD}" \
  ELASTIC_INGEST_PASSWORD="${ELASTIC_INGEST_PASSWORD}" \
  bash "${ES_DIR}/apply_es_schema.sh"

# Section 6 below provisions Kibana over the same endpoint/credentials.
BASE="https://localhost:${ES_HTTP_PORT}"
CURL=(curl -sS --cacert "${ES_DIR}/certs/ca/ca.crt" -u "elastic:${ELASTIC_PASSWORD}")

# --- 6. Kibana (overlay; tunnel-only) --------------------------------------
# Mint a kibana_system service-account token now that ES is healthy, stash it
# (+ a saved-objects encryption key) in .env, and bring Kibana up alongside the
# base compose. Non-fatal: ES and the ingest path are already in service if this
# hiccups. DELETE-then-POST keeps token creation idempotent across re-runs.
log "creating kibana_system token + starting Kibana (overlay, :5601 tunnel-only)"
"${CURL[@]}" -X DELETE \
  "${BASE}/_security/service/elastic/kibana/credential/token/kibana-fabric" \
  >/dev/null 2>&1 || true
KB_TOKEN="$( ( "${CURL[@]}" -X POST \
  "${BASE}/_security/service/elastic/kibana/credential/token/kibana-fabric" \
  || true ) | grep -o '"value":"[^"]*"' | cut -d'"' -f4 || true)"
if [ -n "${KB_TOKEN}" ] && [ -f "${ES_DIR}/docker-compose.kibana.yml" ]; then
  { echo "KIBANA_SERVICE_TOKEN=${KB_TOKEN}";
    echo "KIBANA_ENCRYPTION_KEY=$(openssl rand -hex 32)"; } >> "${ES_DIR}/.env"
  if ( cd "${ES_DIR}" && $DOCKER compose \
        -f docker-compose.yml -f docker-compose.kibana.yml up -d kibana ); then
    echo "  Kibana starting on :5601 — tunnel: ssh -L 5601:localhost:5601 … ubuntu@<node>"
  else
    echo "  ! Kibana bring-up failed; ES is unaffected. Start it by hand later."
  fi
else
  echo "  ! kibana token not created (or overlay missing); skipping Kibana. ES is unaffected."
fi

log "done"
echo "Elasticsearch reachable at https://${ES_HOSTNAME}:${ES_HTTP_PORT} (IP ${ES_IP})"
echo "CA for the Vector side: ${ES_DIR}/certs/ca/ca.crt"
