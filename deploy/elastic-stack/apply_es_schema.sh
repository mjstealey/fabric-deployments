#!/usr/bin/env bash
# Apply the workflow-monitor schema (ILM policy, index templates, write-alias
# bootstrap, scoped vector_ingest role/user) to the Elasticsearch on this node.
#
# Safe to re-run against a LIVE cluster -- this is both the schema step of a
# fresh bootstrap (invoked by bootstrap_es_node.sh) and the retrofit path for
# schema additions on a running slice (invoked by provision_es_slice.py
# --apply-schema, e.g. when the monitord-events-* family was added):
#   * templates and the ILM policy are plain PUTs (versioned upserts in ES)
#   * write aliases are bootstrapped only when the alias does not exist yet;
#     an existing family keeps its current write index untouched
#   * the vector_ingest role PUT is a full replace -- how new index patterns
#     reach an existing cluster
#   * the vector_ingest user is created only if missing; an existing user's
#     password is NEVER reset (Vector on the producer slice authenticates with
#     it -- see deploy/README.md)
#
# The index families are derived from templates/*.json, so adding a family is:
# drop a template file in templates/, re-run this script.
#
# Inputs (env vars):
#   ELASTIC_PASSWORD         REQUIRED (on a bootstrapped node: in ES_DIR/.env)
#   ELASTIC_INGEST_PASSWORD  only needed when the vector_ingest user is missing
#   ES_DIR                   uploaded elastic-stack dir   ($HOME/elastic-stack)
#   ES_HTTP_PORT             published HTTP port          (9200)
set -euo pipefail

ES_DIR="${ES_DIR:-$HOME/elastic-stack}"
ES_HTTP_PORT="${ES_HTTP_PORT:-9200}"
ELASTIC_PASSWORD="${ELASTIC_PASSWORD:?set ELASTIC_PASSWORD (on the node: set -a; . ${ES_DIR}/.env)}"
ELASTIC_INGEST_PASSWORD="${ELASTIC_INGEST_PASSWORD:-}"

BASE="https://localhost:${ES_HTTP_PORT}"
CURL=(curl -sS --cacert "${ES_DIR}/certs/ca/ca.crt" -u "elastic:${ELASTIC_PASSWORD}")
log() { printf '\n=== %s ===\n' "$*"; }
status_of() { "${CURL[@]}" -o /dev/null -w '%{http_code}' "$@"; }

FAMILIES=()
for t in "${ES_DIR}"/templates/*.json; do
  FAMILIES+=("$(basename "${t}" .json)")
done
[ "${#FAMILIES[@]}" -gt 0 ] || { echo "error: no templates in ${ES_DIR}/templates" >&2; exit 1; }

log "applying ILM policy workflow-retention"
"${CURL[@]}" -X PUT "${BASE}/_ilm/policy/workflow-retention" \
  -H 'Content-Type: application/json' \
  -d @"${ES_DIR}/ilm/workflow-retention.json"
echo

log "applying index templates: ${FAMILIES[*]}"
for fam in "${FAMILIES[@]}"; do
  "${CURL[@]}" -X PUT "${BASE}/_index_template/${fam}" \
    -H 'Content-Type: application/json' \
    -d @"${ES_DIR}/templates/${fam}.json"
  echo
done

log "bootstrapping write aliases (load-bearing for ILM rollover)"
for fam in "${FAMILIES[@]}"; do
  if [ "$(status_of "${BASE}/_alias/${fam}-default")" = "200" ]; then
    echo "  ${fam}-default alias exists -- current write index left untouched"
    continue
  fi
  # No alias, but something answers at the name: a sink with auto_configure
  # wrote before the alias was bootstrapped and auto-created a CONCRETE index
  # squatting on the alias name. That permanently blocks rollover; creating
  # the alias would fail anyway, so stop with the remediation spelled out.
  if [ "$(status_of "${BASE}/${fam}-default")" = "200" ]; then
    echo "error: '${fam}-default' exists as a concrete index, not an alias." >&2
    echo "  A sink wrote before this schema was applied. Remediate, then re-run:" >&2
    echo "    1. reindex if the stray docs matter:  POST _reindex {\"source\":{\"index\":\"${fam}-default\"},\"dest\":{\"index\":\"${fam}-000001\"}}" >&2
    echo "    2. DELETE /${fam}-default" >&2
    exit 1
  fi
  # %3C / %3E are url-encoded < > for the date-math index name.
  "${CURL[@]}" -X PUT "${BASE}/%3C${fam}-000001%3E" \
    -H 'Content-Type: application/json' \
    -d "{\"aliases\":{\"${fam}-default\":{\"is_write_index\":true}}}"
  echo
done

log "vector_ingest role (full replace; grants: ${FAMILIES[*]/%/-*})"
NAMES="$(printf '"%s-*",' "${FAMILIES[@]}")"
NAMES="[${NAMES%,}]"
"${CURL[@]}" -X PUT "${BASE}/_security/role/vector_ingest" \
  -H 'Content-Type: application/json' -d "{
    \"indices\": [{
      \"names\": ${NAMES},
      \"privileges\": [\"write\",\"create_index\",\"auto_configure\",\"view_index_metadata\"]
    }],
    \"cluster\": [\"monitor\"]
  }"
echo

log "vector_ingest user"
user_status="$(status_of "${BASE}/_security/user/vector_ingest")"
if [ "${user_status}" = "200" ]; then
  echo "  user exists -- password left untouched"
elif [ -z "${ELASTIC_INGEST_PASSWORD}" ]; then
  echo "error: vector_ingest user is missing and ELASTIC_INGEST_PASSWORD is not set" >&2
  exit 1
else
  "${CURL[@]}" -X POST "${BASE}/_security/user/vector_ingest" \
    -H 'Content-Type: application/json' \
    -d "{\"password\":\"${ELASTIC_INGEST_PASSWORD}\",\"roles\":[\"vector_ingest\"]}"
  echo
fi

log "schema apply complete"
