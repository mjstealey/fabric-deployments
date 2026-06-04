#!/usr/bin/env bash
# Import the workflow-monitor Kibana dashboards + create the current-state transform.
#
# Run this AFTER the ES slice is up and Vector has shipped at least one workflow
# (so the indices + fields exist). On the ES node both ES and Kibana are on
# localhost; from a laptop, tunnel first:
#   ssh -L 5601:localhost:5601 -L 9200:localhost:9200 ubuntu@<es-node>
# (provision_es_slice.py --push-kibana does the upload + runs this for you.)
#
# Env (defaults assume the deployed node + deploy/elastic-stack/.env):
#   KIBANA_URL   default http://localhost:5601
#   ES_URL       default https://localhost:9200   (bootstrap rewrites :9210->:9200;
#                                                   use :9210 only for local dev)
#   ELASTIC_PASSWORD / ELASTIC_USER  (ELASTIC_USER default 'elastic')
#   ES_CA        default ../certs/ca/ca.crt   (for the https ES calls)
#
# Usage:
#   set -a; . ../.env; set +a        # pulls ELASTIC_PASSWORD
#   ./import.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
ES_URL="${ES_URL:-https://localhost:9200}"
ELASTIC_USER="${ELASTIC_USER:-elastic}"
ES_CA="${ES_CA:-$HERE/../certs/ca/ca.crt}"
TRANSFORM_ID="workflow-jobstate-current"

if [[ -z "${ELASTIC_PASSWORD:-}" ]]; then
  echo "ELASTIC_PASSWORD is unset. Run:  set -a; . ../.env; set +a" >&2
  exit 1
fi

es() { curl -sS --cacert "$ES_CA" -u "$ELASTIC_USER:$ELASTIC_PASSWORD" "$@"; }
kbn() { curl -sS -u "$ELASTIC_USER:$ELASTIC_PASSWORD" -H 'kbn-xsrf: true' "$@"; }

echo "==> 1/4 dest index template for the transform"
es -X PUT "$ES_URL/_index_template/workflow-jobstate-current" \
   -H 'Content-Type: application/json' \
   --data-binary "@$HERE/transforms/jobstate-current.template.json" | tr -d '\n'; echo

echo "==> 2/4 create + start the current-state transform"
# PUT is idempotent-ish: delete a prior copy first so re-runs don't 409.
es -X POST "$ES_URL/_transform/$TRANSFORM_ID/_stop?force=true&wait_for_completion=true" >/dev/null 2>&1 || true
es -X DELETE "$ES_URL/_transform/$TRANSFORM_ID?force=true" >/dev/null 2>&1 || true
es -X PUT "$ES_URL/_transform/$TRANSFORM_ID" \
   -H 'Content-Type: application/json' \
   --data-binary "@$HERE/transforms/jobstate-current.transform.json" | tr -d '\n'; echo
es -X POST "$ES_URL/_transform/$TRANSFORM_ID/_start" | tr -d '\n'; echo

echo "==> waiting for Kibana to be available ($KIBANA_URL)"
for _ in $(seq 1 60); do
  code="$(kbn -o /dev/null -w '%{http_code}' "$KIBANA_URL/api/status" || true)"
  [ "$code" = "200" ] && { echo "  kibana ready"; break; }
  echo -n "."; sleep 3
done

echo "==> 3/3 create saved objects (data views, panels, dashboards)"
# Use the per-object create API, not _import: our NDJSON is already in current
# 8.15 shape, and create stores it as-is + stamps the version. _import instead
# replays every legacy migration from version zero, which crashes on Lens objects
# (old migrations expect the pre-8.3 'indexpattern' datasource, not 'formBased').
KIBANA_URL="$KIBANA_URL" ELASTIC_USER="$ELASTIC_USER" ELASTIC_PASSWORD="$ELASTIC_PASSWORD" \
python3 - "$HERE/saved-objects/data-views.ndjson" "$HERE/saved-objects/dashboards.ndjson" <<'PY'
import base64, json, os, sys, urllib.request, urllib.error

base = os.environ["KIBANA_URL"].rstrip("/")
auth = base64.b64encode(
    f'{os.environ["ELASTIC_USER"]}:{os.environ["ELASTIC_PASSWORD"]}'.encode()
).decode()

ok = bad = 0
for path in sys.argv[1:]:
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        o = json.loads(line)
        body = json.dumps({"attributes": o["attributes"],
                           "references": o.get("references", [])}).encode()
        url = f'{base}/api/saved_objects/{o["type"]}/{o["id"]}?overwrite=true'
        req = urllib.request.Request(url, data=body, method="POST", headers={
            "Content-Type": "application/json",
            "kbn-xsrf": "true",
            "Authorization": f"Basic {auth}",
        })
        try:
            urllib.request.urlopen(req)
            ok += 1
            print(f"  + {o['type']:14s} {o['id']}")
        except urllib.error.HTTPError as e:
            bad += 1
            print(f"  ! {o['type']:14s} {o['id']}: {e.code} {e.read().decode()[:200]}")
print(f"  -> created/updated {ok}, failed {bad}")
sys.exit(1 if bad else 0)
PY

cat <<EOF

Done. Open the tunnel and browse:
  Fleet overview : $KIBANA_URL/app/dashboards#/view/wf-overview
  Drilldown      : $KIBANA_URL/app/dashboards#/view/wf-drilldown

If any object shows "!" above, read its error. The most common cause is a data
view field that does not exist yet because no workflow has shipped that event
type. Run a workflow (--run-example) and re-run this script.
EOF
