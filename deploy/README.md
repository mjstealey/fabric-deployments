# `deploy/` — workflow-monitor observability stack

This directory contains everything needed to ship Pegasus workflow events
from a submit host into a centralized Elasticsearch cluster, where they
can be queried, alerted on, and (eventually) visualized in Kibana.

The pipeline:

```
   Pegasus workflow                       submit host                   central cluster
  ┌─────────────────┐    writes  ┌──────────────────────────┐  HTTPS   ┌───────────────────┐
  │ pegasus-monitord│───────────▶│ workflow-events.jsonl    │          │ Elasticsearch     │
  │ workflow-monitor│            │ diagnostics-events.jsonl │          │  + ILM policy     │
  └─────────────────┘            └────────────┬─────────────┘          │  + index templates│
                                              │ tails                  └────────▲──────────┘
                                              ▼                                 │
                                       ┌─────────────┐    bulk + basic auth +   │
                                       │   Vector    │───── TLS pinning ────────┘
                                       │ (file→ES)   │
                                       └─────────────┘
```

Why this shape:
- **Zero code changes to workflow-monitor.** Vector tails the JSONL the
  monitor already writes (see `src/workflow_monitor/event_log.py`,
  `diag_log.py`). If the JSONL format evolves, Vector keeps working.
- **Checkpointed at-least-once delivery.** Vector keeps a per-file
  byte-offset checkpoint, so a restart resumes exactly where it left
  off — no dupes, no gaps.
- **Backpressure rather than data loss.** Disk-backed sink buffers
  block the file source if Elasticsearch is unreachable, so events
  queue locally until the cluster comes back.
- **One artifact per concern.** `elastic-stack/` is the receiver side
  (cluster + schema + retention). `vector/` is the shipper side. Each
  half can be operated, updated, and replaced independently.

---

## Configuration templates

Secrets and host-specific config are **not** committed. Instead, each lives as a
`*.template` (or `*.template`-suffixed) file with placeholder values you copy and
fill in. Nothing under `deploy/` ships a real password, key, or absolute author
path.

Deploy-side templates created in this directory:

| Template | Copy to (real file) | Fill in |
|---|---|---|
| `deploy/elastic-stack/.env.template` | `deploy/elastic-stack/.env` | `ELASTIC_PASSWORD`, `ELASTIC_INGEST_PASSWORD` (the latter must match Vector's). `bootstrap_es_node.sh` auto-appends `KIBANA_SERVICE_TOKEN` + `KIBANA_ENCRYPTION_KEY` once ES is healthy — don't set those by hand. |
| `deploy/vector/.env.template` | `deploy/vector/.env` | `ELASTIC_INGEST_PASSWORD` — must equal the value in `deploy/elastic-stack/.env` (same `vector_ingest` user). |
| `deploy/vector/vector.toml.template` | `deploy/vector/vector.toml` | `data_dir`, the two source include globs, and the two `tls.ca_file` paths (replace each `/path/to/...`). Laptop/prototype config only; the in-VM submit node uses `deploy/pegasus-htcondor/vector/vector.toml.tmpl`. |
| `deploy/vector/com.vectordotdev.vector.plist.template` | `deploy/vector/com.vectordotdev.vector.plist` | The `--config` arg, `WorkingDirectory`, and log paths (replace each `/path/to/...`) before `launchctl load`. Host-specific. |
| `deploy/pegasus-htcondor/.pool-password.template` | `deploy/pegasus-htcondor/.pool-password` | A single-line HTCondor pool signing key (a shared secret). Or omit the file and let provisioning auto-generate one. The real file is a bare secret — its template comments are guidance only. |

Copy-and-fill, then lock down the secrets:

```bash
cp deploy/elastic-stack/.env.template              deploy/elastic-stack/.env
cp deploy/vector/.env.template                     deploy/vector/.env
cp deploy/vector/vector.toml.template              deploy/vector/vector.toml
cp deploy/vector/com.vectordotdev.vector.plist.template \
                                                   deploy/vector/com.vectordotdev.vector.plist
cp deploy/pegasus-htcondor/.pool-password.template deploy/pegasus-htcondor/.pool-password

chmod 600 deploy/elastic-stack/.env deploy/vector/.env \
          deploy/pegasus-htcondor/.pool-password
# then edit each file and replace the placeholders / single-line secret

# generate strong secrets, e.g.:
#   openssl rand -hex 32                                   # .env passwords
#   openssl rand -base64 24 > deploy/pegasus-htcondor/.pool-password
```

Credential templates also exist under `config/` (`config/fabric_rc.template`,
`config/.tokens.json.template`). Those are the FABRIC testbed credentials — see
`config/README.md` for how to copy and fill them and the full credential-sourcing
guide. (Don't `source` `config/fabric_rc`; it's for FABlib's parser only.)

**Real files that are gitignored** (created from the templates above, never
committed):

- `deploy/elastic-stack/.env`
- `deploy/vector/.env`
- `deploy/vector/vector.toml`
- `deploy/vector/com.vectordotdev.vector.plist` (when host-specific)
- `deploy/pegasus-htcondor/.pool-password`
- `deploy/elastic-stack/certs/` (self-signed CA + node cert, generated per-deploy)
- `ca.crt` / `**/ca.crt` (CA pulled back from a provisioned ES slice)
- `deploy/vector/data/` (Vector checkpoints + disk buffers, runtime state)

---

## Directory layout

```
deploy/
├── README.md                       # ← you are here
├── elastic-stack/                  # Receiver: ES cluster + schema + retention
│   ├── docker-compose.yml          #   Single-node ES (8.15.0), TLS, security on
│   ├── .env                        #   ELASTIC_PASSWORD — gitignored
│   ├── apply_es_schema.sh          #   Idempotent schema apply (fresh bootstrap +
│   │                                #   running-cluster retrofit; one template = one family)
│   ├── certs/                      #   Self-signed CA + node cert (gitignored)
│   │   ├── ca/   {ca.crt, ca.key}
│   │   └── node/ {node.crt, node.key}
│   ├── templates/                  #   Index templates (mappings + ILM ref)
│   │   ├── workflow-events.json
│   │   ├── workflow-diag.json
│   │   └── monitord-events.json    #   the monitord plugin stream (MONITORD-PLUGIN.md)
│   └── ilm/
│       └── workflow-retention.json #   Rollover at 10 GB/7 d, delete at 90 d
└── vector/                         # Shipper: file → ES
    ├── README.md                   #   Vector-specific operator guide
    ├── vector.toml                 #   Config: sources, transforms, sinks
    ├── .env                        #   ELASTIC_INGEST_PASSWORD — gitignored
    ├── vector.service              #   Hardened systemd unit (Linux)
    ├── com.vectordotdev.vector.plist # launchd agent (macOS)
    └── data/                       #   Vector checkpoint state (runtime, gitignored)
```

The two `.env` files and `certs/` and `data/` are all gitignored by
`.gitignore` at the repo root. Prototype passwords (`changeme-*`) are
the defaults baked into the compose file and Vector unit files — rotate
them before exposing the cluster.

---

## What gets ingested

Three JSONL streams, one document per line:

| Stream | Source file | Vector source | Sink → ES index alias |
|---|---|---|---|
| Workflow events | `<submit_dir>/workflow-events.jsonl` | `sources.workflow_events` | `workflow-events-default` |
| Diagnostics events | `<submit_dir>/diagnostics-events.jsonl` | `sources.workflow_diag` | `workflow-diag-default` |
| Monitord plugin events | `<submit_dir>/monitord-events.jsonl` | `sources.monitord_events` | `monitord-events-default` |

Workflow events include every state transition pegasus-monitord reports
plus the monitor's own snapshots (job start/end, retries, workflow
start/end, periodic `htcondor_poll` ClassAd dumps). Diagnostics events
are emitted by the `--diagnose` mode's stall detector — at most one
per stall episode plus a recovery record. Monitord plugin events are the
same authoritative schema as workflow events but emitted live from
*inside* `pegasus-monitord` by the `wfmonitor` plugin — the push
counterpart to the polled stream, kept in its own index family so the
two paths compare side by side (see
[`MONITORD-PLUGIN.md`](MONITORD-PLUGIN.md); on the FABRIC submit node
only, when a run opts in).

The Vector pipeline:

1. **Source** tails matching files with `read_from = "beginning"` and
   `fingerprint.strategy = "device_and_inode"`. Discovery glob is
   currently scoped to the example-workflows tree; widen the include
   list for your submit-dir layout.
2. **Transform (VRL)** parses each line as JSON, lifts the timestamp
   to ES-friendly `@timestamp`, tags `submit_host` via `get_hostname!()`,
   and tags `stream`.
3. **Sink** writes to the ES bulk API over HTTPS using basic auth as
   the scoped `vector_ingest` user, with the local CA pinned via
   `tls.ca_file`. Bulk target is the rollover alias name — ES routes
   to the current write index.

---

## End-to-end setup (local prototype)

This brings up the full pipeline on a single laptop. Total time: ~5
minutes after Vector is installed.

### 1. Prerequisites

- **Docker Desktop** running.
- **Vector ≥ 0.40** — `brew install vector` (macOS) or
  `curl https://sh.vector.dev | bash` (Linux).
- **Pegasus workflow JSONL on disk** — either real runs in your submit
  dir, or the included example-workflows tree.

### 2. Bring up Elasticsearch

```bash
cd <repo root>

# Provision the .env (one-time)
cat > deploy/elastic-stack/.env <<EOF
ELASTIC_PASSWORD=changeme-elastic
EOF
chmod 600 deploy/elastic-stack/.env

# Generate the self-signed CA + node cert (one-time)
docker run --rm -v "$PWD/deploy/elastic-stack/certs:/certs" -u 0 \
  docker.elastic.co/elasticsearch/elasticsearch:8.15.0 bash -c '
    cd /certs
    /usr/share/elasticsearch/bin/elasticsearch-certutil ca --silent --pem \
      --out /certs/ca.zip </dev/null
    unzip -o /certs/ca.zip
    /usr/share/elasticsearch/bin/elasticsearch-certutil cert --silent \
      --ca-cert /certs/ca/ca.crt --ca-key /certs/ca/ca.key \
      --pem --out /certs/node.zip --name node \
      --dns localhost,workflow-monitor-es --ip 127.0.0.1 </dev/null
    unzip -o /certs/node.zip
    chmod -R a+r /certs'

# Start the cluster
docker compose -f deploy/elastic-stack/docker-compose.yml up -d

# Wait for healthy
until docker inspect --format '{{.State.Health.Status}}' workflow-monitor-es \
  | grep -q healthy; do sleep 2; done
```

The cluster listens on `https://localhost:9210`. Port 9210 instead of
the conventional 9200 because RAATServe/Roon binds 9200 on macOS dev
machines and quietly accepts TCP without speaking HTTP — `curl` returns
"Empty reply from server" if you mix them up.

### 3. Apply schema, retention, and security

```bash
PW=$(grep ELASTIC_PASSWORD deploy/elastic-stack/.env | cut -d= -f2)
CURL='curl -sS --cacert deploy/elastic-stack/certs/ca/ca.crt -u elastic:'"$PW"

# ILM policy (rollover 10 GB / 7 d, delete 90 d)
$CURL -X PUT 'https://localhost:9210/_ilm/policy/workflow-retention' \
  -H 'Content-Type: application/json' \
  -d @deploy/elastic-stack/ilm/workflow-retention.json

# Index templates (mappings + ILM + rollover alias)
for t in workflow-events workflow-diag; do
  $CURL -X PUT "https://localhost:9210/_index_template/$t" \
    -H 'Content-Type: application/json' \
    -d @deploy/elastic-stack/templates/$t.json
done

# Bootstrap the first index of each family with the write alias.
# REQUIRED for ILM rollover — without it, ILM is "managed" but never fires.
for fam in workflow-events workflow-diag; do
  $CURL -X PUT "https://localhost:9210/%3C${fam}-000001%3E" \
    -H 'Content-Type: application/json' \
    -d "{\"aliases\":{\"${fam}-default\":{\"is_write_index\":true}}}"
done

# Scoped ingest role + user (least privilege)
$CURL -X PUT 'https://localhost:9210/_security/role/vector_ingest' \
  -H 'Content-Type: application/json' -d '{
    "indices": [{
      "names": ["workflow-events-*", "workflow-diag-*"],
      "privileges": ["write","create_index","auto_configure","view_index_metadata"]
    }],
    "cluster": ["monitor"]
  }'

$CURL -X POST 'https://localhost:9210/_security/user/vector_ingest' \
  -H 'Content-Type: application/json' \
  -d '{"password":"changeme-vector-ingest","roles":["vector_ingest"]}'
```

### 4. Start Vector

```bash
# Provision Vector's .env (one-time)
cat > deploy/vector/.env <<EOF
ELASTIC_INGEST_PASSWORD=changeme-vector-ingest
EOF
chmod 600 deploy/vector/.env

# Make sure data_dir exists
mkdir -p deploy/vector/data

# Validate config before launching
vector validate deploy/vector/vector.toml

# Run in the foreground (Ctrl+C to stop — checkpoints flush on exit)
set -a && . deploy/vector/.env && set +a
vector --config deploy/vector/vector.toml
```

For long-running unattended operation, install Vector as a service —
see [Production install — Vector as a service](#production-install--vector-as-a-service)
below.

### 5. Verify ingestion

```bash
PW=$(grep ELASTIC_PASSWORD deploy/elastic-stack/.env | cut -d= -f2)

# Doc counts
curl -sk --cacert deploy/elastic-stack/certs/ca/ca.crt \
  -u "elastic:$PW" 'https://localhost:9210/workflow-events-*/_count?pretty'

# Index → alias topology (which index is the current write target?)
curl -sk --cacert deploy/elastic-stack/certs/ca/ca.crt \
  -u "elastic:$PW" 'https://localhost:9210/_cat/aliases/workflow-*-default?v'

# A single document
curl -sk --cacert deploy/elastic-stack/certs/ca/ca.crt \
  -u "elastic:$PW" \
  'https://localhost:9210/workflow-events-default/_search?size=1&pretty'
```

---

## Production install — Vector as a service

The local-prototype flow above runs Vector in the foreground. For real
submit hosts you want it under a service supervisor that restarts it
on crash and starts it on boot. Unit files for both platforms live
under `deploy/vector/`.

### Linux (systemd)

`deploy/vector/vector.service` is a hardened unit:
- Runs as a dedicated `vector` system user
- `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome`,
  `PrivateTmp`, `MemoryDenyWriteExecute`, locked-down syscalls
- `EnvironmentFile=/etc/vector/.env` for the ingest password
- `Restart=on-failure` with a 5-burst limit per 60 s

Install:

```bash
# 1. Create the system user and config dir
sudo useradd --system --shell /usr/sbin/nologin --home /var/lib/vector vector
sudo install -d -o vector -g vector -m 0755 /var/lib/vector
sudo install -d -o root   -g vector -m 0750 /etc/vector

# 2. Copy config + secrets + CA bundle
sudo install -o root -g vector -m 0640 deploy/vector/vector.toml /etc/vector/vector.toml
sudo install -o root -g vector -m 0640 deploy/vector/.env        /etc/vector/.env
sudo install -o root -g vector -m 0644 /path/to/ca.crt           /etc/vector/ca.crt

# 3. IMPORTANT: edit /etc/vector/vector.toml so tls.ca_file = "/etc/vector/ca.crt"
#    and data_dir = "/var/lib/vector"
sudo $EDITOR /etc/vector/vector.toml

# 4. Install the unit and start
sudo install -o root -g root -m 0644 deploy/vector/vector.service /etc/systemd/system/vector.service
sudo systemctl daemon-reload
sudo systemctl enable --now vector

# 5. Verify
systemctl status vector
journalctl -u vector --since "5 min ago" -f
```

Runtime inspection: `systemctl status vector`, `journalctl -u vector -f`,
and `vector top` (the local API is enabled in `vector.toml` by default).

### macOS (launchd)

`deploy/vector/com.vectordotdev.vector.plist` is a launchd agent:
- Points `ProgramArguments` at `/opt/homebrew/bin/vector` and the
  repo's `vector.toml` (edit the path for production layouts)
- `KeepAlive` only on `Crashed: true` with `ThrottleInterval=10`
  (prevents tight crash-loops)
- `RunAtLoad=true` and `SoftResourceLimits.NumberOfFiles=65535`
- Logs to `~/Library/Logs/vector-workflow-monitor.{out,err}.log`

Per-user install (preferred for dev hosts):

```bash
mkdir -p ~/Library/Logs
cp deploy/vector/com.vectordotdev.vector.plist ~/Library/LaunchAgents/

# Provide the secret to launchctl's env BEFORE loading the agent.
# (Visible in `launchctl print` — for real secrets, use a wrapper
# script that sources /etc/vector/.env before exec'ing vector.)
launchctl setenv ELASTIC_INGEST_PASSWORD "<vector_ingest password>"

launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.vectordotdev.vector.plist
launchctl kickstart -p gui/$(id -u)/com.vectordotdev.vector

# Logs
tail -F ~/Library/Logs/vector-workflow-monitor.{out,err}.log

# Unload
launchctl bootout gui/$(id -u)/com.vectordotdev.vector
```

System-wide install: replace `~/Library/LaunchAgents/` with
`/Library/LaunchDaemons/`, `gui/$(id -u)` with `system`, and use a
wrapper script that sources `/etc/vector/.env` so secrets aren't
visible via `launchctl print`.

---

## Operating model

### Indices, aliases, and rollover

Each event family is **alias-backed**:

```
workflow-events-default  →  workflow-events-000001 (read-only after rollover)
                         →  workflow-events-000002 (current write target)
                         …
```

Vector writes to the alias (`bulk.index = "workflow-events-default"`).
Elasticsearch routes the bulk write to whichever concrete index has
`is_write_index: true`. ILM monitors that index and, when it crosses
10 GB primary-shard or 7 days, creates `-000003`, flips the alias's
write flag, and freezes `-000002`. After 90 days the old index is
deleted.

The alias bootstrap (`<workflow-events-000001>` with
`is_write_index: true`) is the load-bearing piece. Without it, Vector
would still write happily — but ILM stays parked at the
`check-rollover-ready` step forever because rollover requires an alias.

To force a test rollover:

```bash
curl -sk --cacert deploy/elastic-stack/certs/ca/ca.crt \
  -u "elastic:$PW" \
  -X POST 'https://localhost:9210/workflow-events-default/_rollover' \
  -H 'Content-Type: application/json' \
  -d '{"conditions":{"max_age":"1s"}}'
```

Note: `min_docs` cannot be used alone — pair it with at least one
`max_*` condition or the API returns 400.

### Schema and dynamic mapping

`deploy/elastic-stack/templates/*.json` defines the mapping for each
family. Top-level fields are explicit and typed (`@timestamp`, `wf_uuid`,
`exitcode`, etc.). Unknown string fields are caught by a
`dynamic_templates` rule and mapped as `keyword` (capped at 1024 bytes)
so a stray field never explodes shard count via text analysis.

Adding a new field is safe. Changing the type of an existing field is
not — ES will refuse mappings that conflict with the active write
index. To change a type, change the template, then rollover so the
new mapping takes effect on the next concrete index.

### Disk buffering and backpressure

Both ES sinks use disk-backed buffers:

```toml
[sinks.es_events.buffer]
type      = "disk"
max_size  = 536870912   # 512 MiB
when_full = "block"
```

`when_full = "block"` is deliberate. If ES is unreachable for long
enough to fill 512 MiB of buffered events, Vector stops reading from
the file sources — but the JSONL on disk is untouched, so when ES
comes back Vector resumes from the checkpointed offset and catches up.
The alternative (`drop_newest`) silently loses events.

If you need a bigger buffer, raise `max_size`. Vector enforces a
minimum of `268435488` bytes (256 MiB + 32 bytes of overhead) — exactly
256 MiB is rejected.

### Auth model

- `elastic` is the cluster superuser. Use only for one-time
  provisioning (ILM, templates, role/user creation) and for human
  debugging via curl.
- `vector_ingest` is the role used by all Vector instances. Privileges
  are scoped to `workflow-events-*` and `workflow-diag-*` plus
  `cluster:monitor`. It cannot read the `.security` index, list users,
  or write outside its namespace. Verified during Phase 3 with positive
  and negative scope tests.
- For production: rotate to API keys (one per submit host) so
  individual hosts can be revoked without touching the others.

### Checkpoints and file discovery

Vector keeps a per-file checkpoint in `data_dir` (= the byte offset
of the last fully-shipped line). On startup it reads the checkpoint
and resumes from there, so restarts produce zero duplicates and zero
gaps as long as `data_dir` is preserved.

Files are identified by `fingerprint.strategy = "device_and_inode"`,
not by path. If you rename a file in place, Vector keeps its
checkpoint. If the fingerprint strategy is changed (e.g. switching
to `checksum`), every file is treated as new and re-read from
byte 0 — so don't change it casually.

`read_from = "beginning"` is set so newly discovered files are read
from the start. The prototype omits `ignore_older_secs` because the
example-workflows runs are months old; in production set it (e.g. to
30 days) to bound discovery cost on submit dirs that accumulate years
of runs.

### Permissions

Vector reads JSONL files as its own user. On Linux that's the `vector`
system user, which has no group membership by default. If submit dirs
live under user homes with `0700` perms, either:
- Add `vector` to the user's group and `chmod g+rx` the relevant tree, or
- Run a per-user Vector instance (one launchd agent or one systemd
  user unit per submit-host user).

The systemd unit's `ReadOnlyPaths=/var/log /home /opt` is a starting
point — adjust to match your actual submit-dir layout.

---

## Common operations

### Restart the cluster (data preserved)

```bash
docker compose -f deploy/elastic-stack/docker-compose.yml stop
docker compose -f deploy/elastic-stack/docker-compose.yml start
```

Data lives in the named Docker volume `elastic-stack_esdata` and
survives `stop`/`down`. Use `down -v` to wipe.

### Restart Vector (no event loss)

Vector flushes checkpoints on graceful shutdown. On startup it reads
the checkpoint and resumes from the recorded byte offset, so a stop +
start mid-ingest produces zero duplicates and zero gaps.

If you wipe `deploy/vector/data/`, Vector treats every file as new and
re-ingests from byte 0. That's a re-ingest of every byte of every
matched file — fine for the prototype, a multi-GB problem in
production. Don't wipe data_dir routinely.

### Inspect Vector at runtime

Vector's local API is enabled on `127.0.0.1:8686`:

```bash
vector top              # process/pipeline overview (TUI)
vector tap parse_events # stream parsed events through the terminal
```

### Re-apply schema after editing templates

```bash
PW=$(grep ELASTIC_PASSWORD deploy/elastic-stack/.env | cut -d= -f2)
for t in workflow-events workflow-diag; do
  curl -sS --cacert deploy/elastic-stack/certs/ca/ca.crt \
    -u "elastic:$PW" \
    -X PUT "https://localhost:9210/_index_template/$t" \
    -H 'Content-Type: application/json' \
    -d @deploy/elastic-stack/templates/$t.json
done
# Template changes affect *future* indices only.
# To pick them up immediately, force a rollover (see above).
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `curl: (52) Empty reply from server` on 9200 | Roon's RAATServe binds 9200 | We map to 9210 in compose; always hit 9210 |
| Vector startup error: `data_dir ... does not exist` | Vector won't create it | `mkdir -p deploy/vector/data` |
| Vector validate: `parameter 'max_buffer_size' was invalid: must be >= 268435488` | Disk buffer exactly 256 MiB | Use 512 MiB (`536870912`) or `268435488` |
| Vector validate: `unexpected argument '--config'` | Vector ≥ 0.40 made it positional | `vector validate <path>` (no flag) |
| ES boot loop: `server ssl configuration requires a key and certificate` | `discovery.type=single-node` skips security auto-config | Generate certs via `elasticsearch-certutil` (step 2 above), bind them, set all `xpack.security.{http,transport}.ssl.*` paths explicitly |
| `elasticsearch-certutil` silently produces no files | Tool waits on stdin even with `--silent` | Add `</dev/null` to close stdin |
| `certutil cert --pem --pass ""`: "password empty" exception | Bouncycastle PEM encryption rejects empty pass | Omit `--pass` entirely for unencrypted key |
| Vector running but ES `_count` flat | Files filtered by `ignore_older_secs`, OR mapping conflict | Drop `ignore_older_secs` for the prototype; check ES logs for `mapper_parsing_exception` |
| ILM `explain` stuck at `check-rollover-ready` forever | Index isn't alias-backed | Bootstrap with `<name-000001>` + `is_write_index: true` alias |
| `_rollover` returns "max_* condition required" | Used `min_docs` alone | Add `max_age` or `max_primary_shard_size` |
| `Healthcheck failed` for ES sink in Vector | TLS path wrong, ES down, or basic-auth creds wrong | Check `tls.ca_file`, hit `https://localhost:9210/_cluster/health` with curl using the same creds |
| Vector restarts re-ingest everything | `data_dir` wiped or `fingerprint.strategy` changed | Preserve `data_dir` across restarts; don't change fingerprint strategy without intent |
| `permission denied` reading JSONL | Vector user can't read submit dirs | Add `vector` to a group with read access, or relax dir perms, or run per-user |
| `mapper_parsing_exception` in ES logs, sink drops batches | New field conflicts with existing mapping type | Don't change types of existing fields; add new fields via the template and rollover |

---

## What's next

Tracked in `vector.md` (repo root) and the project memory:

- **Buffer/queue metrics** — Vector exposes `buffer_*` on the API; nice
  Prometheus scrape candidate.
- **Real ILM dry-run** — 10 GB takes weeks of real volume; could
  schedule a synthetic 1-hour rollover test.
- **Kibana under TLS** — commented out in compose; re-enable with a
  `kibana_system` service-account token.
- **API keys instead of basic auth** — easier per-host revocation.
- **Multi-tenant index naming** (`workflow-events-<group>`) — affects
  ILM, security, and Kibana spaces. Open question.
