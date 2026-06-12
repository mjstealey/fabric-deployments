# Recipe: `pegasus-htcondor`

Provision a FABRIC slice that runs a **Pegasus + HTCondor pool** and ships its
workflow events to the [`elastic-stack`](../elastic-stack/README.md) slice over a
private routed FABNetv4 network. This is the **producer** side; `elastic-stack`
is the **receiver**.

```
  pegasus-htcondor slice (this recipe)                 elasticsearch-host slice
 ┌──────────────────────────────────────┐            ┌──────────────────────────┐
 │ submit/CM: condor + Pegasus           │            │  Elasticsearch 8.15      │
 │   pegasus-monitord → stampede.db      │ FABNetv4   │  :9200 (HTTPS+TLS+auth)  │
 │   workflow-monitor → *.jsonl          │  (L3,      │  ILM / templates / alias │
 │   Vector ── es sink https://<es>:9200 ┼────────────▶  vector_ingest user      │
 │ work1..workN: condor startd (execute) │  private)  └──────────────────────────┘
 │   all nodes on one FABNetv4 "fabnet"  │
 └──────────────────────────────────────┘
        slice A (same project)                              slice B (same project)
```

## What runs where

This recipe (`provision.py`) is a **thin launcher**. The real logic and assets
live under `deploy/` in this same project:

- [`../../deploy/PEGASUS-HTCONDOR.md`](../../deploy/PEGASUS-HTCONDOR.md) — the full
  design + manual path (read this).
- `../../deploy/fabric/provision_pegasus_slice.py` — creates the pool slice
  (submit/CM + `--workers N` execute nodes), wires the inter-slice route, and
  bootstraps the stack.
- `../../deploy/pegasus-htcondor/bootstrap_pegasus_node.sh` — role-aware in-VM
  bring-up (HTCondor + shared pool key on every node; Pegasus + Apptainer
  everywhere; workflow-monitor + Vector on the submit node).
- `../../deploy/pegasus-htcondor/condor/` — the HTCondor config drop-ins.
- `../../deploy/pegasus-htcondor/vector/vector.toml.tmpl` — the node Vector config.
- `../../deploy/pegasus-htcondor/examples/diamond.py` — the smoke-test workflow.

`provision.py` exists only so the provisioner runs with this project's
`config/fabric_rc` and token without needing a `~/work/fabric_config` symlink or
an unsafe `source fabric_rc`. It injects
`FablibManager(fabric_rc=…/config/fabric_rc)` at the `_load_fablib` seam and
passes every flag straight through.

**`fabnet` is load-bearing.** All pool nodes share one FABNetv4 network named
`fabnet`, which is exactly the name the `elastic-stack` recipe's `--peer-slice`
looks for. That is what lets the ES side route back to this pool unmodified.

## Run it (the two-slice handshake)

From the project root, after `uv sync` and a green `preflight.py`:

```bash
# See all flags.
uv run python recipes/pegasus-htcondor/provision.py --help

# 1. Build the pool + install HTCondor/Pegasus/workflow-monitor/Vector
#    (Vector is installed but STAYS STOPPED — there is no ES slice yet).
uv run python recipes/pegasus-htcondor/provision.py \
    --name pegasus-htcondor --site UCSD --workers 2 --bootstrap

# 2. Stand up the ES slice and route it back to this pool (elastic-stack recipe).
#    --peer-slice adds the reverse route + /etc/hosts on every pool node and
#    downloads the CA to deploy/fabric/ca.crt.
ELASTIC_INGEST_PASSWORD=… uv run python recipes/elastic-stack/provision.py \
    --bootstrap-es --peer-slice pegasus-htcondor

# 3. Point Vector at the ES slice and start it (uploads deploy/fabric/ca.crt).
ELASTIC_INGEST_PASSWORD=… uv run python recipes/pegasus-htcondor/provision.py \
    --name pegasus-htcondor --wire-es

# 4. Smoke-test end to end: plan+submit a diamond, run workflow-monitor.
uv run python recipes/pegasus-htcondor/provision.py \
    --name pegasus-htcondor --run-example

# 5. Run a real (containerized) workflow — clone, generate, plan+submit, monitor.
#    Apptainer is already on the workers; condorio is forced; the submit dir lands
#    under the runs root so Vector ships it. No changes on the ES side.
ELASTIC_INGEST_PASSWORD=… uv run python recipes/pegasus-htcondor/provision.py \
    --name pegasus-htcondor \
    --run-workflow https://github.com/pegasus-isi/earthquake-workflow.git \
    --generate-cmd "./workflow_generator.py --regions california \
        --start-date 1994-01-01 --end-date 1994-01-31 --min-magnitude 3.0 \
        -o workflow.yml" \
    --workflow-file workflow.yml --run-name earthquake-run

# Retrofit the Apptainer container runtime onto an existing pool (built before
# it was part of the bootstrap). Idempotent, no condor restart — safe while live.
uv run python recipes/pegasus-htcondor/provision.py --name pegasus-htcondor --install-apptainer

# Retrofit the pegasus-monitord plugin system + the wfmonitor adapter (see
# deploy/MONITORD-PLUGIN.md). Run the ES side's --apply-schema FIRST. Then
# opt in per run with --enable-monitord-plugin — by default the plugin also
# polls condor from monitord's tick() (--monitord-tick-interval, default 5s;
# --no-monitord-condor-poll for the pegasus-events-only regression config):
uv run python recipes/pegasus-htcondor/provision.py --name pegasus-htcondor --install-monitord-plugin
uv run python recipes/pegasus-htcondor/provision.py --name pegasus-htcondor \
    --run-example --enable-monitord-plugin --run-name diamond-tick-2

# Infra + routing only (inspect before installing the stack):
uv run python recipes/pegasus-htcondor/provision.py --name pegasus-htcondor

# Re-apply data-plane IPs + route + restart condor after a reboot:
uv run python recipes/pegasus-htcondor/provision.py --name pegasus-htcondor --reconfigure

# Tear down:
uv run python recipes/pegasus-htcondor/provision.py --name pegasus-htcondor --destroy
```

Use the **same `ELASTIC_INGEST_PASSWORD`** for steps 2 and 3 (and any
`ELASTIC_PASSWORD` you set on the ES side) — it is the scoped `vector_ingest`
credential Vector authenticates with.

### Secrets

- The **HTCondor pool signing key** defaults to a generated value saved to
  `deploy/pegasus-htcondor/.pool-password` (mode 0600, gitignored) so
  `--reconfigure` reuses it. Override with `--pool-password` / `$CONDOR_POOL_PASSWORD`.
- The **Vector ingest password** is `--ingest-password` / `$ELASTIC_INGEST_PASSWORD`
  (default the `changeme-*` prototype value). It must match the `vector_ingest`
  user the ES slice created.

## FABRIC-side prerequisites (not checked by preflight)

`preflight.py` validates your local credentials. These must also be true on the
FABRIC side — see [`../../deploy/PEGASUS-HTCONDOR.md`](../../deploy/PEGASUS-HTCONDOR.md):

- **Same project as the ES slice.** Inter-slice FABNetv4 routing only works
  within one project.
- **Project permissions:** `VM.NoLimitCPU` / `VM.NoLimitRAM` / `VM.NoLimitDisk`
  for nodes larger than 2 cores / 10 GB RAM / 10 GB disk (the defaults here are
  4c/16G submit + 8c/32G workers). `Slice.Multisite` is **only** needed if you
  pass `--worker-site` to spread the pool across sites; a single-site pool does
  not need it.
- **Site choice:** prefer an IPv4-management site (MAX, TACC, MASS, **UCSD**,
  FIU, SRI, BRIST, TOKY) so the in-VM `apt`/Vector pulls don't need DNS64.
- **Leases expire** — `slice.renew()` before expiry or the pool (and its routes)
  vanish. After any reboot, run `--reconfigure` to re-apply data-plane IPs +
  route and restart condor (`CONDOR_HOST` is pinned to the submit FABNet IP).
- **`workflow-monitor`** is installed on the submit node from
  `--workflow-monitor-spec` (a `pip`-installable git URL by default). Point it at
  a reachable source (or an uploaded wheel) if the default isn't available.
  `--install-monitord-plugin` upgrades it separately from
  `--monitord-adapter-spec` (branch-pinned git URL by default, or a **local
  directory** that gets tarred + uploaded — the iteration loop in
  [`../../deploy/MONITORD-PLUGIN.md`](../../deploy/MONITORD-PLUGIN.md)).
