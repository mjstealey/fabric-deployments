# Running a Pegasus/HTCondor pool on a FABRIC slice

This guide explains how to stand up the **producer** side of the pipeline in
[`README.md`](README.md): a [FABRIC](https://portal.fabric-testbed.net/) slice
running a real **HTCondor pool** with **Pegasus** on top, plus `workflow-monitor`
and **Vector**, which ships workflow events to the **Elasticsearch slice**
([`FABRIC.md`](FABRIC.md)) over a private routed FABNetv4 network.

```
        pegasus-htcondor slice (producer)                   elasticsearch-host slice (receiver)
 ┌───────────────────────────────────────────────┐         ┌──────────────────────────┐
 │ node "submit": condor CM + schedd + Pegasus   │         │  Elasticsearch 8.15      │
 │   pegasus-monitord → stampede.db              │ FABNetv4│  :9200 (HTTPS+TLS+auth)  │
 │    └ wfmonitor plugin → monitord-events.jsonl │  (L3,   │  ILM / templates / alias │
 │   workflow-monitor --serve → *.jsonl          │ private)│  vector_ingest user      │
 │   Vector ──── es sinks https://<es>:9200 ─────┼─────────▶  3 index families        │
 │ node "work1..N": condor startd (execute)      │         └──────────────────────────┘
 │   all nodes on ONE FABNetv4 network "fabnet"  │
 └───────────────────────────────────────────────┘
        slice A (same project)                                  slice B (same project)
```

This is the slice the `elastic-stack` recipe assumed already existed (its
`--peer-slice pegasus-htcondor`). The scripted path below builds it.

> **Companion artifacts** in `deploy/`:
> - `fabric/provision_pegasus_slice.py` — FABlib script: creates the pool, wires
>   the inter-slice route, bootstraps the stack, wires Vector to ES, runs a
>   smoke test.
> - `pegasus-htcondor/bootstrap_pegasus_node.sh` — the role-aware in-VM bring-up
>   the script uploads and runs (HTCondor + pool key on every node, Pegasus +
>   Apptainer everywhere, workflow-monitor + Vector on the submit node). Also
>   runnable by hand.
> - `pegasus-htcondor/condor/` — the HTCondor config drop-ins.
> - `pegasus-htcondor/vector/vector.toml.tmpl` — the node Vector config (three
>   streams: workflow-events, diagnostics, monitord-events).
> - `pegasus-htcondor/overlay_monitord_plugin.sh` — overlays the
>   pegasus-monitord plugin system onto the apt install
>   ([`MONITORD-PLUGIN.md`](MONITORD-PLUGIN.md)).
> - `pegasus-htcondor/examples/diamond.py` — the smoke-test workflow.

---

## Why one FABNetv4 network for the whole pool

FABRIC nodes have **two** networks (see `FABRIC.md` and the FABRIC docs,
`docs/guides/networking/network-troubleshooting.md`):

| | Management network | Data plane |
|---|---|---|
| Purpose | **SSH only** (via bastion) | Your experiment traffic |
| Interface | `ens3`/`enp3s0` | added NICs (`enp7s0`, …) |
| Traffic allowed | SSH, basic ICMP | anything |

**HTCondor's pool traffic must ride the data plane.** The collector (`:9618`),
schedd↔shadow, and startd↔starter conversations cannot use the management
network. So every pool node gets a `NIC_Basic` on **one shared FABNetv4
network** named `fabnet`. Because all nodes sit in that one FABNet `/24` they are
directly connected — CM↔worker needs **no** extra route. Each daemon binds to its
own FABNet IP (`NETWORK_INTERFACE` in `condor/00-pool-common.conf`) and
`CONDOR_HOST` is the submit node's FABNet IP.

Two facts make this the producer half of the two-slice design:

1. **The network name `fabnet` matches `NET_NAME` in `provision_es_slice.py`.**
   That is what lets the ES recipe's `--peer-slice pegasus-htcondor` find these
   nodes and add the reverse route + `/etc/hosts workflow-monitor-es` to them.
2. **An inter-slice route** (`10.128.0.0/10` via each node's own FABNet gateway)
   is what lets Vector on the submit node reach the ES slice's *different* FABNet
   subnet. The provisioner installs it (and a systemd one-shot to persist it)
   exactly like the ES recipe does — never touching the management default route.

---

## Prerequisites

- A FABRIC project and a working FABlib environment (this project's
  `config/fabric_rc` + token; `preflight.py` green).
- **Same FABRIC project as the ES slice** — inter-slice routing only works
  within a project.
- Project permissions, as needed:
  - `VM.NoLimitCPU` / `VM.NoLimitRAM` / `VM.NoLimitDisk` for nodes larger than
    2 cores / 10 GB RAM / 10 GB disk (defaults here: 4c/16G submit, 8c/32G
    workers).
  - `Slice.Multisite` **only if** you pass `--worker-site` to spread the pool
    across sites. A single-site pool does not need it.
- Choose an **IPv4-management site** (MAX, TACC, MASS, **UCSD**, FIU, SRI, BRIST,
  TOKY) so the in-VM `apt`/Vector pulls don't need DNS64.

---

## Quick path (scripted)

From the repo root, on a host with FABlib configured (via the recipe launcher so
it picks up `config/fabric_rc`):

```bash
# 1. Build the pool + bring the stack up (Vector installed but STOPPED).
uv run python recipes/pegasus-htcondor/provision.py \
    --name pegasus-htcondor --site UCSD --workers 2 --bootstrap

# 2. Stand up the ES slice and route it back here (see FABRIC.md).
ELASTIC_INGEST_PASSWORD=… uv run python recipes/elastic-stack/provision.py \
    --bootstrap-es --peer-slice pegasus-htcondor

# 3. Point Vector at ES and start it (uploads deploy/fabric/ca.crt).
ELASTIC_INGEST_PASSWORD=… uv run python recipes/pegasus-htcondor/provision.py \
    --name pegasus-htcondor --wire-es

# 4. Smoke-test end to end.
uv run python recipes/pegasus-htcondor/provision.py \
    --name pegasus-htcondor --run-example
```

What step 1 (`--bootstrap`) does, per node, in order:

1. Builds the slice: a submit/CM node + `--workers N` execute nodes, each with a
   `NIC_Basic` on the one `fabnet` FABNetv4 network.
2. Submits and waits for SSH; reads each node's data-plane IP + gateway.
3. Installs the **inter-slice route** (`10.128.0.0/10` via the node's own FABNet
   gateway) + a systemd one-shot, on every node.
4. Uploads `bootstrap_pegasus_node.sh` + the `condor/` drop-ins and runs them
   role-aware:
   - **every node:** installs HTCondor, renders the config drop-ins
     (`CONDOR_HOST`, the node's own bind IP, the FABNet subnet for `ALLOW_*`),
     stores the **shared pool signing key**, installs Pegasus (for `pegasus-keg`
     + the worker tools), and installs **Apptainer** as the container runtime
     for containerized workflows (`INSTALL_APPTAINER=0` to skip).
   - **submit node only:** also prepares the runs dir, installs `workflow-monitor`,
     and installs Vector + renders `vector.toml` (left **stopped** — no CA/.env
     yet).

Steps 3 (`--wire-es`) and 4 (`--run-example`) operate on the existing slice.

The rest of this document explains each step so you can run it by hand, adapt
it, or debug it.

---

## Running a real (containerized) workflow

`--run-example` is the diamond smoke test. To run an arbitrary workflow — e.g.
the [earthquake-workflow](https://github.com/pegasus-isi/earthquake-workflow),
whose every job runs inside `docker://kthare10/earthquake-analysis:latest` — use
`--run-workflow`. It clones the repo onto the submit node under the runs root,
optionally runs a generator to emit the abstract workflow + catalogs, forces a
no-shared-fs data configuration, plans+submits against `condorpool`, and starts
`workflow-monitor` so events flow to ES exactly like the smoke test:

```bash
ELASTIC_INGEST_PASSWORD=… uv run python recipes/pegasus-htcondor/provision.py \
    --name pegasus-htcondor \
    --run-workflow https://github.com/pegasus-isi/earthquake-workflow.git \
    --generate-cmd "./workflow_generator.py --regions california \
        --start-date 1994-01-01 --end-date 1994-01-31 --min-magnitude 3.0 \
        -o workflow.yml" \
    --workflow-file workflow.yml --run-name earthquake-run
```

What it relies on, and the knobs:

- **A container runtime on the workers.** The bootstrap installs Apptainer on
  every node (above); without it, containerized jobs cannot run. Workers pull
  the image from Docker Hub over the management NIC at first use.
- **`condorio` (the default `--data-configuration`).** The pool has no shared
  filesystem, so the Pegasus default `sharedfs` would fail. `--run-workflow`
  appends `pegasus.data.configuration=condorio` to the generated
  `pegasus.properties` **unless the workflow already set one**. Override with
  `--data-configuration nonsharedfs|sharedfs`.
- **The submit dir lands under the runs root**
  (`/opt/workflows/submit/<run-name>`), which is what Vector tails — so
  monitoring needs zero extra wiring.
- `--run-workflow` also accepts a **directory already on the submit node**
  (instead of a git URL); `--workflow-dir` overrides the clone/checkout
  location, and `--generate-cmd` is optional (skip it when the workflow YAML
  already exists in the dir).

> The receiver side never changes. The ES index templates and Vector transforms
> are workflow-agnostic — any workflow whose `workflow-monitor` JSONL lands under
> the runs root is ingested. See [`README.md`](README.md) and [`FABRIC.md`](FABRIC.md).

---

## Testing the monitord plugin path

The pool can also exercise the **pegasus-monitord entry-point plugin system**
(pegasus branch `monitord-plugin-system`) with workflow-monitor's `wfmonitor`
adapter (branch `monitord-plugin-adapter`): monitord itself streams
`monitord-events.jsonl` live, alongside the polling path, into a separate
`monitord-events-*` index family. Full architecture, provenance mechanism, and
verification runbook: [`MONITORD-PLUGIN.md`](MONITORD-PLUGIN.md).

```bash
# Receiver first (adds the monitord-events-* template/alias/role, idempotent):
uv run python recipes/elastic-stack/provision.py --apply-schema

# Retrofit the submit node (3-file overlay + adapter + Vector stream), once:
uv run python recipes/pegasus-htcondor/provision.py --install-monitord-plugin

# Then opt in per run:
uv run python recipes/pegasus-htcondor/provision.py \
    --run-example --enable-monitord-plugin --run-name diamond-plugin-3
```

That order matters — apply the ES schema before the Vector restart, or the
first bulk write auto-creates a concrete index squatting on the
`monitord-events-default` alias name.

---

## Monitor a run remotely (`workflow-monitor --remote`)

`workflow-monitor` is a terminal UI, and it can attach to a run **from your
laptop over SSH** — no port-forward, no web server. Its `--remote` mode re-fetches
the run's `workflow-events.jsonl` through the FABRIC bastion every few seconds and
replays it into the TUI locally. `--run-workflow` already starts the submit-node
`--serve` daemon that keeps that JSONL fresh, so a live run streams and a finished
run shows its final state.

```bash
# Have the tool locally — from a checkout (cd <workflow-monitor>; uv run …) or:
uv tool install "git+https://github.com/pegasus-isi/workflow-monitor.git"

workflow-monitor \
  --remote 'ubuntu@[<submit-mgmt-ipv6>]:/opt/workflows/submit/<run-name>/workflow-events.jsonl' \
  --ssh-config   ~/.ssh/fabric-ssh-config \
  --ssh-identity ~/.ssh/fabric-sliver
```

- **Target the run's event log**, `<runs-dir>/submit/<run-name>/workflow-events.jsonl`
  (default runs-dir `/opt/workflows`; `<run-name>` is the `--run-name` you passed,
  e.g. `earthquake-run`). The clone/workdir `/opt/workflows/<repo>` has **no** JSONL —
  the submit dir does. Unsure? `ssh … 'ls /opt/workflows/submit/*/workflow-events.jsonl'`.
- **The submit node's management IP is IPv6**, so the host needs **square
  brackets**: `ubuntu@[2001:…]:…`. Get it with
  `slice.get_node("submit").get_management_ip()` (or `get_ssh_command()`).
- **`--ssh-config`** is your FABRIC SSH config — its `ProxyJump` does the bastion
  hop; **`--ssh-identity`** is the sliver key for the node. Your laptop only needs
  to reach the bastion; the bastion reaches the node's IPv6.
- **Live vs. done:** the command above shows a completed run's final state; add
  `--once` for a one-shot snapshot, or run it (no `--once`) right after
  `--run-workflow` to watch jobs go `IDLE → RUNNING → SUCCEEDED` (refresh every
  `--sync-interval`, default 5s). For container-planned workflows the default
  `--remap-submit-dir auto` rebases paths; pass `always` if the job table looks off.

It is read-only and safe alongside the running `--serve` daemon.

> **Richer live view — run the TUI *on* the submit node.** SSH in and run it there
> to also query the live HTCondor queue (idle-job reasons, etc.), at the cost of
> staying connected:
>
> ```bash
> ssh -F ~/.ssh/fabric-ssh-config -i ~/.ssh/fabric-sliver ubuntu@'[<submit-mgmt-ipv6>]'
> export PATH=$HOME/.local/bin:$PATH
> workflow-monitor /opt/workflows/submit/<run-name>      # --why-idle explains stalls
> ```

---

## Manual path

### 1. Create the pool slice

```python
from fabrictestbed_extensions.fablib.fablib import FablibManager as fablib_manager
fablib = fablib_manager()

slice = fablib.new_slice(name="pegasus-htcondor")
ifaces = []

submit = slice.add_node(name="submit", site="UCSD")
submit.set_capacities(cores=4, ram=16, disk=50)
submit.set_image("default_ubuntu_22")
ifaces.append(submit.add_component(model="NIC_Basic", name="nic1").get_interfaces()[0])

for i in (1, 2):
    w = slice.add_node(name=f"work{i}", site="UCSD")
    w.set_capacities(cores=8, ram=32, disk=50)
    w.set_image("default_ubuntu_22")
    ifaces.append(w.add_component(model="NIC_Basic", name="nic1").get_interfaces()[0])

# ONE L3 network for the whole pool — same subnet => CM<->worker directly connected.
slice.add_l3network(name="fabnet", interfaces=ifaces, type="IPv4")
slice.submit()
```

### 2. Read the data-plane addressing

```python
slice  = fablib.get_slice("pegasus-htcondor")
submit = slice.get_node("submit")
ip     = submit.get_interface(network_name="fabnet").get_ip_addr()   # e.g. 10.128.5.2
gw     = slice.get_network("fabnet").get_gateway()                   # e.g. 10.128.5.1
```

`CONDOR_HOST` is the submit node's `ip`. Every node's `NETWORK_INTERFACE` is its
own FABNet IP.

### 3. Wire the inter-slice route (so Vector can reach ES)

On **every** pool node, add a route to the whole FABNetv4 space via that node's
own gateway — straight from `network-troubleshooting.md`:

```bash
sudo ip route add 10.128.0.0/10 via 10.128.5.1
```

> ⚠️ **Never replace the default route** — it carries SSH and there is no
> recovery if you break it. Add the specific `10.128.0.0/10` route only.
>
> ⚠️ **Routes and data-plane IPs don't survive reboot.** Re-apply with
> `--reconfigure` (which runs `node.config()`, re-adds the route, and restarts
> condor) or bake the route into the systemd one-shot the script installs.

### 4. Install HTCondor as a pool

Each node gets `condor/00-pool-common.conf` plus one role file
(`10-central-manager.conf` on the submit node, `20-execute.conf` on workers),
with `__CONDOR_HOST__` / `__NODE_IP__` / `__FABNET_CIDR__` substituted. The
submit node runs `CentralManager` + `Submit` (collector + negotiator + schedd);
workers run `Execute` (a partitionable startd).

**Pool security** is a shared signing key ("pool password") plus FABNet-subnet
host authorization (`ALLOW_*`). The provisioner installs the *same* secret on
every node so daemons authenticate to each other:

```bash
printf '%s' "$POOL_PASSWORD" | sudo condor_store_cred -c add
sudo systemctl restart condor
```

Verify the pool from the submit node:

```bash
condor_status            # lists the execute nodes once they advertise
condor_status -schedd    # the submit schedd
```

> **Hardening:** for production, switch from the shared pool password to
> **IDTOKENS** — generate a signing key on the CM, issue one token per node, and
> set `SEC_DEFAULT_AUTHENTICATION_METHODS = IDTOKENS`. The pool-password model
> here is the deterministic, scriptable baseline.

### 5. Install Pegasus (every node)

`apt-get install pegasus` from the ISI repo. The submit node uses the planner
(`pegasus-plan` / `pegasus-run`); the execute nodes need `pegasus-keg` and the
worker tools (`pegasus-kickstart`, `pegasus-transfer`) because the smoke
workflow uses `condorio` with `installed` transformations.

### 5b. Install a container runtime (every node)

The Pegasus package pulls in **no** container runtime, but containerized
workflows (transformations with a `Container`, e.g. the earthquake-workflow's
`docker://…` image) run via **Apptainer/Singularity** on the execute nodes. The
bootstrap installs Apptainer from the `ppa:apptainer/ppa` PPA (falling back to
the release `.deb`) and adds a `singularity` compat symlink. By hand:

```bash
sudo apt-get install -y software-properties-common
sudo add-apt-repository -y ppa:apptainer/ppa
sudo apt-get update -y && sudo apt-get install -y apptainer
command -v singularity || sudo ln -sf "$(command -v apptainer)" /usr/local/bin/singularity
```

The workers pull the image from Docker Hub over the management NIC at first use,
so the diamond smoke test (no container) works even if this step is skipped —
but a containerized workflow will not. Skip it in the bootstrap with
`INSTALL_APPTAINER=0`. On an **already-running** pool, retrofit it on every node
with `--install-apptainer` (see Operational notes) instead of doing it by hand.

### 6. Run workflow-monitor + Vector (submit node)

- `workflow-monitor <submit-dir> --serve --diagnose --log` reads the stampede
  SQLite DB `pegasus-monitord` writes and emits `workflow-events.jsonl` +
  `diagnostics-events.jsonl` into the submit dir. **The authoritative event
  schema is `workflow-monitor/DATA_SOURCES.md` ("Event Types and Schemas")** —
  the Vector transforms and ES index templates here only *mirror* it.
- Vector (`vector/vector.toml.tmpl`, rendered to `/etc/vector/vector.toml`) tails
  those files under the runs root and ships them to
  `https://workflow-monitor-es:9200` as the `vector_ingest` user, pinning the ES
  CA at `/etc/vector/ca.crt`.

The runs dir defaults to **`/opt/workflows`** (not under `/home`) on purpose: the
hardened Vector unit sets `ProtectHome=true`, which would hide a home-relative
submit dir; `/opt` is on its `ReadOnlyPaths`.

`--wire-es` uploads the ES slice's CA (downloaded to `deploy/fabric/ca.crt` by
the ES recipe), writes `/etc/vector/.env` with `ELASTIC_INGEST_PASSWORD`,
validates, and starts the unit.

---

## Verify end-to-end

```bash
# Pool health (submit node)
condor_status
pegasus-version

# Vector is reaching + authenticating to ES (submit node). Vector's own
# healthcheck passing is the proof — `vector_ingest` is WRITE-ONLY, so a read
# (e.g. _cluster/health) returns 403 and is NOT a useful liveness probe.
systemctl is-active vector
vector top                      # or: journalctl -u vector -f  → "Healthcheck passed"

# After --run-example: the workflow runs and JSONL appears
condor_q
pegasus-status -l /opt/workflows/submit/diamond-run
ls -l /opt/workflows/submit/diamond-run/workflow-events.jsonl

# Docs landing in ES — run on the ES node as the read-capable `elastic` user.
# (vector_ingest can write but not search; and /etc/vector is root:vector 0750,
#  so reading the CA on the submit node would need sudo. Querying the ES node
#  over localhost sidesteps both.)
pw=$(grep '^ELASTIC_PASSWORD=' ~/elastic-stack/.env | cut -d= -f2)
curl -sk -u "elastic:$pw" 'https://localhost:9200/workflow-events-*/_count?pretty'

# Optional — prove the submit→ES network path itself (sudo reads the CA; a 200
# from _cluster/health confirms route + TLS + auth end to end):
sudo curl --cacert /etc/vector/ca.crt -u elastic:<ELASTIC_PASSWORD> \
     https://workflow-monitor-es:9200/_cluster/health?pretty
```

If nothing connects, the cause is almost always one of: hitting a **management
IP** instead of the data-plane IP, a **missing inter-slice route** (or a route
lost on reboot), the **CA not distributed** to the submit node, or **condor
daemons bound to the wrong interface** (check `NETWORK_INTERFACE` = the FABNet
IP). The FABRIC `network-troubleshooting.md` checklist is the canonical
reference.

---

## Operational notes

- **Leases expire.** Renew with `slice.renew()` before expiry or the pool and its
  routes vanish.
- **Reboots:** run `--reconfigure` to re-apply data-plane IPs + route and restart
  condor. `CONDOR_HOST` is pinned to the submit FABNet IP, which is stable for
  the life of the lease.
- **Scaling the pool:** raise `--workers` on a fresh build, or add nodes to the
  slice and re-run the bootstrap with the saved `.pool-password` so the new
  workers share the pool key.
- **Retrofitting the container runtime.** A pool built before Apptainer was part
  of the bring-up can get it without a rebuild: `--install-apptainer` uploads and
  runs `install_apptainer.sh` on every node. It is idempotent and does **not**
  restart condor, so it is safe on a running pool:
  ```bash
  uv run python recipes/pegasus-htcondor/provision.py \
      --name pegasus-htcondor --install-apptainer
  ```
  (`--run-workflow` itself needs nothing on the slice — it is driver-side.)
- **Retrofitting the monitord plugin stack:** `--install-monitord-plugin`
  (see [`MONITORD-PLUGIN.md`](MONITORD-PLUGIN.md)). Idempotent; restarts only
  Vector, never condor. Unlike `--wire-es` it does not touch
  `/etc/vector/.env` or the CA.
- **Re-running `--wire-es` overwrites `/etc/vector/.env`** with
  `--ingest-password` (default `changeme-vector-ingest`!). On a wired node,
  pass the real `ELASTIC_INGEST_PASSWORD` or don't re-run it — use
  `--install-monitord-plugin` (config-only refresh) or `--reconfigure`
  (routes) for maintenance instead.
- **Multiple producer slices → one ES.** Each pool ships to the same
  `workflow-monitor-es` over its own FABNet route; ES is the shared sink. Switch
  Vector to **API keys** (one per submit host) so any one can be revoked
  independently (see [`README.md`](README.md) → "Auth model").

---

## File map

```
deploy/
├── PEGASUS-HTCONDOR.md           # ← you are here
├── MONITORD-PLUGIN.md            # the monitord plugin path (overlay + adapter + 3rd stream)
├── fabric/
│   ├── provision_pegasus_slice.py  # FABlib: create pool, route, bootstrap, apptainer,
│   │                                #   monitord-plugin, wire-es, run
│   └── ca.crt                       # the ES CA, downloaded by the elastic-stack recipe (gitignored)
└── pegasus-htcondor/
    ├── bootstrap_pegasus_node.sh    # role-aware in-VM bring-up (uploaded + run by ↑)
    ├── install_apptainer.sh         # container-runtime install (bootstrap + --install-apptainer)
    ├── overlay_monitord_plugin.sh   # monitord plugin-system overlay (--install-monitord-plugin)
    ├── .pool-password               # generated shared pool key (gitignored)
    ├── condor/                      # HTCondor config drop-ins (rendered per node)
    ├── vector/vector.toml.tmpl      # node Vector config (rendered to /etc/vector/vector.toml)
    └── examples/diamond.py          # smoke-test workflow
```
