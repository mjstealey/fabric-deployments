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

# 5. (optional) The PegasusAI Studio webapp on the submit node — see the
#    "PegasusAI Studio webapp" section below.
uv run python recipes/pegasus-htcondor/provision.py \
    --name pegasus-htcondor --install-studio
```

The nodes run **Ubuntu 24 (noble)** and Pegasus from the **development/6.0 apt
channel, unpinned** — the same channel the studio container image bundles, so
host and container Pegasus stay in step (see the studio section for why skew is
fatal). `--pegasus-channel release --pegasus-version 5.1.2` restores the old
stable-channel behavior (on an Ubuntu 22 `--image`; the 6.0 dev debs need
noble's libc).

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

## PegasusAI Studio webapp (container on the submit node)

`--install-studio` runs `ghcr.io/pegasus-isi/studio-webapp:main` on the submit
node. The container **plans with its own bundled Pegasus but submits through
the host schedd** (`--network host` + `_CONDOR_*` env), and DAGMan + all jobs
run under the **host** Pegasus — which is why the pool tracks the same
`development/6.0` apt channel the image bundles. Version skew is fatal, not
cosmetic: a 5.1.2 host `pegasus-exitcode` exits 2 on the 6.0 planner's `-R`/`-M`
POST-script flags, aborting every DAG on its first node.

```bash
uv run python recipes/pegasus-htcondor/provision.py --install-studio
# then, from your machine:
ssh -F ~/.ssh/fabric-submit.conf -L 8888:localhost:8888 fabric-submit
open http://localhost:8888
```

What the installer sets up (every piece is required):

- **docker** (`docker.io` apt package) and the image.
- **passwd/group shims** — `~/.studio/container-passwd` / `container-group` are
  the image's own `/etc/passwd` + `/etc/group` with
  `ubuntu:x:1000:1000::/home/ubuntu:/bin/bash` / `ubuntu:x:1000:` appended,
  bind-mounted read-only. Without them uid 1000 resolves to `?` inside the
  container and Pegasus plans into a literal `?/` submit dir.
- **IDTOKEN auth** — the container can present neither FS identity (does not
  cross the container boundary) nor the root-only pool key, so
  `00-pool-common.conf` lists `IDTOKENS` and the installer mints
  `sudo condor_token_create -identity ubuntu@$(condor_config_val TRUST_DOMAIN)`
  into `~/.condor/tokens.d/studio` (0600), pointed at by
  `_CONDOR_SEC_TOKEN_DIRECTORY`.
- **`_CONDOR_CONDOR_HOST`** = the submit node's FABNet IP;
  **`_CONDOR_SCHEDD_HOST`** = the condor `TRUST_DOMAIN` (the node's OpenStack
  hostname). The container never resolves that name — the collector lookup
  returns the schedd's address.
- **`/home/ubuntu` bind-mounted**, `HOME=/home/ubuntu`, so plans/submit dirs
  land on the host filesystem where the host schedd (and Vector) can see them.
- The monitord plugin install (`--install-monitord-plugin`) writes
  `pegasus.monitord.plugins.wfmonitor.enabled = true` into `~/.pegasusrc` —
  monitord folds only `~/.pegasusrc` into the plan-time properties snapshot, so
  that line is what turns the wfmonitor stream on for studio-planned runs.

Quick health checks: `docker exec studio whoami` → `ubuntu`, and
`docker exec studio condor_q` authenticates (no `AUTHENTICATE:1003`). The
studio's own state (`~/.studio/studio.db`, projects under `~/work/workflows`)
lives on the host and survives container recreation.

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

# Install on the submit node (adapter + ~/.pegasusrc + Vector stream), once.
# On Pegasus >= 6.0 the plugin host is native and the 3-file overlay is
# auto-skipped; on 5.x the overlay is applied first:
uv run python recipes/pegasus-htcondor/provision.py --install-monitord-plugin

# Then opt in per run:
uv run python recipes/pegasus-htcondor/provision.py \
    --run-example --enable-monitord-plugin --run-name diamond-plugin-3
```

That order matters — apply the ES schema before the Vector restart, or the
first bulk write auto-creates a concrete index squatting on the
`monitord-events-default` alias name.

---

## Trying `pegasus-monitor` (pegasus branch `feature/pegasus-monitor*`)

`pegasus-monitor` is the new single-workflow live view / one-shot reporter from
the pegasus `feature/pegasus-monitor*` branches (WP9b tip `1ea245081`). It is a
self-contained pure-Python addition — `Pegasus/monitor/` + `cli/pegasus-monitor.py`
+ a `/usr/bin` wrapper — that only imports `Pegasus.braindump` and
`Pegasus.tools.properties` from the rest of the tree, so it overlays onto the
apt-installed Pegasus without touching pegasus-monitord, the monitord plugin
overlay, or Vector. `overlay_pegasus_monitor.sh` does the byte-compared
install, generates the wrapper exactly as `release-tools/update-python-tools`
would, and writes a provenance manifest at
`/usr/lib/pegasus/python/Pegasus/monitor/.pegasus-monitor-manifest.json`.
It needs `rich` + `PyYAML` importable by the node's `python3` (the bootstrap's
`pip --user` install of workflow-monitor already pulls both in).

```bash
# From a local pegasus checkout/worktree on the branch (its COMMITTED HEAD ships):
uv run python recipes/pegasus-htcondor/provision.py \
    --install-pegasus-monitor --pegasus-monitor-src /path/to/pegasus-wp9b

# Then, on the submit node (note: --version reports the HOST pegasus version):
pegasus-monitor --once /opt/workflows/submit/<run>/ubuntu/pegasus/diamond/run0001
pegasus-monitor --once --diagnose <submit dir>     # source health + diagnostics
pegasus-monitor --why-idle <submit dir>            # bounded condor evidence for idle jobs
pegasus-monitor --serve <submit dir>               # detached JSONL writer (<submit dir>/workflow-events.jsonl)
pegasus-monitor <submit dir> --stop-server
pegasus-monitor --replay <submit dir>/workflow-events.jsonl --speed 0 --once
```

The script is self-contained if the FABlib token is unavailable: build the
tarball with `git archive` (header comment in the script) and run it over SSH
with `STAGE_TARBALL=… PEGASUS_MONITOR_REF=… bash overlay_pegasus_monitor.sh`.
Beware the filename clash: `pegasus-monitor --serve` and `workflow-monitor
--serve` both default to `<submit dir>/workflow-events.jsonl`, the file Vector
tails — give one of them its own `--log PATH` before running both against the
same submit dir (and keep anything not meant for ES out of that glob).

---

## Installing Pegasus from a branch as a wheel (`--install-pegasus-wheel`)

The pegasus repo's developer guide (`CLAUDE.md`, "Build Environment" /
"Installation of the wheel") tests a branch by building ONE self-contained
`pegasus-wms` wheel with `make build` — Python packages, Java planner JARs, the
C tools, the Go tools and a worker-package tarball — and `pip install`ing it
into a venv. `--install-pegasus-wheel` does exactly that on the submit node,
then installs the same wheel file on every worker, so the planner, PegasusLite
and the worker tools are one version pool-wide. The apt Pegasus stays installed
next to it; nothing in `/usr` is overwritten.

```bash
# A pushed branch/tag (cloned on the submit node):
uv run python recipes/pegasus-htcondor/provision.py \
    --install-pegasus-wheel --pegasus-wheel-ref GH-2270

# Or a local checkout's COMMITTED HEAD (git-archived and uploaded):
uv run python recipes/pegasus-htcondor/provision.py \
    --install-pegasus-wheel --pegasus-wheel-src ~/GitHub/pegasusai/pegasus

# Works on a fresh build too (runs after the pool bootstrap):
uv run python recipes/pegasus-htcondor/provision.py --install-pegasus-wheel --pegasus-wheel-ref GH-2270
```

About 3½ minutes from scratch (toolchain + clone + ~2 min `make build` + three
installs). `deploy/pegasus-htcondor/install_pegasus_wheel.sh` does the node
work (`MODE=build` on the submit node, `MODE=install` on workers) and writes a
provenance manifest at `<prefix>/.pegasus-wheel-manifest.json` (version, wheel
sha256, source commit). Afterwards `--run-example` / `--run-workflow` put the
wheel first on PATH automatically and print the planner they used
(`planner: /opt/pegasus-env/bin/pegasus-plan (6.0.0.dev2270)`); by hand,
`export PATH=/opt/pegasus-env/bin:$PATH`.

Three choices differ from the guide's literal commands, each for a reason:

- **Prefix `/opt/pegasus-env`, not `~/pegasus-env`** (`--pegasus-env`). The
  pool's `UID_DOMAIN` is per-host, so worker jobs run as `nobody`, which cannot
  traverse `/home/ubuntu` (0750). PegasusLite then finds only the apt binaries
  in `/usr/bin`, rejects their version, and the job fails. The script refuses
  to finish unless `nobody` can run `<prefix>/bin/pegasus-config`.
- **`pegasus.transfer.worker.package = true`** in the submit node's
  `~/.pegasusrc` (`--no-pegasus-wheel-worker-package` to skip). A dev build's
  worker package is not on download.pegasus.isi.edu, so a job whose site does
  not point `PEGASUS_HOME` at an install must carry the wheel's bundled one —
  otherwise PegasusLite exits 71 after two 404s.
- **Go 1.24.4 from go.dev.** The `go.mod` files need 1.24; noble ships 1.22,
  and `GOTOOLCHAIN=auto` fails on `pegasus-globus-online`'s bare `go 1.24`
  (no such release).

Gotcha for workflow generators that compute `PEGASUS_HOME` themselves (e.g.
`predictable-workflow-failures`): they prepend `$CONDOR_HOME/bin` = `/usr/bin`
to PATH, which puts the apt `pegasus-version` first, so the Pegasus API plans
with the apt planner. Export `PEGASUS_HOME`, `PEGASUS_PYTHON_LIB` and
`CONDOR_HOME` pointing at the prefix, and check `planner_version` in the run's
`braindump.yml`.

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
submit.set_image("default_ubuntu_24")
ifaces.append(submit.add_component(model="NIC_Basic", name="nic1").get_interfaces()[0])

for i in (1, 2):
    w = slice.add_node(name=f"work{i}", site="UCSD")
    w.set_capacities(cores=8, ram=32, disk=50)
    w.set_image("default_ubuntu_24")
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
    ├── overlay_pegasus_monitor.sh   # pegasus-monitor CLI overlay (--install-pegasus-monitor)
    ├── .pool-password               # generated shared pool key (gitignored)
    ├── condor/                      # HTCondor config drop-ins (rendered per node)
    ├── vector/vector.toml.tmpl      # node Vector config (rendered to /etc/vector/vector.toml)
    └── examples/diamond.py          # smoke-test workflow
```
