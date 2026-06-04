# Recipe: `elastic-stack`

Provision a FABRIC slice that hosts the **workflow-monitor Elasticsearch
receiver**, reachable over a private routed FABNetv4 network from the
Pegasus/HTCondor slice where Vector runs.

```
  Pegasus / HTCondor slice                          Elasticsearch slice (this recipe)
 ┌─────────────────────────────┐                   ┌──────────────────────────┐
 │ pegasus-monitord → *.jsonl  │                   │  Elasticsearch 8.15      │
 │            │                │   FABNetv4 (L3)   │  :9200 (HTTPS+TLS+auth)  │
 │         Vector ─────────────────────────────────▶  ILM / templates / alias │
 │   es sink: https://<es>:9200│  routed, private  │  data on persistent vol  │
 └─────────────────────────────┘                   └──────────────────────────┘
        slice A (same project)                          slice B (same project)
```

## What runs where

This recipe (`provision.py`) is a **thin launcher**. The real logic and the
authoritative documentation live under `deploy/` in this same project:

- [`../../deploy/FABRIC.md`](../../deploy/FABRIC.md) — the full design + manual
  path (read this).
- `../../deploy/fabric/provision_es_slice.py` — creates the slice, wires
  inter-slice routing, optionally bootstraps Elasticsearch end-to-end.
- `../../deploy/fabric/bootstrap_es_node.sh` — in-VM bring-up (Docker, certs
  with the FABNet IP in the SAN, `docker compose up`, ILM / templates /
  write-aliases / scoped `vector_ingest` user).
- `../../deploy/elastic-stack/` — the receiver config it uploads.

`provision.py` exists only so the provisioner runs with this project's
`config/fabric_rc` and token without needing a `~/work/fabric_config` symlink or
an unsafe `source fabric_rc`. It injects
`FablibManager(fabric_rc=…/config/fabric_rc)` at the `_load_fablib` seam and
passes every flag straight through.

## Run it

From the project root, after `uv sync` and a green `preflight.py`:

```bash
# See all flags (they are the provisioner's flags, verbatim).
uv run python recipes/elastic-stack/provision.py --help

# Full path: create the slice, wire routing, bring ES up, route the Vector side.
uv run python recipes/elastic-stack/provision.py \
    --name elasticsearch-host --site UCSD \
    --cores 8 --ram 32 --disk 100 \
    --bootstrap-es \
    --peer-slice pegasus-htcondor

# Infra + routing only (inspect before bringing ES up):
uv run python recipes/elastic-stack/provision.py --name elasticsearch-host --site UCSD

# Re-apply data-plane IP + route after a node reboot:
uv run python recipes/elastic-stack/provision.py --name elasticsearch-host --reconfigure

# Tear down:
uv run python recipes/elastic-stack/provision.py --name elasticsearch-host --destroy
```

On success the script prints the ES FABNet IP and the exact `vector.toml` edits
for the submit host, and (with `--bootstrap-es`) downloads the generated CA to
`deploy/fabric/ca.crt` for distribution to the Vector side.

### Secrets

Passwords default to the `changeme-*` prototype values. Override them (they are
applied to the ES node's `elastic` superuser and the `vector_ingest` user):

```bash
ELASTIC_PASSWORD=… ELASTIC_INGEST_PASSWORD=… \
  uv run python recipes/elastic-stack/provision.py --name elasticsearch-host --bootstrap-es
# (or pass --elastic-password / --ingest-password)
```

## FABRIC-side prerequisites (not checked by preflight)

`preflight.py` validates your local credentials. These must also be true on the
FABRIC side — see [`../../deploy/FABRIC.md`](../../deploy/FABRIC.md) for detail:

- **Project permissions:** `VM.NoLimitCPU` / `VM.NoLimitRAM` / `VM.NoLimitDisk`
  for the 8-core / 32 GB / 100 GB node (defaults cap at 2 / 10 GB / 10 GB);
  `Component.Storage` only if you attach a persistent volume.
  `Slice.Multisite` is **not** needed (single-site ES slice).
- **Same project, both slices.** Inter-slice FABNetv4 routing only works within
  one project.
- **The peer (Pegasus/HTCondor) slice exists** and has a data-plane NIC on its
  own FABNetv4 network named `fabnet`. Build it with the
  [`pegasus-htcondor`](../pegasus-htcondor/README.md) recipe (it names the network
  `fabnet` so this `--peer-slice` step works unmodified). `--peer-slice` adds the
  reverse route + `/etc/hosts` entry on each of its nodes. Typical order:
  `pegasus-htcondor --bootstrap` → this recipe `--bootstrap-es --peer-slice
  pegasus-htcondor` → `pegasus-htcondor --wire-es`.
- **Site choice:** prefer an IPv4-management site (MAX, TACC, MASS, **UCSD**,
  FIU, SRI, BRIST, TOKY) so the in-VM `apt`/Docker pulls don't need DNS64.
- **Leases expire** — `slice.renew()` before expiry or the node and its routes
  vanish (persistent storage survives and can be re-attached).
