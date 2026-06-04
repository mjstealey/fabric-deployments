# Hosting Elasticsearch on a FABRIC slice

This guide explains how to move the **Elasticsearch receiver** (the
right-hand side of the pipeline in [`README.md`](README.md)) onto its
own [FABRIC](https://portal.fabric-testbed.net/) slice, separate from
the Pegasus/HTCondor slice that runs Vector. Nothing about the shipper
side changes except the address Vector points at.

```
  Pegasus / HTCondor slice                          Elasticsearch slice
 ┌─────────────────────────────┐                   ┌──────────────────────────┐
 │ pegasus-monitord → *.jsonl  │                   │  Elasticsearch 8.15      │
 │            │                │   FABNetv4 (L3)   │  :9200 (HTTPS+TLS+auth)  │
 │         Vector ─────────────────────────────────▶  ILM / templates / alias │
 │   es sink: https://<es>:9200│  routed, private  │  data on persistent vol  │
 └─────────────────────────────┘                   └──────────────────────────┘
        slice A (same project)                          slice B (same project)
```

The split is almost entirely a **networking** problem: `localhost:9210`
becomes a routed private address on another slice. The receiver config
(`elastic-stack/`) and shipper config (`vector/`) are otherwise reused
verbatim.

> **Companion artifacts** in `deploy/fabric/`:
> - `provision_es_slice.py` — FABlib script that creates the slice,
>   wires inter-slice routing, and (optionally) brings up Elasticsearch
>   end-to-end.
> - `bootstrap_es_node.sh` — the in-VM bring-up the script uploads and
>   runs (installs Docker, regenerates certs with the slice's IP,
>   `docker compose up`, applies ILM/templates/aliases/role/user). Also
>   runnable by hand.

---

## Why FABNetv4 (and not a public IP)

FABRIC nodes have **two** networks, and conflating them is the most
common mistake (see the FABRIC docs,
`docs/guides/networking/network-troubleshooting.md`):

| | Management network | Data plane |
|---|---|---|
| Purpose | **SSH only** (via bastion) | Your experiment traffic |
| Interface | `ens3`/`enp3s0` | added NICs (`enp7s0`, …) |
| Traffic allowed | SSH, basic ICMP | anything |

**Elasticsearch traffic must ride the data plane.** You cannot serve
:9200 over the management network.

For the data-plane link we use **FABNetv4** — a private, routed L3
network FABRIC addresses and routes automatically
(`docs/guides/networking/l3-networks.md`). Two slices *in the same
project* reach each other across their FABNetv4 subnets by adding one
route (`network-troubleshooting.md` → "Inter-slice communication").

We prefer this over `FABNetv4Ext` (public IPs) because public IPs need
the `Net.FABNetv4Ext` permission, undergo a security review, and would
expose the datastore to the Internet. Inter-slice FABNetv4 keeps
Elasticsearch private to your project. (If you do want a public IP — to
ingest from submit hosts *outside* FABRIC — see
[Alternatives](#alternatives) below.)

---

## Prerequisites

- A FABRIC project and a working FABlib environment (JupyterHub on the
  FABRIC portal, or a local `fabric_rc` + tokens). `provision_es_slice.py`
  imports `fabrictestbed_extensions.fablib`.
- **Both slices in the same FABRIC project** — inter-slice routing only
  works within a project.
- Project permissions, as needed:
  - `VM.NoLimitCPU` / `VM.NoLimitRAM` / `VM.NoLimitDisk` for a node
    larger than 2 cores / 10 GB RAM / 10 GB disk (Elasticsearch wants
    more than the defaults).
  - `Component.Storage` **only if** you attach a persistent volume for
    the ES data directory (recommended; see [Storage](#storage)).
  - `Slice.Multisite` is **not** required for the single-site ES slice.
    It is only needed if a *single* slice spans multiple sites.
- The Pegasus/HTCondor slice (the producer running Vector), with a
  data-plane NIC on its own FABNetv4 network named `fabnet`. Build it
  with the `pegasus-htcondor` recipe — see
  [`PEGASUS-HTCONDOR.md`](PEGASUS-HTCONDOR.md) and
  `recipes/pegasus-htcondor/README.md`. (The recipe names the network
  `fabnet`, which is what the `--peer-slice` step below routes back to.)

---

## Quick path (scripted)

From the repo root, on a host with FABlib configured:

```bash
# 1. Create the ES slice + wire routing + bring Elasticsearch up.
python deploy/fabric/provision_es_slice.py \
    --name elasticsearch-host \
    --site UCSD \
    --cores 8 --ram 32 --disk 100 \
    --bootstrap-es \
    --peer-slice pegasus-htcondor      # optional: auto-route the Vector side

# 2. The script prints the ES FABNet IP and the exact vector.toml edits.
#    It also downloads the generated CA to deploy/fabric/ca.crt.
```

What the script does, in order:

1. Builds the slice: one node (sized via `--cores/--ram/--disk`), an
   Ubuntu image, an optional persistent storage volume, a `NIC_Basic`
   on a new **FABNetv4** network.
2. Submits and waits for SSH.
3. Reads the node's **data-plane IP and gateway**.
4. Installs the **inter-slice route** (`10.128.0.0/10` via the node's
   own FABNet gateway) and a small systemd one-shot so it survives
   reboot — never touching the default route.
5. With `--bootstrap-es`: uploads `elastic-stack/` + `bootstrap_es_node.sh`,
   installs Docker, **regenerates the node cert with the FABNet IP in
   its SAN**, `docker compose up`, then applies ILM/templates/aliases
   and the scoped `vector_ingest` role/user.
6. With `--peer-slice`: fetches that slice and, on each of its nodes,
   adds the **reverse route** + an `/etc/hosts` entry mapping the ES IP
   to `workflow-monitor-es`.
7. Prints a handoff block with the precise `vector.toml` changes.

Then edit the Pegasus slice's `vector.toml` as printed and restart
Vector. Skip to [Verify](#verify-end-to-end).

The rest of this document explains each step so you can run it by hand,
adapt it, or debug it.

---

## Manual path

### 1. Create the slice

```python
from fabrictestbed_extensions.fablib.fablib import FablibManager as fablib_manager
fablib = fablib_manager()

slice = fablib.new_slice(name="elasticsearch-host")

# IPv4-management site keeps apt/docker simple (no NAT64). UCSD/TACC qualify.
node = slice.add_node(name="es1", site="UCSD")
node.set_capacities(cores=8, ram=32, disk=100)   # >10 GB disk → VM.NoLimitDisk
node.set_image("default_ubuntu_22")

# Optional persistent data volume (needs Component.Storage); see Storage below.
node.add_storage(name="es-data")

# Data-plane NIC on a private routed network.
iface = node.add_component(model="NIC_Basic", name="nic1").get_interfaces()[0]
net   = slice.add_l3network(name="fabnet", interfaces=[iface], type="IPv4")

slice.submit()   # blocks until provisioned + post-boot config applied
```

API references: `docs/guides/slices/create-slice.md`,
`docs/guides/networking/l3-networks.md`,
`docs/guides/storage/persistent-storage.md`.

> **Site choice matters for setup traffic.** The Docker image pull and
> `apt` run over the *management* network. On IPv6-management sites you
> must use FABRIC's DNS64 (use hostnames, not raw IPv4) and the IPv6
> Docker registry `registry.ipv6.docker.com`. Choosing an IPv4-management
> site (MAX, TACC, MASS, **UCSD**, FIU, SRI, BRIST, TOKY) sidesteps both.

### 2. Read the data-plane addressing

```python
slice = fablib.get_slice("elasticsearch-host")
node  = slice.get_node("es1")
iface = node.get_interface(network_name="fabnet")
print("ES data-plane IP:", iface.get_ip_addr())     # e.g. 10.128.5.2
net   = slice.get_network("fabnet")
print("FABNet gateway:  ", net.get_gateway())        # e.g. 10.128.5.1
```

Say the IP is **`10.128.5.2`** and the gateway **`10.128.5.1`**. Do the
same on the Pegasus slice's submit node to learn *its* FABNet gateway,
e.g. `10.131.9.1`.

### 3. Wire the inter-slice route

Add a route to the **whole** FABNetv4 space via each node's *own*
gateway — straight from `network-troubleshooting.md`:

```bash
# On the ES node (so replies route back):
sudo ip route add 10.128.0.0/10 via 10.128.5.1

# On the Pegasus submit node:
sudo ip route add 10.128.0.0/10 via 10.131.9.1
```

> ⚠️ **Never replace the default route** — it carries SSH and there is
> no recovery if you break it. Add the specific `10.128.0.0/10` route
> only.
>
> ⚠️ **Routes and data-plane IPs don't survive reboot by default.**
> Re-apply with `slice.get_node(...).config()` from FABlib, bake the
> route into a systemd one-shot (what `provision_es_slice.py` does), or
> configure netplan. Re-running the script with `--reconfigure`
> re-applies both.

Verify before continuing:

```bash
ping 10.128.5.2          # from the Pegasus submit node
```

### 4. Bring up Elasticsearch on the node

Reuse `elastic-stack/` unchanged except for two things — the cert SANs
and the published port. `bootstrap_es_node.sh` automates all of this;
the steps it performs are:

1. **Mount storage** (if you attached a volume) — see [Storage](#storage).
2. **Regenerate the node cert to include the FABNet IP** so Vector's
   TLS check passes when it connects by IP. Same `elasticsearch-certutil`
   command as `README.md`, plus the IP:

   ```bash
   ... elasticsearch-certutil cert --silent \
       --ca-cert /certs/ca/ca.crt --ca-key /certs/ca/ca.key \
       --pem --out /certs/node.zip --name node \
       --dns localhost,workflow-monitor-es --ip 127.0.0.1,10.128.5.2 </dev/null
   ```

3. **Publish on :9200.** On a clean FABRIC VM there's no Roon/RAATServe
   collision, so use the conventional port. The bootstrap rewrites the
   uploaded compose's `9210:9200` → `9200:9200` (the committed file is
   untouched). It also sets the heap to ~half the node RAM.

4. `docker compose up -d`, wait for `healthy`.

5. **Apply schema + security** exactly as `README.md` does — ILM policy,
   the two index templates, the bootstrap write-aliases
   (`workflow-events-000001` / `workflow-diag-000001` with
   `is_write_index:true` — load-bearing; ILM stays parked without them),
   and the scoped `vector_ingest` role/user — all via `curl` against
   `https://localhost:9200` on the node.

6. **Host firewall.** FABRIC imposes no firewall on the data plane, but
   Ubuntu's `ufw` might. If enabled, allow 9200 from the FABNet subnet.
   You do **not** need 9300 across slices (that's inter-node transport;
   you're single-node).

### 5. Point Vector at the remote Elasticsearch

On the Pegasus slice, the only edits to `vector/vector.toml` are the two
sink endpoints (and the CA path). Everything else — sources, VRL
transforms, disk buffers, basic auth — is unchanged.

**TLS — two options:**

- **Connect by hostname (no cert regen needed).** The committed node
  cert already carries `DNS:workflow-monitor-es` as a SAN. Map the ES IP
  to that name on the submit node and keep using the hostname:

  ```bash
  echo "10.128.5.2 workflow-monitor-es" | sudo tee -a /etc/hosts
  ```
  ```toml
  endpoints = ["https://workflow-monitor-es:9200"]
  ```
  (`provision_es_slice.py --peer-slice` adds this `/etc/hosts` line for
  you.)

- **Connect by IP.** Use the cert the bootstrap regenerated with the IP
  in its SAN:
  ```toml
  endpoints = ["https://10.128.5.2:9200"]
  ```

Either way:

```toml
[sinks.es_events]
endpoints   = ["https://workflow-monitor-es:9200"]   # was https://localhost:9210
tls.ca_file = "/etc/vector/ca.crt"                   # the CA from the ES slice
# (es_diag sink: same endpoint change)
```

Copy the ES slice's CA (`deploy/fabric/ca.crt`, downloaded by the
script, or `elastic-stack/certs/ca/ca.crt` if you generated locally) to
`/etc/vector/ca.crt` on the submit node, keep `ELASTIC_INGEST_PASSWORD`
in `/etc/vector/.env`, then restart Vector. The disk buffer with
`when_full = "block"` now earns its keep: if the ES slice is briefly
unreachable, Vector applies backpressure and resumes from its
byte-offset checkpoints — no loss.

---

## Verify end-to-end

```bash
# From the submit node: TLS + auth + reachability in one shot.
curl --cacert /etc/vector/ca.crt -u vector_ingest:$ELASTIC_INGEST_PASSWORD \
     https://workflow-monitor-es:9200/_cluster/health?pretty

vector validate /etc/vector/vector.toml
sudo systemctl restart vector && journalctl -u vector -f

# On the ES node: confirm docs are landing.
curl --cacert ~/elastic-stack/certs/ca/ca.crt -u elastic:$ELASTIC_PASSWORD \
     'https://localhost:9200/workflow-events-default/_count?pretty'
```

If nothing connects, the cause is almost always one of: hitting a
**management IP** instead of the data-plane IP, a **missing inter-slice
route** (or a route lost on reboot), or the **CA not distributed** to
the submit node. Check those first; the FABRIC
`network-troubleshooting.md` "Nodes can't ping each other" checklist is
the canonical reference.

---

## Storage

Attaching a persistent volume keeps the ES index across slice deletion:

```python
node.add_storage(name="es-data")     # references a project storage allocation
```

Notes (`docs/guides/storage/persistent-storage.md`):

- The named volume is a **project storage allocation** requested through
  the FABRIC portal; `add_storage(name=...)` *attaches* it. Create the
  allocation first (with the size you want) and pass its name.
- Persistent storage is **rotating disk, not SSD, and not backed up**.
  Fine for monitoring data; you own the backups. Elasticsearch prefers
  fast disks, so for heavy volume consider local NVMe (`Component.NVME`,
  non-persistent) instead.
- Mount it at the ES data dir before first boot (the bootstrap script
  prints, but does **not** auto-run, the destructive `mkfs` — format
  manually so you never wipe an existing volume):

  ```bash
  lsblk                                   # find the new device, e.g. /dev/vdb
  sudo mkfs.ext4 /dev/vdb                  # FIRST TIME ONLY — destroys data
  sudo mkdir -p /data/es && sudo mount /dev/vdb /data/es
  sudo chown -R 1000:1000 /data/es         # ES container runs as uid 1000
  ```
  Then bind-mount `/data/es` → `/usr/share/elasticsearch/data` in the
  compose file instead of the named `esdata` volume.

If you skip persistent storage, ES data lives on the node's root disk
(sized by `--disk`) and is lost when the slice is deleted.

---

## Operational notes

- **Leases expire.** Both slices are leased; renew with `slice.renew()`
  before expiry or the ES node (and its routes) vanish. Persistent
  storage survives and can be re-attached to a fresh slice.
- **Multiple Pegasus slices → one ES.** Repeat steps 3 and 5 per
  producer slice (or pass each as `--peer-slice`). All route to the same
  `10.128.0.0/10` space and the same ES IP; ES is the shared sink.
- **Credentials.** The prototype uses basic auth with a scoped
  `vector_ingest` user. With several producer slices, switch to **API
  keys** (one per submit host) so any single host can be revoked without
  touching the others — see `README.md` → "Auth model".
- **Reboots.** After a node reboot, re-apply data-plane config:
  `python deploy/fabric/provision_es_slice.py --name <slice> --reconfigure`,
  or `slice.get_node(...).config()` from FABlib.

---

## Alternatives

| Goal | Approach | Trade-off |
|---|---|---|
| Ingest from submit hosts **outside** FABRIC | `FABNetv4Ext` public IP on the ES node (`docs/guides/networking/external-access.md`) | Needs `Net.FABNetv4Ext` + security review; exposes ES to the Internet. Use policy routing so it doesn't clobber the default route. |
| Quick look at a Kibana UI from your laptop | SSH tunnel: `ssh -L 5601:localhost:5601 ...`, or Tailscale (`external-access.md`) | No FABRIC permission needed. Don't expose Kibana publicly. |
| Highest Vector→ES throughput | Dedicated NIC (`NIC_ConnectX_5/6`) + jumbo MTU 9000 instead of `NIC_Basic` | More resources; only matters at high event rates. `NIC_Basic` (~10 Gbps) is ample for workflow events. |

---

## File map

```
deploy/
├── FABRIC.md                     # ← you are here
├── fabric/
│   ├── provision_es_slice.py     # FABlib: create slice, route, bootstrap
│   ├── bootstrap_es_node.sh      # in-VM ES bring-up (uploaded + run by ↑)
│   └── ca.crt                    # downloaded CA after bootstrap (gitignored)
├── elastic-stack/                # reused unchanged (receiver: ES + schema)
└── vector/                       # reused; only sink endpoints/CA path change
```
