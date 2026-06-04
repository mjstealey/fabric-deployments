# fabric-deployments

A [`uv`](https://docs.astral.sh/uv/)-managed [FABlib][fablib] runtime, FABRIC
deployment assets, and slice recipes for running **Pegasus / HTCondor**
workflows on the [FABRIC Testbed][fabric], with a **workflow-monitor → Vector →
Elasticsearch (+ Kibana)** observability pipeline shipped over a private, routed
FABNetv4 network.

It bundles three things:

- a reproducible Python environment (FABlib, pinned via `uv`),
- the deployment logic + Elasticsearch / Vector configuration under `deploy/`,
- thin, credentialed launchers under `recipes/` that provision FABRIC slices.

The two recipes build the two halves of one pipeline, in the **same FABRIC
project**:

- [`recipes/pegasus-htcondor/`](recipes/pegasus-htcondor/README.md) — the
  **producer**: a Pegasus + HTCondor pool running workflow-monitor + Vector.
- [`recipes/elastic-stack/`](recipes/elastic-stack/README.md) — the **receiver**:
  the Elasticsearch (+ optional Kibana) slice Vector ships into.

They are wired together over FABNetv4: the producer names its network `fabnet`,
and the receiver's `--peer-slice` routes back to it. See
[`deploy/PEGASUS-HTCONDOR.md`](deploy/PEGASUS-HTCONDOR.md) and
[`deploy/FABRIC.md`](deploy/FABRIC.md) for the authoritative walkthroughs.

> **Related repos.** The example workflows and the
> [`workflow-monitor`](https://github.com/pegasus-isi/workflow-monitor) project
> are maintained separately; this repo consumes workflow-monitor's JSONL event
> stream but does not vendor it.

---

## Repository layout

```
fabric-deployments/            # uv project root
├── pyproject.toml             # uv project: fabrictestbed-extensions (FABlib)
├── uv.lock                    # resolved dependency lockfile
├── .python-version            # pinned interpreter (3.12)
├── preflight.py               # read-only env doctor: "what am I missing?"
├── refresh_token.py           # refresh the FABRIC token from its refresh token
├── README.md                  # ← you are here
├── CREDITS.md                 # upstream-project acknowledgments
├── LICENSE                    # Apache-2.0
├── config/
│   ├── README.md              # step-by-step credential setup (start here)
│   ├── fabric_rc.template     # → copy to fabric_rc and fill in
│   ├── fabric_rc              # FABRIC credentials/config (read by FABlib; gitignored)
│   ├── .tokens.json.template  # → copy to .tokens.json and paste your token
│   └── .tokens.json           # FABRIC API token (gitignored; you provide)
├── deploy/                    # deployment assets
│   ├── FABRIC.md              #   ES-on-FABRIC walkthrough (receiver)
│   ├── PEGASUS-HTCONDOR.md    #   Pegasus/HTCondor pool walkthrough (producer)
│   ├── README.md              #   the Vector → Elasticsearch pipeline
│   ├── fabric/                #   provision_*_slice.py + in-VM bootstraps
│   ├── elastic-stack/         #   receiver: ES compose + schema/ILM (certs gitignored)
│   ├── pegasus-htcondor/      #   producer: condor configs + Vector tmpl + diamond example
│   └── vector/                #   shipper: vector.toml + service units (data/ gitignored)
└── recipes/
    ├── elastic-stack/         # recipe: provision the ES receiver slice
    │   ├── provision.py       #   launcher → deploy/fabric/provision_es_slice.py
    │   └── README.md          #   recipe usage + FABRIC-side prerequisites
    └── pegasus-htcondor/      # recipe: provision the Pegasus/HTCondor pool
        ├── provision.py       #   launcher → deploy/fabric/provision_pegasus_slice.py
        └── README.md          #   recipe usage + the two-slice handshake
```

`deploy/` holds the deployment logic and the Elasticsearch / Vector configs;
`config/` holds your credentials; `recipes/` holds thin, credentialed entry
points. The Vector config (`deploy/vector/`) and ES schema
(`deploy/elastic-stack/templates/`) are coupled to workflow-monitor's JSONL
output by data contract — keep them in step when that schema changes.

---

## Prerequisites

Before you can provision anything you need a working FABRIC identity. The
**step-by-step setup lives in [`config/README.md`](config/README.md)** — read it
first. In summary, you need:

- **A FABRIC account and project** with the permissions the recipes require
  (e.g. `VM.NoLimit*` for larger nodes; see each recipe README for specifics).
- **SSH keys** generated locally and **registered in the FABRIC portal** (a
  bastion key pair and a slice/sliver key pair).
- **A FABRIC API token** downloaded from the portal
  (Experiments ▸ Manage Tokens ▸ Create Token).

`preflight.py` validates that your local credentials are present and consistent,
but it **cannot** check FABRIC-side facts (project permissions, the same-project
requirement for inter-slice routing, an existing peer slice, lease state). Those
are covered in the recipe READMEs and `deploy/FABRIC.md`.

---

## First run

```bash
# 1. Clone the repo.
git clone https://github.com/mjstealey/fabric-deployments.git
cd fabric-deployments

# 2. Create your config/secret files from the templates, then edit them.
cp config/fabric_rc.template          config/fabric_rc
cp config/.tokens.json.template       config/.tokens.json
cp deploy/elastic-stack/.env.template deploy/elastic-stack/.env
cp deploy/vector/.env.template        deploy/vector/.env
#   - config/fabric_rc        : paths, project UUID, bastion username (see config/README.md)
#   - config/.tokens.json     : paste the token JSON from the FABRIC portal
#   - deploy/**/.env          : ES / Vector passwords and endpoints

# 3. Build the environment (installs FABlib + deps into .venv).
uv sync

# 4. Validate. Read-only, no network. Expect all-PASS before provisioning.
uv run python preflight.py
```

`uv sync` is the only build step — `uv` creates `.venv`, installs the locked
dependencies, and `uv run` executes inside it. No `pip install`, no manual venv
activation, and **never** `source config/fabric_rc` (see "How config is
resolved" below).

If `preflight.py` flags the token as expiring or expired, mint a fresh one from
the refresh token (no portal visit needed while it is < ~24h old):

```bash
uv run python refresh_token.py            # --check to inspect, --force to refresh now
```

Then provision — see the recipe READMEs for all flags:

```bash
uv run python recipes/pegasus-htcondor/provision.py --help   # producer
uv run python recipes/elastic-stack/provision.py   --help    # receiver
```

---

## Configuration & secrets

Each credential or secret file ships as a `*.template`. **Copy each template to
its real name and fill it in** (see the First run section above and
[`config/README.md`](config/README.md) for what goes where).

The real files are **gitignored** and must never be committed. They are:

- `config/fabric_rc` — FABRIC control-plane hosts, project UUID, key paths.
- `config/.tokens.json` — your FABRIC API token (short-lived).
- `deploy/**/.env` — Elasticsearch / Vector passwords and endpoints
  (`deploy/elastic-stack/.env`, `deploy/vector/.env`).
- `deploy/pegasus-htcondor/.pool-password` — generated HTCondor pool signing key.
- `deploy/elastic-stack/certs/` — TLS material generated per deploy.
- `deploy/fabric/ca.crt` — the CA pulled back from a provisioned ES slice.
- `deploy/vector/vector.toml` — the rendered Vector config (endpoints/secrets).
- `deploy/vector/data/` — Vector runtime state (checkpoints + disk buffers).

The FABRIC token is short-lived: the identity token lasts ~4h and the refresh
token ~24h (rotated on each use). Use `uv run python refresh_token.py` to renew
locally; only once the refresh token itself lapses must you re-download from the
portal. `preflight.py` flags it when it is expiring.

---

## What it takes to deploy on FABRIC

The deployment is essentially a **networking** problem; the Elasticsearch and
Vector configs are reused largely unchanged. End to end:

1. **Two slices, one project** — a Pegasus/HTCondor producer slice and an
   Elasticsearch receiver slice, each with a `NIC_Basic` on a **FABNetv4**
   (private, routed L3) network.
2. **Inter-slice routing** — a route to the FABNetv4 supernet (`10.128.0.0/10`)
   via each node's *own* FABNet gateway, on every node of both slices, persisted
   via a systemd one-shot so it survives reboot. The management/default route is
   never touched.
3. **Elasticsearch on the receiver** — Docker, single-node ES 8.15 over the data
   plane on `:9200`, TLS cert regenerated with the node's FABNet IP in its SAN,
   plus the ILM policy, index templates, write-alias bootstrap, and a scoped
   `vector_ingest` role/user. Kibana is available as an optional compose file.
4. **Vector pointed at it** — on the submit host, the ES sink endpoints point at
   `https://<es-host>:9200` with `tls.ca_file` set to the CA pulled back from the
   ES slice. The disk-backed buffer (`when_full = "block"`) absorbs brief ES
   outages without loss.

The recipes automate this and print the exact cross-slice edits. The
authoritative walkthroughs (including the manual path and data-flow diagrams) are
[`deploy/FABRIC.md`](deploy/FABRIC.md) (receiver) and
[`deploy/PEGASUS-HTCONDOR.md`](deploy/PEGASUS-HTCONDOR.md) (producer).

---

## How config is resolved (important)

FABlib's `FablibManager()` resolves settings in this precedence:

> constructor args  ▸  `fabric_rc` file  ▸  environment variables  ▸  defaults

Two facts drove the design of this project (both verified against
`fabrictestbed-extensions` 2.0.6):

1. **FABlib does *not* read a `FABRIC_RC` environment variable.** With no
   constructor argument it looks only at `~/work/fabric_config/fabric_rc`, then
   falls back to env vars, then defaults. So `config/fabric_rc` here is *not*
   picked up automatically.
2. **`config/fabric_rc` is not safe to `source`.** `FABRIC_SSH_COMMAND_LINE`
   (Jinja `{{ }}` placeholders) and `FABRIC_AVOID` contain spaces/braces that a
   shell would word-split or glob. The file is written for FABlib's own
   line-by-line parser, not for `source`. Note that `FABRIC_AVOID` must be a
   plain comma list (`'EDUKY,EDC,GATECH,GPN'`), **not** Python-list syntax —
   FABlib does a literal `.split(",")`, so brackets/quotes become part of the
   site names and silently fail to avoid the intended sites.

The launchers sidestep both by injecting
`FablibManager(fabric_rc=<repo>/config/fabric_rc)` directly — no symlink, no
sourcing. `preflight.py` resolves config the same way, so what it reports is
exactly what a recipe will see.

If you ever want a *bare* `uv run python …` (no launcher) to find this config,
symlink it to FABlib's default location:

```bash
mkdir -p ~/work/fabric_config
ln -sf "$PWD/config/fabric_rc" ~/work/fabric_config/fabric_rc
```

---

## Adding more slice recipes

Each recipe is a subdirectory of `recipes/` with its own `provision.py` (or
notebook/script) and `README.md`. Reuse the shared runtime two ways:

- **Wrap an existing provisioner** (like `elastic-stack/`): import the script
  from `deploy/…` and inject
  `FablibManager(fabric_rc=<repo>/config/fabric_rc)` at its `_load_fablib` seam.
- **Write a fresh script:** build the manager explicitly —
  ```python
  from pathlib import Path
  from fabrictestbed_extensions.fablib.fablib import FablibManager
  RC = Path(__file__).resolve().parents[2] / "config" / "fabric_rc"
  fablib = FablibManager(fabric_rc=str(RC))
  slice = fablib.new_slice(name="my-slice")
  ...
  ```

Run any of them with `uv run python recipes/<name>/<script>.py`. Run
`preflight.py` first — the credential checks apply to every recipe.

---

## References

- [`deploy/FABRIC.md`](deploy/FABRIC.md) — ES-on-FABRIC design + manual path.
- [`deploy/PEGASUS-HTCONDOR.md`](deploy/PEGASUS-HTCONDOR.md) — producer design + manual path.
- [`deploy/README.md`](deploy/README.md) — the Vector → Elasticsearch pipeline.
- FABRIC documentation — <https://learn.fabric-testbed.net/>
- Pegasus WMS documentation — <https://pegasus.isi.edu/documentation/>
- HTCondor documentation — <https://htcondor.readthedocs.io/>

---

## License

This project is licensed under the **Apache License 2.0** — see
[`LICENSE`](LICENSE). For acknowledgment of the upstream projects this
deployment builds on, see [`CREDITS.md`](CREDITS.md).

[fablib]: https://github.com/fabric-testbed/fabrictestbed-extensions
[fabric]: https://fabric-testbed.net/
