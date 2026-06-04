# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

A **`uv`-managed FABlib runtime + FABRIC deployment assets + slice recipes**.
It holds the FABRIC testbed credentials (`config/fabric_rc`, `config/.tokens.json`,
referenced SSH keys), a Python environment with FABlib installed, the deployment
code under `deploy/`, and credentialed entry points under `recipes/`. It is the
FABRIC side of running Pegasus/HTCondor workflows on FABRIC.

The `elastic-stack` recipe stands up the **workflow-monitor Elasticsearch
receiver** on its own slice, reachable over private FABNetv4 from the
Pegasus/HTCondor slice where Vector ships workflow events. See `README.md` for
the full picture and the gap analysis.

Layout: `config/` (credentials) · `deploy/` (deployment logic + ES/Vector
config) · `recipes/` (thin launchers) · `preflight.py` (env doctor).

## Commands

```bash
uv sync                                              # build .venv from uv.lock
uv run python preflight.py                           # read-only env doctor
uv run python recipes/elastic-stack/provision.py --help
```

There is no build/lint/test suite. `preflight.py` is the validation tool — run
it (and expect all-PASS) before provisioning, and after any `config/fabric_rc` edit.

## Architecture / key facts

- **This project is the source of truth for the deployment.** The provisioner
  (`deploy/fabric/provision_es_slice.py`), the in-VM bootstrap
  (`deploy/fabric/bootstrap_es_node.sh`), the receiver config
  (`deploy/elastic-stack/`), and the shipper config (`deploy/vector/`) all live
  here — moved out of `workflow-monitor/deploy/` (which now keeps a forwarding
  stub). Read `deploy/FABRIC.md` before changing provisioning behavior.
- **Recipes are thin launchers.** `recipes/elastic-stack/provision.py` imports
  `deploy/fabric/provision_es_slice.py` and overrides its `_load_fablib` seam to
  inject `FablibManager(fabric_rc=…/config/fabric_rc)`, passing all flags through.
  `provision_es_slice.py` resolves its own asset paths relative to itself
  (`deploy/fabric/` → `deploy/elastic-stack/`), so don't break that layout.
- **Two recipes, two halves of one pipeline.** `elastic-stack` is the *receiver*
  (ES slice); `pegasus-htcondor` is the *producer* (HTCondor pool + Pegasus +
  workflow-monitor + Vector). `recipes/pegasus-htcondor/provision.py` →
  `deploy/fabric/provision_pegasus_slice.py` → `deploy/pegasus-htcondor/` (same
  launcher/asset pattern as above). **The FABNetv4 network name `fabnet` is
  load-bearing:** both provisioners share `NET_NAME = "fabnet"`, which is what
  lets the ES side's `--peer-slice pegasus-htcondor` route back to the pool
  unmodified. Read `deploy/PEGASUS-HTCONDOR.md` before changing producer behavior.
- **Config resolution (FABlib 2.0.6).** Precedence is constructor args ▸
  `fabric_rc` file ▸ env vars ▸ defaults. FABlib does **not** read a `FABRIC_RC`
  env var; with no constructor arg it reads `~/work/fabric_config/fabric_rc`.
  That is why recipes pass `fabric_rc=` explicitly.
- **Do NOT `source config/fabric_rc`.** `FABRIC_SSH_COMMAND_LINE` (Jinja `{{ }}`)
  and `FABRIC_AVOID` contain spaces/braces that break shell word-splitting. The
  file is for FABlib's line parser only. To resolve config from a script, build
  `FablibManager(fabric_rc=<path>)` or use `Config(fabric_rc=<path>, offline=True)`.
- **`FABRIC_AVOID` format matters.** It must be a plain comma list
  (`'EDUKY,EDC,GATECH,GPN'`), NOT Python-list syntax — FABlib `.split(",")`s the
  raw value, so brackets/quotes produce garbled site names that silently fail to
  avoid the intended sites.
- **Coupling to workflow-monitor is by data contract.** The **authoritative event
  schema lives in `workflow-monitor/DATA_SOURCES.md`** ("Event Types and Schemas")
  — workflow-monitor is the *producer*. Everything here is a downstream *consumer*
  that mirrors it: `deploy/vector/vector.toml` (and the `vector.toml.tmpl` for the
  FABRIC submit node) tail the JSONL it writes, and `deploy/elastic-stack/templates/`
  map its fields. Keep them in step with `DATA_SOURCES.md` when the schema changes.
  Note: the `deploy/` pipeline assets (Vector config, ES templates/ILM) were
  removed from the workflow-monitor repo (they were only ever on a deleted branch)
  and now live here exclusively.

## Conventions and cautions

- **Secrets stay out of git.** Gitignored: `config/fabric_rc`,
  `config/.tokens.json`, `**/.env`, `deploy/pegasus-htcondor/.pool-password`,
  `deploy/elastic-stack/certs/`, any pulled-back `ca.crt`, the host-specific
  `deploy/vector/vector.toml` + `com.vectordotdev.vector.plist`, and
  `deploy/vector/data/`. Each has a committed `*.template` counterpart (copy it to
  the real name and fill it in — see `config/README.md` and `deploy/README.md`).
  The FABRIC token is short-lived (identity token ~4h) —
  run `uv run python refresh_token.py` to exchange the refresh token (valid ~24h,
  rotated on each use) for a fresh one without a portal visit; only once the
  refresh token itself lapses must you re-download from the portal
  (Experiments ▸ Manage Tokens). `preflight.py` flags it when expiring.
- **Paths are absolute and host-specific** (e.g. `/path/to/fabric-deployments/...`,
  `~/.ssh/...`). They live in `config/fabric_rc` (copied from `config/fabric_rc.template`),
  not in code. Moving machines means changing every path and `FABRIC_BASTION_USERNAME`
  together; `preflight.py` will catch missing files.
- **Python is `uv`-managed and pinned to 3.12** (`requires-python` `>=3.10,<3.13`).
  The system Python (3.14) is too new for FABlib's deps — always go through
  `uv run`, never the system interpreter.
- **This project is under git** (prepared for public release, Apache-2.0). Before
  any commit, confirm `git status` lists none of the gitignored secret files above
  — the `*.template` files are what's committed in their place.
- **FABRIC-side prerequisites are not local-config.** Project permissions, the
  same-project requirement for inter-slice routing, an existing peer slice with a
  FABNetv4 NIC, and lease renewal live in `recipes/elastic-stack/README.md` and
  `deploy/FABRIC.md`, and `preflight.py` cannot check them.
