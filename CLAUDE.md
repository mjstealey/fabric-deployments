# fabric-deployments

## What this is
A `uv`-managed FABlib runtime plus FABRIC deployment assets and slice recipes — the FABRIC side of running Pegasus/HTCondor workflows. Two recipes are two halves of one pipeline: `elastic-stack` (ES *receiver* slice) and `pegasus-htcondor` (HTCondor + Pegasus + workflow-monitor + Vector *producer*), linked over private FABNetv4.

## Stack
Python 3.12 (pinned; system 3.14 breaks FABlib deps), FABlib 2.0.6 (`fabrictestbed-extensions`), Vector, Elasticsearch. No installable package — recipes run as scripts.

## Layout
`config/` (credentials) · `deploy/` (provisioners + ES/Vector config) · `recipes/` (thin launchers) · `preflight.py` (env doctor). This repo is the source of truth for the deployment; assets were moved out of `workflow-monitor/deploy/`.

## Deeper docs
Read these before changing the matching surface — they hold the detail this file condenses:
- `README.md` — full pipeline picture + gap analysis.
- `deploy/FABRIC.md` — provisioning behavior + the routing/reboot troubleshooting runbook.
- `deploy/PEGASUS-HTCONDOR.md` — producer-side (HTCondor pool + Pegasus + Vector) behavior.
- `deploy/MONITORD-PLUGIN.md` — the monitord plugin retrofit (architecture, deploy order, 3 streams, verification).
- `config/README.md`, `deploy/README.md`, `recipes/*/README.md` — `*.template`→real secret setup, and FABRIC-side prerequisites `preflight.py` can't check (project permissions, same-project inter-slice routing, peer slice with a FABNetv4 NIC, lease renewal).

## Commands
```bash
uv sync                                              # build .venv from uv.lock
uv run python preflight.py                           # read-only env doctor (expect all-PASS)
uv run python refresh_token.py                       # refresh FABRIC identity token (~4h)
uv run python recipes/elastic-stack/provision.py --help
```
No build/test suite. Run `preflight.py` before provisioning and after any `config/fabric_rc` edit. Always go through `uv run`, never the system interpreter.

## Conventions
- Recipes are thin launchers that import `deploy/fabric/provision_*_slice.py` and inject `FablibManager(fabric_rc=…/config/fabric_rc)`. Provisioners resolve asset paths relative to themselves (`deploy/fabric/` → `deploy/<recipe>/`) — don't break that layout.
- `NET_NAME = "fabnet"` is load-bearing in both provisioners — it lets `--peer-slice` route between halves unmodified.
- ES schema changes go through `deploy/elastic-stack/apply_es_schema.sh`; one `templates/*.json` per index family, aliases/role-grants derived from filenames.
- Event schema is a data contract owned by `workflow-monitor/DATA_SOURCES.md` (producer). Keep `deploy/vector/vector.toml` and `deploy/elastic-stack/templates/` in step when it changes.
- Secrets stay out of git; each has a committed `*.template`. Confirm `git status` shows no secret files before commit (Apache-2.0, public release).

## Monitord plugin (active surface — flags live in `deploy/fabric/provision_pegasus_slice.py`; see `deploy/MONITORD-PLUGIN.md`)
- Retrofit, not rebuild: the pegasus-monitord entry-point plugin system (pegasus branch `monitord-plugin-system`, pinned by `MONITORD_PLUGIN_REF`, currently `da1db847c…`) is a 3-file overlay onto the apt-installed Pegasus + the `wfmonitor` adapter (workflow-monitor branch `monitord-plugin-adapter`). Install once with `--install-monitord-plugin`; opt in per run with `--enable-monitord-plugin`.
- Three parallel streams → three index families, so push vs poll compare side by side: `workflow-events-*` (workflow-monitor `--serve` poll path — the stream Vector ships; toggled by `--serve-monitor`/`--no-serve-monitor`, default on), `workflow-diag-*` (`--serve --diagnose`), and `monitord-events-*` (the plugin push path). The `monitord-events-*` template mirrors `workflow-events-*` field-for-field and reuses the `workflow-retention` ILM.
- Condor polling is folded into the plugin via `tick()`: `--monitord-condor-poll` (default on; `--no-monitord-condor-poll` is the regression config) adds `htcondor_poll`/`htcondor_history`/`pool_status` event types, cadence `--monitord-tick-interval` (5s). A production single-path setup runs `--serve --no-condor-poll`.
- Standalone `--enable-wfevents-plugin` writes `wfevents.jsonl` (same schema) consumed via `workflow-monitor --remote` — Vector does NOT tail it; install once with `--install-wfevents-plugin` (needs the overlay; no Vector change). `--wfevents-condor-poll` lets it poll condor; enable polling on ONE plugin only (pair with `--no-monitord-condor-poll` if `wfmonitor` is also on).

## Gotchas
- Pipeline gaps are usually routing, not Kibana/ingest: a lost inter-slice FABNetv4 route after a VM reboot makes Vector time out reaching ES (events still land in local `workflow-events.jsonl`). Diagnose backwards (ES `_count` → `journalctl -u vector` → `ping` ES FABNet IP → `ip route | grep 10.128.0.0/10`), fix with `--reconfigure`.
- monitord-plugin ordering: run ES `--apply-schema` BEFORE any Vector restart adding a sink for a new index family, or the sink auto-creates a concrete index on the alias and blocks rollover.
- A run's LAST 1–2 events (`workflow_stats`/`workflow_end`) can sit undelivered in Vector's disk buffer once writes stop — ES count a hair under `wc -l *.jsonl`, checkpoint at EOF, no errors, healthchecks green (it acks into the buffer, not ES); `systemctl restart vector` replays them. Diagnose with per-`wf_uuid` aggs, not `_count`.
- Version pins in `bootstrap_pegasus_node.sh` are load-bearing: Vector **0.56.0** held (0.57 dropped `${VAR}` TOML interpolation → every ES sink 401s with a correct `.env`); HTCondor via `--htcondor-version` (channel from the major). wisc's repo needs its InCommon intermediate anchored or apt/GnuTLS rejects the chain.
- FABlib config precedence: constructor arg ▸ `fabric_rc` ▸ env ▸ defaults. It does NOT read `FABRIC_RC`; with no arg it reads `~/work/fabric_config/fabric_rc` — hence the explicit `fabric_rc=`. Paths there are absolute/host-specific: moving machines means changing every path and `FABRIC_BASTION_USERNAME`.
- `FABRIC_AVOID` must be a plain comma list (`EDUKY,EDC,...`), not Python-list syntax — FABlib `.split(",")`s it; brackets/quotes silently fail to avoid sites. Don't `source config/fabric_rc` either — spaces/braces in `FABRIC_SSH_COMMAND_LINE`/`FABRIC_AVOID` break word-splitting; use `FablibManager(fabric_rc=…)` or `Config(fabric_rc=…, offline=True)`.
