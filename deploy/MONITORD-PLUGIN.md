# The pegasus-monitord plugin path

How the **pegasus-monitord entry-point plugin system** (pegasus-isi/pegasus,
branch `monitord-plugin-system`) and the **workflow-monitor `wfmonitor` plugin
adapter** (pegasus-isi/workflow-monitor, branch `monitord-plugin-adapter`) are
deployed and tested on the two FABRIC slices, and how to verify the result.
This is the *push* counterpart to the existing *poll* path: the same workflow
events, but emitted live from inside `pegasus-monitord` instead of polled back
out of the stampede DB.

Validated end-to-end on 2026-06-11 (runs `diamond-plugin-2`, `diamond-plugin-3`
on the `pegasus-htcondor` slice; docs in the `elasticsearch-host` slice's
`monitord-events-*` index).

---

## 1. The two halves

### 1a. Pegasus side — the plugin host (branch `monitord-plugin-system`)

Pure-Python addition to `pegasus-monitord`, pinned in this repo at commit
`2a6a23fca065c5bcadd0fd0273588097d92a04d3` (= branch tip: the entry-point
system, the cross-thread payload race fix, **and the tick() hook** — see §6).
Three files under `packages/pegasus-python/src/Pegasus/`:

| File | Status | Role |
|---|---|---|
| `monitoring/plugin.py` | new | `MonitordEventPlugin` base class, `_PluginWorker` (one daemon thread + bounded queue per plugin), `MonitordPluginManager` (entry-point discovery) |
| `monitoring/event_output.py` | modified | `PluginHostEventSink` + the synthetic `plugins://` sink URL, fanned out by the existing multiplex machinery alongside the stampede-DB sink |
| `cli/pegasus-monitord.py` | modified | injects the `plugins://` endpoint when any `pegasus.monitord.plugins.*` property is set; threads the full properties object through |

Mechanics:

- Plugins are discovered in the entry-point group **`pegasus.monitord.plugins`**
  via `importlib.metadata` — any pip-installed package can register one.
- A plugin is **off unless** `pegasus.monitord.plugins.<name>.enabled = true`
  appears in the run's properties. With no plugin properties at all, monitord
  behavior is byte-identical to stock.
- `start(props)`/`stop()` run on monitord's main thread; `handle_event(event,
  kw)` runs on the plugin's own thread, fed from a bounded queue (default
  10000, `…<name>.queue_size`). Queue overflow drops events (counted + logged)
  rather than ever blocking monitord; a wedged plugin is abandoned after
  `…<name>.join_timeout` (default 10s). A crashing plugin never kills monitord.
- Event payloads are **snapshotted at enqueue** (`dict(kw)`) — the race fix.
- **`tick()`** (added in `2a6a23fca`): with `…<name>.tick_interval` set to a
  positive number of seconds, the plugin's *existing* worker thread waits with
  `get(timeout=…)` and calls `plugin.tick()` when the queue is idle (plus a
  starvation guard that ticks between events under continuous flow) — wall
  -clock callbacks with **no additional thread**, on the same thread as
  `handle_event` (no locking between them), exception-isolated identically,
  and never after the shutdown sentinel. Default `0` = the byte-identical
  blocking loop as before.

### 1b. workflow-monitor side — the `wfmonitor` adapter (branch `monitord-plugin-adapter`)

`src/workflow_monitor/monitord_plugin.py` registers (in `pyproject.toml`):

```toml
[project.entry-points."pegasus.monitord.plugins"]
wfmonitor = "workflow_monitor.monitord_plugin:WorkflowMonitorPlugin"
```

`WorkflowMonitorPlugin` consumes the live stampede event stream and writes
**`monitord-events.jsonl`** — the same authoritative schema as the polling
path's `workflow-events.jsonl` (`workflow-monitor/DATA_SOURCES.md`), emitting
`workflow_start`, `jobs_init`, `workflow_state`, and `job_state` events. It
keeps its own correlation state (job/task roster maps) in place of the
stampede-DB joins, and demultiplexes nothing: one monitord, one workflow, one
file.

**With `condor_poll=true`** the plugin also absorbs the HTCondor polling the
standalone `--serve` loop performs: its `tick()` runs gated
`condor_q`/`condor_history`/`condor_status` queries and emits
`htcondor_poll`/`htcondor_history`/`pool_status` into the same JSONL —
**one process, one writer, no thread whose only job is polling condor**.
Mechanics mirror `server.py` exactly: queries scoped to the workflow with the
same `Cmd`-prefix constraint (built from `wf.plan`'s planner-recorded
`submit_dir`; the plugin never polls before `wf.plan`), a plugin-private
`CondorBackoff` (exponential backoff on schedd failure; history/pool skipped
while failing), history at ≥ 3× the tick base (min 10s) emitting the full
ClusterId-deduped merged cache, pool at ≥ 5× (min 15s), fingerprint dedup
reusing `EventLogger`'s helpers, and a final queue/history/pool flush in
`stop()` for terminal ClassAds. When the plugin polls condor, start `--serve`
with **`--no-condor-poll`** so the two paths don't double-poll the schedd.

Properties (read from the run's `pegasus.properties`, prefix
`pegasus.monitord.plugins.wfmonitor.`):

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | must be `true` for monitord to start the plugin |
| `events_path` | `./monitord-events.jsonl` | output JSONL (use an absolute path under the runs root so Vector's glob sees it) |
| `restart` | `false` | truncate instead of append |
| `tick_interval` | `0` (no ticks) | host-level: tick cadence in seconds; also the condor-poll base the adapter sub-throttles from |
| `condor_poll` | `false` | poll condor from `tick()` and emit the three condor event types |
| `schedd`, `collector`, `token_path`, `cert_path`, `key_path`, `password_file` | unset | optional passthroughs to the condor queries (not needed on this pool — FS auth as the submitting user) |

---

## 2. How it is deployed (this repo is the source of truth)

The plugin host is **not** in any Pegasus release; the apt-installed
Pegasus 5.1.2 is retrofitted by overlaying the three files. Because the change
is pure Python, the overlay *is* the install — no rebuild, and the entry point
needs no registration on the Pegasus side.

One command does the whole producer-side retrofit (idempotent):

```bash
uv run python recipes/pegasus-htcondor/provision.py --install-monitord-plugin
```

which runs four steps on the submit node:

1. **Overlay** (`deploy/pegasus-htcondor/overlay_monitord_plugin.sh`): stage
   the 3 files — by default `curl` from raw.githubusercontent.com pinned to
   `--pegasus-plugin-ref` (full SHA), or uploaded from a local checkout with
   `--pegasus-src ~/GitHub/pegasusai/pegasus` — then `py_compile`-gate,
   byte-compare, and `install` only what differs. A `<file>.orig` backup is
   taken only when the current file still matches dpkg's md5 for the pegasus
   package (pristine apt content); stale `__pycache__` entries are purged.
2. **Adapter install**: `pip install --user` of `--monitord-adapter-spec` —
   default `git+https://github.com/pegasus-isi/workflow-monitor.git@monitord-plugin-adapter`,
   or a **local directory** (tarred, uploaded, installed from the extracted
   tree — the fast iteration loop; provenance records the git describe,
   including `-dirty`). Installed twice: once plain (covers dependencies),
   once `--force-reinstall --no-deps` (the version number never moves, so pip
   would otherwise consider it satisfied).
3. **Entry-point verification** with `/usr/bin/python3` — the same interpreter
   the `pegasus-monitord` wrapper resolves (`which python3`; the `--user` site
   is visible to it). Fails loudly: without this, monitord silently runs
   without the plugin.
4. **Vector refresh**: re-render `/etc/vector/vector.toml` from the current
   template (now including the `monitord_events` source/transform/sink) and
   restart Vector. Unlike `--wire-es`, this never touches `/etc/vector/.env`
   (the live ingest password) or `ca.crt`, and validates the candidate config
   (`vector validate --no-environment`) before replacing the working one.

Everything is recorded in a provenance manifest on the node:

```bash
sudo cat /usr/lib/pegasus/python/Pegasus/monitoring/.monitord-plugin-manifest.json
# applied_at, source mode + ref/describe, per-file sha256 before/after +
# changed flags + .orig policy, adapter spec + resolved commit, entry_point_ok
```

### Receiver side (do this FIRST)

The plugin stream gets its own index family. Apply the schema to the running
cluster **before** the Vector restart in `--install-monitord-plugin`:

```bash
uv run python recipes/elastic-stack/provision.py --apply-schema
```

`deploy/elastic-stack/apply_es_schema.sh` is idempotent and running-cluster
-safe: templates and ILM are plain upserts, the `monitord-events-000001` write
alias is bootstrapped only if absent, the `vector_ingest` role is re-PUT with
`monitord-events-*` added, and the existing user's password is never reset.

**Order is load-bearing**: if Vector's `es_monitord` sink writes before the
alias exists, its `auto_configure` privilege creates a *concrete index* named
`monitord-events-default`, which permanently blocks the alias + ILM rollover.
(`apply_es_schema.sh` detects this and prints the remediation.)

### Per-run opt-in

```bash
uv run python recipes/pegasus-htcondor/provision.py \
    --run-example --enable-monitord-plugin --run-name diamond-plugin-3
```

`--enable-monitord-plugin` (also honored by `--run-workflow`) writes into the
run's `pegasus.properties` before `pegasus-plan`:

```
pegasus.monitord.plugins.wfmonitor.enabled=true
pegasus.monitord.plugins.wfmonitor.events_path=/opt/workflows/submit/<run-name>/monitord-events.jsonl
pegasus.monitord.plugins.wfmonitor.tick_interval=5      # --monitord-tick-interval
pegasus.monitord.plugins.wfmonitor.condor_poll=true     # omit with --no-monitord-condor-poll
```

(the stale block from a previous run is sed-deleted first — `events_path`
embeds the run name). Without the flag, runs behave exactly as before;
`--no-monitord-condor-poll` is the regression configuration (plugin on,
condor polling off — only the four pegasus event types, ~88 events for the
diamond example).

---

## 3. Three streams, three index families

The polling-path `workflow-monitor --serve` keeps running unchanged on every
run, so the same workflow produces both telemetry paths for side-by-side
comparison. (That investigation — folding the HTCondor polling into the
plugin instead of two interleaved collectors — is now **implemented** via the
tick() + `condor_poll` mechanism above; `--serve` is kept polling on these
test runs precisely to generate the comparison data. A production
single-path setup would run `--serve --no-condor-poll`.)

| JSONL in the submit dir | Producer | Vector `stream` tag | ES index family |
|---|---|---|---|
| `workflow-events.jsonl` | workflow-monitor `--serve` (polls stampede DB + condor_q/history/status) | `workflow_events` | `workflow-events-*` |
| `diagnostics-events.jsonl` | workflow-monitor `--serve --diagnose` (stall detection) | `workflow_diag` | `workflow-diag-*` |
| `monitord-events.jsonl` | **pegasus-monitord, via the wfmonitor plugin** (pegasus events pushed live + condor polls from tick()) | `monitord_plugin` | `monitord-events-*` |

All three are tailed by the same Vector instance
(`deploy/pegasus-htcondor/vector/vector.toml.tmpl`) with identical transforms
(`@timestamp` from `timestamp`, `submit_host`, `stream`), shipped to the same
ES with the same scoped `vector_ingest` user, disk-buffered identically. The
`monitord-events-*` template mirrors `workflow-events-*` field-for-field and
reuses the `workflow-retention` ILM policy. Kibana gets a
"Monitord plugin events" data view via `--push-kibana`.

Notable comparison points observed on the diamond runs:

- Without `condor_poll`, the plugin path emits **88 events** for a 12-job
  diamond run — pure state transitions. With it (`diamond-tick-2`,
  2026-06-12), the plugin emitted **103** events: the same 88 plus
  `htcondor_poll: 7`, `htcondor_history: 3`, `pool_status: 5` — and the
  polling path's counts for the same run were `8/3/5`. The two collectors see
  near-identical condor activity at a 5s tick vs the 2s serve loop.
- Plugin events appear in the JSONL **as monitord processes them** (line
  -buffered), i.e. seconds before the polling path can see the same
  transition land in the stampede DB and be polled back out.

---

## 4. Verification runbook

On the **submit** node, after a run with the plugin enabled
(`D=/opt/workflows/submit/<run-name>`):

```bash
grep -i plugin $D/monitord.log          # expect: "Enabling monitord event plugin host",
                                        # "wfmonitor plugin writing events to ...",
                                        # "started monitord event plugin 'wfmonitor'";
                                        # with condor_poll also "condor polling enabled";
                                        # NO "tick() raised" tracebacks
wc -l $D/monitord-events.jsonl          # diamond: ~88 without condor_poll, ~100+ with
# with condor_poll: the three condor event types are present (counts vary
# with run timing; expect htcondor_poll >= 2, pool_status >= 1, and
# htcondor_history >= 1 near completion):
python3 -c "import json,sys,collections; print(collections.Counter(
 json.loads(l)['event_type'] for l in open(sys.argv[1])))" $D/monitord-events.jsonl
tail -2 $D/jobstate.log                 # DAGMAN_FINISHED 0, MONITORD_FINISHED 0
# every timestamp must be a sane epoch (catches the §6 failure modes):
python3 -c "import json,sys; bad=[e for e in map(json.loads, open(sys.argv[1]))
 if not (isinstance(e.get('timestamp'),(int,float)) and e['timestamp']>1e9)];
print('bad-timestamp events:', len(bad))" $D/monitord-events.jsonl
ls $D/*.jsonl                           # all three streams present (dual path alive)
```

On the **es1** node (query as `elastic`; `vector_ingest` is write-only):

```bash
pw=$(grep '^ELASTIC_PASSWORD=' ~/elastic-stack/.env | cut -d= -f2)
curl -sk -u "elastic:$pw" 'https://localhost:9200/monitord-events-*/_count?q=wf_uuid:<uuid>'
# count == the local JSONL line count; <uuid> from $D/braindump.yml
curl -sk -u "elastic:$pw" 'https://localhost:9200/_alias/monitord-events-default?pretty'
# monitord-events-00000N with "is_write_index": true
```

And on the submit node, `sudo journalctl -u vector -n 50` should show all
three sinks' healthchecks passing. If `es_monitord` reports
"Network is unreachable", it is the inter-slice route, not the plugin — see
`deploy/FABRIC.md` → Troubleshooting (a reboot on *either* slice loses the
route; `--reconfigure` the slice whose `ip route` lacks `10.128.0.0/10`).

---

## 5. Iterating on the adapter

The loop used to develop and fix the adapter (no GitHub round-trip):

```bash
# edit ~/GitHub/pegasusai/workflow-monitor/src/workflow_monitor/monitord_plugin.py
uv run python recipes/pegasus-htcondor/provision.py --install-monitord-plugin \
    --monitord-adapter-spec /Users/stealey/GitHub/pegasusai/workflow-monitor
uv run python recipes/pegasus-htcondor/provision.py \
    --run-example --enable-monitord-plugin --run-name <fresh-name>
```

The manifest records the working tree's `git describe --always --dirty`, so a
node running uncommitted code is visible. Same loop for the Pegasus side with
`--pegasus-src /Users/stealey/GitHub/pegasusai/pegasus` (uploads the 3 files
from the local checkout instead of fetching the pinned SHA).

Use a **fresh `--run-name` per attempt**: the run actions `rm -rf` the submit
dir of the name they're given, and distinct names keep prior JSONLs around
for comparison (Vector checkpoints by inode and never re-ships old lines).

---

## 6. Known-issue history (what testing surfaced)

**Cross-thread payload race (fixed in the pinned SHA).** The initial plugin
host enqueued the *live* event dict to plugin worker threads while monitord's
main thread kept reusing/mutating it (the rc.meta loop overwrites and re-sends
one dict; `wf.plan` gains `db_url` post-dispatch). Workers observed torn
payloads. Fixed by snapshotting at enqueue (`dict(kw)`) in pegasus commit
`c3d6be873`; design notes live in `monitord-plugin-payload-race.md` on the
pegasus branch. The node's first overlay (2026-06-09) predated the fix —
`--install-monitord-plugin` re-overlaying `plugin.py` to the pinned tip was
the repair.

**Planner 1970-era roster timestamps (worked around in the adapter).** The
`jobs_init` event of runs 1–2 carried `timestamp` values like `539286` /
`692252` — *not* the race: `pegasus-plan`'s Java netlogger writes the
`<dag>.static.bp` roster events (`task.info`, `job.info`, maps) with a
monotonic/uptime clock rendered as 1970-era ISO stamps
(`ts=1970-01-09T00:17:32Z` ≈ node uptime; the two runs' values differed by
exactly the wall-clock gap between them). monitord replays the file verbatim,
so plugins receive non-epoch `ts` values — and the same artifact sits in
every 5.1.2 stampede DB. The adapter now ignores `ts < 1e9` when tracking the
last event time and stamps `jobs_init` with "now" if no wall-clock ts has been
seen (workflow-monitor `monitord_plugin.py`, `_MIN_EPOCH_TS`). The two
1970-`@timestamp` `jobs_init` docs from the pre-fix runs were left in
`monitord-events-*` deliberately, as a record of the failure mode.

**ClassAd `ExprTree` values broke JSON serialization (fixed).** The first
tick run (`diamond-tick-1`, 2026-06-11) emitted `pool_status` but zero
`htcondor_poll`/`htcondor_history`: with the htcondor python bindings present
inside monitord, `query_queue` returns `dict(ad)` ClassAds whose values can
be `classad.ExprTree` (unevaluated expressions), and the plugin's plain
`json.dumps` raised `Object of type ExprTree is not JSON serializable` on
every write — isolated by the plugin host (`tick() raised` in monitord.log),
so the workflow was unharmed but the events never landed. `EventLogger` had
always serialized with `default=str`; the plugin's `_write` now does the
same (workflow-monitor commit `d033d9f`).

**Lost inter-slice route, again.** The first post-retrofit Vector restart
surfaced `Network is unreachable` on the new sink — both slices had lost
their FABNetv4 route (ES-side reboot). Diagnosed exactly per the
`deploy/FABRIC.md` runbook; fixed with `--reconfigure` on each slice. Note
that the route can be missing on *either or both* sides; check `ip route |
grep 10.128.0.0/10` on every node.
