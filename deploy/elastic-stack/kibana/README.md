# Kibana dashboards for Pegasus workflow monitoring

Importable Kibana saved objects + an Elasticsearch transform for observing
Pegasus/HTCondor workflow progress, built on the two indices `workflow-monitor`
ships into via Vector:

| Index | Contents | Data view id |
|---|---|---|
| `workflow-events-*` | lifecycle: `workflow_start`, `jobs_init`, `job_state`, `htcondor_poll`, `pool_status`, `workflow_stats`, `workflow_end` | `wf-events` |
| `workflow-diag-*` | health: `stall_detected`, `idle_diagnosis`, `hold_diagnosis`, `failure_diagnosis`, `stall_resolved` | `wf-diag` |
| `workflow-jobstate-current` | **derived** — latest state per job (a transform, see below) | `wf-jobstate-current` |

Target stack: **Elasticsearch + Kibana 8.15.0** (`deploy/elastic-stack/docker-compose*.yml`).

## The one thing to understand first

workflow-monitor emits an **event-on-change stream**, not evenly-sampled metrics.
A job emits one event per *transition* (SUBMIT → EXECUTE → SUCCESS …). So:

- **Progress over time** = *cumulative sum* of `job_state` transitions → the
  burn-up chart. Works directly on `workflow-events-*`.
- **Current job state** ("how many jobs are RUNNING right now") = the
  **`workflow-jobstate-current` transform**, which collapses the stream to the
  latest state per `(wf_uuid, exec_job_id)`. A raw count of `job_state` events
  would count transitions, not jobs, and badly overcount.
- **Pool capacity over time** = `pool_status` events carry a *scalar* `pool.*`
  object, so they chart cleanly. (Avoid `htcondor_poll.jobs[]` /
  `htcondor_history.jobs[]` for aggregation — they are arrays mapped as `object`,
  not `nested`, so per-element math cross-contaminates.)
- Time field is **`@timestamp`** (Vector derives it), not the raw `timestamp` double.

## What gets created

- **3 data views** — `data-views.ndjson`
- **6 Lens panels** — burn-up, pool CPUs, diagnostics activity, current-state
  donut, jobs-by-type table, fleet throughput
- **3 saved searches** — event feed, diagnostics feed, workflow-level state feed
- **2 dashboards** — `wf-overview` (fleet) and `wf-drilldown` (single workflow,
  with `wf_uuid` + `dax_label` controls)
- **1 transform** + its dest index template — `transforms/`

The `.ndjson` files are generated from `build_saved_objects.py`; edit the Python,
not the NDJSON:

```bash
uv run python deploy/elastic-stack/kibana/build_saved_objects.py
```

## Prerequisites

1. The ES slice is up and **Vector has shipped at least one workflow's events**,
   so the indices and their fields exist. Easiest: run the smoke test first —
   `recipes/pegasus-htcondor/provision.py --run-example`. Importing data views
   before any document exists leaves them with no fields.
2. A tunnel to the ES node (Kibana is tunnel-only — never exposed):
   ```bash
   ssh -L 5601:localhost:5601 -L 9200:localhost:9200 ubuntu@<es-node>
   ```
3. The `elastic` password, from the slice's gitignored env file:
   ```bash
   set -a; . deploy/elastic-stack/.env; set +a   # exports ELASTIC_PASSWORD
   ```

## Import

### From your workstation (recommended) — push over FABlib

No tunnel or manual copy needed. This uploads the assets to the node and runs
the importer in-VM (where ES/Kibana are on localhost and `~/elastic-stack/.env`
already holds the password):

```bash
uv run python recipes/elastic-stack/provision.py \
    --name elasticsearch-host --push-kibana
```

Regenerate the NDJSON first if you changed the generator
(`uv run python deploy/elastic-stack/kibana/build_saved_objects.py`). The slice
must already be bootstrapped (`--bootstrap-es`).

### On the node / over a tunnel — run import.sh directly

```bash
cd deploy/elastic-stack/kibana
set -a; . ../.env; set +a
./import.sh
```

Either path is idempotent (safe to re-run): it installs the dest index template,
(re)creates and starts the transform, waits for Kibana, then imports both NDJSON
bundles with `overwrite=true`. Then browse:

- Fleet overview — `http://localhost:5601/app/dashboards#/view/wf-overview`
- Drilldown — `http://localhost:5601/app/dashboards#/view/wf-drilldown`

## Verify / operate the transform

```bash
ES=https://localhost:9210; CA=../certs/ca/ca.crt
curl -s --cacert $CA -u elastic:$ELASTIC_PASSWORD \
  "$ES/_transform/workflow-jobstate-current/_stats?pretty"   # state, docs_processed
curl -s --cacert $CA -u elastic:$ELASTIC_PASSWORD \
  "$ES/workflow-jobstate-current/_search?size=2&pretty"      # sample rows
```

It runs **continuous** (`sync` on `@timestamp`, `frequency: 10s`), so the
current-state panels track live workflows within ~1–2 minutes of each transition.
To rebuild from scratch, just re-run `import.sh` (it stops/deletes/recreates).

## Troubleshooting

- **`"success": false` on import** with a missing-field error → the field's event
  type hasn't been shipped yet. Run a workflow and re-import; refresh the data
  view's field list under Stack Management ▸ Data Views.
- **Empty current-state panels** → the transform hasn't produced docs. Check
  `_transform/.../_stats`; confirm `job_state` events exist in `workflow-events-*`.
- **Controls didn't import** (older/newer minor) → on the dashboard, *Add control
  ▸ Options list*, data view `wf-events`, field `wf_uuid` (and `dax_label`). 30s.
- **A Lens panel won't load after a Kibana upgrade** → the Lens state schema is
  version-pinned to 8.15. Re-create it from the recipe below (a couple of minutes)
  or re-export from a working instance and replace the NDJSON.

## Appendix — rebuild any panel by hand

Each panel maps to plain Lens operations. Create a Lens viz, pick the data view,
set the query, drop these dimensions:

| Panel | Data view | Query (KQL) | Type | Dimensions |
|---|---|---|---|---|
| **Burn-up (progress)** | `wf-events` | `event_type : "job_state"` | Area | X = `@timestamp` (date histogram); Breakdown = `state`; Y = **Cumulative sum** of *Count* |
| **Pool CPUs** | `wf-events` | `event_type : "pool_status"` | Line | X = `@timestamp`; Y = **Max** of `pool.total_cpus` and **Max** of `pool.idle_cpus` |
| **Diagnostics activity** | `wf-diag` | *(none)* | Stacked bar | X = `@timestamp`; Breakdown = `event_type`; Y = Count |
| **Current job state** | `wf-jobstate-current` | *(none)* | Donut | Slice by `current.state`; Metric = Count |
| **Jobs by type & state** | `wf-jobstate-current` | *(none)* | Table | Rows = `current.type_desc`, `current.state`; Metric = Count |
| **Fleet throughput** | `wf-events` | `event_type : "job_state" and (state : "JOB_SUCCESS" or state : "POST_SCRIPT_SUCCESS")` | Stacked bar | X = `@timestamp`; Breakdown = `wf_uuid`; Y = Count |

Saved searches (Discover): on `wf-events` show `event_type, exec_job_id, state,
exitcode, type_desc`; on `wf-diag` show `event_type, stall_type, job_name,
summary, reason`. Sort `@timestamp` desc. Add the `wf_uuid` control and pick a
workflow to scope the whole drilldown.

> Note: the burn-up slightly overcounts when jobs retry (SUBMIT/EXECUTE re-emit).
> It's a trend view; for exact final tallies use the `workflow_stats` /
> `workflow_end` event (`done`, `failed`, `total_jobs`).
