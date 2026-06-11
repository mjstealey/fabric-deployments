#!/usr/bin/env python3
"""Generate Kibana saved-object NDJSON for the workflow-monitor dashboards.

Targets Kibana 8.15.0 (see deploy/elastic-stack/docker-compose.kibana.yml).
Pure stdlib, no network — run it to (re)produce the .ndjson files under
saved-objects/, which are then imported via the Saved Objects _import API
(see import.sh / README.md).

Why a generator instead of hand-written NDJSON: every saved object must be a
single minified JSON line, and Lens `state` is a deeply nested object. Building
dicts in Python keeps the intent readable and guarantees well-formed JSON.

Data model note (drives every panel): workflow-monitor emits an *event-on-change*
stream into two indices — workflow-events-* (lifecycle) and workflow-diag-*
(health). Counting raw events counts *transitions*, not jobs, so:
  * progress over time  -> cumulative_sum of job_state transitions (burn-up)
  * current job state   -> the workflow-jobstate-current TRANSFORM (latest state
                           per job), never a raw count of job_state events.
"""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "saved-objects"
CORE = "8.15.0"

# Data view (index-pattern) saved-object ids — referenced by every panel.
DV_EVENTS = "wf-events"
DV_DIAG = "wf-diag"
DV_JOBSTATE = "wf-jobstate-current"
DV_MONITORD = "wf-monitord-events"


# --------------------------------------------------------------------------
# small builders
# --------------------------------------------------------------------------
def so(type_, id_, attributes, references=None, type_migration=None):
    """Wrap attributes into a Kibana saved object envelope."""
    obj = {
        "id": id_,
        "type": type_,
        "attributes": attributes,
        "references": references or [],
        "coreMigrationVersion": CORE,
    }
    if type_migration:
        obj["typeMigrationVersion"] = type_migration
    return obj


def col_count(label="Count of records"):
    return {
        "label": label,
        "dataType": "number",
        "operationType": "count",
        "isBucketed": False,
        "scale": "ratio",
        "sourceField": "___records___",
        "params": {"emptyAsNull": True},
    }


def col_date(field="@timestamp", interval="auto"):
    return {
        "label": field,
        "dataType": "date",
        "operationType": "date_histogram",
        "sourceField": field,
        "isBucketed": True,
        "scale": "interval",
        "params": {
            "interval": interval,
            "includeEmptyRows": True,
            "dropPartials": False,
        },
    }


def col_terms(field, size, order_col, label=None):
    return {
        "label": label or f"Top {size} values of {field}",
        "dataType": "string",
        "operationType": "terms",
        "scale": "ordinal",
        "sourceField": field,
        "isBucketed": True,
        "params": {
            "size": size,
            "orderBy": {"type": "column", "columnId": order_col},
            "orderDirection": "desc",
            "otherBucket": True,
            "missingBucket": False,
            "parentFormat": {"id": "terms"},
            "include": [],
            "exclude": [],
            "includeIsRegex": False,
            "excludeIsRegex": False,
        },
    }


def col_cumulative(ref_count_id, label="Cumulative count"):
    return {
        "label": label,
        "dataType": "number",
        "operationType": "cumulative_sum",
        "isBucketed": False,
        "scale": "ratio",
        "references": [ref_count_id],
        "params": {},
    }


def col_max(field, label=None):
    return {
        "label": label or f"Maximum of {field}",
        "dataType": "number",
        "operationType": "max",
        "sourceField": field,
        "isBucketed": False,
        "scale": "ratio",
        "params": {"emptyAsNull": True},
    }


def formbased(layer_id, columns, column_order):
    return {
        "formBased": {
            "layers": {
                layer_id: {
                    "columns": columns,
                    "columnOrder": column_order,
                    "incompleteColumns": {},
                    "sampling": 1,
                }
            }
        }
    }


def lens(
    id_, title, vis_type, visualization, datasource_states, dv_id, layer_id, query=""
):
    attrs = {
        "title": title,
        "description": "",
        "visualizationType": vis_type,
        "state": {
            "visualization": visualization,
            "query": {"query": query, "language": "kuery"},
            "filters": [],
            "datasourceStates": datasource_states,
            "internalReferences": [],
            "adHocDataViews": {},
        },
    }
    refs = [
        {
            "type": "index-pattern",
            "id": dv_id,
            "name": f"indexpattern-datasource-layer-{layer_id}",
        }
    ]
    return so("lens", id_, attrs, refs)


# --------------------------------------------------------------------------
# data views
# --------------------------------------------------------------------------
def data_views():
    return [
        so(
            "index-pattern",
            DV_EVENTS,
            {
                "title": "workflow-events-*",
                "name": "Workflow events",
                "timeFieldName": "@timestamp",
            },
            type_migration="8.0.0",
        ),
        so(
            "index-pattern",
            DV_DIAG,
            {
                "title": "workflow-diag-*",
                "name": "Workflow diagnostics",
                "timeFieldName": "@timestamp",
            },
            type_migration="8.0.0",
        ),
        # The pegasus-monitord plugin path (deploy/MONITORD-PLUGIN.md): same
        # event schema as workflow-events-*, separate family so the plugin
        # stream and the polling stream compare side by side.
        so(
            "index-pattern",
            DV_MONITORD,
            {
                "title": "monitord-events-*",
                "name": "Monitord plugin events",
                "timeFieldName": "@timestamp",
            },
            type_migration="8.0.0",
        ),
        # No time field: 'current state' must show every job regardless of the
        # dashboard time picker (the transform keeps one doc per job).
        so(
            "index-pattern",
            DV_JOBSTATE,
            {
                "title": "workflow-jobstate-current",
                "name": "Workflow job state (current)",
            },
            type_migration="8.0.0",
        ),
    ]


# --------------------------------------------------------------------------
# Lens panels
# --------------------------------------------------------------------------
def panel_burnup():
    lid = "burnup"
    cols = {
        "x": col_date(),
        "brk": col_terms("state", 12, "cnt", label="state"),
        "csum": col_cumulative("cnt", label="Cumulative transitions"),
        "cnt": col_count(),
    }
    vis = {
        "legend": {"isVisible": True, "position": "right"},
        "valueLabels": "hide",
        "fittingFunction": "Linear",
        "preferredSeriesType": "area",
        "axisTitlesVisibilitySettings": {"x": False, "yLeft": True, "yRight": True},
        "tickLabelsVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
        "gridlinesVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
        "labelsOrientation": {"x": 0, "yLeft": 0, "yRight": 0},
        "layers": [
            {
                "layerId": lid,
                "layerType": "data",
                "seriesType": "area",
                "xAccessor": "x",
                "splitAccessor": "brk",
                "accessors": ["csum"],
                "position": "top",
                "showGridlines": False,
            }
        ],
    }
    return lens(
        "wf-burnup",
        "Progress — cumulative job-state transitions",
        "lnsXY",
        vis,
        formbased(lid, cols, ["x", "brk", "csum", "cnt"]),
        DV_EVENTS,
        lid,
        query='event_type : "job_state"',
    )


def panel_pool():
    lid = "pool"
    cols = {
        "x": col_date(),
        "total": col_max("pool.total_cpus", "Total CPUs"),
        "idle": col_max("pool.idle_cpus", "Idle CPUs"),
    }
    vis = {
        "legend": {"isVisible": True, "position": "right"},
        "valueLabels": "hide",
        "fittingFunction": "Linear",
        "preferredSeriesType": "line",
        "axisTitlesVisibilitySettings": {"x": False, "yLeft": True, "yRight": True},
        "tickLabelsVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
        "gridlinesVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
        "labelsOrientation": {"x": 0, "yLeft": 0, "yRight": 0},
        "layers": [
            {
                "layerId": lid,
                "layerType": "data",
                "seriesType": "line",
                "xAccessor": "x",
                "accessors": ["total", "idle"],
                "position": "top",
                "showGridlines": False,
            }
        ],
    }
    return lens(
        "wf-pool-cpus",
        "Pool capacity — total vs idle CPUs",
        "lnsXY",
        vis,
        formbased(lid, cols, ["x", "total", "idle"]),
        DV_EVENTS,
        lid,
        query='event_type : "pool_status"',
    )


def panel_diag_activity():
    lid = "diag"
    cols = {
        "x": col_date(),
        "brk": col_terms("event_type", 10, "cnt", label="event_type"),
        "cnt": col_count("Diagnostic events"),
    }
    vis = {
        "legend": {"isVisible": True, "position": "right"},
        "valueLabels": "hide",
        "fittingFunction": "None",
        "preferredSeriesType": "bar_stacked",
        "axisTitlesVisibilitySettings": {"x": False, "yLeft": True, "yRight": True},
        "tickLabelsVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
        "gridlinesVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
        "labelsOrientation": {"x": 0, "yLeft": 0, "yRight": 0},
        "layers": [
            {
                "layerId": lid,
                "layerType": "data",
                "seriesType": "bar_stacked",
                "xAccessor": "x",
                "splitAccessor": "brk",
                "accessors": ["cnt"],
                "position": "top",
                "showGridlines": False,
            }
        ],
    }
    return lens(
        "wf-diag-activity",
        "Diagnostics activity (stalls / holds / failures)",
        "lnsXY",
        vis,
        formbased(lid, cols, ["x", "brk", "cnt"]),
        DV_DIAG,
        lid,
    )


def panel_current_donut():
    lid = "now"
    cols = {
        "brk": col_terms("current.state", 12, "cnt", label="current state"),
        "cnt": col_count("Jobs"),
    }
    vis = {
        "shape": "donut",
        "layers": [
            {
                "layerId": lid,
                "layerType": "data",
                "primaryGroups": ["brk"],
                "metrics": ["cnt"],
                "numberDisplay": "value",
                "categoryDisplay": "default",
                "legendDisplay": "default",
                "nestedLegend": False,
            }
        ],
    }
    return lens(
        "wf-current-donut",
        "Current job state (now)",
        "lnsPie",
        vis,
        formbased(lid, cols, ["brk", "cnt"]),
        DV_JOBSTATE,
        lid,
    )


def panel_current_table():
    lid = "nowt"
    cols = {
        "brk": col_terms("current.type_desc", 20, "cnt", label="job type"),
        "st": col_terms("current.state", 1, "cnt", label="state"),
        "cnt": col_count("Jobs"),
    }
    vis = {
        "layerId": lid,
        "layerType": "data",
        "columns": [
            {"columnId": "brk", "isTransposed": False},
            {"columnId": "st", "isTransposed": False},
            {"columnId": "cnt", "isTransposed": False, "alignment": "right"},
        ],
    }
    return lens(
        "wf-current-table",
        "Jobs by type and current state",
        "lnsDatatable",
        vis,
        formbased(lid, cols, ["brk", "st", "cnt"]),
        DV_JOBSTATE,
        lid,
    )


def panel_throughput():
    lid = "thru"
    cols = {
        "x": col_date(),
        "brk": col_terms("wf_uuid", 10, "cnt", label="workflow"),
        "cnt": col_count("Completed jobs"),
    }
    vis = {
        "legend": {"isVisible": True, "position": "right"},
        "valueLabels": "hide",
        "fittingFunction": "None",
        "preferredSeriesType": "bar_stacked",
        "axisTitlesVisibilitySettings": {"x": False, "yLeft": True, "yRight": True},
        "tickLabelsVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
        "gridlinesVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
        "labelsOrientation": {"x": 0, "yLeft": 0, "yRight": 0},
        "layers": [
            {
                "layerId": lid,
                "layerType": "data",
                "seriesType": "bar_stacked",
                "xAccessor": "x",
                "splitAccessor": "brk",
                "accessors": ["cnt"],
                "position": "top",
                "showGridlines": False,
            }
        ],
    }
    return lens(
        "wf-throughput",
        "Job completions over time (all workflows)",
        "lnsXY",
        vis,
        formbased(lid, cols, ["x", "brk", "cnt"]),
        DV_EVENTS,
        lid,
        query='event_type : "job_state" and (state : "JOB_SUCCESS" '
        'or state : "POST_SCRIPT_SUCCESS")',
    )


# --------------------------------------------------------------------------
# saved searches (Discover)
# --------------------------------------------------------------------------
def search(id_, title, dv_id, columns, query=""):
    ssj = {
        "query": {"query": query, "language": "kuery"},
        "filter": [],
        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
    }
    attrs = {
        "title": title,
        "description": "",
        "columns": columns,
        "sort": [["@timestamp", "desc"]],
        "grid": {},
        "hideChart": False,
        "isTextBasedQuery": False,
        "timeRestore": False,
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(ssj)},
    }
    refs = [
        {
            "type": "index-pattern",
            "id": dv_id,
            "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
        }
    ]
    return so("search", id_, attrs, refs)


def saved_searches():
    return [
        search(
            "wf-feed",
            "Workflow event feed",
            DV_EVENTS,
            ["event_type", "exec_job_id", "state", "exitcode", "type_desc"],
        ),
        search(
            "wf-diag-feed",
            "Diagnostics feed",
            DV_DIAG,
            ["event_type", "stall_type", "job_name", "summary", "reason"],
        ),
        search(
            "wf-wfstate-feed",
            "Workflow-level state feed",
            DV_EVENTS,
            ["event_type", "wf_uuid", "dax_label", "user", "state", "status"],
            query='event_type : "workflow_start" or event_type : "workflow_state" '
            'or event_type : "workflow_end"',
        ),
    ]


# --------------------------------------------------------------------------
# dashboards
# --------------------------------------------------------------------------
def grid(x, y, w, h, i):
    return {"x": x, "y": y, "w": w, "h": h, "i": i}


def dash_panel(panel_index, ref_type, w, h, x, y, title=None):
    """One dashboard panel + its reference entry (by-reference embeddable)."""
    ref_name = f"panel_{panel_index}"
    panel = {
        "version": CORE,
        "type": ref_type,
        "gridData": grid(x, y, w, h, panel_index),
        "panelIndex": panel_index,
        "embeddableConfig": {"enhancements": {}},
        "panelRefName": ref_name,
    }
    if title:
        panel["title"] = title
    return panel, ref_name


def options_control(control_id, dv_id, field, title, order, width="medium"):
    return control_id, {
        "type": "optionsListControl",
        "order": order,
        "grow": True,
        "width": width,
        "explicitInput": {
            "id": control_id,
            "dataViewId": dv_id,
            "fieldName": field,
            "title": title,
            "selectedOptions": [],
            "enhancements": {},
        },
    }


def control_group(controls):
    panels = {}
    refs = []
    for cid, cfg in controls:
        panels[cid] = cfg
        refs.append(
            {
                "type": "index-pattern",
                "id": cfg["explicitInput"]["dataViewId"],
                "name": f"controlGroup_{cid}:optionsListDataView",
            }
        )
    cg = {
        "controlStyle": "oneLine",
        "chainingSystem": "HIERARCHICAL",
        "showApplySelections": False,
        "ignoreParentSettingsJSON": json.dumps(
            {
                "ignoreFilters": False,
                "ignoreQuery": False,
                "ignoreTimerange": False,
                "ignoreValidations": False,
            }
        ),
        "panelsJSON": json.dumps(panels),
    }
    return cg, refs


def dashboard(id_, title, description, panel_specs, controls=None):
    """panel_specs: list of (panel_index, target_id, ref_type, w, h, x, y, title)."""
    panels = []
    refs = []
    for pidx, target_id, ref_type, w, h, x, y, ptitle in panel_specs:
        panel, ref_name = dash_panel(pidx, ref_type, w, h, x, y, ptitle)
        panels.append(panel)
        refs.append({"type": ref_type, "id": target_id, "name": f"{pidx}:{ref_name}"})
    attrs = {
        "title": title,
        "description": description,
        "panelsJSON": json.dumps(panels),
        "optionsJSON": json.dumps(
            {
                "useMargins": True,
                "syncColors": False,
                "syncCursor": True,
                "syncTooltips": False,
                "hidePanelTitles": False,
            }
        ),
        "timeRestore": False,
        "version": 3,
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps(
                {"query": {"query": "", "language": "kuery"}, "filter": []}
            )
        },
    }
    if controls:
        cg, cg_refs = control_group(controls)
        attrs["controlGroupInput"] = cg
        refs.extend(cg_refs)
    return so("dashboard", id_, attrs, refs)


def dashboards():
    # ---- Drilldown: one workflow, deep ------------------------------------
    controls = [
        options_control(
            "ctrl-wf", DV_EVENTS, "wf_uuid", "Workflow (wf_uuid)", 0, width="large"
        ),
        options_control("ctrl-label", DV_EVENTS, "dax_label", "Label", 1),
    ]
    drill_specs = [
        # pidx, target, type, w, h, x, y, title
        (
            "p1",
            "wf-burnup",
            "lens",
            24,
            13,
            0,
            0,
            "Progress — cumulative job-state transitions",
        ),
        ("p2", "wf-current-donut", "lens", 8, 12, 0, 13, "Current job state (now)"),
        (
            "p3",
            "wf-pool-cpus",
            "lens",
            16,
            12,
            8,
            13,
            "Pool capacity — total vs idle CPUs",
        ),
        (
            "p4",
            "wf-diag-activity",
            "lens",
            12,
            11,
            0,
            25,
            "Diagnostics activity (stalls / holds / failures)",
        ),
        (
            "p5",
            "wf-current-table",
            "lens",
            12,
            11,
            12,
            25,
            "Jobs by type and current state",
        ),
        ("p6", "wf-feed", "search", 24, 13, 0, 36, "Workflow event feed"),
        ("p7", "wf-diag-feed", "search", 24, 11, 0, 49, "Diagnostics feed"),
    ]
    drill = dashboard(
        "wf-drilldown",
        "Pegasus workflow — drilldown",
        "Single-workflow view. Pick a wf_uuid in the control bar. Burn-up is a "
        "cumulative count of job_state transitions; current-state panels read the "
        "workflow-jobstate-current transform (latest state per job).",
        drill_specs,
        controls=controls,
    )

    # ---- Overview: the fleet ----------------------------------------------
    ov_specs = [
        (
            "o1",
            "wf-throughput",
            "lens",
            16,
            12,
            0,
            0,
            "Job completions over time (all workflows)",
        ),
        (
            "o2",
            "wf-diag-activity",
            "lens",
            8,
            12,
            16,
            0,
            "Diagnostics activity (all workflows)",
        ),
        ("o3", "wf-wfstate-feed", "search", 24, 14, 0, 12, "Workflow-level state feed"),
    ]
    overview = dashboard(
        "wf-overview",
        "Pegasus workflows — fleet overview",
        "Cross-workflow throughput and health. Drill into a single workflow via "
        "the 'Pegasus workflow — drilldown' dashboard.",
        ov_specs,
    )
    return [drill, overview]


# --------------------------------------------------------------------------
def write_ndjson(path, objects):
    path.write_text(
        "".join(json.dumps(o, separators=(",", ":")) + "\n" for o in objects)
    )
    print(f"  wrote {len(objects):2d} objects -> {path.relative_to(HERE)}")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    print("Generating Kibana saved objects (target: Kibana %s)" % CORE)
    write_ndjson(OUT / "data-views.ndjson", data_views())
    panels = [
        panel_burnup(),
        panel_pool(),
        panel_diag_activity(),
        panel_current_donut(),
        panel_current_table(),
        panel_throughput(),
    ]
    dashboards_bundle = panels + saved_searches() + dashboards()
    write_ndjson(OUT / "dashboards.ndjson", dashboards_bundle)
    print("Done.")


if __name__ == "__main__":
    main()
