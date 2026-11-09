#!/usr/bin/env python3
"""Provision a FABRIC slice that runs a Pegasus + HTCondor pool (the producer).

This is the *producer* side of the deploy/ pipeline (see deploy/README.md and
deploy/PEGASUS-HTCONDOR.md): a real HTCondor pool with Pegasus on top, plus
workflow-monitor (which turns pegasus-monitord output into the
workflow-events.jsonl / diagnostics-events.jsonl the ES index templates expect)
and Vector, which ships those events to a separate Elasticsearch slice over a
private, routed FABNetv4 network.

It is the peer that the elastic-stack recipe points at: the ES provisioner's
``--peer-slice pegasus-htcondor`` discovers this slice's ``fabnet`` interfaces
(the network name MUST match, see NET_NAME) and adds the reverse route +
``/etc/hosts`` entry on each of these nodes.

What it does
------------
  build      create the slice: a submit/central-manager node + ``--workers N``
             execute nodes, each with a NIC_Basic on ONE shared FABNetv4 network
  route      install the inter-slice route (10.128.0.0/10 via the node's own
             FABNet gateway) plus a systemd one-shot so it survives reboot --
             never touching the management default route
  bootstrap  (--bootstrap) upload + run bootstrap_pegasus_node.sh on every node:
             install HTCondor (role-aware), share a pool signing key, install
             Pegasus + Apptainer everywhere, and on the submit node install
             workflow-monitor + Vector (Vector stays stopped until --wire-es)
  apptainer  (--install-apptainer) retrofit just the container runtime onto an
             existing slice (idempotent, no condor restart -- safe on a live pool)
  plugin     (--install-monitord-plugin) install the monitord plugin stack on
             the submit node: the wfmonitor adapter + ~/.pegasusrc enablement +
             the monitord-events Vector stream (deploy/MONITORD-PLUGIN.md).
             On Pegasus >= 6.0 the plugin host is native (the 3-file overlay is
             auto-skipped); on 5.x the overlay is applied first. Per-run
             opt-in via --enable-monitord-plugin on the run actions
  monitor    (--install-pegasus-monitor --pegasus-monitor-src DIR) overlay the
             pegasus-monitor CLI from a local pegasus checkout onto the submit
             node's apt Pegasus (pure Python; independent of the plugin overlay)
  wire-es    (--wire-es) upload the ES slice's CA, finalize the Vector sink, and
             start Vector on the submit node (--es-ip also writes /etc/hosts)
  studio     (--install-studio) run the PegasusAI Studio webapp container on
             the submit node: it plans with the image's own Pegasus (the
             development/6.0 channel this pool tracks) through the HOST schedd,
             authenticating with an IDTOKEN
  example    (--run-example) plan+submit a tiny diamond workflow under
             workflow-monitor as an end-to-end smoke test
  workflow   (--run-workflow URL|DIR) clone/use a real workflow, optionally
             generate it (--generate-cmd), plan+submit it (forcing condorio so
             the no-shared-fs pool works), and start workflow-monitor

Usage
-----
  # 1. Build the pool and bring HTCondor/Pegasus/Vector up (Vector stays stopped).
  python provision_pegasus_slice.py --name pegasus-htcondor --site UCSD \
      --workers 2 --bootstrap

  # 2. (then build the ES slice with --peer-slice pegasus-htcondor, which routes
  #     these nodes to it and downloads the CA to deploy/fabric/ca.crt)

  # 3. Point Vector at the ES slice and start it.
  python provision_pegasus_slice.py --name pegasus-htcondor --wire-es

  # 4. Smoke-test the whole chain.
  python provision_pegasus_slice.py --name pegasus-htcondor --run-example

  # Re-apply data-plane IPs + route after a reboot, or tear down:
  python provision_pegasus_slice.py --name pegasus-htcondor --reconfigure
  python provision_pegasus_slice.py --name pegasus-htcondor --destroy

References: docs/guides/{slices/create-slice,networking/l3-networks,
networking/network-troubleshooting,nodes/post-boot-tasks}.md in the FABRIC docs;
~/.claude/references/{htcondor,pegasus}.md for HTCondor pool roles and Pegasus.
"""

from __future__ import annotations

import argparse
import os
import secrets
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

# The whole FABNetv4 address space. A route to this /10 via the node's own
# FABNet gateway lets it reach any OTHER slice's FABNetv4 subnet in the same
# project (docs/guides/networking/network-troubleshooting.md, "Inter-slice
# communication") -- here, the Elasticsearch slice. The pool's own /24 stays a
# directly-connected route, so CM<->worker traffic needs nothing extra.
FABNETV4_SUPERNET = "10.128.0.0/10"

# MUST match NET_NAME in provision_es_slice.py: the ES recipe's --peer-slice
# finds this network by name to wire the reverse route + /etc/hosts.
NET_NAME = "fabnet"
NIC_NAME = "nic1"  # NIC_Basic component name (same on every node)

DEFAULT_ES_HOSTNAME = "workflow-monitor-es"  # matches the ES cert SAN + /etc/hosts

# Local layout (relative to repo root, two levels up from this file).
REPO_ROOT = Path(__file__).resolve().parents[2]
PEG_DIR = REPO_ROOT / "deploy" / "pegasus-htcondor"
BOOTSTRAP_SH = PEG_DIR / "bootstrap_pegasus_node.sh"
APPTAINER_SH = PEG_DIR / "install_apptainer.sh"
CONDOR_DIR = PEG_DIR / "condor"
VECTOR_TMPL = PEG_DIR / "vector" / "vector.toml.tmpl"
VECTOR_SERVICE = REPO_ROOT / "deploy" / "vector" / "vector.service"
EXAMPLE_WF = PEG_DIR / "examples" / "diamond.py"
OVERLAY_SH = PEG_DIR / "overlay_monitord_plugin.sh"
PEGASUS_MONITOR_SH = PEG_DIR / "overlay_pegasus_monitor.sh"
PEGASUS_WHEEL_SH = PEG_DIR / "install_pegasus_wheel.sh"
PEGASUS_REPO_URL = "https://github.com/pegasus-isi/pegasus.git"
POOL_PW_FILE = PEG_DIR / ".pool-password"
# Same path the ES provisioner downloads its CA to (this file is in deploy/fabric/).
CA_DEFAULT = Path(__file__).resolve().parent / "ca.crt"

# Remote layout on each node (home-relative).
REMOTE_DIR = "pegasus-htcondor"

CONDOR_FILES = [
    "00-pool-common.conf",
    "10-central-manager.conf",
    "20-execute.conf",
]

# --- monitord plugin stack (deploy/MONITORD-PLUGIN.md) ----------------------
# The pegasus-monitord entry-point plugin system is pure Python: these three
# files overlaid onto the apt-installed tree ARE the install. Pinned to the
# tip of pegasus-isi/pegasus branch monitord-plugin-system (entry-point
# system + cross-thread payload race fix + the tick() hook + plugin-host
# edge-case hardening: reserved synthetic endpoint name, multiplex-aware
# replay/recovery DB-purge guard, worker-lifecycle cleanup). Current pin is
# the 2026-08-04 merge of main -- the ref the live slices actually carry; the
# three overlay files are byte-identical to the previous pins 317a020a3 and
# e7da14941 (the 50-commit delta from 317a020a3 is main-side only: monitoring/
# workflow.py, docs, java -- it never touches the overlay surface).
# On the development/6.0 apt channel the plugin system is IN the deb:
# install_monitord_plugin detects the native host and skips the overlay, so
# this ref only matters for 5.x trees.
MONITORD_PLUGIN_REF = "da1db847c685cb6c1f34d7c8de8c7af26f5c1997"
MONITORD_PLUGIN_FILES = [  # repo-relative under packages/pegasus-python/src/Pegasus/
    "monitoring/plugin.py",
    "monitoring/event_output.py",
    "cli/pegasus-monitord.py",
]
# The workflow-monitor build that registers the wfmonitor plugin in the
# pegasus.monitord.plugins entry-point group. Separate from
# --workflow-monitor-spec so the fresh-bootstrap default stays on main.
MONITORD_ADAPTER_SPEC = (
    "git+https://github.com/pegasus-isi/workflow-monitor.git@monitord-plugin-adapter"
)
MONITORD_MANIFEST = (
    "/usr/lib/pegasus/python/Pegasus/monitoring/.monitord-plugin-manifest.json"
)
# The standalone wfevents plugin (separate repo, own release cadence). It rides
# the SAME plugin host as wfmonitor, so the 3-file overlay must already be in
# place -- but it needs no Vector change, because Vector does not tail
# wfevents.jsonl (consume it with workflow-monitor --remote/--replay).
WFEVENTS_PLUGIN_SPEC = (
    "git+https://github.com/mjstealey/pegasus-monitord-plugins.git"
    "#subdirectory=plugins/wfevents"
)
# pip distribution name (NOT "wfevents" -- the import package is monitord_wfevents).
WFEVENTS_DIST = "pegasus-monitord-plugin-wfevents"

# PegasusAI Studio webapp (--install-studio). The :main image bundles Pegasus
# from the development/6.0 apt channel — the reason the pool tracks that same
# channel: the container plans, but DAGMan + jobs run under the HOST schedd, so
# host/container Pegasus skew aborts DAGs (5.1.2 pegasus-exitcode exits 2 on
# the 6.0 planner's -R/-M POST-script flags).
STUDIO_IMAGE = "ghcr.io/pegasus-isi/studio-webapp:main"


def _load_fablib():
    """Import FABlib with a friendly error if the environment isn't set up."""
    try:
        from fabrictestbed_extensions.fablib.fablib import (
            FablibManager as fablib_manager,
        )
    except ImportError:
        sys.exit(
            "error: fabrictestbed_extensions not importable.\n"
            "Run this from a FABRIC JupyterHub, or `pip install "
            "fabrictestbed-extensions` with your tokens / fabric_rc configured."
        )
    return fablib_manager()


# ---------------------------------------------------------------------------
# Networking helpers (shared shape with provision_es_slice.py)
# ---------------------------------------------------------------------------


def _node_fabnet(slice_obj, node_name=None):
    """Return (node, data-plane IP, gateway) for a node's FABNet interface.

    If node_name is None, use the first node that has a FABNet interface.
    Returns (node, ip, gateway) with ip/gateway possibly None if unconfigured.
    """
    nodes = [slice_obj.get_node(node_name)] if node_name else slice_obj.get_nodes()
    for node in nodes:
        try:
            iface = node.get_interface(network_name=NET_NAME)
        except Exception:
            iface = None
        if iface is None:
            continue
        # get_ip_addr() returns an ipaddress.IPv4Address once a FABNetv4 IP is
        # assigned (or a str link-local when unconfigured). Normalize to a dotted
        # -quad str so the IPv4 guard, _fabnet_cidr(), and CONDOR_HOST/NODE_IP all
        # get a plain string.
        ip = iface.get_ip_addr()
        if ip is not None:
            ip = str(ip)
        gateway = None
        try:
            gw = slice_obj.get_network(NET_NAME).get_gateway()
            gateway = str(gw) if gw is not None else None
        except Exception:
            pass
        return node, ip, gateway
    return (nodes[0] if nodes else None), None, None


def _fabnet_cidr(slice_obj, fallback_ip):
    """Return the pool's FABNet subnet (e.g. 10.128.5.0/24) for ALLOW rules.

    Scopes HTCondor's host-based authorization to the pool's own subnet rather
    than the whole routed /10. Falls back to deriving a /24 from the submit IP.
    """
    try:
        subnet = slice_obj.get_network(NET_NAME).get_subnet()
        if subnet:
            return str(subnet)
    except Exception:
        pass
    if fallback_ip and fallback_ip.count(".") == 3:
        return ".".join(fallback_ip.split(".")[:3]) + ".0/24"
    return FABNETV4_SUPERNET


def _install_route(node, gateway):
    """Add the inter-slice route now and persist it via a systemd one-shot.

    Adds 10.128.0.0/10 via the node's own FABNet gateway so Vector (on the
    submit node) can reach the ES slice's FABNet subnet. Idempotent. Never
    touches the default route (that carries SSH; replacing it is unrecoverable
    -- docs/guides/networking/network-troubleshooting.md).
    """
    if not gateway:
        print("  ! no FABNet gateway discovered; skipping route install")
        return
    node.execute(f"sudo ip route add {FABNETV4_SUPERNET} via {gateway} || true")
    unit = (
        "[Unit]\n"
        "Description=pegasus-htcondor inter-slice FABNet route\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n\n"
        "[Service]\n"
        "Type=oneshot\n"
        "RemainAfterExit=yes\n"
        f"ExecStart=/sbin/ip route replace {FABNETV4_SUPERNET} via {gateway}\n\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
    )
    node.execute(
        "sudo tee /etc/systemd/system/fabnet-interslice-route.service "
        f">/dev/null <<'UNIT'\n{unit}UNIT"
    )
    node.execute(
        "sudo systemctl daemon-reload && "
        "sudo systemctl enable fabnet-interslice-route.service"
    )
    print(f"  + route {FABNETV4_SUPERNET} via {gateway} (active + persistent)")


# ---------------------------------------------------------------------------
# Build / submit
# ---------------------------------------------------------------------------


def _worker_names(args):
    return [f"{args.worker_prefix}{i}" for i in range(1, args.workers + 1)]


def build(fablib, args):
    print(f"==> creating pool slice '{args.name}' at site {args.site}")
    slice_obj = fablib.new_slice(name=args.name)
    ifaces = []

    submit = slice_obj.add_node(name=args.submit_node, site=args.site)
    submit.set_capacities(cores=args.cores, ram=args.ram, disk=args.disk)
    submit.set_image(args.image)
    submit_iface = submit.add_component(
        model="NIC_Basic", name=NIC_NAME
    ).get_interfaces()[0]
    # "auto" mode makes Interface.config() allocate + apply a FABNetv4 IPv4 from
    # the network's pool. The default mode is "manual", under which config() only
    # brings the link up and assigns NO address (get_ip_addr() then returns the
    # IPv6 link-local and the inter-slice route has no valid IPv4 nexthop).
    submit_iface.set_mode("auto")
    ifaces.append(submit_iface)
    print(f"  + submit/CM node '{args.submit_node}' ({args.cores}c/{args.ram}G)")

    worker_site = args.worker_site or args.site
    for wname in _worker_names(args):
        w = slice_obj.add_node(name=wname, site=worker_site)
        w.set_capacities(
            cores=args.worker_cores, ram=args.worker_ram, disk=args.worker_disk
        )
        w.set_image(args.image)
        w_iface = w.add_component(model="NIC_Basic", name=NIC_NAME).get_interfaces()[0]
        w_iface.set_mode("auto")
        ifaces.append(w_iface)
        print(f"  + execute node '{wname}' ({args.worker_cores}c/{args.worker_ram}G)")

    # ONE FABNetv4 network for the whole pool: same subnet => CM<->worker is
    # directly connected (no inter-pool route needed).
    slice_obj.add_l3network(name=NET_NAME, interfaces=ifaces, type="IPv4")

    print("  submitting (blocks until provisioned + post-boot config)...")
    slice_obj.submit()
    return slice_obj


# ---------------------------------------------------------------------------
# Pool password (shared HTCondor signing key)
# ---------------------------------------------------------------------------


def _pool_password(args):
    """Resolve one pool signing key for every node, persisting it locally.

    Precedence: --pool-password / $CONDOR_POOL_PASSWORD ▸ saved
    deploy/pegasus-htcondor/.pool-password ▸ freshly generated. Saved (0600,
    gitignored) so --reconfigure and later worker additions reuse the same key.
    """
    pw = args.pool_password
    if not pw and POOL_PW_FILE.exists():
        pw = POOL_PW_FILE.read_text().strip()
    if not pw:
        pw = secrets.token_urlsafe(24)
    try:
        POOL_PW_FILE.write_text(pw + "\n")
        os.chmod(POOL_PW_FILE, 0o600)
    except Exception as exc:  # noqa: BLE001
        print(f"  ! could not persist pool password to {POOL_PW_FILE}: {exc}")
    return pw


# ---------------------------------------------------------------------------
# Bootstrap the pool
# ---------------------------------------------------------------------------


def _upload_assets(node, *, with_submit_assets):
    """Upload the bootstrap script + condor drop-ins (+ submit-only assets)."""
    if not BOOTSTRAP_SH.exists():
        sys.exit(f"error: {BOOTSTRAP_SH} not found")
    node.execute(
        f"mkdir -p ~/{REMOTE_DIR}/condor ~/{REMOTE_DIR}/vector ~/{REMOTE_DIR}/examples"
    )
    node.upload_file(str(BOOTSTRAP_SH), f"{REMOTE_DIR}/bootstrap_pegasus_node.sh")
    # Container runtime install, shared with --install-apptainer (every node).
    if not APPTAINER_SH.exists():
        sys.exit(f"error: {APPTAINER_SH} not found")
    node.upload_file(str(APPTAINER_SH), f"{REMOTE_DIR}/install_apptainer.sh")
    for fname in CONDOR_FILES:
        local = CONDOR_DIR / fname
        if not local.exists():
            sys.exit(f"error: missing {local}")
        node.upload_file(str(local), f"{REMOTE_DIR}/condor/{fname}")
    if with_submit_assets:
        for local, remote in [
            (VECTOR_TMPL, f"{REMOTE_DIR}/vector/vector.toml.tmpl"),
            (VECTOR_SERVICE, f"{REMOTE_DIR}/vector/vector.service"),
            (EXAMPLE_WF, f"{REMOTE_DIR}/examples/diamond.py"),
        ]:
            if not local.exists():
                sys.exit(f"error: missing {local}")
            node.upload_file(str(local), remote)


def _bootstrap_node(node, role, node_ip, submit_ip, fabnet_cidr, pw, args):
    print(f"==> bootstrapping {node.get_name()} (role={role}, ip={node_ip})")
    _upload_assets(node, with_submit_assets=(role == "cm"))

    env = (
        f"ROLE={role} "
        f"CONDOR_HOST={submit_ip} "
        f"NODE_IP={node_ip} "
        f"FABNET_CIDR={fabnet_cidr} "
        f"POOL_PASSWORD={pw} "
        f"PEGASUS_CHANNEL={args.pegasus_channel} "
        f"PEG_DIR=$HOME/{REMOTE_DIR}"
    )
    if args.pegasus_version:
        env += f" PEGASUS_VERSION={args.pegasus_version}"
    if args.htcondor_version:
        env += f" HTCONDOR_VERSION={args.htcondor_version}"
    if role == "cm":
        env += (
            f" RUNS_DIR={args.runs_dir}"
            f" ES_HOST={args.es_host}"
            f" WORKFLOW_MONITOR_SPEC={args.workflow_monitor_spec!r}"
            " INSTALL_VECTOR=1"
        )
    stdout, stderr = node.execute(
        f"chmod +x ~/{REMOTE_DIR}/bootstrap_pegasus_node.sh && "
        f"{env} bash ~/{REMOTE_DIR}/bootstrap_pegasus_node.sh"
    )
    if stderr and stderr.strip():
        print(f"  [{node.get_name()} bootstrap stderr]\n" + stderr)
    # fablib's execute() does not surface the remote exit code, so grep for the
    # script's completion sentinel — a set -e death (e.g. an apt failure)
    # otherwise reads as success and leaves a half-bootstrapped node.
    if "BOOTSTRAP_COMPLETE" not in (stdout or ""):
        sys.exit(
            f"error: bootstrap did NOT complete on {node.get_name()} (no "
            "BOOTSTRAP_COMPLETE sentinel; scroll up for the failing step). "
            "Fix the cause and re-run with --rebootstrap — the script is "
            "idempotent."
        )


def bootstrap_pool(slice_obj, submit_ip, fabnet_cidr, args):
    print("==> bootstrapping the HTCondor pool")
    pw = _pool_password(args)
    for node in slice_obj.get_nodes():
        name = node.get_name()
        _, node_ip, _ = _node_fabnet(slice_obj, name)
        role = "cm" if name == args.submit_node else "execute"
        _bootstrap_node(node, role, node_ip, submit_ip, fabnet_cidr, pw, args)
    print(
        "  pool bootstrap complete; on the submit node `condor_status` should "
        "list the execute nodes once they advertise."
    )


def install_apptainer(slice_obj, args):
    """Retrofit the container runtime onto every node of an existing slice.

    For pools bootstrapped before Apptainer was part of the bring-up. Uploads and
    runs install_apptainer.sh (the same script the bootstrap uses) on each node:
    idempotent, no condor restart, safe on a running pool. After this, the worker
    nodes can run containerized workflows (e.g. via --run-workflow).
    """
    if not APPTAINER_SH.exists():
        sys.exit(f"error: {APPTAINER_SH} not found")
    print("==> installing Apptainer on every node of the existing slice")
    for node in slice_obj.get_nodes():
        name = node.get_name()
        node.execute(f"mkdir -p ~/{REMOTE_DIR}")
        node.upload_file(str(APPTAINER_SH), f"{REMOTE_DIR}/install_apptainer.sh")
        print(f"  -> {name}")
        out, _ = node.execute(
            f"chmod +x ~/{REMOTE_DIR}/install_apptainer.sh && "
            f"INSTALL_APPTAINER=1 bash ~/{REMOTE_DIR}/install_apptainer.sh 2>&1 | tail -15"
        )
        print(out)
    print("  done. Verify on a worker:  apptainer --version")


# ---------------------------------------------------------------------------
# Monitord plugin stack (retrofit; see deploy/MONITORD-PLUGIN.md)
# ---------------------------------------------------------------------------


def _git_desc(path):
    """`git describe --always --dirty` for provenance, or 'unknown'."""
    try:
        return subprocess.run(
            ["git", "-C", str(path), "describe", "--always", "--dirty"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except Exception:  # noqa: BLE001
        return "unknown"


def _upload_adapter_tree(submit, src):
    """Tar a local workflow-monitor tree (sans VCS/venv cruft), upload, extract.

    The fast iteration loop for adapter development: the node installs exactly
    the local working tree, no push to GitHub needed. Returns the remote dir
    to hand to pip.
    """
    fd, tarball = tempfile.mkstemp(suffix=".tar.gz")
    os.close(fd)
    try:
        subprocess.run(
            [
                "tar",
                "czf",
                tarball,
                "--exclude=.git",
                "--exclude=.venv",
                "--exclude=__pycache__",
                "--exclude=dist",
                "--exclude=*.egg-info",
                "-C",
                str(src.parent),
                src.name,
            ],
            check=True,
        )
        submit.upload_file(tarball, f"{REMOTE_DIR}/workflow-monitor-src.tar.gz")
    finally:
        os.unlink(tarball)
    submit.execute(
        f"rm -rf ~/{REMOTE_DIR}/workflow-monitor-src && "
        f"mkdir -p ~/{REMOTE_DIR}/workflow-monitor-src && "
        f"tar xzf ~/{REMOTE_DIR}/workflow-monitor-src.tar.gz "
        f"-C ~/{REMOTE_DIR}/workflow-monitor-src --strip-components=1"
    )
    return f"$HOME/{REMOTE_DIR}/workflow-monitor-src"


def _update_vector_config(submit, args):
    """Re-render /etc/vector/vector.toml from the current template and restart.

    Unlike --wire-es this NEVER touches /etc/vector/.env (the live ingest
    password) or ca.crt -- it only refreshes the config (e.g. to pick up the
    monitord-events stream), preserving the ES host already wired into the
    live file. Validates the candidate before replacing the working config.
    """
    print("  re-rendering Vector config from the current template")
    submit.execute(f"mkdir -p ~/{REMOTE_DIR}/vector")
    submit.upload_file(str(VECTOR_TMPL), f"{REMOTE_DIR}/vector/vector.toml.tmpl")
    out, _ = submit.execute(
        "set -e; "
        # Keep whatever ES endpoint the live config ships to (wire_es may have
        # sed-ed an override in); fall back to --es-host on a fresh file.
        "host=$(sudo grep -om1 'https://[^:\"]*:9200' /etc/vector/vector.toml "
        "2>/dev/null | sed -e 's|https://||' -e 's|:9200||'); "
        f'[ -n "$host" ] || host={args.es_host}; '
        f'echo "  ES endpoint preserved: $host"; '
        f"sed -e 's|__RUNS_DIR__|{args.runs_dir}|g' -e \"s|__ES_HOST__|$host|g\" "
        f"-e 's|__CA_FILE__|/etc/vector/ca.crt|g' "
        f"~/{REMOTE_DIR}/vector/vector.toml.tmpl > /tmp/vector.toml.new; "
        # Validate with the env file loaded (the config references
        # ${ELASTIC_INGEST_PASSWORD}); abort BEFORE replacing a working config.
        # --no-environment: skip runtime checks (the LIVE vector holds the disk
        # -buffer locks, so a full validate would fail spuriously); config parse
        # + topology + VRL compile still run, which is what a template render
        # can break. No .env means the node was never --wire-es'd: render the
        # config but leave Vector alone (wire-es starts it later). sudo test:
        # /etc/vector is 0750 root:vector, invisible to the ssh user.
        "if ! sudo test -f /etc/vector/.env; then echo VECTOR_ENV_MISSING; "
        "elif sudo sh -c 'set -a; . /etc/vector/.env; set +a; "
        "vector validate --no-environment /tmp/vector.toml.new'; "
        "then echo VECTOR_VALIDATE_OK; else echo VECTOR_VALIDATE_FAILED; fi"
    )
    out = out or ""
    if "VECTOR_VALIDATE_OK" not in out and "VECTOR_ENV_MISSING" not in out:
        sys.exit(
            "error: new vector.toml failed validation; the live config was NOT "
            "replaced. Inspect /tmp/vector.toml.new on the submit node."
        )
    submit.execute(
        "sudo install -m 0640 -o root -g vector /tmp/vector.toml.new "
        "/etc/vector/vector.toml"
    )
    if "VECTOR_ENV_MISSING" in out:
        print(
            "  ! /etc/vector/.env not found (node not --wire-es'd yet): config "
            "rendered, Vector NOT restarted -- --wire-es will start it."
        )
        return
    submit.execute(
        "sudo systemctl restart vector && sleep 2 && "
        "systemctl --no-pager --lines=3 status vector || true"
    )
    print("  Vector restarted with the monitord-events stream.")


def install_monitord_plugin(slice_obj, args):
    """Install the monitord plugin stack onto the submit node (idempotent).

    Steps, in order:
      1. overlay the 3 pegasus-monitord plugin files onto the apt-installed
         tree (overlay_monitord_plugin.sh; provenance manifest on the node).
         SKIPPED when the apt tree already ships the plugin host natively —
         the monitord-plugin-system branch merged for 6.0, so the
         development/6.0 debs carry it; only 5.x trees need the retrofit.
      2. install the workflow-monitor build that registers the wfmonitor
         plugin (--monitord-adapter-spec: pip spec or local directory)
      3. verify the entry point loads with the same python3 the monitord
         wrapper picks (fails loudly -- monitord would silently run without
         the plugin otherwise), and enable the plugin in ~/.pegasusrc so runs
         planned OUTSIDE this script (e.g. the studio container) get it too
      4. re-render the Vector config (adds the monitord-events stream) and
         restart Vector

    Run `recipes/elastic-stack/provision.py --apply-schema` FIRST: if Vector
    restarts before the monitord-events-default alias exists, its first bulk
    write auto-creates a concrete index squatting on the alias name.
    """
    submit = slice_obj.get_node(args.submit_node)
    print(f"==> installing the monitord plugin stack on '{args.submit_node}'")

    # (1) overlay the pegasus files — unless the plugin host is native. A
    # manifest without the native marker means a previous 5.x overlay (re-run
    # it, byte-compared and idempotent); plugin.py with no manifest means the
    # deb itself ships the plugin system (Pegasus >= 6.0) — nothing to overlay.
    out, _ = submit.execute(
        f"if test -f {MONITORD_MANIFEST} "
        f"&& ! grep -q native-plugin-host {MONITORD_MANIFEST}; then echo OVERLAID; "
        "elif test -f /usr/lib/pegasus/python/Pegasus/monitoring/plugin.py; "
        "then echo NATIVE; else echo NEEDS_OVERLAY; fi",
        quiet=True,
    )
    native = "NATIVE" in (out or "")
    if native:
        ver, _ = submit.execute("pegasus-version", quiet=True)
        print(
            f"  plugin host is native in this Pegasus ({(ver or '?').strip()}); "
            "skipping the 3-file overlay"
        )
    else:
        if not OVERLAY_SH.exists():
            sys.exit(f"error: {OVERLAY_SH} not found")
        submit.execute(f"mkdir -p ~/{REMOTE_DIR}/monitord-plugin")
        submit.upload_file(str(OVERLAY_SH), f"{REMOTE_DIR}/overlay_monitord_plugin.sh")
        env = f"PEGASUS_PLUGIN_REF={args.pegasus_plugin_ref} "
        if args.pegasus_src:
            src = Path(args.pegasus_src).expanduser().resolve()
            base = src / "packages" / "pegasus-python" / "src" / "Pegasus"
            for rel in MONITORD_PLUGIN_FILES:
                local = base / rel
                if not local.exists():
                    sys.exit(f"error: {local} not found (is --pegasus-src a checkout?)")
                submit.upload_file(
                    str(local), f"{REMOTE_DIR}/monitord-plugin/{Path(rel).name}"
                )
            env += f"SOURCE_MODE=uploaded PEGASUS_SRC_DESC={_git_desc(src)!r} "
            print(f"  uploaded 3 files from {src} ({_git_desc(src)})")
        else:
            env += "SOURCE_MODE=fetch "
        out, err = submit.execute(
            f"chmod +x ~/{REMOTE_DIR}/overlay_monitord_plugin.sh && {env}"
            f"STAGE_DIR=$HOME/{REMOTE_DIR}/monitord-plugin "
            f"bash ~/{REMOTE_DIR}/overlay_monitord_plugin.sh"
        )
        print(out)
        if "OVERLAY_COMPLETE" not in (out or ""):
            if err and err.strip():
                print("  [overlay stderr]\n" + err)
            sys.exit("error: monitord plugin overlay did not complete")

    # (2) install the adapter build of workflow-monitor
    spec = args.monitord_adapter_spec
    local_dir = Path(spec).expanduser()
    if local_dir.is_dir():
        print(f"==> uploading local workflow-monitor tree {local_dir}")
        pip_spec = _upload_adapter_tree(submit, local_dir.resolve())
        adapter_desc = _git_desc(local_dir)
    else:
        pip_spec = spec
        adapter_desc = spec
    print(f"==> pip installing workflow-monitor ({adapter_desc})")
    # Two passes: the first covers dependencies if this node never had
    # workflow-monitor; the second guarantees the package itself is rebuilt at
    # the requested ref even though the version number (0.1.0) never moves.
    # PIP_BREAK_SYSTEM_PACKAGES: noble's python3 is externally managed (PEP
    # 668); pip <23 (jammy) never reads the env var, so it is harmless there.
    out, err = submit.execute(
        'export PATH="$HOME/.local/bin:$PATH" PIP_BREAK_SYSTEM_PACKAGES=1; '
        f'python3 -m pip install --user --quiet "{pip_spec}" && '
        f'python3 -m pip install --user --quiet --force-reinstall --no-deps "{pip_spec}" '
        "&& echo PIP_OK"
    )
    if "PIP_OK" not in (out or ""):
        print(out)
        if err and err.strip():
            print("  [pip stderr]\n" + err)
        sys.exit("error: workflow-monitor adapter install failed")

    # (3) entry-point check with the interpreter monitord's wrapper resolves
    # (`which python3` -> /usr/bin/python3, which sees the --user site).
    out, _ = submit.execute(
        'python3 -c "from importlib.metadata import entry_points; '
        "eps = {e.name: e for e in entry_points(group='pegasus.monitord.plugins')}; "
        "assert 'wfmonitor' in eps, sorted(eps); eps['wfmonitor'].load(); "
        "print('WFMONITOR_OK')\""
    )
    if "WFMONITOR_OK" not in (out or ""):
        print(out)
        sys.exit(
            "error: wfmonitor entry point not loadable by /usr/bin/python3 -- "
            "pegasus-monitord would run without the plugin"
        )
    print("  wfmonitor entry point loads (pegasus.monitord.plugins)")

    # Record the adapter provenance in the manifest. On a native (>= 6.0) tree
    # the overlay never ran, so create the manifest here with the marker the
    # step-1 probe greps for — that keeps a re-run of this function from
    # mistaking the native tree for an overlaid one.
    commit_q = (
        "import importlib.metadata as m, json; "
        "raw = m.distribution('workflow-monitor').read_text('direct_url.json'); "
        "d = json.loads(raw) if raw else {}; "
        "print(d.get('vcs_info', {}).get('commit_id') or d.get('url') or 'unknown')"
    )
    commit, _ = submit.execute(f'python3 -c "{commit_q}"', quiet=True)
    commit = (commit or "unknown").strip().splitlines()[-1]
    submit.execute(
        f"sudo python3 -c \"import json, os; p = '{MONITORD_MANIFEST}'; "
        "m = json.load(open(p)) if os.path.exists(p) "
        "else {'source': 'native-plugin-host'}; "
        f"m['adapter_spec'] = '{spec}'; m['adapter_resolved'] = '{commit}'; "
        "m['entry_point_ok'] = True; "
        "json.dump(m, open(p, 'w'), indent=2)\""
    )
    print(f"  adapter resolved: {commit}")

    # Plan-time enablement for runs planned outside this script: pegasus-monitord
    # folds ONLY ~/.pegasusrc (not /etc or env) into the plan-time properties
    # snapshot, so this line is what turns the wfmonitor plugin on for e.g. the
    # studio container's Run button. --enable-monitord-plugin still writes the
    # per-run properties for --run-example/--run-workflow.
    submit.execute(
        "grep -qs '^pegasus.monitord.plugins.wfmonitor.enabled' ~/.pegasusrc "
        "|| echo 'pegasus.monitord.plugins.wfmonitor.enabled = true' >> ~/.pegasusrc"
    )
    print("  ~/.pegasusrc: pegasus.monitord.plugins.wfmonitor.enabled = true")

    # (4) refresh Vector so the monitord-events stream ships
    _update_vector_config(submit, args)
    print(
        "  done. Enable per-run with --run-example/--run-workflow "
        "--enable-monitord-plugin."
    )


def install_pegasus_monitor(slice_obj, args):
    """Overlay the pegasus-monitor CLI from a local pegasus checkout (idempotent).

    pegasus-monitor (pegasus branch feature/pegasus-monitor*) is a
    self-contained pure-Python addition: Pegasus/monitor/ + cli/pegasus-monitor.py
    + a /usr/bin wrapper. `git archive` those two paths at the checkout's HEAD,
    upload the tarball, and let overlay_pegasus_monitor.sh do the byte-compared
    install, wrapper generation and provenance manifest. Independent of the
    monitord plugin overlay: touches neither pegasus-monitord nor Vector.
    """
    src = Path(args.pegasus_monitor_src or "").expanduser()
    if (
        not args.pegasus_monitor_src
        or not (
            src / "packages" / "pegasus-python" / "src" / "Pegasus" / "monitor"
        ).is_dir()
    ):
        sys.exit(
            "error: --install-pegasus-monitor needs --pegasus-monitor-src DIR, a "
            "pegasus checkout on a feature/pegasus-monitor* branch "
            "(packages/pegasus-python/src/Pegasus/monitor/ must exist)"
        )
    src = src.resolve()
    if not PEGASUS_MONITOR_SH.exists():
        sys.exit(f"error: {PEGASUS_MONITOR_SH} not found")
    ref = subprocess.run(
        ["git", "-C", str(src), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    desc = _git_desc(src)
    submit = slice_obj.get_node(args.submit_node)
    print(f"==> installing pegasus-monitor on '{args.submit_node}' from {src} ({desc})")
    if desc.endswith("-dirty"):
        print(
            "  WARNING: checkout is dirty -- git archive ships the COMMITTED tree "
            "at HEAD; uncommitted edits are not uploaded"
        )

    fd, tarball = tempfile.mkstemp(suffix=".tar.gz")
    os.close(fd)
    remote_tar = f"{REMOTE_DIR}/pegasus-monitor-{ref[:9]}.tar.gz"
    try:
        subprocess.run(
            [
                "git",
                "-C",
                str(src),
                "archive",
                "--format=tar.gz",
                "-o",
                tarball,
                "HEAD",
                "packages/pegasus-python/src/Pegasus/monitor",
                "packages/pegasus-python/src/Pegasus/cli/pegasus-monitor.py",
            ],
            check=True,
        )
        submit.execute(f"mkdir -p ~/{REMOTE_DIR}")
        submit.upload_file(tarball, remote_tar)
    finally:
        os.unlink(tarball)
    submit.upload_file(
        str(PEGASUS_MONITOR_SH), f"{REMOTE_DIR}/overlay_pegasus_monitor.sh"
    )
    out, err = submit.execute(
        f"chmod +x ~/{REMOTE_DIR}/overlay_pegasus_monitor.sh && "
        f"STAGE_TARBALL=$HOME/{remote_tar} PEGASUS_MONITOR_REF={ref} "
        f"PEGASUS_SRC_DESC={desc!r} STAGE_DIR=$HOME/{REMOTE_DIR}/pegasus-monitor "
        f"bash ~/{REMOTE_DIR}/overlay_pegasus_monitor.sh"
    )
    print(out)
    if "OVERLAY_COMPLETE" not in (out or ""):
        if err and err.strip():
            print("  [overlay stderr]\n" + err)
        sys.exit("error: pegasus-monitor overlay did not complete")
    print(
        "  done. Smoke-test on the submit node: "
        "pegasus-monitor --once <submit dir>  (deploy/PEGASUS-HTCONDOR.md)"
    )


def _run_wheel_script(node, env):
    """Run install_pegasus_wheel.sh on node with env; return (stdout, ok)."""
    node.execute(f"mkdir -p ~/{REMOTE_DIR}")
    node.upload_file(str(PEGASUS_WHEEL_SH), f"{REMOTE_DIR}/install_pegasus_wheel.sh")
    assignments = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env.items())
    out, err = node.execute(
        f"chmod +x ~/{REMOTE_DIR}/install_pegasus_wheel.sh && "
        f"{assignments} bash ~/{REMOTE_DIR}/install_pegasus_wheel.sh 2>&1"
    )
    ok = "WHEEL_INSTALL_COMPLETE" in (out or "")
    if not ok and err and err.strip():
        out = (out or "") + "\n[stderr]\n" + err
    return out or "", ok


def install_pegasus_wheel(slice_obj, args):
    """Build the pegasus-wms wheel on the submit node and install it pool-wide.

    Follows the pegasus repo's developer guide (CLAUDE.md "Build Environment"
    / "Installation of the wheel"): `make build` in a build venv, then
    `pip install --force-reinstall` of the resulting self-contained wheel
    (Python + Java planner + C/Go tools + worker package) into a venv. The same
    wheel file is then installed on every worker, so planner, PegasusLite and
    the worker tools are one version everywhere. The apt Pegasus is left in
    place; runs pick the wheel by PATH (see _pegasus_env_prefix).

    Source: --pegasus-wheel-ref (branch/tag cloned from --pegasus-wheel-repo),
    or --pegasus-wheel-src DIR (git-archive of that checkout's COMMITTED HEAD).
    """
    if bool(args.pegasus_wheel_ref) == bool(args.pegasus_wheel_src):
        sys.exit(
            "error: --install-pegasus-wheel needs exactly one of "
            "--pegasus-wheel-ref REF or --pegasus-wheel-src DIR"
        )
    if not PEGASUS_WHEEL_SH.exists():
        sys.exit(f"error: {PEGASUS_WHEEL_SH} not found")
    prefix = args.pegasus_env
    submit = slice_obj.get_node(args.submit_node)
    env = {"MODE": "build", "PEGASUS_ENV": prefix}
    tarball = None
    if args.pegasus_wheel_src:
        src = Path(args.pegasus_wheel_src).expanduser().resolve()
        if not (src / "pyproject.toml").is_file() or not (src / "Makefile").is_file():
            sys.exit(f"error: {src} does not look like a pegasus checkout")
        ref = subprocess.run(
            ["git", "-C", str(src), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        desc = _git_desc(src)
        if desc.endswith("-dirty"):
            print(
                "  WARNING: checkout is dirty -- git archive ships the COMMITTED "
                "tree at HEAD; uncommitted edits are not uploaded"
            )
        fd, tarball = tempfile.mkstemp(suffix=".tar.gz")
        os.close(fd)
        remote_tar = f"{REMOTE_DIR}/pegasus-src-{ref[:9]}.tar.gz"
        try:
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(src),
                    "archive",
                    "--format=tar.gz",
                    "-o",
                    tarball,
                    "HEAD",
                ],
                check=True,
            )
            submit.execute(f"mkdir -p ~/{REMOTE_DIR}")
            submit.upload_file(tarball, remote_tar)
        finally:
            os.unlink(tarball)
        env.update(
            STAGE_TARBALL=f"$HOME/{remote_tar}",
            PEGASUS_SOURCE_COMMIT=ref,
            PEGASUS_SRC_DESC=f"{src.name}@{desc}",
        )
        label = f"{src} ({desc})"
    else:
        env.update(
            PEGASUS_REF=args.pegasus_wheel_ref, PEGASUS_REPO=args.pegasus_wheel_repo
        )
        label = f"{args.pegasus_wheel_repo}@{args.pegasus_wheel_ref}"
    print(f"==> building the pegasus-wms wheel on '{args.submit_node}' from {label}")
    # STAGE_TARBALL carries $HOME, so it alone is left unquoted for the shell.
    node_env = " ".join(
        f"{k}={v}" if k == "STAGE_TARBALL" else f"{k}={shlex.quote(str(v))}"
        for k, v in env.items()
    )
    submit.execute(f"mkdir -p ~/{REMOTE_DIR}")
    submit.upload_file(str(PEGASUS_WHEEL_SH), f"{REMOTE_DIR}/install_pegasus_wheel.sh")
    out, err = submit.execute(
        f"chmod +x ~/{REMOTE_DIR}/install_pegasus_wheel.sh && "
        f"{node_env} bash ~/{REMOTE_DIR}/install_pegasus_wheel.sh 2>&1 | "
        "grep -v -E '^(-- |\\[ *[0-9]+%\\]|gmake|cmake |Building |Scanning )'"
    )
    print(out)
    wheel_path = next(
        (
            line.split(None, 1)[1].strip()
            for line in (out or "").splitlines()
            if line.startswith("WHEEL_PATH ")
        ),
        None,
    )
    if "WHEEL_INSTALL_COMPLETE" not in (out or "") or not wheel_path:
        if err and err.strip():
            print("  [build stderr]\n" + err[-4000:])
        sys.exit(
            "error: pegasus wheel build/install did not complete on the submit node"
        )

    workers = _worker_names(args)
    if workers:
        fd, local_wheel = tempfile.mkstemp(suffix=".whl")
        os.close(fd)
        try:
            print(
                f"==> fetching {Path(wheel_path).name} to install on {', '.join(workers)}"
            )
            submit.download_file(local_wheel, wheel_path)
            remote_wheel = f"{REMOTE_DIR}/{Path(wheel_path).name}"
            for name in workers:
                node = slice_obj.get_node(name)
                node.execute(f"mkdir -p ~/{REMOTE_DIR}")
                node.upload_file(local_wheel, remote_wheel)
                wout, ok = _run_wheel_script(
                    node,
                    {
                        "MODE": "install",
                        "PEGASUS_ENV": prefix,
                        "WHEEL": f"/home/ubuntu/{remote_wheel}",
                        "PEGASUS_SRC_DESC": label,
                    },
                )
                print(f"  [{name}] " + "\n  ".join(wout.strip().splitlines()[-3:]))
                if not ok:
                    sys.exit(f"error: wheel install did not complete on {name}")
        finally:
            os.unlink(local_wheel)

    if args.pegasus_wheel_worker_package:
        # A dev build's worker package is not on download.pegasus.isi.edu, so
        # jobs whose site does not point PEGASUS_HOME at an install must carry
        # the wheel's bundled worker package. ~/.pegasusrc is folded into every
        # plan on the submit node.
        submit.execute(
            "touch ~/.pegasusrc && "
            "grep -qs '^pegasus.transfer.worker.package' ~/.pegasusrc "
            "|| echo 'pegasus.transfer.worker.package = true' >> ~/.pegasusrc"
        )
        print("  ~/.pegasusrc: pegasus.transfer.worker.package = true")
    print(
        f"  done. Runs from this provisioner now prefer {prefix}/bin; by hand: "
        f"export PATH={prefix}/bin:$PATH  (deploy/PEGASUS-HTCONDOR.md)"
    )


def _pegasus_env_prefix(args):
    """Shell prefix that puts the wheel install first on PATH when present."""
    prefix = shlex.quote(args.pegasus_env)
    return f"if [ -x {prefix}/bin/pegasus-plan ]; then export PATH={prefix}/bin:$PATH; fi; "


def install_wfevents_plugin(slice_obj, args):
    """Retrofit the standalone wfevents monitord plugin onto the submit node.

    Unlike --install-monitord-plugin this touches neither the pegasus tree nor
    Vector: wfevents is an ordinary pip package that registers itself in the
    pegasus.monitord.plugins entry-point group, and Vector does not tail the
    wfevents.jsonl it writes. It does need a plugin host to discover the entry
    point: native in Pegasus >= 6.0, otherwise the 3-file overlay
    (--install-monitord-plugin) must already be on the node.

    Idempotent; opt in per run with --enable-wfevents-plugin.
    """
    submit = slice_obj.get_node(args.submit_node)
    print(f"==> installing the wfevents monitord plugin on '{args.submit_node}'")

    # The plugin host is what discovers entry points -- warn loudly if absent
    # rather than leaving a silently inert plugin behind.
    out, _ = submit.execute(
        "test -f /usr/lib/pegasus/python/Pegasus/monitoring/plugin.py "
        "&& echo HOST_PRESENT || echo HOST_ABSENT",
        quiet=True,
    )
    if "HOST_PRESENT" not in (out or ""):
        print(
            "  ! no monitord plugin host on this node (neither a >= 6.0 native "
            "tree nor the 5.x overlay) — wfevents will install but "
            "pegasus-monitord cannot load it.\n"
            "    Run --install-monitord-plugin first."
        )

    spec = args.wfevents_plugin_spec
    print(f"==> pip installing wfevents ({spec})")
    # Two passes for the same reason as the adapter: the version number does not
    # move between commits, so only --force-reinstall guarantees the requested ref.
    out, err = submit.execute(
        'export PATH="$HOME/.local/bin:$PATH" PIP_BREAK_SYSTEM_PACKAGES=1; '
        f'python3 -m pip install --user --quiet "{spec}" && '
        f'python3 -m pip install --user --quiet --force-reinstall --no-deps "{spec}" '
        "&& echo PIP_OK"
    )
    if "PIP_OK" not in (out or ""):
        print(out)
        if err and err.strip():
            print("  [pip stderr]\n" + err)
        sys.exit("error: wfevents plugin install failed")

    # Entry-point check with the interpreter monitord's wrapper resolves.
    out, _ = submit.execute(
        'python3 -c "from importlib.metadata import entry_points; '
        "eps = {e.name: e for e in entry_points(group='pegasus.monitord.plugins')}; "
        "assert 'wfevents' in eps, sorted(eps); eps['wfevents'].load(); "
        "print('WFEVENTS_OK')\""
    )
    if "WFEVENTS_OK" not in (out or ""):
        print(out)
        sys.exit(
            "error: wfevents entry point not loadable by /usr/bin/python3 -- "
            "pegasus-monitord would run without the plugin"
        )
    print("  wfevents entry point loads (pegasus.monitord.plugins)")

    # Record provenance next to the adapter's, when the overlay manifest exists.
    ver_q = (
        "import importlib.metadata as m, json; "
        f"d = m.distribution('{WFEVENTS_DIST}'); "
        "raw = d.read_text('direct_url.json'); "
        "u = json.loads(raw) if raw else {}; "
        "print(d.version, u.get('vcs_info', {}).get('commit_id') or u.get('url') or 'unknown')"
    )
    resolved, _ = submit.execute(f'python3 -c "{ver_q}"', quiet=True)
    resolved = (resolved or "unknown unknown").strip().splitlines()[-1]
    version, _, commit = resolved.partition(" ")
    submit.execute(
        f'test -f {MONITORD_MANIFEST} && sudo python3 -c "import json; '
        f"p = '{MONITORD_MANIFEST}'; m = json.load(open(p)); "
        f"m['wfevents_spec'] = '{spec}'; m['wfevents_version'] = '{version}'; "
        f"m['wfevents_resolved'] = '{commit}'; "
        "json.dump(m, open(p, 'w'), indent=2)\" || true"
    )
    print(f"  wfevents resolved: {version} @ {commit}")
    print(
        "  done. Enable per-run with --run-example/--run-workflow "
        "--enable-wfevents-plugin (Vector does NOT ship this stream)."
    )


# ---------------------------------------------------------------------------
# PegasusAI Studio webapp container
# ---------------------------------------------------------------------------


def install_studio(slice_obj, args):
    """Run the PegasusAI Studio webapp container on the submit node (idempotent).

    The studio plans/submits THROUGH the host's schedd: --network host plus the
    _CONDOR_* env point its condor tools at the host pool, and it authenticates
    with an IDTOKEN minted here (FS auth cannot cross the container boundary —
    00-pool-common.conf lists IDTOKENS for exactly this client).

    Pieces, ALL required:
      - docker (docker.io apt package) + the image
      - container-passwd/group: the image's own /etc/passwd + /etc/group with a
        uid-1000 'ubuntu' line appended, bind-mounted read-only. Without them
        uid 1000 resolves to '?' inside the container and Pegasus plans into a
        literal '?/' submit dir.
      - an IDTOKEN for ubuntu@TRUST_DOMAIN in ~/.condor/tokens.d/studio
      - /home/ubuntu bind-mounted so plans land on the host filesystem

    Re-running recreates the container (docker rm -f + run); the studio's own
    state (~/.studio/studio.db) lives on the host and survives.
    """
    submit = slice_obj.get_node(args.submit_node)
    _, submit_ip, _ = _node_fabnet(slice_obj, args.submit_node)
    if not submit_ip or ":" in submit_ip:
        sys.exit(
            f"error: submit node has no FABNet IPv4 (got {submit_ip!r}); "
            "run --reconfigure first"
        )
    print(
        f"==> installing the studio container on '{args.submit_node}' "
        f"({args.studio_image})"
    )

    submit.execute(
        "command -v docker >/dev/null 2>&1 || "
        "{ sudo apt-get update -y && sudo apt-get install -y docker.io; }; "
        "sudo usermod -aG docker $(id -un)"
    )
    submit.execute(f"sudo docker pull -q {args.studio_image}")

    # passwd/group shims: the image's own files + the host uid-1000 identity.
    submit.execute(
        "mkdir -p ~/.studio ~/.condor/tokens.d && "
        "chmod 700 ~/.condor ~/.condor/tokens.d"
    )
    submit.execute(
        f"sudo docker run --rm --entrypoint cat {args.studio_image} /etc/passwd "
        "> ~/.studio/container-passwd && "
        f"sudo docker run --rm --entrypoint cat {args.studio_image} /etc/group "
        "> ~/.studio/container-group && "
        "grep -q '^ubuntu:' ~/.studio/container-passwd || "
        "echo 'ubuntu:x:1000:1000::/home/ubuntu:/bin/bash' >> ~/.studio/container-passwd; "
        "grep -q '^ubuntu:' ~/.studio/container-group || "
        "echo 'ubuntu:x:1000:' >> ~/.studio/container-group"
    )

    # IDTOKEN: minted by root (only root reads the pool signing key), owned by
    # the user. TRUST_DOMAIN doubles as _CONDOR_SCHEDD_HOST — the container
    # never resolves it, the collector lookup returns the schedd's address.
    out, _ = submit.execute("condor_config_val TRUST_DOMAIN", quiet=True)
    trust_domain = (out or "").strip().splitlines()[-1] if out else ""
    if not trust_domain:
        sys.exit("error: condor_config_val TRUST_DOMAIN returned nothing — condor up?")
    submit.execute(
        f"sudo condor_token_create -identity ubuntu@{trust_domain} "
        "> ~/.condor/tokens.d/studio && chmod 600 ~/.condor/tokens.d/studio"
    )

    submit.execute("sudo docker rm -f studio >/dev/null 2>&1 || true")
    submit.execute(
        "sudo docker run -d --name studio --restart unless-stopped --network host "
        "-u 1000:1000 -e HOME=/home/ubuntu -e USER=ubuntu "
        f"-e STUDIO_ALLOWED_ORIGINS={args.studio_origins} "
        f"-e _CONDOR_CONDOR_HOST={submit_ip} "
        f"-e _CONDOR_SCHEDD_HOST={trust_domain} "
        "-e _CONDOR_SEC_TOKEN_DIRECTORY=/home/ubuntu/.condor/tokens.d "
        "-v /home/ubuntu:/home/ubuntu "
        "-v /home/ubuntu/.studio/container-passwd:/etc/passwd:ro "
        "-v /home/ubuntu/.studio/container-group:/etc/group:ro "
        f"{args.studio_image}"
    )

    # Verify the two things that break silently: uid resolution and condor auth.
    out, _ = submit.execute(
        "sleep 3; sudo docker exec studio whoami; "
        "sudo docker exec studio condor_q 2>&1 | tail -2",
        quiet=True,
    )
    out = out or ""
    if "ubuntu" not in out:
        print(out)
        sys.exit("error: uid 1000 does not resolve to 'ubuntu' in the container")
    if "AUTHENTICATE" in out:
        print(out)
        sys.exit(
            "error: condor_q from the container failed to authenticate — check "
            "IDTOKENS in SEC_DEFAULT_AUTHENTICATION_METHODS and the token"
        )
    print("  studio container up: whoami=ubuntu, condor_q authenticates (IDTOKEN)")
    print(
        "  reach it from your machine:\n"
        "    ssh -F ~/.ssh/fabric-submit.conf -L 8888:localhost:8888 fabric-submit\n"
        "    open http://localhost:8888"
    )


# ---------------------------------------------------------------------------
# Wire Vector to the Elasticsearch slice
# ---------------------------------------------------------------------------


def wire_es(slice_obj, args):
    print(f"==> wiring Vector on '{args.submit_node}' to {args.es_host}")
    submit = slice_obj.get_node(args.submit_node)

    ca = Path(args.es_ca)
    if not ca.exists():
        sys.exit(
            f"error: ES CA not found at {ca}\n"
            "  Run the elastic-stack recipe with --bootstrap-es first; it "
            f"downloads the CA to {CA_DEFAULT}."
        )
    submit.upload_file(str(ca), "ca.crt")
    submit.execute("sudo install -D -m 0644 ~/ca.crt /etc/vector/ca.crt")

    # A rebuilt pool has no /etc/hosts entry for the ES hostname until the ES
    # recipe's --peer-slice is re-run; --es-ip is the one-node shortcut (same
    # grep-guarded append wire_peer uses).
    if args.es_ip:
        submit.execute(
            f"grep -q '{args.es_host}' /etc/hosts || "
            f"echo '{args.es_ip} {args.es_host}' | sudo tee -a /etc/hosts >/dev/null"
        )

    # The bootstrap rendered /etc/vector/vector.toml with ES_HOST already; re-seds
    # only if the operator overrides --es-host now.
    submit.execute(
        "sudo sed -i 's|https://[^:\"]*:9200|https://"
        f"{args.es_host}:9200|g' /etc/vector/vector.toml"
    )
    submit.execute(
        f"printf 'ELASTIC_INGEST_PASSWORD=%s\\n' {args.ingest_password!r} "
        "| sudo tee /etc/vector/.env >/dev/null && sudo chown root:vector /etc/vector/.env "
        "&& sudo chmod 640 /etc/vector/.env"
    )
    out, err = submit.execute("sudo vector validate /etc/vector/vector.toml || true")
    submit.execute(
        "sudo systemctl daemon-reload && sudo systemctl enable --now vector && "
        "sleep 2 && systemctl --no-pager --lines=5 status vector || true"
    )
    print("  Vector started. Check: journalctl -u vector -f on the submit node.")


# ---------------------------------------------------------------------------
# End-to-end smoke test
# ---------------------------------------------------------------------------


def _looks_like_git(src):
    """True if src should be `git clone`d rather than treated as an existing dir."""
    return src.endswith(".git") or src.startswith(
        ("http://", "https://", "git@", "ssh://", "git://")
    )


def _workflow_basename(src):
    """Repo/dir name from a git URL or path (trailing slash and .git stripped)."""
    name = src.rstrip("/").rsplit("/", 1)[-1]
    if name.endswith(".git"):
        name = name[:-4]
    return name or "workflow"


def _plan_and_monitor(
    submit,
    runs,
    run_name,
    workdir,
    wf_file,
    data_conf="condorio",
    enable_plugin=False,
    tick_interval=5.0,
    condor_poll=True,
    enable_wfevents=False,
    wfevents_condor_poll=False,
    wfevents_join_timeout=60.0,
    serve=True,
    env_prefix="",
):
    """Plan+submit wf_file from workdir into runs/submit/<run_name>, then monitor.

    Shared tail of --run-example and --run-workflow. Ensures a no-shared-fs data
    configuration is set (the pool has no shared filesystem, so the Pegasus
    default 'sharedfs' would fail), plans with a deterministic submit dir under
    the runs root, and launches a headless workflow-monitor whose JSONL Vector is
    already tailing. Returns the submit dir.

    With enable_plugin, the run's pegasus.properties also turns on the wfmonitor
    monitord plugin (deploy/MONITORD-PLUGIN.md), so pegasus-monitord itself
    writes monitord-events.jsonl into the submit dir live -- the third stream
    Vector tails -- while the polling-path workflow-monitor keeps running
    unchanged for side-by-side comparison. With condor_poll (the default), the
    plugin also polls condor_q/history/status from monitord's tick() at
    tick_interval seconds; --no-monitord-condor-poll is the regression
    configuration (plugin on, polling off, pre-tick behavior).

    With enable_wfevents, the run additionally turns on the standalone wfevents
    plugin (mjstealey/pegasus-monitord-plugins, installed separately with pip
    --user), which writes wfevents.jsonl into the submit dir — same schema,
    consumable by workflow-monitor --remote/--replay. Vector does NOT tail it.
    wfevents_condor_poll gives the condor polling to wfevents instead (same
    tick_interval cadence) — enable it on ONE plugin only. Polling makes the
    plugin's final condor flush the slow step at stop(), so the run also gets
    pegasus.monitord.plugins.wfevents.join_timeout=wfevents_join_timeout:
    the host's default stop() budget (10s) is below the worst-case flush
    (~42s), and wfevents >=0.3.0 skips the final polls that do not fit it.

    env_prefix is a shell prefix (see _pegasus_env_prefix) that puts a
    --install-pegasus-wheel install first on PATH for planning and monitoring.

    serve=False skips the headless workflow-monitor --serve daemon entirely:
    no workflow-events.jsonl is written, so nothing from the run reaches ES
    unless a monitord plugin stream is enabled. Use for pure plugin runs
    observed via workflow-monitor --remote.
    """
    sub_dir = f"{runs}/submit/{run_name}"
    # Force the pool's data config unless the workflow already chose one. Workers
    # cannot see the submit node's filesystem, so 'sharedfs' (the Pegasus default)
    # must not win by omission.
    submit.execute(
        f"cd {workdir} && touch pegasus.properties && "
        "grep -q '^pegasus.data.configuration' pegasus.properties || "
        f"printf 'pegasus.data.configuration=%s\\n' {data_conf} >> pegasus.properties"
    )
    if enable_plugin or enable_wfevents:
        # events_path embeds the run name, and pegasus.properties persists in
        # the workdir across runs: drop any stale plugin block, then append the
        # current one. (Properties must be in place before pegasus-plan bakes
        # them into the submit dir's run properties for monitord.)
        plugin_props = []
        if enable_plugin:
            plugin_props += [
                "pegasus.monitord.plugins.wfmonitor.enabled=true",
                f"pegasus.monitord.plugins.wfmonitor.events_path={sub_dir}/monitord-events.jsonl",
            ]
            if condor_poll:
                plugin_props += [
                    f"pegasus.monitord.plugins.wfmonitor.tick_interval={tick_interval:g}",
                    "pegasus.monitord.plugins.wfmonitor.condor_poll=true",
                ]
        if enable_wfevents:
            plugin_props += [
                "pegasus.monitord.plugins.wfevents.enabled=true",
                f"pegasus.monitord.plugins.wfevents.events_path={sub_dir}/wfevents.jsonl",
            ]
            if wfevents_condor_poll:
                if enable_plugin and condor_poll:
                    print(
                        "  ! both wfmonitor and wfevents have condor_poll on — "
                        "they will double-poll the schedd (pass "
                        "--no-monitord-condor-poll to give polling to wfevents)"
                    )
                plugin_props += [
                    f"pegasus.monitord.plugins.wfevents.tick_interval={tick_interval:g}",
                    "pegasus.monitord.plugins.wfevents.condor_poll=true",
                    f"pegasus.monitord.plugins.wfevents.join_timeout={wfevents_join_timeout:g}",
                ]
        block = "\\n".join(plugin_props) + "\\n"
        submit.execute(
            f"cd {workdir} && "
            "sed -i '/^pegasus\\.monitord\\.plugins\\./d' pegasus.properties && "
            f"printf '{block}' >> pegasus.properties"
        )
    out, _ = submit.execute(
        f"{env_prefix}cd {workdir} && rm -rf {sub_dir} && "
        'echo "  planner: $(command -v pegasus-plan) ($(pegasus-version))"; '
        "pegasus-plan --conf pegasus.properties "
        f"--dir {runs}/submit --relative-submit-dir {run_name} "
        "--sites condorpool --output-sites local --cleanup leaf "
        f"--submit {wf_file} 2>&1 | tail -40"
    )
    print(out)
    if serve:
        # Headless monitor: writes workflow-events.jsonl + diagnostics-events.jsonl
        # into the submit dir, which Vector is already tailing.
        submit.execute(
            # ~/.local/bin (pip --user install of workflow-monitor) is not on PATH for
            # non-interactive SSH (Ubuntu's .bashrc early-returns), so make it explicit.
            f'{env_prefix}export PATH="$HOME/.local/bin:$PATH"; '
            # pegasus-monitord creates the stampede DB a few seconds AFTER submit, and
            # workflow-monitor --serve exits immediately if it is not there yet. Wait
            # for it (up to ~60s) before launching the monitor.
            f"for i in $(seq 1 30); do ls {sub_dir}/*.stampede.db >/dev/null 2>&1 && break; sleep 2; done; "
            f"nohup workflow-monitor {sub_dir} --serve --diagnose --log "
            f">{runs}/monitor-{run_name}.out 2>&1 & sleep 8; "
            f"ls -l {sub_dir}/workflow-events.jsonl 2>/dev/null || "
            "echo 'no JSONL yet — check pegasus-monitord / workflow-monitor output'"
        )
    else:
        print(
            "  --no-serve-monitor: skipping workflow-monitor --serve "
            "(no workflow-events.jsonl for this run)"
        )
        if enable_wfevents:
            submit.execute(
                f"for i in $(seq 1 30); do ls {sub_dir}/wfevents.jsonl >/dev/null 2>&1 && break; sleep 2; done; "
                f"ls -l {sub_dir}/wfevents.jsonl 2>/dev/null || "
                "echo 'no wfevents.jsonl yet — check pegasus-monitord output'"
            )
    return sub_dir


def _watch_hint(sub_dir):
    return (
        "  submitted. Watch progress on the submit node:\n"
        f"    condor_q ; pegasus-status -l {sub_dir}\n"
        "  confirm docs landed in ES — run on the ES node as the read-capable\n"
        "  'elastic' user (vector_ingest is write-only, so _count returns 403):\n"
        "    pw=$(grep '^ELASTIC_PASSWORD=' ~/elastic-stack/.env | cut -d= -f2)\n"
        '    curl -sk -u "elastic:$pw" '
        "https://localhost:9200/workflow-events-*/_count?pretty"
    )


def run_example(slice_obj, args):
    print("==> running the diamond smoke-test workflow")
    submit = slice_obj.get_node(args.submit_node)
    runs = args.runs_dir
    # --run-name matters here too: _plan_and_monitor rm -rf's the submit dir,
    # so a fresh name preserves earlier runs for comparison.
    run_name = args.run_name or "diamond-run"
    # Generate the abstract workflow + catalogs in the runs dir; diamond.py sets
    # condorio itself, so _plan_and_monitor's guard leaves it intact.
    submit.execute(
        f"{_pegasus_env_prefix(args)}cd {runs} && "
        f"python3 ~/{REMOTE_DIR}/examples/diamond.py 2>&1 | tail -30"
    )
    sub_dir = _plan_and_monitor(
        submit,
        runs,
        run_name,
        runs,
        "diamond-workflow.yml",
        enable_plugin=args.enable_monitord_plugin,
        tick_interval=args.monitord_tick_interval,
        condor_poll=args.monitord_condor_poll,
        enable_wfevents=args.enable_wfevents_plugin,
        wfevents_condor_poll=args.wfevents_condor_poll,
        wfevents_join_timeout=args.wfevents_join_timeout,
        serve=args.serve_monitor,
        env_prefix=_pegasus_env_prefix(args),
    )
    print(_watch_hint(sub_dir))


def run_workflow(slice_obj, args):
    """Clone/use a real workflow on the submit node, generate it, plan+submit, monitor.

    Generic sibling of run_example. --run-workflow is a git URL (cloned under the
    runs root) or an existing submit-node directory. --generate-cmd (optional)
    runs inside that dir to emit the abstract workflow + catalogs; then we plan
    with a deterministic submit dir and start workflow-monitor. Containerized
    workflows need a container runtime on the workers — the bootstrap installs
    Apptainer unless INSTALL_APPTAINER=0.
    """
    submit = slice_obj.get_node(args.submit_node)
    runs = args.runs_dir
    source = args.run_workflow
    run_name = args.run_name or f"{_workflow_basename(source)}-run"

    if _looks_like_git(source):
        workdir = args.workflow_dir or f"{runs}/{_workflow_basename(source)}"
        print(f"==> cloning {source} -> {workdir}")
        out, _ = submit.execute(
            f"rm -rf {workdir} && git clone --depth 1 {source} {workdir} 2>&1 | tail -5"
        )
        print(out)
    else:
        workdir = args.workflow_dir or source
        check, _ = submit.execute(f"test -d {workdir} && echo OK || echo MISSING")
        if "MISSING" in check:
            sys.exit(
                f"error: --run-workflow {source!r} is not a git URL and no such "
                f"directory exists on the submit node ({workdir}). Pass a git URL, "
                "or a path that already exists on the submit node."
            )
        print(f"==> using existing workflow dir {workdir}")

    if args.generate_cmd:
        print(f"==> generating workflow: {args.generate_cmd}")
        out, _ = submit.execute(
            f'{_pegasus_env_prefix(args)}export PATH="$HOME/.local/bin:$PATH"; '
            f"cd {workdir} && {args.generate_cmd} 2>&1 | tail -30"
        )
        print(out)

    print(
        f"==> planning {args.workflow_file} from {workdir} (data_conf={args.data_configuration})"
    )
    sub_dir = _plan_and_monitor(
        submit,
        runs,
        run_name,
        workdir,
        args.workflow_file,
        args.data_configuration,
        enable_plugin=args.enable_monitord_plugin,
        tick_interval=args.monitord_tick_interval,
        condor_poll=args.monitord_condor_poll,
        enable_wfevents=args.enable_wfevents_plugin,
        wfevents_condor_poll=args.wfevents_condor_poll,
        wfevents_join_timeout=args.wfevents_join_timeout,
        serve=args.serve_monitor,
        env_prefix=_pegasus_env_prefix(args),
    )
    print(_watch_hint(sub_dir))


# ---------------------------------------------------------------------------
# Reconfigure / destroy / handoff
# ---------------------------------------------------------------------------


def reconfigure(fablib, args):
    print(f"==> reconfiguring slice '{args.name}' (post-reboot IPs + route)")
    slice_obj = fablib.get_slice(args.name)
    for node in slice_obj.get_nodes():
        try:
            node.config()
        except Exception as exc:  # noqa: BLE001
            print(f"  ! {node.get_name()} config() failed: {exc}")
    for node in slice_obj.get_nodes():
        n, ip, gw = _node_fabnet(slice_obj, node.get_name())
        _install_route(node, gw)
        # CONDOR_HOST is pinned to the submit FABNet IP; a restart re-reads it.
        node.execute("sudo systemctl restart condor || true")
    print("  re-applied data-plane IPs, routes, and restarted condor.")


def destroy(fablib, args):
    print(f"==> deleting slice '{args.name}'")
    try:
        fablib.get_slice(args.name).delete()
        print("  deleted")
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"error: {exc}")


def print_handoff(args, submit_ip):
    print("\n" + "=" * 70)
    print("Pegasus/HTCondor pool slice is ready.")
    print("=" * 70)
    print(f"  submit/CM FABNet IP : {submit_ip}")
    print(f"  execute nodes       : {', '.join(_worker_names(args)) or '(none)'}")
    if not args.bootstrap:
        print("\n  (infra + route only — re-run with --bootstrap to install the stack)")
    print("\nNext, stand up the Elasticsearch slice and route it back to this pool:")
    print(
        f"  uv run python recipes/elastic-stack/provision.py \\\n"
        f"      --bootstrap-es --peer-slice {args.name}"
    )
    print("\nThen point Vector at it and start shipping, and smoke-test:")
    print(
        f"  uv run python recipes/pegasus-htcondor/provision.py --name {args.name} --wire-es\n"
        f"  uv run python recipes/pegasus-htcondor/provision.py --name {args.name} --run-example"
    )
    print("=" * 70)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Provision a FABRIC Pegasus/HTCondor pool slice (the producer).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--name", default="pegasus-htcondor", help="slice name")
    p.add_argument("--submit-node", default="submit", help="submit/CM node name")
    p.add_argument("--worker-prefix", default="work", help="execute node name prefix")
    p.add_argument("--workers", type=int, default=2, help="number of execute nodes")
    p.add_argument(
        "--site",
        default="UCSD",
        help="FABRIC site (IPv4-management sites simplify apt/Docker pulls)",
    )
    p.add_argument(
        "--worker-site",
        default=None,
        help="place execute nodes at a different site (needs Slice.Multisite)",
    )
    # Submit/CM sizing.
    p.add_argument("--cores", type=int, default=4)
    p.add_argument("--ram", type=int, default=16, help="GB")
    p.add_argument("--disk", type=int, default=50, help="GB (root disk)")
    # Worker sizing.
    p.add_argument("--worker-cores", type=int, default=8)
    p.add_argument("--worker-ram", type=int, default=32, help="GB")
    p.add_argument("--worker-disk", type=int, default=50, help="GB (root disk)")
    p.add_argument(
        "--image",
        default="default_ubuntu_24",
        help="node image; the Pegasus development/6.0 debs need Ubuntu 24 "
        "(noble-only dists, libc6 >= 2.38)",
    )

    p.add_argument(
        "--bootstrap",
        action="store_true",
        help="install + configure HTCondor pool, Pegasus, workflow-monitor, Vector",
    )
    p.add_argument(
        "--rebootstrap",
        action="store_true",
        help="re-run the role-aware node bootstrap on every node of the "
        "EXISTING slice (idempotent — installed steps are skipped). The "
        "recovery path when a fresh --bootstrap died partway",
    )
    p.add_argument(
        "--pool-password",
        default=os.environ.get("CONDOR_POOL_PASSWORD"),
        help="shared HTCondor pool signing key (default: generated + saved, gitignored)",
    )
    p.add_argument(
        "--htcondor-version",
        default=None,
        help="exact HTCondor apt version to pin, e.g. 25.12.2 (repo channel derived "
        "from the major: 25.12.2 -> 25.x); default: latest in the 24.x channel",
    )
    p.add_argument(
        "--pegasus-version",
        default=None,
        help="pin a Pegasus apt version within the channel (best-effort, prefix-"
        "matched). Default: UNPINNED — track the channel's latest, like the "
        "studio-webapp :main image does (host/container Pegasus skew breaks runs)",
    )
    p.add_argument(
        "--pegasus-channel",
        default="development/6.0",
        help="Pegasus apt channel under download.pegasus.isi.edu: 'release' "
        "(stable top-level repo) or a subdir like development/6.0 "
        "(noble/resolute dists only)",
    )
    p.add_argument(
        "--workflow-monitor-spec",
        default="git+https://github.com/pegasus-isi/workflow-monitor.git",
        help="pip spec for workflow-monitor (git URL, PyPI name, or an uploaded wheel path)",
    )
    p.add_argument(
        "--runs-dir",
        default="/opt/workflows",
        help="absolute submit-dir root on the submit node (Vector tails it)",
    )

    # ES wiring (post-hoc; the ES slice must exist first).
    p.add_argument(
        "--wire-es", action="store_true", help="upload CA, finalize + start Vector"
    )
    p.add_argument(
        "--es-host", default=DEFAULT_ES_HOSTNAME, help="ES hostname Vector connects to"
    )
    p.add_argument(
        "--es-ca",
        default=str(CA_DEFAULT),
        help="path to the ES slice CA (deploy/fabric/ca.crt)",
    )
    p.add_argument(
        "--es-ip",
        default=None,
        help="with --wire-es: also map --es-host to this FABNet IP in /etc/hosts "
        "on the submit node (a rebuilt pool has no entry until the ES recipe's "
        "--peer-slice re-runs)",
    )
    p.add_argument(
        "--ingest-password",
        default=os.environ.get("ELASTIC_INGEST_PASSWORD", "changeme-vector-ingest"),
    )

    # PegasusAI Studio webapp container (post-hoc; the pool must be up).
    p.add_argument(
        "--install-studio",
        action="store_true",
        help="run the PegasusAI Studio webapp container on the submit node: "
        "docker + image, passwd/group shims, IDTOKEN into the pool. Idempotent "
        "(recreates the container; ~/.studio state survives)",
    )
    p.add_argument(
        "--studio-image", default=STUDIO_IMAGE, help="studio container image"
    )
    p.add_argument(
        "--studio-origins",
        default="http://localhost:8888",
        metavar="ORIGINS",
        help="STUDIO_ALLOWED_ORIGINS for the webapp (the ssh -L tunnel origin)",
    )

    p.add_argument(
        "--run-example", action="store_true", help="plan+submit the diamond smoke test"
    )

    # Generic workflow runner (post-hoc; the pool must already be bootstrapped).
    p.add_argument(
        "--run-workflow",
        default=None,
        metavar="GIT_URL_OR_DIR",
        help="clone (git URL) or use (existing submit-node dir) a workflow, "
        "generate + plan + submit it, and start workflow-monitor",
    )
    p.add_argument(
        "--generate-cmd",
        default=None,
        metavar="CMD",
        help="command run inside the workflow dir to emit the abstract workflow + "
        'catalogs (e.g. "./workflow_generator.py --regions california ... -o workflow.yml")',
    )
    p.add_argument(
        "--workflow-file",
        default="workflow.yml",
        help="abstract workflow YAML (in the workflow dir) to plan",
    )
    p.add_argument(
        "--workflow-dir",
        default=None,
        help="submit-node working dir for the clone/checkout (default: <runs-dir>/<name>)",
    )
    p.add_argument(
        "--run-name",
        default=None,
        help="relative submit-dir name under <runs-dir>/submit (default: "
        "diamond-run for --run-example, <name>-run for --run-workflow)",
    )
    p.add_argument(
        "--data-configuration",
        default="condorio",
        choices=["condorio", "nonsharedfs", "sharedfs"],
        help="pegasus.data.configuration to ensure (the FABRIC pool has no shared FS)",
    )

    p.add_argument(
        "--install-apptainer",
        action="store_true",
        help="retrofit the Apptainer container runtime onto every node of an "
        "existing slice (idempotent, no condor restart)",
    )

    # Monitord plugin stack (retrofit; see deploy/MONITORD-PLUGIN.md).
    p.add_argument(
        "--install-monitord-plugin",
        action="store_true",
        help="retrofit the pegasus-monitord plugin system (3-file overlay) + the "
        "wfmonitor adapter onto the submit node and restart Vector with the "
        "monitord-events stream (run elastic-stack --apply-schema FIRST)",
    )
    p.add_argument(
        "--install-wfevents-plugin",
        action="store_true",
        help="install the standalone wfevents monitord plugin (pip, submit node "
        "only; needs the --install-monitord-plugin overlay for its plugin host, "
        "but touches neither pegasus nor Vector). Opt in per run with "
        "--enable-wfevents-plugin",
    )
    p.add_argument(
        "--wfevents-plugin-spec",
        default=WFEVENTS_PLUGIN_SPEC,
        help="pip spec for the wfevents plugin (pin a ref with ...git@<sha>#subdirectory=...)",
    )
    p.add_argument(
        "--pegasus-plugin-ref",
        default=MONITORD_PLUGIN_REF,
        help="pegasus-isi/pegasus commit SHA the 3 monitord plugin files are "
        "fetched at (full SHA for reproducibility)",
    )
    p.add_argument(
        "--pegasus-src",
        default=None,
        metavar="DIR",
        help="local pegasus checkout to upload the 3 monitord plugin files from, "
        "instead of fetching --pegasus-plugin-ref from GitHub (iteration loop)",
    )
    p.add_argument(
        "--install-pegasus-wheel",
        action="store_true",
        help="build the pegasus-wms wheel on the submit node with `make build` "
        "(the pegasus repo's developer-guide method) and install that one "
        "wheel into --pegasus-env on every node, next to the apt Pegasus. "
        "Needs --pegasus-wheel-ref or --pegasus-wheel-src. Runs started by "
        "this provisioner then prefer the wheel (deploy/PEGASUS-HTCONDOR.md)",
    )
    p.add_argument(
        "--pegasus-wheel-ref",
        default=None,
        metavar="REF",
        help="with --install-pegasus-wheel: branch or tag to clone from "
        "--pegasus-wheel-repo (e.g. GH-2270)",
    )
    p.add_argument(
        "--pegasus-wheel-repo",
        default=PEGASUS_REPO_URL,
        metavar="URL",
        help=f"with --pegasus-wheel-ref: git URL to clone (default {PEGASUS_REPO_URL})",
    )
    p.add_argument(
        "--pegasus-wheel-src",
        default=None,
        metavar="DIR",
        help="with --install-pegasus-wheel: local pegasus checkout instead of a "
        "ref; its COMMITTED HEAD is git-archived and built on the submit node",
    )
    p.add_argument(
        "--pegasus-env",
        default="/opt/pegasus-env",
        metavar="DIR",
        help="install prefix (venv) for --install-pegasus-wheel; must be "
        "world-readable because worker jobs run as 'nobody' "
        "(default /opt/pegasus-env)",
    )
    p.add_argument(
        "--pegasus-wheel-worker-package",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="with --install-pegasus-wheel: set pegasus.transfer.worker.package "
        "= true in the submit node's ~/.pegasusrc so jobs carry the wheel's "
        "worker package (a dev build has none on download.pegasus.isi.edu)",
    )
    p.add_argument(
        "--install-pegasus-monitor",
        action="store_true",
        help="overlay the pegasus-monitor CLI (Pegasus/monitor/ + cli shim + "
        "/usr/bin wrapper) onto the submit node's apt Pegasus from "
        "--pegasus-monitor-src; idempotent, independent of the monitord overlay",
    )
    p.add_argument(
        "--pegasus-monitor-src",
        default=None,
        metavar="DIR",
        help="local pegasus checkout/worktree on a feature/pegasus-monitor* branch; "
        "its COMMITTED HEAD is git-archived and uploaded (required with "
        "--install-pegasus-monitor)",
    )
    p.add_argument(
        "--monitord-adapter-spec",
        default=MONITORD_ADAPTER_SPEC,
        metavar="SPEC_OR_DIR",
        help="pip spec OR local directory for the workflow-monitor build that "
        "registers the wfmonitor plugin (separate from --workflow-monitor-spec, "
        "which the fresh bootstrap uses)",
    )
    p.add_argument(
        "--enable-monitord-plugin",
        action="store_true",
        help="with --run-example/--run-workflow: write the "
        "pegasus.monitord.plugins.wfmonitor.* properties into the run so "
        "pegasus-monitord streams monitord-events.jsonl live "
        "(needs --install-monitord-plugin done once)",
    )
    p.add_argument(
        "--monitord-tick-interval",
        type=float,
        default=5.0,
        metavar="SECONDS",
        help="with --enable-monitord-plugin: the plugin host's tick cadence "
        "(drives the in-plugin condor polling; history/pool sub-throttle off it)",
    )
    p.add_argument(
        "--enable-wfevents-plugin",
        action="store_true",
        help="with --run-example/--run-workflow: also enable the standalone "
        "wfevents monitord plugin so the run writes wfevents.jsonl into the "
        "submit dir (same schema; consume with workflow-monitor --remote — "
        "Vector does not tail it). Install it once with "
        "--install-wfevents-plugin",
    )
    p.add_argument(
        "--wfevents-condor-poll",
        action="store_true",
        help="with --enable-wfevents-plugin: let the wfevents plugin poll "
        "condor_q/history/status from its tick() (cadence from "
        "--monitord-tick-interval). Enable polling on ONE plugin only — "
        "combine with --no-monitord-condor-poll if wfmonitor is also enabled.",
    )
    p.add_argument(
        "--wfevents-join-timeout",
        type=float,
        default=60.0,
        metavar="SECONDS",
        help="with --wfevents-condor-poll: the run's "
        "pegasus.monitord.plugins.wfevents.join_timeout — the stop() budget "
        "the plugin host grants wfevents. 60 covers the worst-case final "
        "condor flush (~42s); at the host default (10) wfevents >=0.3.0 "
        "skips the final polls that do not fit the budget.",
    )
    p.add_argument(
        "--serve-monitor",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="start the headless workflow-monitor --serve for the run (writes "
        "workflow-events.jsonl, the stream Vector ships to ES). "
        "--no-serve-monitor skips it — e.g. for a pure wfevents run observed "
        "with workflow-monitor --remote.",
    )
    p.add_argument(
        "--monitord-condor-poll",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="with --enable-monitord-plugin: also poll condor_q/history/status "
        "from inside pegasus-monitord via the plugin's tick(). "
        "--no-monitord-condor-poll is the regression configuration "
        "(plugin on, condor polling off)",
    )
    p.add_argument(
        "--reconfigure",
        action="store_true",
        help="re-apply data-plane IPs + route + restart condor (post-reboot)",
    )
    p.add_argument("--destroy", action="store_true", help="delete the slice and exit")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    fablib = _load_fablib()

    if args.destroy:
        destroy(fablib, args)
        return
    if args.reconfigure:
        reconfigure(fablib, args)
        return

    # Post-hoc actions operate on the EXISTING slice (the ES slice must already
    # be up for these), so they never rebuild. Install actions run before the
    # run actions so a one-shot `--install-monitord-plugin --run-example
    # --enable-monitord-plugin` works.
    if (
        args.wire_es
        or args.run_example
        or args.run_workflow
        or args.install_apptainer
        or args.install_monitord_plugin
        or args.install_wfevents_plugin
        or args.install_pegasus_monitor
        or args.install_pegasus_wheel
        or args.install_studio
        or args.rebootstrap
    ):
        slice_obj = fablib.get_slice(args.name)
        if args.rebootstrap:
            _, submit_ip, _ = _node_fabnet(slice_obj, args.submit_node)
            if submit_ip is None or ":" in submit_ip:
                sys.exit(
                    f"error: submit node has no FABNet IPv4 (got {submit_ip!r}); "
                    "run --reconfigure first"
                )
            bootstrap_pool(
                slice_obj, submit_ip, _fabnet_cidr(slice_obj, submit_ip), args
            )
        if args.install_apptainer:
            install_apptainer(slice_obj, args)
        if args.install_monitord_plugin:
            install_monitord_plugin(slice_obj, args)
        if args.install_wfevents_plugin:
            install_wfevents_plugin(slice_obj, args)
        if args.install_pegasus_wheel:
            install_pegasus_wheel(slice_obj, args)
        if args.install_pegasus_monitor:
            install_pegasus_monitor(slice_obj, args)
        if args.wire_es:
            wire_es(slice_obj, args)
        if args.install_studio:
            install_studio(slice_obj, args)
        if args.run_example:
            run_example(slice_obj, args)
        if args.run_workflow:
            run_workflow(slice_obj, args)
        return

    # Fresh build.
    slice_obj = build(fablib, args)

    # FABNetv4 addresses are allocated by FABRIC's IPAM but must be applied to
    # the data-plane NICs *inside* each VM; submit()'s post-boot config does not
    # do this for L3 networks. Without it, get_ip_addr() returns the interface's
    # IPv6 link-local (fe80::...) and the inter-slice route has no valid IPv4
    # nexthop. reconfigure() already does this on the post-reboot path.
    print("==> configuring data-plane interfaces (node.config())")
    for node in slice_obj.get_nodes():
        try:
            node.config()
        except Exception as exc:  # noqa: BLE001
            print(f"  ! {node.get_name()} config() failed: {exc}")

    submit_node, submit_ip, submit_gw = _node_fabnet(slice_obj, args.submit_node)
    if submit_ip is None or ":" in submit_ip:
        sys.exit(
            f"error: submit node's FABNet interface has no IPv4 data-plane "
            f"address (got {submit_ip!r}); the FABNetv4 IP was not applied. "
            "Check `slice.get_node(...).get_interface(network_name='fabnet')` "
            "and that node.config() succeeded."
        )
    fabnet_cidr = _fabnet_cidr(slice_obj, submit_ip)
    print(
        f"==> submit/CM data-plane IP {submit_ip} (gateway {submit_gw}, subnet {fabnet_cidr})"
    )

    for node in slice_obj.get_nodes():
        _, _, gw = _node_fabnet(slice_obj, node.get_name())
        _install_route(node, gw)

    if args.bootstrap:
        bootstrap_pool(slice_obj, submit_ip, fabnet_cidr, args)
        if args.install_pegasus_wheel:
            install_pegasus_wheel(slice_obj, args)

    print_handoff(args, submit_ip)


if __name__ == "__main__":
    main()
