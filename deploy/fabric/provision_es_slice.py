#!/usr/bin/env python3
"""Provision a FABRIC slice that hosts the workflow-monitor Elasticsearch.

This is the receiver side of the deploy/ pipeline (see deploy/README.md and
deploy/FABRIC.md) running on its own FABRIC slice, reachable from a separate
Pegasus/HTCondor slice (where Vector runs) over a private, routed FABNetv4
network.

What it does
------------
  build      create the slice: one node, an Ubuntu image, an optional
             persistent data volume, and a NIC_Basic on a new FABNetv4 network
  route      install the inter-slice route (10.128.0.0/10 via the node's own
             FABNet gateway) plus a systemd one-shot so it survives reboot --
             never touching the management default route
  bootstrap  (--bootstrap-es) upload elastic-stack/ + bootstrap_es_node.sh,
             install Docker, regenerate the node cert with the FABNet IP in its
             SAN, `docker compose up`, and apply ILM/templates/aliases/role/user
  peer       (--peer-slice NAME) on each node of the peer slice, add the reverse
             route and an /etc/hosts entry so Vector can reach this ES by name
  handoff    print the exact vector.toml edits for the Pegasus slice

Usage
-----
  # Full path: create, wire routing, bring ES up, and route the Vector side.
  python provision_es_slice.py --name elasticsearch-host --site UCSD \
      --cores 8 --ram 32 --disk 100 --bootstrap-es --peer-slice pegasus-htcondor

  # Just the infrastructure + routing (inspect before bringing ES up):
  python provision_es_slice.py --name elasticsearch-host --site UCSD

  # Re-apply data-plane IP + route after a node reboot:
  python provision_es_slice.py --name elasticsearch-host --reconfigure

  # Tear it down:
  python provision_es_slice.py --name elasticsearch-host --destroy

References: docs/guides/{slices/create-slice,networking/l3-networks,
networking/network-troubleshooting,storage/persistent-storage,
nodes/post-boot-tasks}.md in the FABRIC documentation.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# The whole FABNetv4 address space. A route to this /10 via the node's own
# FABNet gateway lets it reach any other slice's FABNetv4 subnet in the same
# project (docs/guides/networking/network-troubleshooting.md, "Inter-slice
# communication"). The node's own /24 stays a directly-connected route.
FABNETV4_SUPERNET = "10.128.0.0/10"

NET_NAME = "fabnet"  # FABNetv4 network name within the slice
NIC_NAME = "nic1"  # NIC_Basic component name
DEFAULT_HOSTNAME = "workflow-monitor-es"  # matches the committed cert SAN

# Local layout (relative to repo root, two levels up from this file).
REPO_ROOT = Path(__file__).resolve().parents[2]
ELASTIC_STACK = REPO_ROOT / "deploy" / "elastic-stack"
KIBANA_DIR = ELASTIC_STACK / "kibana"
BOOTSTRAP_SH = Path(__file__).resolve().parent / "bootstrap_es_node.sh"
CA_DOWNLOAD = Path(__file__).resolve().parent / "ca.crt"

# Remote layout on the ES node (home-relative).
REMOTE_STACK = "elastic-stack"
REMOTE_KIBANA = f"{REMOTE_STACK}/kibana"
REMOTE_BOOTSTRAP = "bootstrap_es_node.sh"


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
# Networking helpers
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
        # -quad str so the IPv4 guard, the route install, and the ES hostname all
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


def _install_route(node, gateway):
    """Add the inter-slice route now and persist it via a systemd one-shot.

    Adds 10.128.0.0/10 via the node's own FABNet gateway. Idempotent. Never
    touches the default route (that carries SSH; replacing it is unrecoverable
    -- docs/guides/networking/network-troubleshooting.md).
    """
    if not gateway:
        print("  ! no FABNet gateway discovered; skipping route install")
        return

    # Apply immediately (|| true so a pre-existing route isn't an error).
    cmd = f"sudo ip route add {FABNETV4_SUPERNET} via {gateway} || true"
    node.execute(cmd)

    # Persist across reboots without clobbering the default route. A one-shot
    # after network-online is the least-surprising mechanism on a FABRIC VM.
    unit = (
        "[Unit]\n"
        "Description=workflow-monitor inter-slice FABNet route\n"
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


def build(fablib, args):
    print(f"==> creating slice '{args.name}' at site {args.site}")
    slice_obj = fablib.new_slice(name=args.name)

    node = slice_obj.add_node(name=args.node, site=args.site)
    node.set_capacities(cores=args.cores, ram=args.ram, disk=args.disk)
    node.set_image(args.image)

    if args.storage_name:
        # Attaches an existing project storage allocation by name
        # (docs/guides/storage/persistent-storage.md). Requires Component.Storage.
        node.add_storage(name=args.storage_name)
        print(f"  + persistent storage '{args.storage_name}'")

    iface = node.add_component(model="NIC_Basic", name=NIC_NAME).get_interfaces()[0]
    # "auto" mode makes Interface.config() allocate + apply a FABNetv4 IPv4 from
    # the network's pool. The default mode is "manual", under which config() only
    # brings the link up and assigns NO address (get_ip_addr() then returns the
    # IPv6 link-local and the inter-slice route has no valid IPv4 nexthop).
    iface.set_mode("auto")
    slice_obj.add_l3network(name=NET_NAME, interfaces=[iface], type="IPv4")

    print("  submitting (blocks until provisioned + post-boot config)...")
    slice_obj.submit()
    return slice_obj


# ---------------------------------------------------------------------------
# Bootstrap Elasticsearch on the node
# ---------------------------------------------------------------------------


def bootstrap_es(node, es_ip, args):
    print("==> bootstrapping Elasticsearch on the node")
    if not BOOTSTRAP_SH.exists():
        sys.exit(f"error: {BOOTSTRAP_SH} not found")

    # Upload the receiver config (templates, ilm, compose) + the bring-up
    # script. We upload individual files to avoid shipping the local certs/
    # and .env (the bootstrap regenerates certs fresh, with the FABNet IP).
    node.execute(
        f"mkdir -p ~/{REMOTE_STACK}/templates ~/{REMOTE_STACK}/ilm "
        f"~/{REMOTE_STACK}/certs"
    )
    uploads = [
        (ELASTIC_STACK / "docker-compose.yml", f"{REMOTE_STACK}/docker-compose.yml"),
        (
            ELASTIC_STACK / "docker-compose.kibana.yml",
            f"{REMOTE_STACK}/docker-compose.kibana.yml",
        ),
        (
            ELASTIC_STACK / "templates" / "workflow-events.json",
            f"{REMOTE_STACK}/templates/workflow-events.json",
        ),
        (
            ELASTIC_STACK / "templates" / "workflow-diag.json",
            f"{REMOTE_STACK}/templates/workflow-diag.json",
        ),
        (
            ELASTIC_STACK / "ilm" / "workflow-retention.json",
            f"{REMOTE_STACK}/ilm/workflow-retention.json",
        ),
    ]
    for local, remote in uploads:
        if not local.exists():
            sys.exit(f"error: missing {local}")
        node.upload_file(str(local), remote)
    node.upload_file(str(BOOTSTRAP_SH), REMOTE_BOOTSTRAP)

    # Heap ~= half RAM, capped at 31 GB (ES compressed-oops boundary).
    heap_gb = max(1, min(args.ram // 2, 31))

    env = (
        f"ES_IP={es_ip} "
        f"ES_HOSTNAME={args.hostname} "
        f"ES_VERSION={args.es_version} "
        f"ES_HTTP_PORT=9200 "
        f"ES_HEAP={heap_gb}g "
        f"ELASTIC_PASSWORD={args.elastic_password} "
        f"ELASTIC_INGEST_PASSWORD={args.ingest_password} "
        f"ES_DIR=$HOME/{REMOTE_STACK}"
    )
    print(
        "  running bootstrap_es_node.sh (Docker install + certs + compose + schema)..."
    )
    stdout, stderr = node.execute(
        f"chmod +x ~/{REMOTE_BOOTSTRAP} && {env} bash ~/{REMOTE_BOOTSTRAP}"
    )
    if stderr and stderr.strip():
        print("  [bootstrap stderr]\n" + stderr)

    # Pull the freshly-generated CA back so it can be distributed to the
    # Vector side (Vector pins it via tls.ca_file).
    try:
        node.download_file(str(CA_DOWNLOAD), f"{REMOTE_STACK}/certs/ca/ca.crt")
        print(f"  + downloaded CA to {CA_DOWNLOAD}")
    except Exception as exc:  # noqa: BLE001
        print(
            f"  ! could not download CA ({exc}); fetch ~/{REMOTE_STACK}/certs/ca/ca.crt by hand"
        )


# ---------------------------------------------------------------------------
# Kibana dashboards (post-hoc; the slice must be bootstrapped)
# ---------------------------------------------------------------------------


def push_kibana(fablib, args):
    """Upload deploy/elastic-stack/kibana/ and import it in-VM.

    Runs against an EXISTING, bootstrapped slice: it uploads the saved-object
    NDJSON + the transform JSON and runs import.sh on the node, where ES (:9200,
    rewritten from the committed :9210 by bootstrap_es_node.sh) and Kibana (:5601)
    are on localhost and ~/elastic-stack/.env already holds ELASTIC_PASSWORD.
    """
    print(f"==> pushing Kibana assets to slice '{args.name}'")
    if not KIBANA_DIR.exists():
        sys.exit(f"error: {KIBANA_DIR} not found")
    slice_obj = fablib.get_slice(args.name)
    node, _, _ = _node_fabnet(slice_obj, args.node)
    if node is None:
        sys.exit("error: no node found in slice")

    node.execute(
        f"mkdir -p ~/{REMOTE_KIBANA}/saved-objects ~/{REMOTE_KIBANA}/transforms"
    )
    uploads = [
        (KIBANA_DIR / "import.sh", f"{REMOTE_KIBANA}/import.sh"),
        (
            KIBANA_DIR / "saved-objects" / "data-views.ndjson",
            f"{REMOTE_KIBANA}/saved-objects/data-views.ndjson",
        ),
        (
            KIBANA_DIR / "saved-objects" / "dashboards.ndjson",
            f"{REMOTE_KIBANA}/saved-objects/dashboards.ndjson",
        ),
        (
            KIBANA_DIR / "transforms" / "jobstate-current.transform.json",
            f"{REMOTE_KIBANA}/transforms/jobstate-current.transform.json",
        ),
        (
            KIBANA_DIR / "transforms" / "jobstate-current.template.json",
            f"{REMOTE_KIBANA}/transforms/jobstate-current.template.json",
        ),
    ]
    for local, remote in uploads:
        if not local.exists():
            sys.exit(
                f"error: missing {local}\n"
                "  Generate the NDJSON first:\n"
                "  uv run python deploy/elastic-stack/kibana/build_saved_objects.py"
            )
        node.upload_file(str(local), remote)
    print(f"  uploaded {len(uploads)} files to ~/{REMOTE_KIBANA}")

    print("  running import.sh on the node (transform + saved-object import)...")
    cmd = (
        f"cd ~/{REMOTE_KIBANA} && chmod +x import.sh && "
        f"set -a; . ~/{REMOTE_STACK}/.env; set +a; "
        f"ES_URL=https://localhost:{args.es_http_port} "
        f"KIBANA_URL=http://localhost:5601 bash import.sh"
    )
    out, err = node.execute(cmd, quiet=True)
    if out:
        print(out)
    if err and err.strip():
        print("  [import stderr]\n" + err)
    print(
        "\n  Browse over the tunnel:\n"
        "    ssh -L 5601:localhost:5601 ubuntu@<es-node>\n"
        "    http://localhost:5601/app/dashboards#/view/wf-overview"
    )


# ---------------------------------------------------------------------------
# Peer slice (Vector side) routing
# ---------------------------------------------------------------------------


def wire_peer(fablib, args, es_ip):
    print(f"==> wiring peer slice '{args.peer_slice}' to reach {es_ip}")
    try:
        peer = fablib.get_slice(args.peer_slice)
    except Exception as exc:  # noqa: BLE001
        print(f"  ! could not load peer slice: {exc}")
        return

    for node in peer.get_nodes():
        try:
            iface = node.get_interface(network_name=NET_NAME)
        except Exception:
            iface = None
        gateway = None
        if iface is not None:
            try:
                gateway = peer.get_network(NET_NAME).get_gateway()
            except Exception:
                pass
        if not gateway:
            print(
                f"  ! {node.get_name()}: no '{NET_NAME}' FABNet interface/gateway; "
                "add a NIC on a FABNetv4 network to this node, then re-run"
            )
            continue
        node.execute(f"sudo ip route add {FABNETV4_SUPERNET} via {gateway} || true")
        node.execute(
            f"grep -q '{args.hostname}' /etc/hosts || "
            f"echo '{es_ip} {args.hostname}' | sudo tee -a /etc/hosts >/dev/null"
        )
        print(
            f"  + {node.get_name()}: route via {gateway} + /etc/hosts {args.hostname}"
        )


# ---------------------------------------------------------------------------
# Handoff / reconfigure / destroy
# ---------------------------------------------------------------------------


def print_handoff(args, es_ip):
    host = args.hostname
    print("\n" + "=" * 70)
    print("Elasticsearch slice is ready.")
    print("=" * 70)
    print(f"  ES FABNet IP : {es_ip}")
    print(f"  ES endpoint  : https://{host}:9200  (or https://{es_ip}:9200)")
    if args.bootstrap_es:
        print(
            f"  CA cert      : {CA_DOWNLOAD}  (copy to /etc/vector/ca.crt on the submit host)"
        )
    print("\nOn the Pegasus/HTCondor submit host, update deploy/vector/vector.toml:")
    print(f"""
  [sinks.es_events]
  endpoints   = ["https://{host}:9200"]   # was https://localhost:9210
  tls.ca_file = "/etc/vector/ca.crt"

  [sinks.es_diag]
  endpoints   = ["https://{host}:9200"]
  tls.ca_file = "/etc/vector/ca.crt"
""")
    if not args.peer_slice:
        print("If you did NOT pass --peer-slice, also on the submit host:")
        print(f"  sudo ip route add {FABNETV4_SUPERNET} via <its-own-FABNet-gateway>")
        print(f"  echo '{es_ip} {host}' | sudo tee -a /etc/hosts")
    print(
        "\nThen: vector validate /etc/vector/vector.toml && sudo systemctl restart vector"
    )
    print("=" * 70)


def reconfigure(fablib, args):
    print(f"==> reconfiguring slice '{args.name}' (post-reboot IP + route)")
    slice_obj = fablib.get_slice(args.name)
    # Re-apply FABLib's data-plane network config (IPs are not persistent
    # across reboot -- network-troubleshooting.md).
    for node in slice_obj.get_nodes():
        try:
            node.config()
        except Exception as exc:  # noqa: BLE001
            print(f"  ! {node.get_name()} config() failed: {exc}")
    node, es_ip, gateway = _node_fabnet(slice_obj, args.node)
    if node is not None:
        _install_route(node, gateway)
    print(f"  data-plane IP now: {es_ip}")


def destroy(fablib, args):
    print(f"==> deleting slice '{args.name}'")
    try:
        fablib.get_slice(args.name).delete()
        print("  deleted")
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"error: {exc}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Provision a FABRIC slice hosting workflow-monitor Elasticsearch.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--name", default="elasticsearch-host", help="slice name")
    p.add_argument("--node", default="es1", help="node name within the slice")
    p.add_argument(
        "--site",
        default="UCSD",
        help="FABRIC site (IPv4-management sites simplify apt/docker)",
    )
    p.add_argument("--cores", type=int, default=8)
    p.add_argument("--ram", type=int, default=32, help="GB")
    p.add_argument("--disk", type=int, default=100, help="GB (root disk)")
    p.add_argument("--image", default="default_ubuntu_22")
    p.add_argument(
        "--storage-name",
        default=None,
        help="attach an existing project persistent-storage volume by name",
    )
    p.add_argument(
        "--hostname",
        default=DEFAULT_HOSTNAME,
        help="hostname Vector uses (must match a cert SAN)",
    )

    p.add_argument(
        "--bootstrap-es",
        action="store_true",
        help="upload elastic-stack/ and bring Elasticsearch up end-to-end",
    )
    p.add_argument("--es-version", default="8.15.0")
    p.add_argument(
        "--elastic-password",
        default=os.environ.get("ELASTIC_PASSWORD", "changeme-elastic"),
    )
    p.add_argument(
        "--ingest-password",
        default=os.environ.get("ELASTIC_INGEST_PASSWORD", "changeme-vector-ingest"),
    )

    p.add_argument(
        "--peer-slice",
        default=None,
        help="Pegasus/HTCondor slice to auto-route to this ES (same project)",
    )

    p.add_argument(
        "--push-kibana",
        action="store_true",
        help="upload deploy/elastic-stack/kibana/ to the node and import the "
        "dashboards + current-state transform (slice must be bootstrapped)",
    )
    p.add_argument(
        "--es-http-port",
        type=int,
        default=9200,
        help="ES host port on the node (bootstrap publishes 9200, not the "
        "committed :9210); used by --push-kibana's in-VM import",
    )

    p.add_argument(
        "--reconfigure",
        action="store_true",
        help="re-apply data-plane IP + route to an existing slice (post-reboot)",
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
    # Post-hoc action on the EXISTING slice; never rebuilds.
    if args.push_kibana:
        push_kibana(fablib, args)
        return

    slice_obj = build(fablib, args)

    # FABNetv4 addresses are allocated by FABRIC's IPAM but must be applied to the
    # data-plane NIC *inside* the VM; submit()'s post-boot config does not do this
    # for L3 networks. Without it, get_ip_addr() returns the IPv6 link-local and
    # the inter-slice route has no valid IPv4 nexthop. reconfigure() does the same.
    print("==> configuring data-plane interface (node.config())")
    for n in slice_obj.get_nodes():
        try:
            n.config()
        except Exception as exc:  # noqa: BLE001
            print(f"  ! {n.get_name()} config() failed: {exc}")

    node, es_ip, gateway = _node_fabnet(slice_obj, args.node)
    if es_ip is None or ":" in es_ip:
        sys.exit(
            f"error: ES node's FABNet interface has no IPv4 data-plane address "
            f"(got {es_ip!r}); the FABNetv4 IP was not applied. Check "
            "`slice.get_node(...).get_interface(network_name='fabnet')` and that "
            "node.config() succeeded."
        )
    print(f"==> ES data-plane IP {es_ip} (gateway {gateway})")

    _install_route(node, gateway)

    if args.bootstrap_es:
        bootstrap_es(node, es_ip, args)

    if args.peer_slice:
        wire_peer(fablib, args, es_ip)

    print_handoff(args, es_ip)


if __name__ == "__main__":
    main()
