#!/usr/bin/env python3
"""Launch the Elasticsearch-slice provisioner with this project's ``fabric_rc``.

Why a launcher instead of running ``deploy/fabric/provision_es_slice.py``
directly? FABlib's ``FablibManager()`` (which that script calls) looks for its
config at ``~/work/fabric_config/fabric_rc`` and otherwise falls back to
environment variables — it does **not** read a ``FABRIC_RC`` env var, and this
project's ``config/fabric_rc`` is not safe to ``source`` (the
``FABRIC_SSH_COMMAND_LINE`` and ``FABRIC_AVOID`` lines contain spaces/braces/
brackets that break shell parsing).

So this launcher imports the provisioner unchanged and overrides the single
seam where it builds its manager (``_load_fablib``), injecting
``FablibManager(fabric_rc=<repo>/config/fabric_rc)``. Every CLI flag passes
straight through:

    uv run python recipes/elastic-stack/provision.py --help
    uv run python recipes/elastic-stack/provision.py \
        --name elasticsearch-host --site UCSD \
        --cores 8 --ram 32 --disk 100 \
        --bootstrap-es --peer-slice pegasus-htcondor

The provisioning logic, the ``elastic-stack/`` receiver config, and the in-VM
bootstrap all live under ``deploy/`` in this same project (the source of truth).
"""

from __future__ import annotations

import sys
from pathlib import Path

# recipes/elastic-stack/provision.py -> parents[2] == the project root.
ROOT = Path(__file__).resolve().parents[2]
RC = ROOT / "config" / "fabric_rc"
PROV_DIR = ROOT / "deploy" / "fabric"
PROV_SCRIPT = PROV_DIR / "provision_es_slice.py"


def _die(msg: str) -> "None":
    sys.exit(f"error: {msg}")


def main() -> None:
    if not RC.exists():
        _die(f"fabric_rc not found at {RC}")
    if not PROV_SCRIPT.exists():
        _die(f"provisioner not found at {PROV_SCRIPT}")

    sys.path.insert(0, str(PROV_DIR))
    import provision_es_slice as es  # noqa: E402
    from fabrictestbed_extensions.fablib.fablib import (  # noqa: E402
        FablibManager,
    )

    # Inject our config at the one seam the provisioner exposes for it.
    es._load_fablib = lambda: FablibManager(fabric_rc=str(RC))

    print(f"[provision] fabric_rc      : {RC}")
    print(f"[provision] provisioner    : {PROV_SCRIPT}")
    print(f"[provision] elastic-stack  : {es.ELASTIC_STACK}\n")

    try:
        es.main(sys.argv[1:])
    except Exception as exc:  # noqa: BLE001
        # Most likely a config/token problem. Point at the doctor.
        _die(
            f"{type(exc).__name__}: {exc}\n"
            f"  Run the preflight check first:  uv run python "
            f"{ROOT / 'preflight.py'}"
        )


if __name__ == "__main__":
    main()
