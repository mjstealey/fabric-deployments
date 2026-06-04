#!/usr/bin/env python3
"""Launch the Pegasus/HTCondor-pool provisioner with this project's ``fabric_rc``.

Why a launcher instead of running ``deploy/fabric/provision_pegasus_slice.py``
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

    uv run python recipes/pegasus-htcondor/provision.py --help
    uv run python recipes/pegasus-htcondor/provision.py \
        --name pegasus-htcondor --site UCSD --workers 2 --bootstrap

The provisioning logic, the in-VM bootstrap, the HTCondor configs, the Vector
template, and the example workflow all live under ``deploy/pegasus-htcondor/``
and ``deploy/fabric/`` in this same project (the source of truth). This is the
**producer** peer of the ``elastic-stack`` recipe.
"""

from __future__ import annotations

import sys
from pathlib import Path

# recipes/pegasus-htcondor/provision.py -> parents[2] == the project root.
ROOT = Path(__file__).resolve().parents[2]
RC = ROOT / "config" / "fabric_rc"
PROV_DIR = ROOT / "deploy" / "fabric"
PROV_SCRIPT = PROV_DIR / "provision_pegasus_slice.py"


def _die(msg: str) -> "None":
    sys.exit(f"error: {msg}")


def main() -> None:
    if not RC.exists():
        _die(f"fabric_rc not found at {RC}")
    if not PROV_SCRIPT.exists():
        _die(f"provisioner not found at {PROV_SCRIPT}")

    sys.path.insert(0, str(PROV_DIR))
    import provision_pegasus_slice as peg  # noqa: E402
    from fabrictestbed_extensions.fablib.fablib import (  # noqa: E402
        FablibManager,
    )

    # Inject our config at the one seam the provisioner exposes for it.
    peg._load_fablib = lambda: FablibManager(fabric_rc=str(RC))

    print(f"[provision] fabric_rc      : {RC}")
    print(f"[provision] provisioner    : {PROV_SCRIPT}")
    print(f"[provision] pegasus assets : {peg.PEG_DIR}\n")

    try:
        peg.main(sys.argv[1:])
    except Exception as exc:  # noqa: BLE001
        # Most likely a config/token problem. Point at the doctor.
        _die(
            f"{type(exc).__name__}: {exc}\n"
            f"  Run the preflight check first:  uv run python "
            f"{ROOT / 'preflight.py'}"
        )


if __name__ == "__main__":
    main()
