#!/usr/bin/env python3
"""Refresh the FABRIC API token from its refresh token (no portal visit needed).

The token in ``config/.tokens.json`` is short-lived: the **identity token**
lasts ~4 hours (its expiry is the ``expires_at`` field / the JWT ``exp`` claim).
It also carries a **refresh token**, valid ~24 hours from issue, which the FABRIC
Credential Manager will exchange for a *fresh identity token AND a fresh refresh
token* — resetting the 24-hour window each time.

So: run this before the current identity token lapses and you keep a live token
indefinitely without re-downloading. Let the refresh token itself lapse (>24h
since the last refresh) and you must re-download from the portal
(Experiments > Manage Tokens).

It resolves configuration exactly the way ``preflight.py`` and the recipes do —
by parsing ``config/fabric_rc`` offline (no network for config) — then calls
``CredmgrProxy.refresh``, which **atomically rewrites the token file** with the
new tokens. This script additionally stamps an ``expires_at`` derived from the
new identity token so ``preflight.py`` keeps showing the precise window.

Usage::

    uv run python refresh_token.py                 # refresh only if near expiry
    uv run python refresh_token.py --force         # refresh now, regardless
    uv run python refresh_token.py --check         # report status only (no network)
    uv run python refresh_token.py --threshold-minutes 30

It is safe to run on a schedule (idempotent + threshold-gated) — e.g. every few
hours via cron or ``/loop`` — to keep the refresh chain alive. Exit code is 0 on
success or a deliberate skip, 1 on failure (so a scheduler can detect a lapsed
refresh token).
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
RC = HERE / "config" / "fabric_rc"
# Matches the timestamp format the portal / credmgr write (and that preflight.py
# parses): e.g. "2026-06-03 18:39:54 +0000".
TIME_FORMAT = "%Y-%m-%d %H:%M:%S %z"


def _die(msg: str) -> "None":
    sys.exit(f"error: {msg}")


def _id_token_exp(id_token: str) -> int | None:
    """Return the identity token's ``exp`` (unix seconds) from the JWT payload.

    Decodes the middle JWT segment only — no signature check, we just want the
    expiry the server already set.
    """
    try:
        payload = id_token.split(".")[1]
        payload += "=" * (-len(payload) % 4)  # restore base64 padding
        return int(json.loads(base64.urlsafe_b64decode(payload))["exp"])
    except Exception:  # noqa: BLE001
        return None


def _fmt(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime(TIME_FORMAT)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Refresh the FABRIC API token from its refresh token.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="refresh even if the current identity token is still fresh",
    )
    p.add_argument(
        "--check",
        action="store_true",
        help="report token status only; make no network call",
    )
    p.add_argument(
        "--threshold-minutes",
        type=int,
        default=120,
        help="refresh when the identity token expires within this many minutes",
    )
    args = p.parse_args(argv)

    if not RC.exists():
        _die(f"fabric_rc not found at {RC}")

    try:
        from fabrictestbed_extensions.fablib.config.config import Config
    except Exception as exc:  # noqa: BLE001
        _die(f"FABlib not importable ({exc}); run `uv sync` in this directory first")

    cfg = Config(fabric_rc=str(RC), offline=True)
    cm_host = cfg.get_credmgr_host()
    token_location = cfg.get_token_location()
    project_id = cfg.get_project_id()
    project_name = cfg.get_project_name()
    if not cm_host or not token_location:
        _die(
            f"missing credmgr_host ({cm_host!r}) or token_location "
            f"({token_location!r}) in {RC}"
        )

    tok_path = Path(token_location).expanduser()
    if not tok_path.exists():
        _die(
            f"token file not found: {tok_path}\n"
            "  Download one from the FABRIC portal (Experiments > Manage Tokens)."
        )
    data = json.loads(tok_path.read_text())
    refresh_token = data.get("refresh_token")
    if not refresh_token:
        _die(f"no refresh_token in {tok_path}; re-download from the portal")

    exp = _id_token_exp(data.get("id_token", ""))
    now = time.time()
    if exp is not None:
        remaining_min = (exp - now) / 60.0
        print(f"current identity token expires {_fmt(exp)} (~{remaining_min:.0f} min)")
    else:
        remaining_min = None
        print("current identity token expiry unknown (could not decode id_token)")

    near = (
        args.force or remaining_min is None or remaining_min <= args.threshold_minutes
    )

    if args.check:
        created = data.get("created_at")
        if created:
            print(f"tokens created_at: {created}  (refresh token valid ~24h from then)")
        print(f"would refresh now: {near}")
        return 0

    if not near:
        print(
            f"still fresh (> {args.threshold_minutes} min remaining); not refreshing. "
            "Use --force to refresh anyway."
        )
        return 0

    try:
        from fabric_cm.credmgr.credmgr_proxy import CredmgrProxy, Status
    except Exception as exc:  # noqa: BLE001
        _die(f"could not import CredmgrProxy ({exc})")

    who = project_id or project_name
    print(f"refreshing via {cm_host} (project {who}) ...")
    proxy = CredmgrProxy(credmgr_host=cm_host)
    status, result = proxy.refresh(
        scope="all",
        refresh_token=refresh_token,
        file_name=str(tok_path),
        project_id=project_id,
        project_name=project_name,
    )
    if status != Status.OK:
        err = result.get("error") if isinstance(result, dict) else result
        print(f"refresh FAILED: {err}", file=sys.stderr)
        print(
            "  The refresh token may be expired (>24h since last refresh) or "
            "already used.\n  Re-download from the portal: Experiments > Manage "
            "Tokens.",
            file=sys.stderr,
        )
        return 1

    # CredmgrProxy.refresh already wrote id_token/refresh_token/created_at to the
    # file atomically. Stamp an expires_at from the new identity token (same
    # format the portal uses) so preflight.py reports the precise window, and
    # keep the file owner-only.
    new = json.loads(tok_path.read_text())
    new_exp = _id_token_exp(new.get("id_token", ""))
    if new_exp is not None:
        new["expires_at"] = _fmt(new_exp)
        tmp = tok_path.with_suffix(tok_path.suffix + ".tmp")
        tmp.write_text(json.dumps(new))
        tmp.replace(tok_path)
        msg = (
            f"refreshed OK — new identity token expires {new['expires_at']} "
            f"(~{(new_exp - time.time()) / 60:.0f} min); refresh window reset (~24h)."
        )
    else:
        msg = "refreshed OK (could not compute new expiry)."
    try:
        tok_path.chmod(0o600)
    except Exception:  # noqa: BLE001
        pass

    print(msg)
    print(f"  wrote {tok_path}")
    print("  verify: uv run python preflight.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
