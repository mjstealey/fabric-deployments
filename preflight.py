#!/usr/bin/env python3
"""Preflight 'doctor' for the FABRIC slice-recipes environment.

Validates that this directory's ``fabric_rc``, the SSH keys it references, and
the FABRIC API token are present and consistent *before* you try to provision a
slice. It resolves configuration exactly the way FABlib does (by parsing the
``fabric_rc`` file), so what it reports is what FABlib will see.

Read-only: it makes no network calls, contacts no FABRIC service, and creates
nothing. Run it any time:

    uv run python preflight.py

Exit code is 0 if nothing is FAIL, 1 otherwise (WARN does not fail the run).
"""

from __future__ import annotations

import json
import os
import stat
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
RC = HERE / "config" / "fabric_rc"

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"
_GLYPH = {PASS: "✓", WARN: "!", FAIL: "✗"}
_results: list[tuple[str, str, str]] = []  # (level, title, detail)


def record(level: str, title: str, detail: str = "") -> None:
    _results.append((level, title, detail))


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def check_referenced_file(
    cfg: dict, key: str, label: str, *, private_key: bool = False
) -> None:
    """Check a path-valued fabric_rc setting: present in config + file exists."""
    value = cfg.get(key)
    if not value:
        record(FAIL, label, f"{key} is not set in fabric_rc")
        return
    p = Path(os.path.expanduser(value))
    if not p.exists():
        record(FAIL, label, f"missing file: {p}")
        return
    if private_key:
        m = _mode(p)
        if m & (stat.S_IRWXG | stat.S_IRWXO):
            record(
                WARN,
                label,
                f"{p} is mode {m:04o}; private keys should be 0600 (chmod 600 {p})",
            )
            return
    record(PASS, label, str(p))


def check_token(cfg: dict) -> None:
    loc = cfg.get("token_location")
    label = "FABRIC API token"
    if not loc:
        record(FAIL, label, "token_location not set in fabric_rc")
        return
    p = Path(os.path.expanduser(loc))
    if not p.exists():
        record(
            FAIL,
            label,
            f"missing: {p}\n"
            "       Download from the FABRIC portal: Experiments > Manage "
            "Tokens (Create Token), save the JSON to the path above.",
        )
        return
    try:
        data = json.loads(p.read_text())
    except Exception as exc:  # noqa: BLE001
        record(FAIL, label, f"{p} is not valid JSON ({exc})")
        return
    missing = [k for k in ("id_token", "refresh_token") if k not in data]
    if missing:
        record(WARN, label, f"{p} present but missing keys: {', '.join(missing)}")
        return

    # Freshness check. Prefer the authoritative `expires_at` on the token;
    # fall back to a created_at + ~24h heuristic (local refresh-token lifetime).
    note = ""
    exp = _parse_ts(data.get("expires_at"))
    if exp is not None:
        hrs = (exp - time.time()) / 3600.0
        if hrs <= 0:
            record(
                WARN,
                label,
                f"{p}: EXPIRED — run `uv run python refresh_token.py` (if the "
                "refresh token is still valid, ~24h), else re-download from the "
                "portal (Experiments > Manage Tokens).",
            )
            return
        note = f"expires in ~{hrs:.1f}h"
    else:
        created = _parse_ts(data.get("created_at"))
        if created is not None:
            age_h = (time.time() - created) / 3600.0
            note = f"created ~{age_h:.1f}h ago"
            if age_h >= 24:
                record(
                    WARN,
                    label,
                    f"{p}: {note} — refresh token likely expired "
                    "(local lifetime ~24h). Re-download from the portal.",
                )
                return
    record(PASS, label, f"{p}" + (f" ({note})" if note else ""))


def _parse_ts(value) -> float | None:
    """Return a POSIX timestamp from an epoch number or ISO-8601 string."""
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        s = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:  # noqa: BLE001
        return None


def check_project_id(cfg: dict) -> None:
    pid = cfg.get("project_id")
    label = "Project ID"
    if not pid:
        record(FAIL, label, "FABRIC_PROJECT_ID is not set (no default)")
        return
    if "<" in pid or ">" in pid:
        record(FAIL, label, f"{pid!r} contains angle brackets — use the bare GUID")
        return
    # Loose GUID shape check (8-4-4-4-12).
    parts = pid.split("-")
    if len(parts) != 5:
        record(WARN, label, f"{pid!r} does not look like a project GUID")
        return
    record(PASS, label, pid)


def check_bastion_username(cfg: dict) -> None:
    u = cfg.get("bastion_username")
    if not u:
        record(
            FAIL, "Bastion username", "FABRIC_BASTION_USERNAME is not set (no default)"
        )
    else:
        record(PASS, "Bastion username", u)


def check_avoid(cfg: dict) -> None:
    avoid = cfg.get("avoid")
    if not avoid:
        record(PASS, "Sites to avoid", "(none)")
        return
    # If FABRIC_AVOID was written in Python-list syntax, FABlib mis-parses it
    # into tokens containing brackets/quotes. Catch that here.
    bad = [s for s in avoid if any(c in s for c in "[]'\"")]
    if bad:
        record(
            WARN,
            "Sites to avoid",
            f"malformed entries {bad!r} — use a plain comma list, e.g. "
            "export FABRIC_AVOID='EDUKY,EDC,GATECH,GPN'",
        )
    else:
        record(PASS, "Sites to avoid", ", ".join(avoid))


def main() -> int:
    print(f"fabric-deployments preflight\n  fabric_rc: {RC}\n")

    if not RC.exists():
        print(f"{_GLYPH[FAIL]} FAIL  fabric_rc not found at {RC}")
        return 1

    try:
        from fabrictestbed_extensions import __version__ as fablib_version
        from fabrictestbed_extensions.fablib.config.config import Config
    except Exception as exc:  # noqa: BLE001
        print(
            f"{_GLYPH[FAIL]} FAIL  FABlib not importable ({exc}).\n"
            "       Run `uv sync` in this directory first."
        )
        return 1
    record(PASS, "FABlib import", f"fabrictestbed-extensions {fablib_version}")

    # offline=True: parse + resolve config without requiring a token or network.
    try:
        cfg = Config(fabric_rc=str(RC), offline=True).get_config()
    except Exception as exc:  # noqa: BLE001
        record(FAIL, "fabric_rc parse", str(exc))
        cfg = {}

    if cfg:
        check_project_id(cfg)
        check_bastion_username(cfg)
        check_referenced_file(
            cfg, "bastion_key_location", "Bastion private key", private_key=True
        )
        check_referenced_file(
            cfg, "slice_private_key_file", "Slice private key", private_key=True
        )
        check_referenced_file(cfg, "slice_public_key_file", "Slice public key")
        check_referenced_file(cfg, "bastion_ssh_config_file", "Bastion SSH config")
        check_token(cfg)
        check_avoid(cfg)

    # Report
    width = max((len(t) for _, t, _ in _results), default=0)
    n_fail = n_warn = 0
    print("Checks:")
    for level, title, detail in _results:
        n_fail += level == FAIL
        n_warn += level == WARN
        line = f"  {_GLYPH[level]} {level}  {title.ljust(width)}"
        if detail:
            line += f"  {detail}"
        print(line)

    print()
    if n_fail:
        print(
            f"{n_fail} blocking issue(s), {n_warn} warning(s). "
            "Resolve FAILs before provisioning."
        )
        return 1
    if n_warn:
        print(f"No blocking issues; {n_warn} warning(s) to review.")
        return 0
    print("All checks passed — environment looks ready to provision.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
