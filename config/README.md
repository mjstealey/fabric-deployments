# `config/` — FABRIC credentials

This directory holds the credentials FABlib needs to talk to the FABRIC testbed.
**The real credential files are gitignored and must be created locally** (see
[Quickstart](#quickstart)). This README documents every credential, its format,
and where to obtain it.

| File | Tracked in git? | What it is |
| --- | --- | --- |
| `fabric_rc.template` | yes (committed) | Template for `fabric_rc`. |
| `.tokens.json.template` | yes (committed) | Template for `.tokens.json`. |
| `README.md` | yes (committed) | This file. |
| `fabric_rc` | **NO — create locally** | Your filled-in FABlib runtime config. |
| `.tokens.json` | **NO — gitignored, create locally** | Your FABRIC API token JSON. |

> The `.tokens.json` file contains live secrets and is matched by `.gitignore`.
> Treat `fabric_rc` as private too: it carries your project UUID, bastion
> username, and absolute key paths — do not commit it.

---

## Quickstart

```bash
# 1. Copy the templates into place
cp config/fabric_rc.template        config/fabric_rc
cp config/.tokens.json.template     config/.tokens.json   # or download the real
                                                          # tokens from the portal

# 2. Edit both files — replace every /path/to/... and <...> placeholder
#    (see the per-credential sections below)

# 3. Validate (expect all-PASS)
uv run python preflight.py
```

Do **NOT** `source config/fabric_rc`. Its `{{ }}` Jinja placeholders and
comma-separated lists break shell word-splitting; the file is parsed only by
FABlib. To resolve config from a script, build
`FablibManager(fabric_rc=<path>)`.

---

## Credentials

### 1. FABRIC account + project

- Sign in / register at the **FABRIC portal**: https://portal.fabric-testbed.net/
- Your **project UUID** is on your project's page in the portal. Put it in
  `fabric_rc` as `FABRIC_PROJECT_ID`.
- Ask your **project lead** to grant the permissions these deployments need:
  `VM.NoLimitCPU`, `VM.NoLimitRAM`, `VM.NoLimitDisk`, `Component.Storage`, and
  `Slice.Multisite`.

### 2. API token (`.tokens.json`)

- Portal → **Experiments → Manage Tokens → Create Token**.
- Save the downloaded JSON to `config/.tokens.json` (the path
  `FABRIC_TOKEN_LOCATION` points at in `fabric_rc`).
- Shape is shown in `.tokens.json.template`: `id_token`, `refresh_token`, plus
  `created_at` / `expires_at` / `state` metadata.
- **Lifetimes:** the identity token (`id_token`) lasts ~4h; the `refresh_token`
  lasts ~24h and is **rotated on each use**.
- Refresh locally without revisiting the portal:

  ```bash
  uv run python refresh_token.py
  ```

  Only once the refresh token itself lapses must you create a new token in the
  portal. `preflight.py` flags a token that is expiring.

### 3. SSH keys

You need two keypairs and an SSH config file, all referenced by absolute path in
`fabric_rc`:

| `fabric_rc` key | File |
| --- | --- |
| `FABRIC_BASTION_KEY_LOCATION` | bastion private key |
| `FABRIC_SLICE_PRIVATE_KEY_FILE` | sliver (slice) private key |
| `FABRIC_SLICE_PUBLIC_KEY_FILE` | sliver (slice) public key |
| `FABRIC_BASTION_SSH_CONFIG_FILE` | SSH config used by `FABRIC_SSH_COMMAND_LINE` |

Generate the keys, for example:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/fabric-bastion     # bastion keypair
ssh-keygen -t ed25519 -f ~/.ssh/fabric-sliver      # sliver keypair
```

Then in the portal:

- Register the **public keys** (bastion + sliver) on your account.
- Find your **bastion username** under **User Profile**; set it as
  `FABRIC_BASTION_USERNAME` in `fabric_rc`.

More detail in the **FABRIC Knowledge Base**: https://learn.fabric-testbed.net/

### 4. `fabric_rc` host values and special fields

- The control-plane hosts (`FABRIC_ORCHESTRATOR_HOST`, `FABRIC_CREDMGR_HOST`,
  `FABRIC_CORE_API_HOST`, `FABRIC_AM_HOST`, `FABRIC_CEPH_MGR_HOST`,
  `FABRIC_BASTION_HOST`) are the same for every FABRIC user — leave them as-is.
- `FABRIC_AVOID` is a **plain comma list** (`'EDUKY,EDC,GATECH,GPN'`) — **no
  brackets/quotes**. FABlib does a literal `.split(",")`, so brackets or quotes
  become part of the site names and silently fail to avoid the intended sites.
- `FABRIC_SSH_COMMAND_LINE` keeps its `{{ }}` Jinja placeholders verbatim; only
  change the `-F` path to your `fabric-ssh-config`.

---

## Validation

After editing, always run:

```bash
uv run python preflight.py
```

Expect all-PASS before provisioning, and re-run after any `fabric_rc` edit.
