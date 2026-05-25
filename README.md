# reset-analytics

Capital Reset's analytics pipeline, automated. Daily GitHub Actions pull GA4, Google Search Console, and UOL editorial metadata from their APIs and update the committed `ga4_analysis.db` — a slim SQLite rollup that the `audiencia` Claude skill consumes.

## What lives here

| File | Purpose |
|---|---|
| `GA_Search_Console_DB_updater.py` | Main updater. Pulls GA4 + GSC, applies Discover-leakage correction, rebuilds the analysis DB. Supports `--cloud-mode`. |
| `uol_enrichment_pull.py` | Pulls UOL GA4 360 editorial metadata into `content_uol`. Supports `--cloud-mode`. |
| `ga4_weekly.py` | Fallback weekly script. Manually triggered only. |
| `reset-taxonomy.jsonl` | Editorial taxonomy used by the updater. |
| `ga4_analysis.db` | **System of record in CI.** Rollup SQLite the workflows commit back after each run. Consumed by the `audiencia` skill. |
| `.github/workflows/` | The daily crons + one manual workflow. |

## How cloud mode works

Local runs hit a 920 MB master DB on Sérgio's Mac. Cloud runs never touch it. The `--cloud-mode` flag:

1. Builds an in-memory SQLite with the master schema.
2. Pulls the API window (default: current month-to-date) into memory.
3. Merges the in-memory results into the committed `ga4_analysis.db` rollup tables.
4. Closes everything cleanly. The workflow then commits the updated `.db` back to the repo.

The master DB is the manual archive Sérgio refreshes locally when he wants. CI never sees it.

## Running locally

```bash
# Normal (current month-to-date, master DB + analysis DB):
python3 GA_Search_Console_DB_updater.py

# Cloud-mode test on your Mac (uses local analysis DB, skips master DB):
python3 GA_Search_Console_DB_updater.py --cloud-mode --start 2026-05-20 --end 2026-05-24

# UOL enrichment:
python3 uol_enrichment_pull.py

# UOL cloud-mode (writes into ga4_analysis.db instead of master DB):
python3 uol_enrichment_pull.py --cloud-mode

# Dry runs (no API calls, no writes):
python3 GA_Search_Console_DB_updater.py --dry-run
python3 uol_enrichment_pull.py --dry-run
```

## GitHub Secrets

The workflows expect these secrets in repo settings → Secrets and variables → Actions:

| Secret | Contents | Notes |
|---|---|---|
| `GA4_TOKEN_JSON` | Full JSON contents of your local `ga4_token.json` | Must contain a `refresh_token`. OAuth Desktop-app credentials. |
| `GSC_SA_JSON` | Full JSON contents of `claude-search-console-*.json` | Service account key with Search Console access on `capitalreset.uol.com.br`. |
| `UOL_SA_JSON` | Full JSON contents of `ga reader key.json` | Service account key with Viewer access on UOL GA4 360 property 345118020. |

At workflow runtime, each secret is written to a temp file and pointed to via env vars (`GA4_TOKEN_PATH`, `GSC_SERVICE_ACCOUNT_PATH`, `UOL_SA_KEY_PATH`). The workflows also set `GA4_ANALYSIS_DB_PATH` to the repo-root `ga4_analysis.db`.

## Schedule

| Workflow | Cron (UTC) | What it does |
|---|---|---|
| `daily-ga4-gsc.yml` | `30 10 * * *` (10:30) | GA4 + GSC pull → merge into analysis DB → commit |
| `daily-uol.yml` | `45 10 * * *` (10:45) | UOL enrichment pull → merge into analysis DB → commit |
| `manual-ga4-weekly.yml` | `workflow_dispatch` only | Fallback weekly script |

Both daily workflows share a `concurrency: db-writer` group so they serialize on `ga4_analysis.db` and never race.

## Recovery: GA4 OAuth token expires (`invalid_grant`)

GA4 uses OAuth with a long-lived refresh token. If the token is revoked (Google sees unusual activity, password change, 6 months idle, manual revoke), the workflow will fail with `invalid_grant`. To fix:

1. On your Mac, run `python3 GA_Search_Console_DB_updater.py` once. It will pop a browser, re-auth, and save a fresh `ga4_token.json`.
2. Open the new `ga4_token.json`, copy the entire contents.
3. In GitHub: Settings → Secrets → Actions → `GA4_TOKEN_JSON` → Update — paste, save.
4. Re-run the failed workflow.

## Failure notifications

Enable "Send notifications for failed workflows only" in your GitHub account settings (Settings → Notifications → Actions). No YAML changes needed.

## Out of scope (intentionally)

- GitHub Pages dashboard
- Master DB syncing to cloud (920 MB local archive stays on Sérgio's Mac)
- Service-account auth for GA4 (separate follow-up)
- BigQuery export from UOL 360

## Project memory

Full migration history and decision log lives in Claude's memory under `project_analytics_migration_*` files.
