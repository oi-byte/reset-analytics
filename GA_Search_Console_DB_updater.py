"""
GA + Search Console DB Updater — Unified pull, correction, and database update for Capital Reset.

Single script that does three jobs in one run:
  1. Pull GA4 data from API → correct → upsert into ga4_reset.db (5 tables)
  2. Pull GSC data from API → upsert into ga4_reset.db (2 new tables: gsc_web_search, gsc_discover)
  2b. Discover leakage correction: reclassify Direct sessions attributable to
      Google Discover (0.97 daily correlation, 0.74 leakage ratio — see April 2026 analysis)
  3. Query canonical DB → rebuild ga4_analysis.db (compact file for Cowork sessions)

GA4 tables: traffic (with UOL + Discover correction), content, timing, events, campaigns
GSC tables: gsc_web_search (date, page, query, clicks, impressions, ctr, position),
            gsc_discover (date, page, clicks, impressions, ctr)

Error handling: if GSC fails, GA4 still completes. If GA4 fails, script aborts.

Property (GA4): 352408538 (G-99N2LMR1EL)
Property (GSC): https://capitalreset.uol.com.br/
GA4 auth: OAuth Desktop app, token cached in ga4_token.json
GSC auth: Service account (claude-search-console-*.json)

⚠  MUST BE RUN LOCALLY ON MAC — not from Cowork sandbox (binary writes fail).

Usage:
    # Normal weekly run (pulls current month-to-date for GA4 + GSC):
    python3 GA_Search_Console_DB_updater.py

    # Custom date range:
    python3 GA_Search_Console_DB_updater.py --start 2026-04-01 --end 2026-04-10

    # Full backfill — use sparingly:
    python3 GA_Search_Console_DB_updater.py --backfill

    # GA4 only (skip GSC):
    python3 GA_Search_Console_DB_updater.py --ga4-only

    # GSC only (skip GA4):
    python3 GA_Search_Console_DB_updater.py --gsc-only

    # Skip analysis DB rebuild:
    python3 GA_Search_Console_DB_updater.py --no-analysis

    # Dry run (shows what would be pulled, no API calls):
    python3 GA_Search_Console_DB_updater.py --dry-run
"""

import argparse
import csv
import json
import os
import shutil
import sqlite3
import statistics
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

# ──────────────────────────────────────────────────────────────────────
# GA4 IMPORTS
# ──────────────────────────────────────────────────────────────────────
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    Metric,
    OrderBy,
    RunReportRequest,
)
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# ──────────────────────────────────────────────────────────────────────
# GSC IMPORTS
# ──────────────────────────────────────────────────────────────────────
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ══════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════

SCRIPT_DIR = Path(__file__).resolve().parent

# ── Path resolution ──
# Every credential and data path can be overridden via env var. This lets the
# same script run locally on Sérgio's Mac (defaults below) and in GitHub
# Actions (env vars set by the workflow). Defaults point at the original
# Google Analytics/ folder so a local run of this copy produces the same
# files in the same places as running the original script.
LOCAL_GA_FOLDER = SCRIPT_DIR.parent / "Google Analytics"

def _env_path(name: str, default: Path) -> Path:
    val = os.environ.get(name)
    return Path(val) if val else default

# — Headless mode —
# When set (e.g. in CI), the GA4 auth helper exits cleanly on token refresh
# failure instead of falling back to the interactive browser flow (which would
# hang the GitHub runner indefinitely).
GA4_HEADLESS = os.environ.get("GA4_HEADLESS", "").lower() in ("1", "true", "yes")

# — GA4 —
GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "352408538")
GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
GA4_CLIENT_SECRET = _env_path(
    "GA4_CLIENT_SECRET_PATH",
    LOCAL_GA_FOLDER / "client_secret_141022466268-c5s8q4b5tql7rftko1h3vdcjtmr2aj59.apps.googleusercontent.com.json",
)
GA4_TOKEN_CACHE = _env_path("GA4_TOKEN_PATH", LOCAL_GA_FOLDER / "ga4_token.json")

# — GSC —
GSC_SITE_URL = os.environ.get("GSC_SITE_URL", "https://capitalreset.uol.com.br/")
GSC_SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
GSC_CREDENTIALS_DIR = SCRIPT_DIR.parent / "GSC"
GSC_SERVICE_ACCOUNT_FILE = _env_path(
    "GSC_SERVICE_ACCOUNT_PATH",
    GSC_CREDENTIALS_DIR / "claude-search-console-492017-49718fabe7a8.json",
)
GSC_PROPERTY_START = date(2024, 11, 19)  # earliest GSC data available
GSC_ROW_LIMIT = 25_000  # API max per request
GSC_DATA_STATE = "all"  # "all" matches GSC dashboard; "final" = confirmed only
GSC_TOP_QUERIES = 200  # keep only top N queries per month by clicks (gsc_web_search only)

# — Database —
# DB_PATH is the master DB (920 MB, full historical raw rows). Local-only by
# default. In --cloud-mode it is NEVER read or written.
# ANALYSIS_DB_PATH is the small (~1.7 MB) rollup DB that audiencia consumes
# and that GitHub Actions commits back to the repo.
DB_PATH = _env_path("GA4_DB_PATH", LOCAL_GA_FOLDER / "ga4_reset.db")
ANALYSIS_DB_PATH = _env_path("GA4_ANALYSIS_DB_PATH", LOCAL_GA_FOLDER / "ga4_analysis.db")

# — CSV archive —
# In local mode, defaults match the original script (writes to the GA folder).
# In CI, the workflow sets these to paths inside the cloned repo, so the
# committed CSV archive ends up in the right place.
RAW_CSV_DIR = _env_path("GA4_RAW_CSV_DIR", LOCAL_GA_FOLDER / "API pulls")
CORRECTED_CSV_DIR = _env_path("GA4_CORRECTED_CSV_DIR", LOCAL_GA_FOLDER / "API pulls" / "corrected")

# — Shared —
GA4_PROPERTY_START = date(2023, 2, 3)
ROW_LIMIT = 100_000  # GA4 API row limit per request


# ══════════════════════════════════════════════════════════════════════
# GA4 TABLE DEFINITIONS
# ══════════════════════════════════════════════════════════════════════

TRAFFIC_PULL_DIMENSIONS = [
    "date",
    "sessionDefaultChannelGroup",
    "sessionSource",
    "sessionMedium",
    "pageReferrer",
]

TRAFFIC_FINAL_DIMENSIONS = [
    "date",
    "sessionDefaultChannelGroup",
    "sessionSource",
    "sessionMedium",
]

TRAFFIC_METRICS = [
    "sessions",
    "totalUsers",
    "newUsers",
    "screenPageViews",
    "engagedSessions",
    "engagementRate",
    "averageSessionDuration",
    "eventCount",
    "bounceRate",
    "sessionsPerUser",
]

SUMMABLE_METRICS = {
    "sessions", "totalUsers", "newUsers", "screenPageViews",
    "engagedSessions", "eventCount",
}

RATE_METRICS = {
    "engagementRate",
    "averageSessionDuration",
    "bounceRate",
    "sessionsPerUser",
}

GA4_TABLE_DEFS = {
    "traffic": {
        "dimensions": TRAFFIC_FINAL_DIMENSIONS,
        "metrics": TRAFFIC_METRICS,
        "create_sql": """
            CREATE TABLE IF NOT EXISTS traffic (
                date TEXT NOT NULL,
                sessionDefaultChannelGroup TEXT NOT NULL,
                sessionSource TEXT NOT NULL,
                sessionMedium TEXT NOT NULL,
                sessions INTEGER,
                totalUsers INTEGER,
                newUsers INTEGER,
                screenPageViews INTEGER,
                engagedSessions INTEGER,
                engagementRate REAL,
                averageSessionDuration REAL,
                eventCount INTEGER,
                bounceRate REAL,
                sessionsPerUser REAL,
                PRIMARY KEY (date, sessionDefaultChannelGroup, sessionSource, sessionMedium)
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_traffic_date ON traffic(date)",
            "CREATE INDEX IF NOT EXISTS idx_traffic_channel ON traffic(sessionDefaultChannelGroup)",
            "CREATE INDEX IF NOT EXISTS idx_traffic_source ON traffic(sessionSource)",
        ],
    },
    "content": {
        "dimensions": ["date", "pagePath", "pageTitle", "landingPage"],
        "metrics": [
            "sessions", "totalUsers", "screenPageViews", "engagedSessions",
            "averageSessionDuration", "userEngagementDuration", "eventCount", "bounceRate",
        ],
        "create_sql": """
            CREATE TABLE IF NOT EXISTS content (
                date TEXT NOT NULL,
                pagePath TEXT NOT NULL,
                pageTitle TEXT,
                landingPage TEXT,
                sessions INTEGER,
                totalUsers INTEGER,
                screenPageViews INTEGER,
                engagedSessions INTEGER,
                averageSessionDuration REAL,
                userEngagementDuration REAL,
                eventCount INTEGER,
                bounceRate REAL,
                PRIMARY KEY (date, pagePath, landingPage)
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_content_date ON content(date)",
            "CREATE INDEX IF NOT EXISTS idx_content_path ON content(pagePath)",
            "CREATE INDEX IF NOT EXISTS idx_content_landing ON content(landingPage)",
        ],
    },
    "timing": {
        "dimensions": ["date", "hour", "deviceCategory", "newVsReturning", "language"],
        "metrics": [
            "sessions", "totalUsers", "newUsers", "screenPageViews",
            "engagedSessions", "engagementRate", "averageSessionDuration", "eventCount",
        ],
        "create_sql": """
            CREATE TABLE IF NOT EXISTS timing (
                date TEXT NOT NULL,
                hour TEXT NOT NULL,
                deviceCategory TEXT NOT NULL,
                newVsReturning TEXT,
                language TEXT,
                sessions INTEGER,
                totalUsers INTEGER,
                newUsers INTEGER,
                screenPageViews INTEGER,
                engagedSessions INTEGER,
                engagementRate REAL,
                averageSessionDuration REAL,
                eventCount INTEGER,
                PRIMARY KEY (date, hour, deviceCategory, newVsReturning, language)
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_timing_date ON timing(date)",
            "CREATE INDEX IF NOT EXISTS idx_timing_hour ON timing(hour)",
            "CREATE INDEX IF NOT EXISTS idx_timing_device ON timing(deviceCategory)",
        ],
    },
    "events": {
        "dimensions": ["date", "eventName"],
        "metrics": ["eventCount", "totalUsers"],
        "create_sql": """
            CREATE TABLE IF NOT EXISTS events (
                date TEXT NOT NULL,
                eventName TEXT NOT NULL,
                eventCount INTEGER,
                totalUsers INTEGER,
                PRIMARY KEY (date, eventName)
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_events_date ON events(date)",
            "CREATE INDEX IF NOT EXISTS idx_events_name ON events(eventName)",
        ],
    },
    "campaigns": {
        "dimensions": ["date", "sessionSource", "sessionMedium", "sessionCampaignName"],
        "metrics": ["sessions", "totalUsers", "newUsers", "engagedSessions", "screenPageViews"],
        "create_sql": """
            CREATE TABLE IF NOT EXISTS campaigns (
                date TEXT NOT NULL,
                sessionSource TEXT NOT NULL,
                sessionMedium TEXT NOT NULL,
                sessionCampaignName TEXT NOT NULL,
                sessions INTEGER,
                totalUsers INTEGER,
                newUsers INTEGER,
                engagedSessions INTEGER,
                screenPageViews INTEGER,
                PRIMARY KEY (date, sessionSource, sessionMedium, sessionCampaignName)
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_campaigns_date ON campaigns(date)",
            "CREATE INDEX IF NOT EXISTS idx_campaigns_source ON campaigns(sessionSource)",
            "CREATE INDEX IF NOT EXISTS idx_campaigns_campaign ON campaigns(sessionCampaignName)",
        ],
    },
}

INTEGER_METRICS = {
    "sessions", "totalUsers", "newUsers", "screenPageViews",
    "engagedSessions", "eventCount",
}


# ══════════════════════════════════════════════════════════════════════
# GSC TABLE DEFINITIONS
# ══════════════════════════════════════════════════════════════════════

GSC_TABLE_DEFS = {
    "gsc_web_search": {
        "search_type": "WEB",
        "dimensions": ["date", "page", "query"],
        "create_sql": """
            CREATE TABLE IF NOT EXISTS gsc_web_search (
                date TEXT NOT NULL,
                page TEXT NOT NULL,
                query TEXT NOT NULL,
                clicks INTEGER,
                impressions INTEGER,
                ctr REAL,
                position REAL,
                PRIMARY KEY (date, page, query)
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_gsc_web_date ON gsc_web_search(date)",
            "CREATE INDEX IF NOT EXISTS idx_gsc_web_page ON gsc_web_search(page)",
            "CREATE INDEX IF NOT EXISTS idx_gsc_web_query ON gsc_web_search(query)",
        ],
    },
    "gsc_discover": {
        "search_type": "DISCOVER",
        "dimensions": ["date", "page"],
        "create_sql": """
            CREATE TABLE IF NOT EXISTS gsc_discover (
                date TEXT NOT NULL,
                page TEXT NOT NULL,
                clicks INTEGER,
                impressions INTEGER,
                ctr REAL,
                PRIMARY KEY (date, page)
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_gsc_disc_date ON gsc_discover(date)",
            "CREATE INDEX IF NOT EXISTS idx_gsc_disc_page ON gsc_discover(page)",
        ],
    },
}


# ══════════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════════

def get_ga4_credentials() -> Credentials:
    """Authenticate for GA4 via OAuth Desktop app (same as ga4_weekly.py)."""
    creds = None
    if GA4_TOKEN_CACHE.exists():
        with open(GA4_TOKEN_CACHE, "r") as f:
            token_data = json.load(f)
        creds = Credentials.from_authorized_user_info(token_data, GA4_SCOPES)

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        from google.auth.transport.requests import Request
        try:
            creds.refresh(Request())
            _save_ga4_token(creds)
            return creds
        except Exception as e:
            if GA4_HEADLESS:
                sys.exit(
                    f"GA4 token refresh failed in headless mode: {e}\n"
                    f"The cached refresh token is invalid or revoked.\n"
                    f"Re-seed the GA4_TOKEN_JSON GitHub Secret with a fresh token\n"
                    f"from a successful local run of the script."
                )
            print(f"Token refresh failed ({e}), re-authenticating...")

    if GA4_HEADLESS:
        sys.exit(
            "GA4 auth requires interactive browser flow but GA4_HEADLESS is set.\n"
            "This means the cached token at GA4_TOKEN_PATH is missing or unusable.\n"
            "Re-seed the GA4_TOKEN_JSON GitHub Secret from a working local token."
        )

    if not GA4_CLIENT_SECRET.exists():
        sys.exit(f"Client secret not found at {GA4_CLIENT_SECRET}")

    flow = InstalledAppFlow.from_client_secrets_file(str(GA4_CLIENT_SECRET), GA4_SCOPES)
    creds = flow.run_local_server(port=0)
    _save_ga4_token(creds)
    return creds


def _save_ga4_token(creds: Credentials):
    token_data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes or GA4_SCOPES),
    }
    with open(GA4_TOKEN_CACHE, "w") as f:
        json.dump(token_data, f, indent=2)


def get_gsc_service():
    """Authenticate for GSC via service account and return a Search Console service object."""
    if not GSC_SERVICE_ACCOUNT_FILE.exists():
        raise FileNotFoundError(
            f"GSC service account credentials not found at {GSC_SERVICE_ACCOUNT_FILE}\n"
            f"Expected in: {GSC_CREDENTIALS_DIR}"
        )

    creds = service_account.Credentials.from_service_account_file(
        str(GSC_SERVICE_ACCOUNT_FILE), scopes=GSC_SCOPES
    )
    return build("searchconsole", "v1", credentials=creds, cache_discovery=False)


# ══════════════════════════════════════════════════════════════════════
# DATE HELPERS
# ══════════════════════════════════════════════════════════════════════

def monthly_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Split a date range into per-month chunks."""
    chunks = []
    current = start
    while current <= end:
        if current.month == 12:
            month_end = date(current.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(current.year, current.month + 1, 1) - timedelta(days=1)
        chunk_end = min(month_end, end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def current_month_range() -> tuple[date, date]:
    """First of current month through yesterday."""
    today = date.today()
    start = date(today.year, today.month, 1)
    end = today - timedelta(days=1)
    if end < start:
        if today.month == 1:
            start = date(today.year - 1, 12, 1)
        else:
            start = date(today.year, today.month - 1, 1)
        end = today - timedelta(days=1)
    return start, end


# ══════════════════════════════════════════════════════════════════════
# GA4 API CALLS
# ══════════════════════════════════════════════════════════════════════

def pull_traffic_with_referrer(
    client: BetaAnalyticsDataClient,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """Pull traffic data WITH pageReferrer for in-memory correction."""
    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name=d) for d in TRAFFIC_PULL_DIMENSIONS],
        metrics=[Metric(name=m) for m in TRAFFIC_METRICS],
        date_ranges=[
            DateRange(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            )
        ],
        order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
        limit=ROW_LIMIT,
        keep_empty_rows=False,
    )

    response = client.run_report(request)

    rows = []
    for row in response.rows:
        record = {}
        for i, dim in enumerate(TRAFFIC_PULL_DIMENSIONS):
            record[dim] = row.dimension_values[i].value
        for i, met in enumerate(TRAFFIC_METRICS):
            record[met] = row.metric_values[i].value
        rows.append(record)

    if response.row_count >= ROW_LIMIT:
        print(f"  ⚠ ROW LIMIT REACHED ({ROW_LIMIT}) for traffic {start_date}–{end_date}!")
        print(f"    Data is TRUNCATED. Split into smaller date ranges.")

    return rows


def pull_ga4_table(
    client: BetaAnalyticsDataClient,
    table_name: str,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """Pull a non-traffic GA4 table (content, timing, events, campaigns)."""
    tdef = GA4_TABLE_DEFS[table_name]

    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name=d) for d in tdef["dimensions"]],
        metrics=[Metric(name=m) for m in tdef["metrics"]],
        date_ranges=[
            DateRange(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            )
        ],
        order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
        limit=ROW_LIMIT,
        keep_empty_rows=False,
    )

    response = client.run_report(request)

    rows = []
    for row in response.rows:
        record = {}
        for i, dim in enumerate(tdef["dimensions"]):
            record[dim] = row.dimension_values[i].value
        for i, met in enumerate(tdef["metrics"]):
            record[met] = row.metric_values[i].value
        rows.append(record)

    if response.row_count >= ROW_LIMIT:
        print(f"  ⚠ ROW LIMIT REACHED ({ROW_LIMIT}) for {table_name} {start_date}–{end_date}!")

    return rows


# ══════════════════════════════════════════════════════════════════════
# GA4 CORRECTION LOGIC
# ══════════════════════════════════════════════════════════════════════

def correct_traffic_rows(rows: list[dict]) -> tuple[list[dict], int]:
    """
    Reclassify misclassified Direct sessions:
      - If channel is "Direct" and pageReferrer contains "uol.com.br"
      - Move to channel="Organic Referral", source="uol.com.br", medium="referral"
    """
    corrected = []
    reclassified = 0

    for row in rows:
        new_row = dict(row)
        referrer = row.get("pageReferrer", "")
        channel = row.get("sessionDefaultChannelGroup", "")

        if channel == "Direct" and "uol.com.br" in referrer:
            new_row["sessionDefaultChannelGroup"] = "Organic Referral"
            new_row["sessionSource"] = "uol.com.br"
            new_row["sessionMedium"] = "referral"
            reclassified += 1

        corrected.append(new_row)

    return corrected, reclassified


def aggregate_traffic(rows: list[dict]) -> list[dict]:
    """
    Collapse pageReferrer dimension: aggregate rows to
    (date, channel, source, medium) grain.
    Summable metrics are summed. Rate metrics are recomputed.
    """
    groups = defaultdict(lambda: {
        "sessions": 0, "totalUsers": 0, "newUsers": 0,
        "screenPageViews": 0, "engagedSessions": 0, "eventCount": 0,
        "weighted_duration": 0.0,
    })

    for row in rows:
        key = (
            row["date"],
            row["sessionDefaultChannelGroup"],
            row["sessionSource"],
            row["sessionMedium"],
        )
        g = groups[key]
        sessions = int(float(row["sessions"]))
        g["sessions"] += sessions
        g["totalUsers"] += int(float(row["totalUsers"]))
        g["newUsers"] += int(float(row["newUsers"]))
        g["screenPageViews"] += int(float(row["screenPageViews"]))
        g["engagedSessions"] += int(float(row["engagedSessions"]))
        g["eventCount"] += int(float(row["eventCount"]))
        g["weighted_duration"] += sessions * float(row["averageSessionDuration"])

    result = []
    for (dt, channel, source, medium), g in sorted(groups.items()):
        sessions = g["sessions"]
        total_users = g["totalUsers"]

        engagement_rate = g["engagedSessions"] / sessions if sessions > 0 else 0.0
        bounce_rate = 1.0 - engagement_rate
        sessions_per_user = sessions / total_users if total_users > 0 else 0.0
        avg_duration = g["weighted_duration"] / sessions if sessions > 0 else 0.0

        result.append({
            "date": dt,
            "sessionDefaultChannelGroup": channel,
            "sessionSource": source,
            "sessionMedium": medium,
            "sessions": sessions,
            "totalUsers": total_users,
            "newUsers": g["newUsers"],
            "screenPageViews": g["screenPageViews"],
            "engagedSessions": g["engagedSessions"],
            "engagementRate": engagement_rate,
            "averageSessionDuration": avg_duration,
            "eventCount": g["eventCount"],
            "bounceRate": bounce_rate,
            "sessionsPerUser": sessions_per_user,
        })

    return result


# ══════════════════════════════════════════════════════════════════════
# DISCOVER LEAKAGE CORRECTION
# ══════════════════════════════════════════════════════════════════════

# Google Discover traffic in the Google app often arrives without a referrer
# header, causing GA4 to classify it as "Direct". Daily correlation between
# GSC Discover clicks and GA4 Direct sessions is 0.97 across 475 days.
# Approximately 74% of Discover clicks end up misclassified as Direct.
#
# This correction runs AFTER both GA4 and GSC data are in the DB. It:
#   1. Reads daily Discover clicks from gsc_discover
#   2. Calculates a baseline Direct level from low-Discover days
#   3. Attributes the excess Direct above baseline to "Discover (estimated)"
#   4. Caps reclassification at min(excess, clicks × 0.74, 95% of Direct)
#
# The correction is applied to the traffic table in-place, mirroring how
# the UOL referral correction works upstream.

DISCOVER_LEAKAGE_RATIO = 0.74  # Empirical: 74% of Discover clicks → Direct
DISCOVER_MIN_CLICKS = 50       # Don't bother below this threshold
DISCOVER_MAX_DIRECT_FRACTION = 0.95  # Always leave some real Direct
DISCOVER_LOW_CLICKS_THRESHOLD = 200  # "Low Discover" for baseline calc

def correct_discover_leakage(conn: sqlite3.Connection) -> int:
    """
    Reclassify Direct sessions attributable to Google Discover leakage.
    Reads gsc_discover and traffic tables, creates 'Discover (estimated)' rows.
    Returns number of sessions reclassified.
    """
    cur = conn.cursor()

    # Check if gsc_discover has data
    cur.execute("SELECT COUNT(*) FROM gsc_discover")
    if cur.fetchone()[0] == 0:
        print("  No Discover data — skipping Discover leakage correction")
        return 0

    # Step 1: Daily Discover clicks
    cur.execute("SELECT date, SUM(clicks) FROM gsc_discover GROUP BY date")
    discover_daily = {r[0]: r[1] for r in cur.fetchall()}

    # Step 2: Daily Direct sessions from traffic
    # traffic.date is YYYYMMDD, gsc_discover.date is YYYY-MM-DD
    cur.execute("""
        SELECT rowid, date, sessions, totalUsers, newUsers, screenPageViews,
               engagedSessions, averageSessionDuration, eventCount
        FROM traffic
        WHERE sessionDefaultChannelGroup = 'Direct'
        ORDER BY date
    """)
    direct_rows = cur.fetchall()

    # Aggregate Direct per day (usually 1 row/day, but safe to aggregate)
    direct_by_day = {}
    for r in direct_rows:
        d = f"{r[1][:4]}-{r[1][4:6]}-{r[1][6:]}"
        direct_by_day.setdefault(d, 0)
        direct_by_day[d] += r[2]

    # Overlapping dates
    overlap = sorted(set(discover_daily.keys()) & set(direct_by_day.keys()))
    if not overlap:
        print("  No overlapping dates between Discover and Direct — skipping")
        return 0

    # Step 3: Baseline Direct (median of low-Discover days)
    low_disc = [direct_by_day[d] for d in overlap
                if discover_daily.get(d, 0) < DISCOVER_LOW_CLICKS_THRESHOLD]
    if not low_disc:
        print("  Cannot compute baseline (no low-Discover days) — skipping")
        return 0
    baseline = statistics.median(low_disc)

    # Step 4: Calculate proportions per day
    day_props = {}
    for d in overlap:
        disc = discover_daily.get(d, 0)
        if disc < DISCOVER_MIN_CLICKS:
            continue
        dir_sess = direct_by_day[d]
        if dir_sess <= 0:
            continue
        estimated = disc * DISCOVER_LEAKAGE_RATIO
        excess = max(0, dir_sess - baseline)
        reclassify = min(excess, estimated, dir_sess * DISCOVER_MAX_DIRECT_FRACTION)
        if reclassify < 10:
            continue
        day_props[d] = reclassify / dir_sess

    if not day_props:
        print("  No days qualify for Discover correction")
        return 0

    # Step 5: Remove any existing Discover (estimated) rows for these dates
    # Convert YYYY-MM-DD back to YYYYMMDD for SQL
    raw_dates = [d.replace("-", "") for d in day_props.keys()]
    cur.execute("DELETE FROM traffic WHERE sessionDefaultChannelGroup = 'Discover (estimated)'")
    deleted = cur.rowcount

    # Step 6: For each Direct row on a qualifying day, split it
    updates = []
    inserts = []
    total_moved = 0

    for r in direct_rows:
        rowid = r[0]
        date_raw = r[1]
        d = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:]}"
        if d not in day_props:
            continue

        prop = day_props[d]
        sess, users, new, pv, eng, dur, evts = r[2], r[3], r[4], r[5], r[6], r[7], r[8]

        move_sess = min(round(sess * prop), sess - 1)
        if move_sess < 1:
            continue

        move_users = round(users * prop)
        move_new = round(new * prop)
        move_pv = round(pv * prop)
        move_eng = round(eng * prop)
        move_evts = round(evts * prop)

        rem_sess = sess - move_sess
        rem_users = users - move_users
        rem_new = new - move_new
        rem_pv = pv - move_pv
        rem_eng = eng - move_eng
        rem_evts = evts - move_evts

        rem_eng_rate = rem_eng / rem_sess if rem_sess > 0 else 0
        rem_bounce = 1.0 - rem_eng_rate
        rem_spu = rem_sess / rem_users if rem_users > 0 else 0

        disc_eng_rate = move_eng / move_sess if move_sess > 0 else 0
        disc_bounce = 1.0 - disc_eng_rate
        disc_spu = move_sess / move_users if move_users > 0 else 0

        updates.append((
            rem_sess, rem_users, rem_new, rem_pv, rem_eng,
            rem_eng_rate, dur, rem_evts, rem_bounce, rem_spu,
            rowid
        ))
        inserts.append((
            date_raw, 'Discover (estimated)', 'google', 'discover',
            move_sess, move_users, move_new, move_pv, move_eng,
            disc_eng_rate, dur, move_evts, disc_bounce, disc_spu
        ))
        total_moved += move_sess

    # Execute
    cur.executemany("""
        UPDATE traffic SET
            sessions = ?, totalUsers = ?, newUsers = ?, screenPageViews = ?,
            engagedSessions = ?, engagementRate = ?, averageSessionDuration = ?,
            eventCount = ?, bounceRate = ?, sessionsPerUser = ?
        WHERE rowid = ?
    """, updates)

    cur.executemany("""
        INSERT INTO traffic (date, sessionDefaultChannelGroup, sessionSource, sessionMedium,
            sessions, totalUsers, newUsers, screenPageViews, engagedSessions,
            engagementRate, averageSessionDuration, eventCount, bounceRate, sessionsPerUser)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, inserts)

    conn.commit()

    if deleted > 0:
        print(f"  Cleared {deleted} old Discover (estimated) rows")
    print(f"  Discover leakage: {len(day_props)} days, {total_moved:,} sessions reclassified")
    print(f"  Baseline Direct: {baseline:.0f} sessions/day, leakage ratio: {DISCOVER_LEAKAGE_RATIO}")

    return total_moved


# ══════════════════════════════════════════════════════════════════════
# GSC API CALLS
# ══════════════════════════════════════════════════════════════════════

def pull_gsc_table(
    gsc_service,
    table_name: str,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """
    Pull GSC data for a given table (gsc_web_search or gsc_discover).
    Handles pagination — the API returns max 25,000 rows per request.
    """
    tdef = GSC_TABLE_DEFS[table_name]
    search_type = tdef["search_type"]
    dimensions = tdef["dimensions"]

    all_rows = []
    start_row = 0

    while True:
        request_body = {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
            "dimensions": dimensions,
            "searchType": search_type,
            "rowLimit": GSC_ROW_LIMIT,
            "startRow": start_row,
            "dataState": GSC_DATA_STATE,
        }

        response = gsc_service.searchanalytics().query(
            siteUrl=GSC_SITE_URL, body=request_body
        ).execute()

        rows = response.get("rows", [])
        if not rows:
            break

        for row in rows:
            record = {}
            keys = row.get("keys", [])
            for i, dim in enumerate(dimensions):
                record[dim] = keys[i] if i < len(keys) else ""
            record["clicks"] = int(row.get("clicks", 0))
            record["impressions"] = int(row.get("impressions", 0))
            record["ctr"] = float(row.get("ctr", 0.0))
            if "position" in row:
                record["position"] = float(row.get("position", 0.0))
            all_rows.extend([record])

        # If we got fewer rows than the limit, we've reached the end
        if len(rows) < GSC_ROW_LIMIT:
            break

        start_row += len(rows)
        time.sleep(0.3)  # be polite to the API

    return all_rows


def filter_top_queries(rows: list[dict], top_n: int = GSC_TOP_QUERIES) -> list[dict]:
    """
    Keep only the daily rows belonging to the top N queries per month (by clicks).

    Works on gsc_web_search rows (which have a 'query' key).
    Groups by month (YYYY-MM from the date field) + query, sums clicks,
    ranks, then keeps all daily rows for the winning queries.
    """
    if not rows:
        return rows

    # 1. Sum clicks per month+query
    month_query_clicks: dict[tuple[str, str], int] = defaultdict(int)
    for r in rows:
        month = r["date"][:7]  # YYYY-MM
        month_query_clicks[(month, r["query"])] += r["clicks"]

    # 2. Rank within each month, collect winning queries
    months: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for (month, query), clicks in month_query_clicks.items():
        months[month].append((query, clicks))

    winning_queries: dict[str, set[str]] = {}
    for month, entries in months.items():
        entries.sort(key=lambda x: x[1], reverse=True)
        winning_queries[month] = {q for q, _ in entries[:top_n]}

    # 3. Filter: keep only rows whose month+query is in the winning set
    kept = [r for r in rows if r["query"] in winning_queries.get(r["date"][:7], set())]
    return kept


# ══════════════════════════════════════════════════════════════════════
# CSV I/O
# ══════════════════════════════════════════════════════════════════════

def write_csv(rows: list[dict], filepath: Path, fieldnames: list[str]) -> int:
    """Write rows to CSV. Returns row count. Verifies after write."""
    if not rows:
        print(f"  (no data, skipping {filepath.name})")
        return 0

    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        saved_count = sum(1 for _ in reader) - 1

    if saved_count != len(rows):
        raise RuntimeError(
            f"CSV VERIFICATION FAILED: wrote {len(rows)} rows but file has {saved_count}. "
            f"File: {filepath}"
        )

    print(f"  ✓ {filepath.name} — {saved_count} rows (verified)")
    return saved_count


# ══════════════════════════════════════════════════════════════════════
# DATABASE — MASTER (ga4_reset.db)
# ══════════════════════════════════════════════════════════════════════

def init_db(db_path: Path) -> sqlite3.Connection:
    """Initialize the master database with all GA4 + GSC tables."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    # GA4 tables
    for table_name, tdef in GA4_TABLE_DEFS.items():
        conn.execute(tdef["create_sql"])
        for idx_sql in tdef["indexes"]:
            conn.execute(idx_sql)

    # GSC tables
    for table_name, tdef in GSC_TABLE_DEFS.items():
        conn.execute(tdef["create_sql"])
        for idx_sql in tdef["indexes"]:
            conn.execute(idx_sql)

    conn.commit()
    return conn


def delete_date_range(conn: sqlite3.Connection, table_name: str, start: date, end: date) -> int:
    """Delete rows in a date range so we can cleanly replace them."""
    # GA4 dates are YYYYMMDD format, GSC dates are YYYY-MM-DD format
    if table_name.startswith("gsc_"):
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
    else:
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")

    cursor = conn.execute(
        f"DELETE FROM {table_name} WHERE date >= ? AND date <= ?",
        (start_str, end_str),
    )
    conn.commit()
    return cursor.rowcount


def upsert_ga4_rows(conn: sqlite3.Connection, table_name: str, rows: list[dict]):
    """Upsert rows into a GA4 table."""
    if not rows:
        return

    tdef = GA4_TABLE_DEFS[table_name]
    columns = tdef["dimensions"] + tdef["metrics"]
    placeholders = ", ".join(["?"] * len(columns))
    col_names = ", ".join(columns)
    sql = f"INSERT OR REPLACE INTO {table_name} ({col_names}) VALUES ({placeholders})"

    data = []
    for row in rows:
        values = []
        for col in columns:
            val = row.get(col)
            if val is None or val == "":
                values.append(None)
            elif col in INTEGER_METRICS:
                values.append(int(float(val)))
            elif col in tdef["metrics"]:
                values.append(float(val))
            else:
                values.append(str(val))
        data.append(values)

    conn.executemany(sql, data)
    conn.commit()


def upsert_gsc_rows(conn: sqlite3.Connection, table_name: str, rows: list[dict]):
    """Upsert rows into a GSC table."""
    if not rows:
        return

    tdef = GSC_TABLE_DEFS[table_name]
    # Columns: dimensions + metrics (clicks, impressions, ctr, and optionally position)
    if table_name == "gsc_web_search":
        columns = tdef["dimensions"] + ["clicks", "impressions", "ctr", "position"]
    else:  # gsc_discover — no position column
        columns = tdef["dimensions"] + ["clicks", "impressions", "ctr"]

    placeholders = ", ".join(["?"] * len(columns))
    col_names = ", ".join(columns)
    sql = f"INSERT OR REPLACE INTO {table_name} ({col_names}) VALUES ({placeholders})"

    data = []
    for row in rows:
        values = [row.get(col) for col in columns]
        data.append(values)

    conn.executemany(sql, data)
    conn.commit()


# ══════════════════════════════════════════════════════════════════════
# DATABASE — VERIFICATION
# ══════════════════════════════════════════════════════════════════════

def verify_db(db_path: Path, expected_counts: dict[str, int]) -> bool:
    """Reopen the database from disk and verify row counts and integrity."""
    if not db_path.exists():
        print(f"\n✗ VERIFICATION FAILED: database file does not exist at {db_path}")
        return False

    file_size = os.path.getsize(db_path)
    if file_size == 0:
        print(f"\n✗ VERIFICATION FAILED: database file is 0 bytes at {db_path}")
        return False

    try:
        conn = sqlite3.connect(str(db_path))
        result = conn.execute("PRAGMA integrity_check").fetchone()
        if result[0] != "ok":
            print(f"\n✗ VERIFICATION FAILED: integrity_check returned '{result[0]}'")
            conn.close()
            return False

        all_ok = True
        print(f"\n{'='*60}")
        print(f"DATABASE VERIFICATION — {db_path.name} ({file_size:,} bytes)")
        print(f"{'='*60}")

        for table_name, expected in expected_counts.items():
            actual = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            min_date = conn.execute(f"SELECT MIN(date) FROM {table_name}").fetchone()[0]
            max_date = conn.execute(f"SELECT MAX(date) FROM {table_name}").fetchone()[0]

            if actual >= expected:
                status = "✓"
            else:
                status = "✗ MISMATCH"
                all_ok = False

            print(f"  {status} {table_name:18s} — {actual:>9,} rows  (expected ≥ {expected:,})  [{min_date} → {max_date}]")

        conn.close()

        if all_ok:
            print(f"\n  All tables verified ✓")
        else:
            print(f"\n  ⚠ SOME TABLES HAVE FEWER ROWS THAN EXPECTED — check above")

        return all_ok

    except Exception as e:
        print(f"\n✗ VERIFICATION FAILED: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════
# SAFE COPY
# ══════════════════════════════════════════════════════════════════════

def safe_copy(src: Path, dst: Path):
    """Copy with size verification. Keeps src intact on failure."""
    src_size = os.path.getsize(src)
    if src_size == 0:
        raise RuntimeError(f"Source database is 0 bytes: {src}")

    if dst.exists():
        backup = dst.with_suffix(".db.bak")
        shutil.copy2(dst, backup)
        print(f"  Backed up existing DB to {backup.name}")

    shutil.copy2(src, dst)
    dst_size = os.path.getsize(dst)

    if dst_size != src_size:
        raise RuntimeError(
            f"COPY FAILED: source is {src_size:,} bytes but destination is {dst_size:,} bytes.\n"
            f"Good database is at: {src}\n"
            f"Recover: cp '{src}' '{dst}'"
        )

    print(f"  Database saved to {dst} ({dst_size:,} bytes) ✓")


# ══════════════════════════════════════════════════════════════════════
# ANALYSIS DB (ga4_analysis.db) — rebuilt from scratch each run
# ══════════════════════════════════════════════════════════════════════

ANALYSIS_TABLES = {
    # ── GA4 rollups ──
    "monthly_traffic_by_channel": """
        CREATE TABLE monthly_traffic_by_channel AS
        SELECT
            substr(date, 1, 6) AS month,
            sessionDefaultChannelGroup AS channel,
            SUM(sessions) AS sessions,
            SUM(totalUsers) AS users,
            SUM(engagedSessions) AS engaged_sessions,
            ROUND(1.0 * SUM(engagedSessions) / NULLIF(SUM(sessions), 0), 4) AS engagement_rate,
            ROUND(1.0 - 1.0 * SUM(engagedSessions) / NULLIF(SUM(sessions), 0), 4) AS bounce_rate,
            SUM(screenPageViews) AS pageviews,
            SUM(newUsers) AS new_users
        FROM traffic
        GROUP BY month, channel
        ORDER BY month DESC, sessions DESC
    """,

    "monthly_content_top100": """
        CREATE TABLE monthly_content_top100 AS
        SELECT month, pagePath, pageTitle, pageviews, sessions, users,
               engaged_sessions, engagement_duration_sec
        FROM (
            SELECT
                substr(date, 1, 6) AS month,
                pagePath,
                MAX(pageTitle) AS pageTitle,
                SUM(screenPageViews) AS pageviews,
                SUM(sessions) AS sessions,
                SUM(totalUsers) AS users,
                SUM(engagedSessions) AS engaged_sessions,
                ROUND(SUM(userEngagementDuration), 1) AS engagement_duration_sec,
                ROW_NUMBER() OVER (PARTITION BY substr(date, 1, 6) ORDER BY SUM(screenPageViews) DESC) AS rn
            FROM content
            GROUP BY substr(date, 1, 6), pagePath
        )
        WHERE rn <= 100
        ORDER BY month DESC, pageviews DESC
    """,

    "monthly_engagement_by_device": """
        CREATE TABLE monthly_engagement_by_device AS
        SELECT
            substr(date, 1, 6) AS month,
            deviceCategory,
            newVsReturning,
            SUM(sessions) AS sessions,
            SUM(totalUsers) AS users,
            SUM(engagedSessions) AS engaged_sessions,
            ROUND(1.0 * SUM(engagedSessions) / NULLIF(SUM(sessions), 0), 4) AS engagement_rate,
            SUM(screenPageViews) AS pageviews
        FROM timing
        GROUP BY month, deviceCategory, newVsReturning
        ORDER BY month DESC, sessions DESC
    """,

    "weekly_traffic_recent": """
        CREATE TABLE weekly_traffic_recent AS
        SELECT
            -- ISO week: date is YYYYMMDD, convert to YYYY-Www
            substr(date, 1, 4) || '-W' ||
                CASE
                    WHEN length(CAST(strftime('%W',
                        substr(date,1,4)||'-'||substr(date,5,2)||'-'||substr(date,7,2)
                    ) AS TEXT)) = 1
                    THEN '0' || strftime('%W',
                        substr(date,1,4)||'-'||substr(date,5,2)||'-'||substr(date,7,2))
                    ELSE strftime('%W',
                        substr(date,1,4)||'-'||substr(date,5,2)||'-'||substr(date,7,2))
                END AS week,
            sessionDefaultChannelGroup AS channel,
            SUM(sessions) AS sessions,
            SUM(totalUsers) AS users,
            SUM(engagedSessions) AS engaged_sessions,
            SUM(screenPageViews) AS pageviews
        FROM traffic
        WHERE date >= strftime('%Y%m%d', 'now', '-90 days')
        GROUP BY week, channel
        ORDER BY week DESC, sessions DESC
    """,

    # ── GSC rollups ──
    "monthly_gsc_web_by_page": """
        CREATE TABLE monthly_gsc_web_by_page AS
        SELECT
            substr(date, 1, 7) AS month,
            page,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            ROUND(1.0 * SUM(clicks) / NULLIF(SUM(impressions), 0), 4) AS ctr,
            ROUND(1.0 * SUM(impressions * position) / NULLIF(SUM(impressions), 0), 1) AS avg_position
        FROM gsc_web_search
        GROUP BY month, page
        ORDER BY month DESC, clicks DESC
    """,

    "monthly_gsc_discover_by_page": """
        CREATE TABLE monthly_gsc_discover_by_page AS
        SELECT
            substr(date, 1, 7) AS month,
            page,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            ROUND(1.0 * SUM(clicks) / NULLIF(SUM(impressions), 0), 4) AS ctr
        FROM gsc_discover
        GROUP BY month, page
        ORDER BY month DESC, clicks DESC
    """,

    "monthly_gsc_top_queries": """
        CREATE TABLE monthly_gsc_top_queries AS
        SELECT * FROM (
            SELECT
                substr(date, 1, 7) AS month,
                query,
                SUM(clicks) AS clicks,
                SUM(impressions) AS impressions,
                ROUND(1.0 * SUM(clicks) / NULLIF(SUM(impressions), 0), 4) AS ctr,
                ROUND(1.0 * SUM(impressions * position) / NULLIF(SUM(impressions), 0), 1) AS avg_position,
                ROW_NUMBER() OVER (PARTITION BY substr(date, 1, 7) ORDER BY SUM(clicks) DESC) AS rn
            FROM gsc_web_search
            GROUP BY month, query
        )
        WHERE rn <= 200
        ORDER BY month DESC, clicks DESC
    """,

    "weekly_gsc_totals_recent": """
        CREATE TABLE weekly_gsc_totals_recent AS
        SELECT
            -- GSC dates are YYYY-MM-DD so strftime works directly
            strftime('%Y-W%W', date) AS week,
            'web' AS search_type,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            ROUND(1.0 * SUM(clicks) / NULLIF(SUM(impressions), 0), 4) AS ctr
        FROM gsc_web_search
        WHERE date >= strftime('%Y-%m-%d', 'now', '-90 days')
        GROUP BY week

        UNION ALL

        SELECT
            strftime('%Y-W%W', date) AS week,
            'discover' AS search_type,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            ROUND(1.0 * SUM(clicks) / NULLIF(SUM(impressions), 0), 4) AS ctr
        FROM gsc_discover
        WHERE date >= strftime('%Y-%m-%d', 'now', '-90 days')
        GROUP BY week

        ORDER BY week DESC, search_type
    """,
}


def build_analysis_db(master_db_path: Path, analysis_db_path: Path, has_gsc: bool = True):
    """
    Rebuild ga4_analysis.db from scratch by querying the master DB.
    Attaches master DB, creates rollup tables via CREATE TABLE ... AS SELECT.
    """
    print(f"\n{'─'*60}")
    print(f"BUILDING ANALYSIS DB")
    print(f"{'─'*60}")

    # Delete old analysis DB if it exists
    if analysis_db_path.exists():
        os.remove(analysis_db_path)
        print(f"  Removed old {analysis_db_path.name}")

    conn = sqlite3.connect(str(analysis_db_path))
    conn.execute(f"ATTACH DATABASE '{master_db_path}' AS master")

    tables_built = 0
    for table_name, create_sql in ANALYSIS_TABLES.items():
        # Skip GSC rollup tables if we don't have GSC data
        if not has_gsc and "gsc_" in table_name:
            print(f"  Skipping {table_name} (no GSC data)")
            continue

        try:
            # Replace bare table references with master.table
            # The CREATE TABLE ... AS SELECT queries reference source tables without schema
            # We need to prefix them with "master." so they read from the attached DB
            adjusted_sql = create_sql
            for src_table in list(GA4_TABLE_DEFS.keys()) + list(GSC_TABLE_DEFS.keys()):
                # Replace "FROM table" and "JOIN table" patterns
                adjusted_sql = adjusted_sql.replace(f"FROM {src_table}", f"FROM master.{src_table}")
                adjusted_sql = adjusted_sql.replace(f"JOIN {src_table}", f"JOIN master.{src_table}")

            conn.execute(adjusted_sql)
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  ✓ {table_name} — {row_count:,} rows")
            tables_built += 1
        except Exception as e:
            print(f"  ✗ {table_name} — FAILED: {e}")

    conn.execute("DETACH DATABASE master")
    conn.execute("VACUUM")
    conn.close()

    file_size = os.path.getsize(analysis_db_path)
    print(f"\n  Analysis DB: {analysis_db_path.name} ({file_size:,} bytes, {tables_built} tables) ✓")


# ══════════════════════════════════════════════════════════════════════
# CLOUD MODE
# ══════════════════════════════════════════════════════════════════════
# Cloud mode is the path GitHub Actions runs daily. It NEVER touches the
# master DB (ga4_reset.db). Instead:
#   1. Pull current-month-to-date (or --start/--end window) from GA4+GSC.
#   2. Build a tiny in-memory SQLite holding ONLY the pulled rows, with
#      the same schema as the master DB.
#   3. Run the same UOL correction + Discover leakage correction logic
#      against that in-memory DB.
#   4. Open the COMMITTED analysis DB (ga4_analysis.db, ~1.7 MB). For each
#      rollup table, delete rows in the affected window (current month for
#      monthly rollups; recent 90d for weekly recent), then INSERT new
#      rollup rows computed from the in-memory pull.
#   5. Save the updated analysis DB. GitHub Actions commits it back.
# Historical rollup rows from previous days/months are left untouched —
# the analysis DB itself is the system of record in CI.

def run_cloud_mode(args) -> None:
    """
    Cloud-mode entry point. Pulls a date window, merges into the committed
    analysis DB without ever touching the master DB.
    """
    # ── Determine window ──
    if args.start and args.end:
        win_start = date.fromisoformat(args.start)
        win_end = date.fromisoformat(args.end)
    else:
        # Default: current month-to-date. GA4 may revise recent days for late-
        # arriving attribution, so always re-pull the entire current month.
        win_start, win_end = current_month_range()

    # Clamp to property starts
    ga4_start = max(win_start, GA4_PROPERTY_START)
    gsc_start = max(win_start, GSC_PROPERTY_START)
    ga4_end = gsc_end = win_end

    do_ga4 = not args.gsc_only
    do_gsc = not args.ga4_only

    print(f"\n{'='*60}")
    print(f"CLOUD MODE — Reset Analytics daily refresh")
    print(f"{'='*60}")
    print(f"  Window: {win_start} → {win_end}")
    print(f"  GA4: {'YES' if do_ga4 else 'NO'} ({ga4_start} → {ga4_end})")
    print(f"  GSC: {'YES' if do_gsc else 'NO'} ({gsc_start} → {gsc_end})")
    print(f"  Master DB: SKIPPED (cloud mode never touches it)")
    print(f"  Analysis DB: {ANALYSIS_DB_PATH}")

    if args.dry_run:
        print(f"\n  DRY RUN — no API calls or writes.")
        return

    # ── Build an in-memory DB with the master schema, populate from API ──
    mem_conn = sqlite3.connect(":memory:")
    mem_conn.execute("PRAGMA journal_mode=MEMORY")
    for tdef in GA4_TABLE_DEFS.values():
        mem_conn.execute(tdef["create_sql"])
        for idx_sql in tdef["indexes"]:
            mem_conn.execute(idx_sql)
    for tdef in GSC_TABLE_DEFS.values():
        mem_conn.execute(tdef["create_sql"])
        for idx_sql in tdef["indexes"]:
            mem_conn.execute(idx_sql)
    mem_conn.commit()

    ga4_chunks = monthly_chunks(ga4_start, ga4_end) if do_ga4 else []
    gsc_chunks = monthly_chunks(gsc_start, gsc_end) if do_gsc else []

    # ── GA4 pull → in-memory ──
    if do_ga4:
        creds = get_ga4_credentials()
        ga4_client = BetaAnalyticsDataClient(credentials=creds)
        print(f"\nGA4 authenticated ✓")

        print(f"\n{'─'*60}\nTRAFFIC (with corrections)\n{'─'*60}")
        all_corrected = []
        for cs, ce in ga4_chunks:
            label = cs.strftime("%Y-%m")
            print(f"  Pulling {label} ({cs} → {ce})...")
            raw_rows = pull_traffic_with_referrer(ga4_client, cs, ce)
            raw_file = RAW_CSV_DIR / f"ga4_{label}.csv"
            write_csv(raw_rows, raw_file, TRAFFIC_PULL_DIMENSIONS + TRAFFIC_METRICS)
            corrected, n_reclass = correct_traffic_rows(raw_rows)
            aggregated = aggregate_traffic(corrected)
            corrected_file = CORRECTED_CSV_DIR / f"ga4_{label}.csv"
            write_csv(aggregated, corrected_file, TRAFFIC_FINAL_DIMENSIONS + TRAFFIC_METRICS)
            print(f"    Raw {len(raw_rows)} → corrected {len(aggregated)} (reclassified {n_reclass})")
            all_corrected.extend(aggregated)
            time.sleep(0.5)
        upsert_ga4_rows(mem_conn, "traffic", all_corrected)

        for table_name in ["content", "timing", "events", "campaigns"]:
            print(f"\n{'─'*60}\n{table_name.upper()}\n{'─'*60}")
            all_rows = []
            for cs, ce in ga4_chunks:
                label = cs.strftime("%Y-%m")
                print(f"  Pulling {label}...", end=" ", flush=True)
                rows = pull_ga4_table(ga4_client, table_name, cs, ce)
                all_rows.extend(rows)
                print(f"{len(rows)} rows")
                time.sleep(0.5)
            upsert_ga4_rows(mem_conn, table_name, all_rows)

    # ── GSC pull → in-memory ──
    gsc_succeeded = False
    if do_gsc:
        try:
            gsc_service = get_gsc_service()
            print(f"\nGSC authenticated ✓")
            for table_name in ["gsc_web_search", "gsc_discover"]:
                tdef = GSC_TABLE_DEFS[table_name]
                print(f"\n{'─'*60}\n{table_name.upper()} ({tdef['search_type']})\n{'─'*60}")
                all_rows = []
                for cs, ce in gsc_chunks:
                    label = cs.strftime("%Y-%m")
                    print(f"  Pulling {label}...", end=" ", flush=True)
                    rows = pull_gsc_table(gsc_service, table_name, cs, ce)
                    all_rows.extend(rows)
                    print(f"{len(rows)} rows")
                    time.sleep(0.5)
                if table_name == "gsc_web_search":
                    raw_n = len(all_rows)
                    all_rows = filter_top_queries(all_rows, GSC_TOP_QUERIES)
                    print(f"  Top-{GSC_TOP_QUERIES} filter: {raw_n:,} → {len(all_rows):,}")
                upsert_gsc_rows(mem_conn, table_name, all_rows)
            gsc_succeeded = True
        except Exception as e:
            print(f"\n  ⚠ GSC pull failed: {e}\n  Continuing with GA4 only.")

    # ── Discover leakage correction (operates on the in-memory DB) ──
    if gsc_succeeded:
        print(f"\n{'─'*60}\nDISCOVER LEAKAGE CORRECTION\n{'─'*60}")
        correct_discover_leakage(mem_conn)

    # ── Merge: update affected rollup rows in the committed analysis DB ──
    print(f"\n{'─'*60}\nMERGING INTO ANALYSIS DB\n{'─'*60}")
    _merge_rollups_into_analysis_db(mem_conn, ANALYSIS_DB_PATH, win_start, win_end, do_ga4, gsc_succeeded)

    mem_conn.close()

    print(f"\n{'='*60}\nCLOUD MODE DONE ✓\n{'='*60}")
    print(f"  Window: {win_start} → {win_end}")
    print(f"  Analysis DB: {ANALYSIS_DB_PATH} ({os.path.getsize(ANALYSIS_DB_PATH):,} bytes)")


def _merge_rollups_into_analysis_db(
    mem_conn: sqlite3.Connection,
    analysis_db_path: Path,
    win_start: date,
    win_end: date,
    do_ga4: bool,
    do_gsc: bool,
) -> None:
    """
    Recompute the rollup rows that fall inside the pulled window and write
    them into the committed analysis DB, replacing any existing rows in the
    same window. Older rollup rows are left untouched.

    Strategy: for each rollup, identify the date partition (month or week),
    compute new rows from the in-memory raw data, DELETE matching partitions
    in the analysis DB, INSERT the new rows. Atomic via a .tmp file rename.
    """
    if not analysis_db_path.exists():
        sys.exit(
            f"Analysis DB not found at {analysis_db_path}. In cloud mode the\n"
            f"analysis DB must already exist (committed in the repo). For first-\n"
            f"time setup, do a local --backfill run, commit ga4_analysis.db,\n"
            f"then enable the cloud workflow."
        )

    # Work on a temp copy so an error doesn't leave a half-written DB
    tmp_path = analysis_db_path.with_suffix(".db.tmp")
    shutil.copy2(analysis_db_path, tmp_path)

    # Affected partitions
    win_months = sorted({d.strftime("%Y%m") for d in _daterange(win_start, win_end)})
    win_months_dashed = sorted({d.strftime("%Y-%m") for d in _daterange(win_start, win_end)})

    out_conn = sqlite3.connect(str(tmp_path))
    out_conn.execute(f"ATTACH DATABASE ':memory:' AS mem")
    # Copy the in-memory tables into mem schema of out_conn so we can JOIN
    for src_table in list(GA4_TABLE_DEFS.keys()) + list(GSC_TABLE_DEFS.keys()):
        row_count = mem_conn.execute(f"SELECT COUNT(*) FROM {src_table}").fetchone()[0]
        if row_count == 0:
            continue
        # Re-create schema in attached mem
        if src_table in GA4_TABLE_DEFS:
            tdef = GA4_TABLE_DEFS[src_table]
        else:
            tdef = GSC_TABLE_DEFS[src_table]
        out_conn.execute(tdef["create_sql"].replace(
            f"CREATE TABLE IF NOT EXISTS {src_table}",
            f"CREATE TABLE mem.{src_table}"
        ))
        rows = mem_conn.execute(f"SELECT * FROM {src_table}").fetchall()
        cols = [d[0] for d in mem_conn.execute(f"SELECT * FROM {src_table} LIMIT 0").description]
        placeholders = ",".join(["?"] * len(cols))
        out_conn.executemany(
            f"INSERT INTO mem.{src_table} ({','.join(cols)}) VALUES ({placeholders})",
            rows,
        )

    out_conn.commit()

    # Re-run each rollup CREATE TABLE AS SELECT, but scoped to the affected
    # partitions and into a temp table; then DELETE old partition rows in the
    # real rollup table and INSERT the new ones.
    for table_name, create_sql in ANALYSIS_TABLES.items():
        # Skip rollups whose source table is empty in our pull
        source_table = _detect_source_table(create_sql)
        if source_table and source_table.startswith("gsc_") and not do_gsc:
            print(f"  Skipped {table_name} (no GSC data this run)")
            continue
        if source_table and not source_table.startswith("gsc_") and not do_ga4:
            print(f"  Skipped {table_name} (no GA4 data this run)")
            continue

        # Rewrite the CREATE TABLE AS SELECT to point at mem.* and produce a
        # temp table holding only the new rows
        adjusted = create_sql.replace(f"CREATE TABLE {table_name}", "CREATE TEMP TABLE _new")
        for src in list(GA4_TABLE_DEFS.keys()) + list(GSC_TABLE_DEFS.keys()):
            adjusted = adjusted.replace(f"FROM {src}", f"FROM mem.{src}")
            adjusted = adjusted.replace(f"JOIN {src}", f"JOIN mem.{src}")

        try:
            out_conn.execute("DROP TABLE IF EXISTS temp._new")
            out_conn.execute(adjusted)
            new_count = out_conn.execute("SELECT COUNT(*) FROM _new").fetchone()[0]

            # Delete old partition rows. Match the partition column shape.
            if "month" in [c[1] for c in out_conn.execute(f"PRAGMA table_info({table_name})").fetchall()]:
                # month column — figure out the format (YYYYMM vs YYYY-MM)
                sample = out_conn.execute(f"SELECT month FROM {table_name} LIMIT 1").fetchone()
                if sample and "-" in str(sample[0]):
                    partitions = win_months_dashed
                else:
                    partitions = win_months
                placeholders = ",".join(["?"] * len(partitions))
                deleted = out_conn.execute(
                    f"DELETE FROM {table_name} WHERE month IN ({placeholders})",
                    partitions,
                ).rowcount
            elif "week" in [c[1] for c in out_conn.execute(f"PRAGMA table_info({table_name})").fetchall()]:
                # Weekly rollups cover last 90 days — just replace the whole table
                deleted = out_conn.execute(f"DELETE FROM {table_name}").rowcount
            else:
                deleted = out_conn.execute(f"DELETE FROM {table_name}").rowcount

            # Insert new rows from _new
            cols_target = [c[1] for c in out_conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
            cols_new = [c[1] for c in out_conn.execute("PRAGMA table_info(_new)").fetchall()]
            common = [c for c in cols_target if c in cols_new]
            col_list = ",".join(common)
            out_conn.execute(f"INSERT INTO {table_name} ({col_list}) SELECT {col_list} FROM _new")
            print(f"  ✓ {table_name}: removed {deleted}, added {new_count}")
        except Exception as e:
            print(f"  ✗ {table_name} merge FAILED: {e}")

    out_conn.execute("DROP TABLE IF EXISTS temp._new")
    out_conn.commit()
    out_conn.execute("DETACH DATABASE mem")
    out_conn.close()

    # Atomic rename
    os.replace(tmp_path, analysis_db_path)


def _detect_source_table(create_sql: str) -> str:
    """Best-effort: pull the first 'FROM <table>' out of a CREATE TABLE AS SELECT."""
    import re
    m = re.search(r"FROM\s+(\w+)", create_sql)
    return m.group(1) if m else ""


def _daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d = d + timedelta(days=1)


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="GA4 + GSC unified pull, correction, and database update — Capital Reset"
    )
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--backfill", action="store_true",
        help="Full backfill from earliest data — use sparingly",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be pulled, no API calls",
    )
    parser.add_argument(
        "--ga4-only", action="store_true",
        help="Skip GSC pull (GA4 only)",
    )
    parser.add_argument(
        "--gsc-only", action="store_true",
        help="Skip GA4 pull (GSC only)",
    )
    parser.add_argument(
        "--no-analysis", action="store_true",
        help="Skip rebuilding ga4_analysis.db",
    )
    parser.add_argument(
        "--analysis-only", action="store_true",
        help="Skip all API pulls, only rebuild ga4_analysis.db from existing master DB",
    )
    parser.add_argument(
        "--cloud-mode", action="store_true",
        help=(
            "CI mode: do NOT touch the master DB. Pulls current month-to-date "
            "(or --start/--end window) from GA4+GSC, updates ONLY those date "
            "rows in the committed analysis DB, leaves historical rollup rows "
            "untouched. Designed for daily GitHub Actions runs."
        ),
    )
    args = parser.parse_args()

    # ── Cloud-mode dispatch ──
    # If --cloud-mode is set, skip the entire master-DB flow and run the
    # rollup-only path that GitHub Actions uses. Returns when done.
    if args.cloud_mode:
        return run_cloud_mode(args)

    do_ga4 = not args.gsc_only and not args.analysis_only
    do_gsc = not args.ga4_only and not args.analysis_only
    do_analysis = not args.no_analysis

    # ── Determine date ranges ──
    if args.start and args.end:
        ga4_start = date.fromisoformat(args.start)
        ga4_end = date.fromisoformat(args.end)
        gsc_start = ga4_start
        gsc_end = ga4_end
    elif args.backfill:
        ga4_start = GA4_PROPERTY_START
        ga4_end = date.today() - timedelta(days=1)
        gsc_start = GSC_PROPERTY_START
        gsc_end = date.today() - timedelta(days=1)
    else:
        ga4_start, ga4_end = current_month_range()
        gsc_start, gsc_end = current_month_range()

    # Clamp to property start dates
    if ga4_start < GA4_PROPERTY_START:
        ga4_start = GA4_PROPERTY_START
    if gsc_start < GSC_PROPERTY_START:
        gsc_start = GSC_PROPERTY_START

    ga4_chunks = monthly_chunks(ga4_start, ga4_end) if do_ga4 else []
    gsc_chunks = monthly_chunks(gsc_start, gsc_end) if do_gsc else []

    print(f"\n{'='*60}")
    print(f"GA4 + GSC UNIFIED UPDATE — Capital Reset")
    print(f"{'='*60}")
    if do_ga4:
        print(f"GA4 Property: {GA4_PROPERTY_ID}")
        print(f"GA4 Range: {ga4_start} → {ga4_end} ({len(ga4_chunks)} month(s))")
        print(f"GA4 Tables: traffic (with correction), content, timing, events, campaigns")
    else:
        print(f"GA4: SKIPPED (--gsc-only)")
    if do_gsc:
        print(f"GSC Property: {GSC_SITE_URL}")
        print(f"GSC Range: {gsc_start} → {gsc_end} ({len(gsc_chunks)} month(s))")
        print(f"GSC Tables: gsc_web_search, gsc_discover")
    else:
        print(f"GSC: SKIPPED (--ga4-only)")
    print(f"Analysis DB: {'YES' if do_analysis else 'SKIPPED'}")
    print(f"Database: {DB_PATH}")

    if args.dry_run:
        print(f"\nDRY RUN — no API calls or writes.\n")
        if do_ga4:
            print(f"GA4 months:")
            for s, e in ga4_chunks:
                print(f"  {s.strftime('%Y-%m')}: {s} → {e}")
        if do_gsc:
            print(f"GSC months:")
            for s, e in gsc_chunks:
                print(f"  {s.strftime('%Y-%m')}: {s} → {e}")
        return

    # ── Analysis-only shortcut: skip all API pulls, just rebuild analysis DB ──
    if args.analysis_only:
        if not DB_PATH.exists():
            print(f"\n⚠ Master DB not found at {DB_PATH} — nothing to analyze.")
            sys.exit(1)
        # Check if GSC tables have data
        check_conn = sqlite3.connect(DB_PATH)
        gsc_rows = check_conn.execute("SELECT COUNT(*) FROM gsc_web_search").fetchone()[0]
        check_conn.close()
        build_analysis_db(DB_PATH, ANALYSIS_DB_PATH, has_gsc=(gsc_rows > 0))
        print(f"\n{'='*60}")
        print(f"DONE ✓  (analysis-only)")
        print(f"{'='*60}")
        print(f"  Analysis DB: {ANALYSIS_DB_PATH.name}")
        return

    # ── Work on temp DB copy ──
    import tempfile
    tmp_dir = tempfile.mkdtemp(prefix="ga4_gsc_update_")
    tmp_db = Path(tmp_dir) / "ga4_reset.db"

    if DB_PATH.exists():
        shutil.copy2(DB_PATH, tmp_db)
        print(f"\nWorking on local copy of database")
    else:
        print(f"\nCreating new database")

    conn = init_db(tmp_db)
    expected_counts = {}
    gsc_succeeded = False

    try:
        # ══════════════════════════════════════════════════════════
        # PART 1: GA4
        # ══════════════════════════════════════════════════════════
        if do_ga4:
            creds = get_ga4_credentials()
            ga4_client = BetaAnalyticsDataClient(credentials=creds)
            print(f"\nGA4 authenticated ✓\n")

            # ── TRAFFIC ──
            print(f"{'─'*60}")
            print(f"TRAFFIC — pulling with pageReferrer for correction")
            print(f"{'─'*60}")

            all_corrected_rows = []
            total_reclassified = 0

            for chunk_start, chunk_end in ga4_chunks:
                label = chunk_start.strftime("%Y-%m")
                print(f"\n  Pulling {label} ({chunk_start} → {chunk_end})...")

                raw_rows = pull_traffic_with_referrer(ga4_client, chunk_start, chunk_end)
                print(f"    Raw: {len(raw_rows)} rows (with pageReferrer)")

                raw_file = RAW_CSV_DIR / f"ga4_{label}.csv"
                write_csv(raw_rows, raw_file, TRAFFIC_PULL_DIMENSIONS + TRAFFIC_METRICS)

                corrected_rows, num_reclassified = correct_traffic_rows(raw_rows)
                total_reclassified += num_reclassified
                if num_reclassified > 0:
                    print(f"    Reclassified: {num_reclassified} rows (Direct → Organic Referral)")

                aggregated = aggregate_traffic(corrected_rows)
                print(f"    Aggregated: {len(aggregated)} rows (final grain)")

                corrected_file = CORRECTED_CSV_DIR / f"ga4_{label}.csv"
                write_csv(aggregated, corrected_file, TRAFFIC_FINAL_DIMENSIONS + TRAFFIC_METRICS)

                all_corrected_rows.extend(aggregated)
                time.sleep(0.5)

            deleted = delete_date_range(conn, "traffic", ga4_start, ga4_end)
            print(f"\n  Cleared {deleted} existing traffic rows for {ga4_start} → {ga4_end}")
            upsert_ga4_rows(conn, "traffic", all_corrected_rows)
            print(f"  Inserted {len(all_corrected_rows)} corrected traffic rows")

            traffic_count = conn.execute("SELECT COUNT(*) FROM traffic").fetchone()[0]
            expected_counts["traffic"] = traffic_count
            print(f"  Traffic table total: {traffic_count:,} rows")
            print(f"  Total reclassified this run: {total_reclassified}")

            # ── OTHER GA4 TABLES ──
            for table_name in ["content", "timing", "events", "campaigns"]:
                print(f"\n{'─'*60}")
                print(f"{table_name.upper()}")
                print(f"{'─'*60}")

                all_rows = []
                for chunk_start, chunk_end in ga4_chunks:
                    label = chunk_start.strftime("%Y-%m")
                    print(f"  Pulling {label} ({chunk_start} → {chunk_end})...", end=" ", flush=True)
                    rows = pull_ga4_table(ga4_client, table_name, chunk_start, chunk_end)
                    all_rows.extend(rows)
                    print(f"{len(rows)} rows")
                    time.sleep(0.5)

                deleted = delete_date_range(conn, table_name, ga4_start, ga4_end)
                print(f"  Cleared {deleted} existing rows, inserting {len(all_rows)} new rows")
                upsert_ga4_rows(conn, table_name, all_rows)

                table_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                expected_counts[table_name] = table_count
                print(f"  {table_name} total: {table_count:,} rows")

        # ══════════════════════════════════════════════════════════
        # PART 2: GSC
        # ══════════════════════════════════════════════════════════
        if do_gsc:
            print(f"\n{'='*60}")
            print(f"GSC PULL")
            print(f"{'='*60}")

            try:
                gsc_service = get_gsc_service()
                print(f"GSC authenticated ✓\n")

                for table_name in ["gsc_web_search", "gsc_discover"]:
                    tdef = GSC_TABLE_DEFS[table_name]
                    print(f"{'─'*60}")
                    print(f"{table_name.upper()} ({tdef['search_type']})")
                    print(f"{'─'*60}")

                    all_rows = []
                    for chunk_start, chunk_end in gsc_chunks:
                        label = chunk_start.strftime("%Y-%m")
                        print(f"  Pulling {label} ({chunk_start} → {chunk_end})...", end=" ", flush=True)
                        rows = pull_gsc_table(gsc_service, table_name, chunk_start, chunk_end)
                        all_rows.extend(rows)
                        print(f"{len(rows)} rows")
                        time.sleep(0.5)

                    # For web search: discard long-tail queries, keep only top N per month
                    if table_name == "gsc_web_search":
                        raw_count = len(all_rows)
                        all_rows = filter_top_queries(all_rows, GSC_TOP_QUERIES)
                        print(f"  Top-{GSC_TOP_QUERIES} query filter: {raw_count:,} → {len(all_rows):,} rows ({raw_count - len(all_rows):,} discarded)")

                    deleted = delete_date_range(conn, table_name, gsc_start, gsc_end)
                    print(f"  Cleared {deleted} existing rows, inserting {len(all_rows):,} new rows")
                    upsert_gsc_rows(conn, table_name, all_rows)

                    table_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    expected_counts[table_name] = table_count
                    print(f"  {table_name} total: {table_count:,} rows")

                gsc_succeeded = True

            except Exception as e:
                print(f"\n  ⚠ GSC PULL FAILED: {e}")
                print(f"  Continuing with GA4 data only...")
                # Don't add GSC tables to expected_counts — they won't be verified

        # ══════════════════════════════════════════════════════════
        # PART 2b: DISCOVER LEAKAGE CORRECTION
        # ══════════════════════════════════════════════════════════
        # Runs after both GA4 traffic and GSC discover are in the DB.
        # Requires gsc_discover data to calculate leakage.
        if gsc_succeeded or conn.execute("SELECT COUNT(*) FROM gsc_discover").fetchone()[0] > 0:
            print(f"\n{'─'*60}")
            print(f"DISCOVER LEAKAGE CORRECTION (Direct → Discover (estimated))")
            print(f"{'─'*60}")
            discover_reclassified = correct_discover_leakage(conn)
            if discover_reclassified > 0:
                traffic_count = conn.execute("SELECT COUNT(*) FROM traffic").fetchone()[0]
                expected_counts["traffic"] = traffic_count
                print(f"  Traffic table total: {traffic_count:,} rows (with Discover correction)")

        # ══════════════════════════════════════════════════════════
        # PART 3: SAVE AND VERIFY MASTER DB
        # ══════════════════════════════════════════════════════════
        # Force WAL data into main file before copying — without this,
        # shutil.copy2 may copy only the .db file and miss unflushed
        # WAL data, resulting in missing tables at the destination.
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()

        print(f"\n{'─'*60}")
        print(f"SAVING MASTER DATABASE")
        print(f"{'─'*60}")
        safe_copy(tmp_db, DB_PATH)

        if not verify_db(DB_PATH, expected_counts):
            print(f"\n⚠ VERIFICATION FAILED!")
            print(f"  Good copy is still at: {tmp_db}")
            print(f"  Recover: cp '{tmp_db}' '{DB_PATH}'")
            print(f"  DO NOT delete {tmp_dir} until you've recovered.")
            sys.exit(1)

        # ══════════════════════════════════════════════════════════
        # PART 4: ANALYSIS DB
        # ══════════════════════════════════════════════════════════
        if do_analysis:
            build_analysis_db(DB_PATH, ANALYSIS_DB_PATH, has_gsc=gsc_succeeded)

        # Clean up temp dir only after everything succeeds
        shutil.rmtree(tmp_dir, ignore_errors=True)

        print(f"\n{'='*60}")
        print(f"DONE ✓")
        print(f"{'='*60}")
        if do_ga4:
            print(f"  GA4: {ga4_start} → {ga4_end}")
        if do_gsc:
            status = "✓" if gsc_succeeded else "FAILED (GA4 data saved)"
            print(f"  GSC: {gsc_start} → {gsc_end} — {status}")
        if do_analysis:
            print(f"  Analysis DB: {ANALYSIS_DB_PATH.name}")

    except Exception as e:
        print(f"\n{'='*60}")
        print(f"ERROR: {e}")
        print(f"{'='*60}")
        print(f"  Your data is safe in: {tmp_db}")
        print(f"  If the error was during save, recover with:")
        print(f"    cp '{tmp_db}' '{DB_PATH}'")
        print(f"  DO NOT delete {tmp_dir}")
        try:
            conn.close()
        except:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
