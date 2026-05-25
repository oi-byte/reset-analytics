"""
GA4 Weekly Pull — Unified pull, correction, and database update for Capital Reset.

One script to rule them all:
  1. Pull traffic data WITH pageReferrer (extra dimension)
  2. Reclassify misclassified Direct sessions (uol.com.br referrer → Organic Referral)
  3. Aggregate corrected rows back to channel/source/medium grain
  4. Save raw + corrected CSVs
  5. Pull content, timing, events, campaigns tables
  6. Upsert everything into ga4_reset.db
  7. Verify the database is intact

Strategy: replaces the full current month on each run (month-to-date).

Property: 352408538 (G-99N2LMR1EL)
OAuth credentials: Desktop app client (claude-ga-493021)

⚠  MUST BE RUN LOCALLY ON MAC — not from Cowork sandbox (binary writes fail).

Usage:
    # Normal weekly run (pulls current month-to-date):
    python3 ga4_weekly.py

    # Custom date range:
    python3 ga4_weekly.py --start 2026-04-01 --end 2026-04-10

    # Full backfill (Feb 2023 – yesterday) — use sparingly:
    python3 ga4_weekly.py --backfill

    # Dry run (shows what would be pulled, no API calls):
    python3 ga4_weekly.py --dry-run
"""

import argparse
import csv
import json
import os
import shutil
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

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
# CONFIG
# ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
LOCAL_GA_FOLDER = SCRIPT_DIR.parent / "Google Analytics"

def _env_path(name: str, default: Path) -> Path:
    val = os.environ.get(name)
    return Path(val) if val else default

# Headless mode: exit cleanly on OAuth refresh failure instead of opening browser
GA4_HEADLESS = os.environ.get("GA4_HEADLESS", "").lower() in ("1", "true", "yes")

PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "352408538")
SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

CLIENT_SECRET = _env_path(
    "GA4_CLIENT_SECRET_PATH",
    LOCAL_GA_FOLDER / "client_secret_141022466268-c5s8q4b5tql7rftko1h3vdcjtmr2aj59.apps.googleusercontent.com.json",
)
TOKEN_CACHE = _env_path("GA4_TOKEN_PATH", LOCAL_GA_FOLDER / "ga4_token.json")
DB_PATH = _env_path("GA4_DB_PATH", LOCAL_GA_FOLDER / "ga4_reset.db")
RAW_CSV_DIR = _env_path("GA4_RAW_CSV_DIR", LOCAL_GA_FOLDER / "API pulls")
CORRECTED_CSV_DIR = _env_path("GA4_CORRECTED_CSV_DIR", LOCAL_GA_FOLDER / "API pulls" / "corrected")

PROPERTY_START = date(2023, 2, 3)
ROW_LIMIT = 100_000

# ──────────────────────────────────────────────────────────────────────
# TRAFFIC PULL: dimensions include pageReferrer for correction
# ──────────────────────────────────────────────────────────────────────
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

# Metrics that can be summed when aggregating rows
SUMMABLE_METRICS = {
    "sessions", "totalUsers", "newUsers", "screenPageViews",
    "engagedSessions", "eventCount",
}

# Metrics that are rates — must be recomputed from summable components
RATE_METRICS = {
    "engagementRate",      # engagedSessions / sessions
    "averageSessionDuration",  # cannot recompute without total duration — see note
    "bounceRate",          # 1 - engagementRate
    "sessionsPerUser",     # sessions / totalUsers
}

# ──────────────────────────────────────────────────────────────────────
# OTHER TABLE DEFINITIONS (same as ga4_backfill.py)
# ──────────────────────────────────────────────────────────────────────
TABLE_DEFS = {
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


# ──────────────────────────────────────────────────────────────────────
# AUTH
# ──────────────────────────────────────────────────────────────────────
def get_credentials() -> Credentials:
    creds = None
    if TOKEN_CACHE.exists():
        with open(TOKEN_CACHE, "r") as f:
            token_data = json.load(f)
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        from google.auth.transport.requests import Request
        try:
            creds.refresh(Request())
            _save_token(creds)
            return creds
        except Exception as e:
            if GA4_HEADLESS:
                sys.exit(
                    f"GA4 token refresh failed in headless mode: {e}\n"
                    f"Re-seed the GA4_TOKEN_JSON GitHub Secret."
                )
            print(f"Token refresh failed ({e}), re-authenticating...")

    if GA4_HEADLESS:
        sys.exit(
            "GA4 auth requires interactive browser flow but GA4_HEADLESS is set.\n"
            "Re-seed the GA4_TOKEN_JSON GitHub Secret from a working local token."
        )

    if not CLIENT_SECRET.exists():
        sys.exit(f"Client secret not found at {CLIENT_SECRET}")

    flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET), SCOPES)
    creds = flow.run_local_server(port=0)
    _save_token(creds)
    return creds


def _save_token(creds: Credentials):
    token_data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes or SCOPES),
    }
    with open(TOKEN_CACHE, "w") as f:
        json.dump(token_data, f, indent=2)


# ──────────────────────────────────────────────────────────────────────
# DATE HELPERS
# ──────────────────────────────────────────────────────────────────────
def monthly_chunks(start: date, end: date) -> list[tuple[date, date]]:
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
        # Edge case: 1st of month, yesterday is prior month
        # Pull prior month instead
        if today.month == 1:
            start = date(today.year - 1, 12, 1)
        else:
            start = date(today.year, today.month - 1, 1)
        end = today - timedelta(days=1)
    return start, end


# ──────────────────────────────────────────────────────────────────────
# API CALLS
# ──────────────────────────────────────────────────────────────────────
def pull_traffic_with_referrer(
    client: BetaAnalyticsDataClient,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """
    Pull traffic data WITH pageReferrer as an extra dimension.
    This allows in-memory correction before aggregation.
    """
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
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


def pull_table(
    client: BetaAnalyticsDataClient,
    table_name: str,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """Pull a non-traffic table (content, timing, events, campaigns)."""
    tdef = TABLE_DEFS[table_name]

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
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


# ──────────────────────────────────────────────────────────────────────
# CORRECTION LOGIC
# ──────────────────────────────────────────────────────────────────────
def correct_traffic_rows(rows: list[dict]) -> tuple[list[dict], int]:
    """
    Reclassify misclassified Direct sessions:
      - If channel is "Direct" and pageReferrer contains "uol.com.br"
      - Move to channel="Organic Referral", source="uol.com.br", medium="referral"

    Returns (corrected_rows, num_reclassified).
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

    Summable metrics are summed. Rate metrics are recomputed:
      - engagementRate = engagedSessions / sessions
      - bounceRate = 1 - engagementRate
      - sessionsPerUser = sessions / totalUsers
      - averageSessionDuration: weighted average by sessions
    """
    # Group by final key
    groups = defaultdict(lambda: {
        "sessions": 0, "totalUsers": 0, "newUsers": 0,
        "screenPageViews": 0, "engagedSessions": 0, "eventCount": 0,
        "weighted_duration": 0.0,  # sessions * avgDuration for weighted avg
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

    # Build output rows with recomputed rates
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


# ──────────────────────────────────────────────────────────────────────
# CSV I/O
# ──────────────────────────────────────────────────────────────────────
def write_csv(rows: list[dict], filepath: Path, fieldnames: list[str]) -> int:
    """Write rows to CSV. Returns row count written. Verifies after write."""
    if not rows:
        print(f"  (no data, skipping {filepath.name})")
        return 0

    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    # Verify: reopen and count rows
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        saved_count = sum(1 for _ in reader) - 1  # minus header

    if saved_count != len(rows):
        raise RuntimeError(
            f"CSV VERIFICATION FAILED: wrote {len(rows)} rows but file has {saved_count}. "
            f"File: {filepath}"
        )

    print(f"  ✓ {filepath.name} — {saved_count} rows (verified)")
    return saved_count


# ──────────────────────────────────────────────────────────────────────
# DATABASE
# ──────────────────────────────────────────────────────────────────────
def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    for table_name, tdef in TABLE_DEFS.items():
        conn.execute(tdef["create_sql"])
        for idx_sql in tdef["indexes"]:
            conn.execute(idx_sql)

    conn.commit()
    return conn


def delete_date_range(conn: sqlite3.Connection, table_name: str, start: date, end: date):
    """Delete rows in a date range so we can cleanly replace them."""
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")
    cursor = conn.execute(
        f"DELETE FROM {table_name} WHERE date >= ? AND date <= ?",
        (start_str, end_str),
    )
    conn.commit()
    return cursor.rowcount


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[dict]):
    if not rows:
        return

    tdef = TABLE_DEFS[table_name]
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


def verify_db(db_path: Path, expected_counts: dict[str, int]) -> bool:
    """
    Reopen the database from disk and verify row counts match expectations.
    This catches silent write failures (0-byte files, corrupt copies).
    """
    if not db_path.exists():
        print(f"\n✗ VERIFICATION FAILED: database file does not exist at {db_path}")
        return False

    file_size = os.path.getsize(db_path)
    if file_size == 0:
        print(f"\n✗ VERIFICATION FAILED: database file is 0 bytes at {db_path}")
        return False

    try:
        conn = sqlite3.connect(str(db_path))
        # Quick integrity check
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

            print(f"  {status} {table_name:12s} — {actual:>9,} rows  (expected ≥ {expected:,})  [{min_date} → {max_date}]")

        conn.close()

        if all_ok:
            print(f"\n  All tables verified ✓")
        else:
            print(f"\n  ⚠ SOME TABLES HAVE FEWER ROWS THAN EXPECTED — check above")

        return all_ok

    except Exception as e:
        print(f"\n✗ VERIFICATION FAILED: {e}")
        return False


# ──────────────────────────────────────────────────────────────────────
# SAFE COPY (temp → final location)
# ──────────────────────────────────────────────────────────────────────
def safe_copy(src: Path, dst: Path):
    """Copy with size verification. Keeps src intact on failure."""
    src_size = os.path.getsize(src)
    if src_size == 0:
        raise RuntimeError(f"Source database is 0 bytes: {src}")

    # Backup existing DB before overwriting
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
    if dst_size == 0:
        raise RuntimeError(
            f"COPY FAILED: destination is 0 bytes.\n"
            f"Good database is at: {src}\n"
            f"Recover: cp '{src}' '{dst}'"
        )

    print(f"  Database saved to {dst} ({dst_size:,} bytes) ✓")


# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="GA4 weekly pull with correction — Capital Reset"
    )
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--backfill", action="store_true",
        help="Full backfill from Feb 2023 — use sparingly",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be pulled, no API calls",
    )
    args = parser.parse_args()

    # ── Determine date range ──
    if args.start and args.end:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
    elif args.backfill:
        start = PROPERTY_START
        end = date.today() - timedelta(days=1)
    else:
        start, end = current_month_range()

    if start < PROPERTY_START:
        start = PROPERTY_START

    chunks = monthly_chunks(start, end)

    print(f"\n{'='*60}")
    print(f"GA4 WEEKLY PULL — Capital Reset")
    print(f"{'='*60}")
    print(f"Property: {PROPERTY_ID}")
    print(f"Range: {start} → {end} ({len(chunks)} month(s))")
    print(f"Tables: traffic (with correction), content, timing, events, campaigns")
    print(f"Output: {CORRECTED_CSV_DIR}/")
    print(f"Database: {DB_PATH}")

    if args.dry_run:
        print(f"\nDRY RUN — no API calls or writes.\n")
        for s, e in chunks:
            print(f"  {s.strftime('%Y-%m')}: {s} → {e}")
        return

    # ── Authenticate ──
    creds = get_credentials()
    client = BetaAnalyticsDataClient(credentials=creds)
    print(f"\nAuthenticated ✓\n")

    # ── Work on temp DB copy ──
    import tempfile
    tmp_dir = tempfile.mkdtemp(prefix="ga4_weekly_")
    tmp_db = Path(tmp_dir) / "ga4_reset.db"

    if DB_PATH.exists():
        shutil.copy2(DB_PATH, tmp_db)
        print(f"Working on local copy of database")
    else:
        print(f"Creating new database")

    conn = init_db(tmp_db)

    # Track expected row counts for verification
    expected_counts = {}

    try:
        # ──────────────────────────────────────────────────────────
        # STEP 1: TRAFFIC (pull with referrer → correct → aggregate)
        # ──────────────────────────────────────────────────────────
        print(f"\n{'─'*60}")
        print(f"TRAFFIC — pulling with pageReferrer for correction")
        print(f"{'─'*60}")

        all_raw_rows = []
        all_corrected_rows = []
        total_reclassified = 0

        for chunk_start, chunk_end in chunks:
            label = chunk_start.strftime("%Y-%m")
            print(f"\n  Pulling {label} ({chunk_start} → {chunk_end})...")

            raw_rows = pull_traffic_with_referrer(client, chunk_start, chunk_end)
            print(f"    Raw: {len(raw_rows)} rows (with pageReferrer)")

            # Save raw CSV (audit trail)
            raw_file = RAW_CSV_DIR / f"ga4_{label}.csv"
            write_csv(raw_rows, raw_file, TRAFFIC_PULL_DIMENSIONS + TRAFFIC_METRICS)

            # Correct
            corrected_rows, num_reclassified = correct_traffic_rows(raw_rows)
            total_reclassified += num_reclassified
            if num_reclassified > 0:
                print(f"    Reclassified: {num_reclassified} rows (Direct → Organic Referral)")

            # Aggregate (collapse pageReferrer)
            aggregated = aggregate_traffic(corrected_rows)
            print(f"    Aggregated: {len(aggregated)} rows (final grain)")

            # Save corrected CSV
            corrected_file = CORRECTED_CSV_DIR / f"ga4_{label}.csv"
            write_csv(aggregated, corrected_file, TRAFFIC_FINAL_DIMENSIONS + TRAFFIC_METRICS)

            all_corrected_rows.extend(aggregated)

            time.sleep(0.5)

        # Delete date range from traffic table, then upsert corrected data
        deleted = delete_date_range(conn, "traffic", start, end)
        print(f"\n  Cleared {deleted} existing traffic rows for {start} → {end}")
        upsert_rows(conn, "traffic", all_corrected_rows)
        print(f"  Inserted {len(all_corrected_rows)} corrected traffic rows")

        traffic_count = conn.execute("SELECT COUNT(*) FROM traffic").fetchone()[0]
        expected_counts["traffic"] = traffic_count
        print(f"  Traffic table total: {traffic_count:,} rows")
        print(f"  Total reclassified this run: {total_reclassified}")

        # ──────────────────────────────────────────────────────────
        # STEP 2: OTHER TABLES (content, timing, events, campaigns)
        # ──────────────────────────────────────────────────────────
        for table_name in ["content", "timing", "events", "campaigns"]:
            print(f"\n{'─'*60}")
            print(f"{table_name.upper()}")
            print(f"{'─'*60}")

            all_rows = []
            for chunk_start, chunk_end in chunks:
                label = chunk_start.strftime("%Y-%m")
                print(f"  Pulling {label} ({chunk_start} → {chunk_end})...", end=" ", flush=True)
                rows = pull_table(client, table_name, chunk_start, chunk_end)
                all_rows.extend(rows)
                print(f"{len(rows)} rows")
                time.sleep(0.5)

            # Delete and replace
            deleted = delete_date_range(conn, table_name, start, end)
            print(f"  Cleared {deleted} existing rows, inserting {len(all_rows)} new rows")
            upsert_rows(conn, table_name, all_rows)

            table_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            expected_counts[table_name] = table_count
            print(f"  {table_name} total: {table_count:,} rows")

        # ──────────────────────────────────────────────────────────
        # STEP 3: SAVE AND VERIFY
        # ──────────────────────────────────────────────────────────
        conn.close()

        print(f"\n{'─'*60}")
        print(f"SAVING DATABASE")
        print(f"{'─'*60}")
        safe_copy(tmp_db, DB_PATH)

        # Final verification: reopen from the SAVED location and check
        if not verify_db(DB_PATH, expected_counts):
            print(f"\n⚠ VERIFICATION FAILED!")
            print(f"  Good copy is still at: {tmp_db}")
            print(f"  Recover: cp '{tmp_db}' '{DB_PATH}'")
            print(f"  DO NOT delete {tmp_dir} until you've recovered.")
            sys.exit(1)

        # Clean up temp dir only after successful verification
        shutil.rmtree(tmp_dir, ignore_errors=True)

        print(f"\n{'='*60}")
        print(f"DONE ✓")
        print(f"{'='*60}")

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
