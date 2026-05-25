"""
UOL Enrichment Pull — Pull editorial metadata from UOL's GA4 360 property.

Reads 5 priority custom dimensions (author, channel, subchannel, publication_date,
tags) plus 2 secondary fields (pageReferrer, contentGroup) from UOL's GA4 360
property and writes them into a `content_uol` table in ga4_reset.db.

Standalone script — does NOT touch the existing GA4/GSC pipeline.
Uses service account auth (ga reader key.json), no OAuth needed.

UOL GA4 360 Property: 345118020
Auth: Service account (ga4-uol-reader@claude-ga-493021.iam.gserviceaccount.com)
Key file: ga reader key.json (same directory)

Table created: content_uol
  Grain: date + pagePath (one row per page per day)
  Custom dims: author, channel, subchannel, publication_date, tags
  Secondary: pageReferrer, contentGroup
  Metrics: sessions, screenPageViews, engagedSessions, engagementRate,
           averageSessionDuration, userEngagementDuration

Usage:
    # Normal run (current month-to-date):
    python3 uol_enrichment_pull.py

    # Custom date range:
    python3 uol_enrichment_pull.py --start 2026-04-01 --end 2026-04-14

    # Full backfill (from earliest UOL data, 2022-12-05):
    python3 uol_enrichment_pull.py --backfill

    # Dry run (shows what would be pulled, no API calls):
    python3 uol_enrichment_pull.py --dry-run

Dependencies:
    google-analytics-data
    google-auth
"""

import argparse
import csv
import os
import re
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    OrderBy,
    RunReportRequest,
)
from google.oauth2 import service_account

# ══════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════

SCRIPT_DIR = Path(__file__).resolve().parent

# ── Path resolution ──
# All paths overridable via env vars. Defaults point at the original locations
# so local runs of this copy produce the same outputs as the original script.
# Important fix vs. the original: the UOL service-account key lives in the
# LinkedIn Analytics folder, NOT Google Analytics. Original script's default
# was broken (key not present at hardcoded path); this default actually works.
LOCAL_GA_FOLDER = SCRIPT_DIR.parent / "Google Analytics"
LOCAL_LINKEDIN_FOLDER = SCRIPT_DIR.parent / "LinkedIn Analytics"

def _env_path(name: str, default: Path) -> Path:
    val = os.environ.get(name)
    return Path(val) if val else default

# — UOL GA4 360 —
UOL_PROPERTY_ID = os.environ.get("UOL_PROPERTY_ID", "345118020")
UOL_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
UOL_SA_KEY = _env_path("UOL_SA_KEY_PATH", LOCAL_LINKEDIN_FOLDER / "ga reader key.json")
UOL_PROPERTY_START = date(2022, 12, 5)  # earliest UOL data per explorer run

# — Database —
# In local mode, writes into the master DB at the GA folder. In cloud mode,
# the workflow points UOL_DB_PATH at the committed analysis DB so content_uol
# lands there instead.
DB_PATH = _env_path("UOL_DB_PATH", LOCAL_GA_FOLDER / "ga4_reset.db")

# — CSV archive —
CSV_DIR = _env_path("UOL_CSV_DIR", LOCAL_GA_FOLDER / "API pulls" / "uol_enrichment")

# — API limits —
ROW_LIMIT = 250_000  # GA4 360 allows up to 1M; 250K is safe margin

# — Author cleaning —
# UOL's author field is sometimes polluted with reading-time strings
# like "1 minuto", "2 minutos", "3 min de leitura", etc.
READING_TIME_PATTERN = re.compile(
    r"^\d+\s*(minuto|minutos|min|min\.|min\s+de\s+leitura)s?$",
    re.IGNORECASE,
)


# ══════════════════════════════════════════════════════════════════════
# TABLE DEFINITION
# ══════════════════════════════════════════════════════════════════════

# Dimensions to pull from the API (7 dims — within GA4's 9-dim limit)
PULL_DIMENSIONS = [
    "date",
    "pagePath",
    "pageTitle",
    "customEvent:author",
    "customEvent:channel",
    "customEvent:subchannel",
    "customEvent:publication_date",
]

# Secondary dimensions pulled in a separate API call to stay within
# the 9-dimension limit and avoid combinatorial explosion
SECONDARY_DIMENSIONS = [
    "date",
    "pagePath",
    "customEvent:tags",
    "pageReferrer",
    "contentGroup",
]

# Metrics (same for both calls)
PULL_METRICS = [
    "sessions",
    "screenPageViews",
    "engagedSessions",
    "engagementRate",
    "averageSessionDuration",
    "userEngagementDuration",
]

# Column names in the DB (cleaned versions of API names)
DB_COLUMNS = [
    "date",
    "pagePath",
    "pageTitle",
    "author",
    "channel",
    "subchannel",
    "publication_date",
    "tags",
    "pageReferrer",
    "contentGroup",
    "sessions",
    "screenPageViews",
    "engagedSessions",
    "engagementRate",
    "averageSessionDuration",
    "userEngagementDuration",
]

INTEGER_METRICS = {"sessions", "screenPageViews", "engagedSessions"}

CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS content_uol (
        date TEXT NOT NULL,
        pagePath TEXT NOT NULL,
        pageTitle TEXT,
        author TEXT,
        channel TEXT,
        subchannel TEXT,
        publication_date TEXT,
        tags TEXT,
        pageReferrer TEXT,
        contentGroup TEXT,
        sessions INTEGER,
        screenPageViews INTEGER,
        engagedSessions INTEGER,
        engagementRate REAL,
        averageSessionDuration REAL,
        userEngagementDuration REAL,
        PRIMARY KEY (date, pagePath)
    )
"""

INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_uol_date ON content_uol(date)",
    "CREATE INDEX IF NOT EXISTS idx_uol_path ON content_uol(pagePath)",
    "CREATE INDEX IF NOT EXISTS idx_uol_author ON content_uol(author)",
    "CREATE INDEX IF NOT EXISTS idx_uol_channel ON content_uol(channel)",
    "CREATE INDEX IF NOT EXISTS idx_uol_pubdate ON content_uol(publication_date)",
]


# ══════════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════════

def get_uol_client() -> BetaAnalyticsDataClient:
    """Authenticate via service account for UOL GA4 360."""
    if not UOL_SA_KEY.exists():
        sys.exit(f"Service account key not found at {UOL_SA_KEY}")
    credentials = service_account.Credentials.from_service_account_file(
        str(UOL_SA_KEY), scopes=UOL_SCOPES
    )
    return BetaAnalyticsDataClient(credentials=credentials)


# ══════════════════════════════════════════════════════════════════════
# DATE HELPERS (same logic as GA_Search_Console_DB_updater.py)
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
# API CALLS
# ══════════════════════════════════════════════════════════════════════

def pull_primary(
    client: BetaAnalyticsDataClient,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """
    Pull primary enrichment dimensions:
    date, pagePath, pageTitle, author, channel, subchannel, publication_date
    + all metrics.
    """
    request = RunReportRequest(
        property=f"properties/{UOL_PROPERTY_ID}",
        dimensions=[Dimension(name=d) for d in PULL_DIMENSIONS],
        metrics=[Metric(name=m) for m in PULL_METRICS],
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
        for i, dim in enumerate(PULL_DIMENSIONS):
            # Strip the customEvent: prefix for cleaner column names
            col = dim.replace("customEvent:", "")
            record[col] = row.dimension_values[i].value
        for i, met in enumerate(PULL_METRICS):
            record[met] = row.metric_values[i].value
        rows.append(record)

    if response.row_count >= ROW_LIMIT:
        print(f"  ⚠ ROW LIMIT REACHED ({ROW_LIMIT}) for primary {start_date}–{end_date}!")
        print(f"    Data is TRUNCATED. Split into smaller date ranges.")

    # Check sampling
    if response.metadata and response.metadata.sampling_metadatas:
        for sm in response.metadata.sampling_metadatas:
            pct = sm.samples_read_count / sm.sampling_space_size * 100 if sm.sampling_space_size else 0
            print(f"  ⚠ SAMPLED at {pct:.1f}% — expected unsampled from 360")

    return rows


def pull_secondary(
    client: BetaAnalyticsDataClient,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """
    Pull secondary dimensions (tags, pageReferrer, contentGroup) in a
    separate API call to avoid combinatorial explosion with primary dims.

    Returns rows keyed by (date, pagePath) for merging.
    """
    request = RunReportRequest(
        property=f"properties/{UOL_PROPERTY_ID}",
        dimensions=[Dimension(name=d) for d in SECONDARY_DIMENSIONS],
        metrics=[Metric(name=m) for m in PULL_METRICS],
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
        for i, dim in enumerate(SECONDARY_DIMENSIONS):
            col = dim.replace("customEvent:", "")
            record[col] = row.dimension_values[i].value
        for i, met in enumerate(PULL_METRICS):
            record[met] = row.metric_values[i].value
        rows.append(record)

    if response.row_count >= ROW_LIMIT:
        print(f"  ⚠ ROW LIMIT REACHED ({ROW_LIMIT}) for secondary {start_date}–{end_date}!")

    return rows


# ══════════════════════════════════════════════════════════════════════
# MERGE & CLEAN
# ══════════════════════════════════════════════════════════════════════

def clean_author(author: str) -> str:
    """
    Clean the author field:
    - If it looks like a reading time ("1 minuto", "3 min de leitura"), return "(not set)"
    - Strip whitespace
    """
    if not author or author == "(not set)":
        return "(not set)"
    author = author.strip()
    if READING_TIME_PATTERN.match(author):
        return "(not set)"
    return author


def merge_and_aggregate(
    primary_rows: list[dict],
    secondary_rows: list[dict],
) -> list[dict]:
    """
    Merge primary and secondary pulls into a single dataset at (date, pagePath) grain.

    Primary pull has: date, pagePath, pageTitle, author, channel, subchannel, publication_date
    Secondary pull has: date, pagePath, tags, pageReferrer, contentGroup

    Since the primary pull has MORE dimensions (author, channel, etc.), there may be
    multiple primary rows per (date, pagePath). We aggregate metrics and pick the
    most common non-null value for each editorial dimension.

    For secondary, tags/pageReferrer/contentGroup also need aggregation: we pick the
    highest-traffic value per (date, pagePath).
    """
    # ── Aggregate primary by (date, pagePath) ──
    primary_groups = defaultdict(lambda: {
        "pageTitle": None,
        "author_counts": defaultdict(int),
        "channel_counts": defaultdict(int),
        "subchannel_counts": defaultdict(int),
        "publication_date_counts": defaultdict(int),
        "sessions": 0,
        "screenPageViews": 0,
        "engagedSessions": 0,
        "weighted_duration": 0.0,
        "total_engagement_duration": 0.0,
    })

    for row in primary_rows:
        key = (row["date"], row["pagePath"])
        g = primary_groups[key]

        sessions = int(float(row["sessions"]))
        g["sessions"] += sessions
        g["screenPageViews"] += int(float(row["screenPageViews"]))
        g["engagedSessions"] += int(float(row["engagedSessions"]))
        g["weighted_duration"] += sessions * float(row["averageSessionDuration"])
        g["total_engagement_duration"] += float(row["userEngagementDuration"])

        # Keep the longest pageTitle (usually the real one, not truncated)
        if row.get("pageTitle") and row["pageTitle"] != "(not set)":
            if g["pageTitle"] is None or len(row["pageTitle"]) > len(g["pageTitle"]):
                g["pageTitle"] = row["pageTitle"]

        # Count dimension values weighted by sessions for majority vote
        author = clean_author(row.get("author", ""))
        if author != "(not set)":
            g["author_counts"][author] += sessions

        for dim in ["channel", "subchannel", "publication_date"]:
            val = row.get(dim, "")
            if val and val != "(not set)":
                g[f"{dim}_counts"][val] += sessions

    # ── Aggregate secondary by (date, pagePath) ──
    secondary_groups = defaultdict(lambda: {
        "tags_counts": defaultdict(int),
        "pageReferrer_counts": defaultdict(int),
        "contentGroup_counts": defaultdict(int),
    })

    for row in secondary_rows:
        key = (row["date"], row["pagePath"])
        g = secondary_groups[key]
        sessions = int(float(row["sessions"]))

        for dim in ["tags", "pageReferrer", "contentGroup"]:
            val = row.get(dim, "")
            if val and val != "(not set)":
                g[f"{dim}_counts"][val] += sessions

    # ── Merge into final rows ──
    def pick_top(counts_dict):
        """Pick the value with the highest session weight."""
        if not counts_dict:
            return None
        return max(counts_dict, key=counts_dict.get)

    result = []
    for (dt, path), g in sorted(primary_groups.items()):
        sessions = g["sessions"]
        eng_rate = g["engagedSessions"] / sessions if sessions > 0 else 0.0
        avg_dur = g["weighted_duration"] / sessions if sessions > 0 else 0.0

        sec = secondary_groups.get((dt, path), {})

        result.append({
            "date": dt,
            "pagePath": path,
            "pageTitle": g["pageTitle"],
            "author": pick_top(g["author_counts"]),
            "channel": pick_top(g["channel_counts"]),
            "subchannel": pick_top(g["subchannel_counts"]),
            "publication_date": pick_top(g["publication_date_counts"]),
            "tags": pick_top(sec.get("tags_counts", {})),
            "pageReferrer": pick_top(sec.get("pageReferrer_counts", {})),
            "contentGroup": pick_top(sec.get("contentGroup_counts", {})),
            "sessions": sessions,
            "screenPageViews": g["screenPageViews"],
            "engagedSessions": g["engagedSessions"],
            "engagementRate": round(eng_rate, 4),
            "averageSessionDuration": round(avg_dur, 1),
            "userEngagementDuration": round(g["total_engagement_duration"], 1),
        })

    return result


# ══════════════════════════════════════════════════════════════════════
# CSV I/O
# ══════════════════════════════════════════════════════════════════════

def write_csv(rows: list[dict], filepath: Path) -> int:
    """Write rows to CSV. Returns row count."""
    if not rows:
        print(f"  (no data, skipping {filepath.name})")
        return 0

    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DB_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    # Verify
    with open(filepath, "r", encoding="utf-8") as f:
        saved = sum(1 for _ in csv.reader(f)) - 1

    if saved != len(rows):
        raise RuntimeError(f"CSV VERIFICATION FAILED: wrote {len(rows)} but file has {saved}")

    print(f"  ✓ {filepath.name} — {saved} rows")
    return saved


# ══════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════

def init_table(conn: sqlite3.Connection):
    """Create the content_uol table and indexes if they don't exist."""
    conn.execute(CREATE_TABLE_SQL)
    for idx in INDEX_SQL:
        conn.execute(idx)
    conn.commit()


def delete_date_range(conn: sqlite3.Connection, start: date, end: date) -> int:
    """Delete rows in a date range. UOL dates come as YYYYMMDD from the API."""
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")
    cursor = conn.execute(
        "DELETE FROM content_uol WHERE date >= ? AND date <= ?",
        (start_str, end_str),
    )
    conn.commit()
    return cursor.rowcount


def upsert_rows(conn: sqlite3.Connection, rows: list[dict]):
    """Insert or replace rows into content_uol."""
    if not rows:
        return

    placeholders = ", ".join(["?"] * len(DB_COLUMNS))
    col_names = ", ".join(DB_COLUMNS)
    sql = f"INSERT OR REPLACE INTO content_uol ({col_names}) VALUES ({placeholders})"

    data = []
    for row in rows:
        values = []
        for col in DB_COLUMNS:
            val = row.get(col)
            if val is None or val == "" or val == "(not set)":
                values.append(None)
            elif col in INTEGER_METRICS:
                values.append(int(float(val)))
            elif col in {"engagementRate", "averageSessionDuration", "userEngagementDuration"}:
                values.append(float(val))
            else:
                values.append(str(val))
        data.append(values)

    conn.executemany(sql, data)
    conn.commit()


def verify_table(db_path: Path, expected_min: int) -> bool:
    """Verify the content_uol table after the run."""
    conn = sqlite3.connect(str(db_path))
    result = conn.execute("PRAGMA integrity_check").fetchone()
    if result[0] != "ok":
        print(f"\n✗ INTEGRITY CHECK FAILED: {result[0]}")
        conn.close()
        return False

    total = conn.execute("SELECT COUNT(*) FROM content_uol").fetchone()[0]
    min_date = conn.execute("SELECT MIN(date) FROM content_uol").fetchone()[0]
    max_date = conn.execute("SELECT MAX(date) FROM content_uol").fetchone()[0]

    # Enrichment stats
    author_fill = conn.execute(
        "SELECT COUNT(*) FROM content_uol WHERE author IS NOT NULL"
    ).fetchone()[0]
    channel_fill = conn.execute(
        "SELECT COUNT(*) FROM content_uol WHERE channel IS NOT NULL"
    ).fetchone()[0]
    tags_fill = conn.execute(
        "SELECT COUNT(*) FROM content_uol WHERE tags IS NOT NULL"
    ).fetchone()[0]

    conn.close()

    status = "✓" if total >= expected_min else "⚠ FEWER THAN EXPECTED"

    print(f"\n{'='*60}")
    print(f"VERIFICATION — content_uol")
    print(f"{'='*60}")
    print(f"  {status} {total:,} rows  (expected >= {expected_min:,})")
    print(f"  Date range: {min_date} → {max_date}")
    print(f"  Fill rates:")
    print(f"    author:   {author_fill:,} / {total:,}  ({author_fill/total*100:.1f}%)" if total else "    (empty)")
    print(f"    channel:  {channel_fill:,} / {total:,}  ({channel_fill/total*100:.1f}%)" if total else "")
    print(f"    tags:     {tags_fill:,} / {total:,}  ({tags_fill/total*100:.1f}%)" if total else "")

    return total >= expected_min


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="UOL Enrichment Pull — editorial metadata from UOL GA4 360 into content_uol"
    )
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--backfill", action="store_true",
        help="Full backfill from 2022-12-05 (earliest UOL data)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be pulled, no API calls",
    )
    parser.add_argument(
        "--cloud-mode", action="store_true",
        help=(
            "Cloud mode (GitHub Actions). Writes content_uol into the committed "
            "ga4_analysis.db in the working directory, NOT the local master DB. "
            "Also skips CSV archive writes (nothing to persist on the runner)."
        ),
    )
    args = parser.parse_args()

    # ── Cloud-mode path overrides ──
    # In cloud mode, target the committed analysis DB in the repo root and
    # skip CSV archive writes. Env-var overrides (UOL_DB_PATH, UOL_CSV_DIR)
    # always win if explicitly set, so local cloud-mode tests on the Mac can
    # still redirect output by exporting env vars.
    global DB_PATH, CSV_DIR
    cloud_mode = args.cloud_mode
    if cloud_mode and "UOL_DB_PATH" not in os.environ:
        DB_PATH = Path.cwd() / "ga4_analysis.db"
    if cloud_mode and "UOL_CSV_DIR" not in os.environ:
        CSV_DIR = None  # signal to skip CSV writes

    # ── Determine date range ──
    if args.start and args.end:
        pull_start = date.fromisoformat(args.start)
        pull_end = date.fromisoformat(args.end)
    elif args.backfill:
        pull_start = UOL_PROPERTY_START
        pull_end = date.today() - timedelta(days=1)
    else:
        pull_start, pull_end = current_month_range()

    if pull_start < UOL_PROPERTY_START:
        pull_start = UOL_PROPERTY_START

    chunks = monthly_chunks(pull_start, pull_end)

    print(f"\n{'='*60}")
    print(f"UOL ENRICHMENT PULL — Capital Reset")
    print(f"{'='*60}")
    print(f"UOL Property: {UOL_PROPERTY_ID}")
    print(f"Range: {pull_start} → {pull_end} ({len(chunks)} month(s))")
    print(f"Mode: {'CLOUD' if cloud_mode else 'LOCAL'}")
    print(f"Table: content_uol in {DB_PATH.name}")
    print(f"DB path: {DB_PATH}")
    print(f"Auth: Service account ({UOL_SA_KEY.name})")

    if args.dry_run:
        print(f"\nDRY RUN — no API calls or writes.\n")
        for s, e in chunks:
            print(f"  {s.strftime('%Y-%m')}: {s} → {e}")
        return

    # ── Auth ──
    client = get_uol_client()
    print(f"\nAuthenticated ✓\n")

    # ── Open DB ──
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    init_table(conn)

    total_rows_inserted = 0

    try:
        for chunk_start, chunk_end in chunks:
            label = chunk_start.strftime("%Y-%m")
            print(f"{'─'*60}")
            print(f"{label} ({chunk_start} → {chunk_end})")
            print(f"{'─'*60}")

            # Pull primary dimensions
            print(f"  Pulling primary (author, channel, subchannel, pub_date)...", end=" ", flush=True)
            primary = pull_primary(client, chunk_start, chunk_end)
            print(f"{len(primary)} rows")
            time.sleep(0.5)

            # Pull secondary dimensions
            print(f"  Pulling secondary (tags, pageReferrer, contentGroup)...", end=" ", flush=True)
            secondary = pull_secondary(client, chunk_start, chunk_end)
            print(f"{len(secondary)} rows")
            time.sleep(0.5)

            # Merge and clean
            merged = merge_and_aggregate(primary, secondary)
            print(f"  Merged to {len(merged)} rows at (date, pagePath) grain")

            # Archive to CSV (skipped in cloud mode — runner disk is ephemeral)
            if CSV_DIR is not None:
                csv_file = CSV_DIR / f"uol_enrichment_{label}.csv"
                write_csv(merged, csv_file)
            else:
                print(f"  CSV archive skipped (cloud mode)")

            # Upsert into DB
            deleted = delete_date_range(conn, chunk_start, chunk_end)
            if deleted > 0:
                print(f"  Cleared {deleted} existing rows")
            upsert_rows(conn, merged)
            print(f"  Inserted {len(merged)} rows into content_uol")

            total_rows_inserted += len(merged)
            time.sleep(0.3)

        # ── Flush WAL and verify ──
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()

        verify_table(DB_PATH, total_rows_inserted)

        print(f"\n{'='*60}")
        print(f"DONE ✓")
        print(f"{'='*60}")
        print(f"  Range: {pull_start} → {pull_end}")
        print(f"  Rows inserted: {total_rows_inserted:,}")
        print(f"  Database: {DB_PATH}")
        if CSV_DIR is not None:
            print(f"  CSV archive: {CSV_DIR}")
        else:
            print(f"  CSV archive: skipped (cloud mode)")

    except Exception as e:
        print(f"\n{'='*60}")
        print(f"ERROR: {e}")
        print(f"{'='*60}")
        try:
            conn.close()
        except:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
