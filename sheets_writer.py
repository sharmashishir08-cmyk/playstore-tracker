"""
Writes Play Store snapshots and mover events to Google Sheets.

Sheet structure:
  • "Snapshots"  — full weekly dump (one row per app per category per week)
  • "Movers"     — delta events (new entrants, climbers, breakouts)
  • "Dashboard"  — summary stats auto-computed by this script

Requires: GOOGLE_SHEETS_ID and GOOGLE_SERVICE_ACCOUNT_JSON env vars.
"""

import os
import json
import datetime
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEET_ID = os.environ.get("GOOGLE_SHEETS_ID", "")

# ── Auth ──────────────────────────────────────────────────────────────────────

def get_client() -> gspread.Client:
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not sa_json:
        raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT_JSON not set")
    creds_dict = json.loads(sa_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

def get_or_create_sheet(wb: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    try:
        return wb.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = wb.add_worksheet(title=name, rows=5000, cols=20)
        return ws

# ── Snapshots tab ─────────────────────────────────────────────────────────────

SNAPSHOT_HEADERS = [
    "date", "category", "rank", "app_id", "title",
    "developer", "score", "ratings", "installs", "url",
]

def write_snapshot(ws: gspread.Worksheet, snapshot: dict, date_str: str):
    # Ensure header row
    existing = ws.get_all_values()
    if not existing or existing[0] != SNAPSHOT_HEADERS:
        ws.insert_row(SNAPSHOT_HEADERS, index=1)

    rows = []
    for cat_id, apps in snapshot.items():
        for a in apps:
            rows.append([
                date_str,
                a.get("category", cat_id),
                a.get("rank"),
                a.get("app_id"),
                a.get("title"),
                a.get("developer"),
                a.get("score"),
                a.get("ratings"),
                a.get("installs"),
                a.get("url"),
            ])

    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"  Snapshots tab: wrote {len(rows)} rows for {date_str}")

# ── Movers tab ────────────────────────────────────────────────────────────────

MOVERS_HEADERS = [
    "date", "type", "category", "rank_prev", "rank_curr",
    "app_id", "title", "developer", "score", "installs", "url",
]

def write_movers(ws: gspread.Worksheet, movers: list[dict], date_str: str):
    existing = ws.get_all_values()
    if not existing or existing[0] != MOVERS_HEADERS:
        ws.insert_row(MOVERS_HEADERS, index=1)

    rows = []
    for m in movers:
        rows.append([
            date_str,
            m.get("type"),
            m.get("category"),
            m.get("prev_rank"),
            m.get("curr_rank"),
            m.get("app_id"),
            m.get("title"),
            m.get("developer"),
            m.get("score"),
            m.get("installs"),
            m.get("url"),
        ])

    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"  Movers tab: wrote {len(rows)} mover events")
    else:
        print("  Movers tab: no movers this week")

# ── Dashboard tab ─────────────────────────────────────────────────────────────

def write_dashboard(ws: gspread.Worksheet, snapshot: dict, movers: list[dict], date_str: str):
    """Writes a human-readable summary to the Dashboard tab."""
    ws.clear()

    rows = [
        [f"Play Store Rankings Dashboard", "", f"Updated: {date_str}"],
        [""],
    ]

    # Mover counts by type
    from collections import Counter
    type_counts = Counter(m["type"] for m in movers)
    rows += [
        ["── This Week's Movers ──────────────────"],
        ["Type", "Count"],
        ["New Entrants (→ rank 25–75)", type_counts.get("new_entrant", 0)],
        ["Climbers (25–75 → top 25)",  type_counts.get("climber",    0)],
        ["Breakouts (→ top 10)",        type_counts.get("breakout",   0)],
        [""],
    ]

    # Top movers table
    rows += [
        ["── Top Mover Events ───────────────────────────────────────────────────────────"],
        ["Type", "App", "Developer", "Category", "Prev Rank", "Curr Rank", "Installs", "URL"],
    ]
    for m in movers[:50]:
        rows.append([
            m.get("type"),
            m.get("title"),
            m.get("developer"),
            m.get("category"),
            m.get("prev_rank") or "new",
            m.get("curr_rank"),
            m.get("installs"),
            m.get("url"),
        ])

    rows += [""]

    # Per-category counts
    rows += [
        ["── Category Coverage ───────────────────────────────────────────────────────────"],
        ["Category", "Apps Tracked", "Breakouts", "Climbers", "New Entrants"],
    ]
    for cat_id, apps in snapshot.items():
        cat_movers = [m for m in movers if m.get("category") == cat_id]
        rows.append([
            cat_id,
            len(apps),
            sum(1 for m in cat_movers if m["type"] == "breakout"),
            sum(1 for m in cat_movers if m["type"] == "climber"),
            sum(1 for m in cat_movers if m["type"] == "new_entrant"),
        ])

    ws.update("A1", rows)
    print(f"  Dashboard tab: updated summary")

# ── Main ──────────────────────────────────────────────────────────────────────

def push_to_sheets(snapshot: dict, movers: list[dict], date_str: str = None):
    if not date_str:
        date_str = datetime.date.today().isoformat()

    if not SHEET_ID:
        print("  GOOGLE_SHEETS_ID not set — skipping Sheets write")
        return

    print("\n► Writing to Google Sheets…")
    client = get_client()
    wb     = client.open_by_key(SHEET_ID)

    write_snapshot(get_or_create_sheet(wb, "Snapshots"), snapshot, date_str)
    write_movers(  get_or_create_sheet(wb, "Movers"),    movers,   date_str)
    write_dashboard(get_or_create_sheet(wb, "Dashboard"), snapshot, movers, date_str)

    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
    print(f"\n  ✓ Sheets updated → {spreadsheet_url}")

if __name__ == "__main__":
    import sys, json
    from pathlib import Path

    # Usage: python sheets_writer.py snapshots/2025-06-01.json snapshots/movers_2025-06-01.json
    if len(sys.argv) >= 3:
        snapshot = json.loads(Path(sys.argv[1]).read_text())
        movers   = json.loads(Path(sys.argv[2]).read_text())
        push_to_sheets(snapshot, movers)
    else:
        print("Usage: python sheets_writer.py <snapshot.json> <movers.json>")
