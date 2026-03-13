"""
Google Play Store Rankings Tracker
Pulls top 100 apps per category weekly, detects movers in/out of rank bands.
"""

import json
import os
import time
import datetime
from pathlib import Path
from google_play_scraper import app, charts
from google_play_scraper.constants.google_play import Sort
from google_play_scraper.features.charts import Charts

# ── Config ──────────────────────────────────────────────────────────────────

CATEGORIES = {
    "FINANCE":          "Finance / BFSI",
    "HEALTH_AND_FITNESS": "Health & Fitness",
    "PRODUCTIVITY":     "Productivity / Tools",
    "BUSINESS":         "Business",
    "MEDICAL":          "Medical",
    "LIFESTYLE":        "Lifestyle",
    "SHOPPING":         "Shopping",
    "EDUCATION":        "Education",
}

COUNTRY       = "in"          # India store
TOP_N         = 100           # how many apps to pull per category
SNAPSHOT_DIR  = Path("snapshots")
MOVER_BANDS   = {
    "new_entrant":  (25, 75),   # entered rank 25–75 for first time
    "climber":      (1, 24),    # moved from 25–75 → top 25
    "breakout":     (1, 9),     # new top 10
}

# ── Helpers ──────────────────────────────────────────────────────────────────

def today_str():
    return datetime.date.today().isoformat()          # e.g. "2025-06-01"

def snapshot_path(date_str: str) -> Path:
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    return SNAPSHOT_DIR / f"{date_str}.json"

def load_snapshot(date_str: str) -> dict:
    p = snapshot_path(date_str)
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return {}

def save_snapshot(date_str: str, data: dict):
    with open(snapshot_path(date_str), "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Snapshot saved → {snapshot_path(date_str)}")

def last_snapshot_path() -> tuple[str, dict]:
    """Return (date_str, data) of the most recent previous snapshot."""
    import re
    files = sorted(SNAPSHOT_DIR.glob("*.json"), reverse=True)
    today = today_str()
    for f in files:
        date_str = f.stem
        # Only match YYYY-MM-DD files — skip movers_* and enrichment_cache files
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str) and date_str != today:
            return date_str, load_snapshot(date_str)
    return None, {}

# ── Scraping ─────────────────────────────────────────────────────────────────

def fetch_category(category_id: str, n: int = TOP_N) -> list[dict]:
    """Pull top-n free apps for a category. Returns list of dicts with rank."""
    try:
        result = charts(
            chart=Charts.TOP_FREE,
            category=category_id,
            country=COUNTRY,
            n=n,
            lang="en",
        )
        apps = []
        for rank, item in enumerate(result, start=1):
            apps.append({
                "rank":        rank,
                "app_id":      item.get("appId", ""),
                "title":       item.get("title", ""),
                "developer":   item.get("developer", ""),
                "score":       item.get("score"),
                "ratings":     item.get("ratings"),
                "installs":    item.get("installs", ""),
                "category":    category_id,
                "url":         item.get("url", f"https://play.google.com/store/apps/details?id={item.get('appId','')}"),
            })
        return apps
    except Exception as e:
        print(f"    ERROR fetching {category_id}: {e}")
        return []

def scrape_all() -> dict:
    """Scrape all categories. Returns {category_id: [app_dict, ...]}"""
    snapshot = {}
    for cat_id, cat_name in CATEGORIES.items():
        print(f"  Scraping {cat_name} ({cat_id})…")
        apps = fetch_category(cat_id)
        snapshot[cat_id] = apps
        print(f"    → {len(apps)} apps fetched")
        time.sleep(2)   # polite delay
    return snapshot

# ── Delta Detection ───────────────────────────────────────────────────────────

def build_rank_map(snapshot: dict) -> dict:
    """
    Returns {app_id: {"rank": int, "title": str, "category": str, ...}}
    across all categories in a snapshot.
    """
    rank_map = {}
    for cat_id, apps in snapshot.items():
        for a in apps:
            key = f"{cat_id}::{a['app_id']}"
            rank_map[key] = a
    return rank_map

def detect_movers(current: dict, previous: dict) -> list[dict]:
    """
    Compare two snapshots, return list of mover events.
    Each event: {type, app_id, title, developer, category, prev_rank, curr_rank, url}
    """
    movers = []
    today = today_str()

    for cat_id, apps in current.items():
        prev_apps   = {a["app_id"]: a for a in previous.get(cat_id, [])}
        curr_apps   = {a["app_id"]: a for a in apps}

        for app_id, curr in curr_apps.items():
            curr_rank = curr["rank"]
            prev      = prev_apps.get(app_id)
            prev_rank = prev["rank"] if prev else None

            # Breakout: entered top 10 (new or climbed)
            if curr_rank <= 9:
                if prev_rank is None or prev_rank > 9:
                    movers.append({**curr, "type": "breakout",
                                   "prev_rank": prev_rank, "curr_rank": curr_rank, "date": today})

            # Climber: moved from 25–75 band into top 25
            elif curr_rank <= 24:
                if prev_rank is not None and 25 <= prev_rank <= 75:
                    movers.append({**curr, "type": "climber",
                                   "prev_rank": prev_rank, "curr_rank": curr_rank, "date": today})

            # New entrant: appeared in 25–75 for the first time
            elif 25 <= curr_rank <= 75:
                if prev_rank is None:
                    movers.append({**curr, "type": "new_entrant",
                                   "prev_rank": None, "curr_rank": curr_rank, "date": today})

    # Sort by type priority then rank
    priority = {"breakout": 0, "climber": 1, "new_entrant": 2}
    movers.sort(key=lambda x: (priority.get(x["type"], 9), x["curr_rank"]))
    return movers

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    today = today_str()
    print(f"\n{'='*60}")
    print(f"  Play Store Tracker — {today}")
    print(f"{'='*60}\n")

    # 1. Scrape
    print("► Scraping Play Store…")
    current_snapshot = scrape_all()
    save_snapshot(today, current_snapshot)

    # 2. Load previous snapshot
    prev_date, prev_snapshot = last_snapshot_path()
    if not prev_snapshot:
        print("\n► No previous snapshot found — first run, no delta available.")
        movers = []
    else:
        print(f"\n► Comparing against previous snapshot: {prev_date}")
        movers = detect_movers(current_snapshot, prev_snapshot)
        print(f"  {len(movers)} mover events detected")

    # 3. Save movers
    movers_path = SNAPSHOT_DIR / f"movers_{today}.json"
    with open(movers_path, "w") as f:
        json.dump(movers, f, indent=2)
    print(f"  Movers saved → {movers_path}")

    # 4. Print summary
    if movers:
        print("\n── Movers Summary ──────────────────────────────────────")
        for m in movers[:20]:   # print first 20
            prev_str = f"#{m['prev_rank']}" if m['prev_rank'] else "new"
            print(f"  [{m['type'].upper():12s}] {m['title'][:35]:35s}  "
                  f"{m['category']:20s}  {prev_str} → #{m['curr_rank']}")

    return current_snapshot, movers

if __name__ == "__main__":
    run()
