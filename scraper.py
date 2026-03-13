"""
Google Play Store Rankings Tracker
Pulls top 100 apps per category weekly, detects movers in/out of rank bands.
Uses direct HTTP requests to Play Store charts.
"""

import json
import re
import time
import datetime
import requests
from pathlib import Path

CATEGORIES = {
    "FINANCE":            "Finance / BFSI",
    "HEALTH_AND_FITNESS": "Health & Fitness",
    "PRODUCTIVITY":       "Productivity / Tools",
    "BUSINESS":           "Business",
    "MEDICAL":            "Medical",
    "LIFESTYLE":          "Lifestyle",
    "SHOPPING":           "Shopping",
    "EDUCATION":          "Education",
}

COUNTRY      = "in"
LANG         = "en"
TOP_N        = 100
SNAPSHOT_DIR = Path("snapshots")


def today_str():
    return datetime.date.today().isoformat()


def snapshot_path(date_str):
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    return SNAPSHOT_DIR / f"{date_str}.json"


def load_snapshot(date_str):
    p = snapshot_path(date_str)
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return {}


def save_snapshot(date_str, data):
    with open(snapshot_path(date_str), "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Snapshot saved -> {snapshot_path(date_str)}")


def last_snapshot_path():
    """Return (date_str, data) of the most recent previous snapshot."""
    files = sorted(SNAPSHOT_DIR.glob("*.json"), reverse=True)
    today = today_str()
    for f in files:
        date_str = f.stem
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str) and date_str != today:
            return date_str, load_snapshot(date_str)
    return None, {}


def fetch_category(category_id, n=TOP_N):
    """
    Fetches top free apps for a category directly from the Play Store
    charts endpoint.
    """
    url = "https://play.google.com/_/PlayStoreUi/data/batchexecute"

    payload_inner = json.dumps([
        None, None,
        [[category_id, None, None, None, None, None, None, None, None],
         [None, 1],
         None, None, None, None, None, None, None, None, None, None, None,
         [LANG, COUNTRY],
         None, None, None, None, None, None, None, None, None,
         [n, None, None, None]
         ]
    ])

    body = "f.req=" + requests.utils.quote(
        json.dumps([[["vyAe2", payload_inner, None, "generic"]]])
    )

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": f"{LANG}-{COUNTRY.upper()},{LANG};q=0.9",
    }

    try:
        resp = requests.post(url, data=body, headers=headers, timeout=30)
        resp.raise_for_status()

        raw = resp.text
        start = raw.find("[[")
        if start == -1:
            print(f"    ERROR: unexpected response format for {category_id}")
            return []

        outer = json.loads(raw[start:])
        inner_str = outer[0][2]
        if not inner_str:
            return []

        inner = json.loads(inner_str)
        app_entries = inner[0][1]

        apps = []
        for rank, entry in enumerate(app_entries[:n], start=1):
            try:
                app_id = entry[0][0]
                title = entry[3]
                dev = entry[14] if len(entry) > 14 else ""
                score = entry[4][1] if entry[4] else None
                installs = entry[13] if len(entry) > 13 else ""
                url_str = f"https://play.google.com/store/apps/details?id={app_id}"
                apps.append({
                    "rank":      rank,
                    "app_id":    app_id,
                    "title":     title,
                    "developer": dev,
                    "score":     score,
                    "ratings":   None,
                    "installs":  installs,
                    "category":  category_id,
                    "url":       url_str,
                })
            except (IndexError, TypeError):
                continue

        return apps

    except Exception as e:
        print(f"    ERROR fetching {category_id}: {e}")
        return []


def scrape_all():
    """Scrape all categories. Returns {category_id: [app_dict, ...]}"""
    snapshot = {}
    for cat_id, cat_name in CATEGORIES.items():
        print(f"  Scraping {cat_name} ({cat_id})...")
        apps = fetch_category(cat_id)
        snapshot[cat_id] = apps
        print(f"    -> {len(apps)} apps fetched")
        time.sleep(3)
    return snapshot


def detect_movers(current, previous):
    """
    Compare two snapshots, return list of mover events.
    """
    movers = []
    today = today_str()

    for cat_id, apps in current.items():
        prev_apps = {a["app_id"]: a for a in previous.get(cat_id, [])}
        curr_apps = {a["app_id"]: a for a in apps}

        for app_id, curr in curr_apps.items():
            curr_rank = curr["rank"]
            prev = prev_apps.get(app_id)
            prev_rank = prev["rank"] if prev else None

            if curr_rank <= 9:
                if prev_rank is None or prev_rank > 9:
                    movers.append({**curr, "type": "breakout",
                                   "prev_rank": prev_rank, "curr_rank": curr_rank, "date": today})
            elif curr_rank <= 24:
                if prev_rank is not None and 25 <= prev_rank <= 75:
                    movers.append({**curr, "type": "climber",
                                   "prev_rank": prev_rank, "curr_rank": curr_rank, "date": today})
            elif 25 <= curr_rank <= 75:
                if prev_rank is None:
                    movers.append({**curr, "type": "new_entrant",
                                   "prev_rank": None, "curr_rank": curr_rank, "date": today})

    priority = {"breakout": 0, "climber": 1, "new_entrant": 2}
    movers.sort(key=lambda x: (priority.get(x["type"], 9), x["curr_rank"]))
    return movers


def run():
    today = today_str()
    print(f"\n{'='*60}")
    print(f"  Play Store Tracker -- {today}")
    print(f"{'='*60}\n")

    print("► Scraping Play Store...")
    current_snapshot = scrape_all()
    save_snapshot(today, current_snapshot)

    prev_date, prev_snapshot = last_snapshot_path()
    if not prev_snapshot:
        print("\n► No previous snapshot found -- first run, no delta available.")
        movers = []
    else:
        print(f"\n► Comparing against previous snapshot: {prev_date}")
        movers = detect_movers(current_snapshot, prev_snapshot)
        print(f"  {len(movers)} mover events detected")

    movers_path = SNAPSHOT_DIR / f"movers_{today}.json"
    with open(movers_path, "w") as f:
        json.dump(movers, f, indent=2)
    print(f"  Movers saved -> {movers_path}")

    if movers:
        print("\n-- Movers Summary --")
        for m in movers[:20]:
            prev_str = f"#{m['prev_rank']}" if m['prev_rank'] else "new"
            print(f"  [{m['type'].upper():12s}] {m['title'][:35]:35s}  "
                  f"{m['category']:20s}  {prev_str} -> #{m['curr_rank']}")

    return current_snapshot, movers


if __name__ == "__main__":
    run()
