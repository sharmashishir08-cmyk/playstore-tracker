"""
main.py — orchestrates scrape → delta → enrich → sheets → dashboard export
Run locally or via GitHub Actions.
"""

import json
import datetime
from pathlib import Path
from scraper import run as scrape_run
from enricher import enrich_movers
from sheets_writer import push_to_sheets

def main():
    today = datetime.date.today().isoformat()

    # 1. Scrape + detect movers
    snapshot, movers = scrape_run()

    # 2. Enrich movers with descriptions + funding data
    enriched_movers = enrich_movers(movers)

    # 3. Push to Google Sheets
    push_to_sheets(snapshot, enriched_movers, today)

    # 4. Export dashboard data for local HTML dashboard
    export_dashboard_json(snapshot, enriched_movers, today)

    print("\n✓ Done.")

def export_dashboard_json(snapshot: dict, movers: list[dict], date_str: str):
    """Write a dashboard_data.json that the HTML dashboard reads."""
    categories = {cat_id: apps for cat_id, apps in snapshot.items()}

    data = {
        "date":       date_str,
        "categories": categories,
        "movers":     movers,
        "summary": {
            "breakouts":    sum(1 for m in movers if m["type"] == "breakout"),
            "climbers":     sum(1 for m in movers if m["type"] == "climber"),
            "new_entrants": sum(1 for m in movers if m["type"] == "new_entrant"),
        }
    }

    out = Path("dashboard_data.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Dashboard JSON → {out.resolve()}")

if __name__ == "__main__":
    main()
