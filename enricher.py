"""
enricher.py — enriches mover app data with:
  1. Full description from google-play-scraper (free, always available)
  2. One-liner summary + funding data via Claude API (claude-haiku-4-5-20251001, cheap + fast)

Writes enrichment cache to snapshots/enrichment_cache.json so each app_id
is only looked up once across all future runs.
"""

import json
import os
import time
import re
from pathlib import Path
import anthropic
from google_play_scraper import app as gps_app

CACHE_PATH      = Path("snapshots/enrichment_cache.json")
ANTHROPIC_KEY   = os.environ.get("ANTHROPIC_API_KEY", "")
# Use Haiku — fast and cheap for structured JSON extraction
CLAUDE_MODEL    = "claude-haiku-4-5-20251001"
MAX_DESC_CHARS  = 800   # trim long descriptions before sending to Claude

# ── Cache helpers ─────────────────────────────────────────────────────────────

def load_cache() -> dict:
    if CACHE_PATH.exists():
        with open(CACHE_PATH) as f:
            return json.load(f)
    return {}

def save_cache(cache: dict):
    CACHE_PATH.parent.mkdir(exist_ok=True)
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)

# ── Step 1: fetch Play Store description ─────────────────────────────────────

def fetch_play_description(app_id: str) -> str:
    """
    Calls google-play-scraper app() to get full description.
    Returns first ~800 chars, cleaned of markdown/html junk.
    Falls back to empty string on any error.
    """
    try:
        details = gps_app(app_id, lang="en", country="in")
        raw = details.get("description", "") or details.get("summary", "")
        # Strip HTML tags
        raw = re.sub(r"<[^>]+>", " ", raw)
        # Collapse whitespace
        raw = re.sub(r"\s+", " ", raw).strip()
        return raw[:MAX_DESC_CHARS]
    except Exception as e:
        print(f"    [enrich] play description fetch failed for {app_id}: {e}")
        return ""

# ── Step 2: Claude enrichment ─────────────────────────────────────────────────

ENRICHMENT_PROMPT = """You are a startup research assistant with deep knowledge of Indian tech companies and apps.

Given this Android app's details, return ONLY a valid JSON object (no markdown, no explanation):

App name: {title}
Developer: {developer}
Play Store description: {description}

Return this exact JSON structure:
{{
  "one_liner": "One sentence (max 15 words) describing what this app does and who it's for",
  "company_type": "startup | enterprise | government | consumer_brand | unknown",
  "funding_stage": "pre-seed | seed | series_a | series_b | series_c | series_d_plus | bootstrapped | acquired | public | unknown",
  "funding_total_usd": "e.g. $12M or $1.2B or unknown",
  "last_round": "e.g. Series B $40M (2023) or unknown",
  "investors": "top 1-2 notable investors or unknown",
  "confidence": "high | medium | low"
}}

Rules:
- one_liner must be factual and specific, not marketing speak
- If you don't know funding details with reasonable confidence, use "unknown" — do not guess
- confidence reflects how certain you are about the funding data specifically
- For well-known Indian startups (Cred, Groww, PhonePe, etc.) you likely have high confidence
- For niche or very new apps, use low confidence and unknown for funding fields"""

def enrich_via_claude(app_id: str, title: str, developer: str, description: str) -> dict:
    """
    Calls Claude Haiku to extract one-liner + funding metadata.
    Returns a dict with enrichment fields. On failure returns safe defaults.
    """
    if not ANTHROPIC_KEY:
        print("    [enrich] ANTHROPIC_API_KEY not set — skipping Claude enrichment")
        return _empty_enrichment()

    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    prompt = ENRICHMENT_PROMPT.format(
        title=title,
        developer=developer,
        description=description or "No description available."
    )

    try:
        message = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = message.content[0].text.strip()

        # Strip fences, then extract JSON object robustly
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        start, end = raw.find('{'), raw.rfind('}')
        if start == -1 or end == -1:
            raise ValueError("No JSON object found in response")
        raw = raw[start:end+1]

        parsed = json.loads(raw)
        # Validate required keys exist
        required = ["one_liner", "company_type", "funding_stage",
                    "funding_total_usd", "last_round", "investors", "confidence"]
        for k in required:
            if k not in parsed:
                parsed[k] = "unknown"
        return parsed

    except json.JSONDecodeError as e:
        print(f"    [enrich] JSON parse error for {app_id}: {e} | raw: {raw[:200]}")
        return _empty_enrichment()
    except anthropic.APIError as e:
        print(f"    [enrich] Claude API error for {app_id}: {e}")
        return _empty_enrichment()
    except Exception as e:
        print(f"    [enrich] Unexpected error for {app_id}: {e}")
        return _empty_enrichment()

def _empty_enrichment() -> dict:
    return {
        "one_liner":          "",
        "company_type":       "unknown",
        "funding_stage":      "unknown",
        "funding_total_usd":  "unknown",
        "last_round":         "unknown",
        "investors":          "unknown",
        "confidence":         "low",
    }

# ── Main enrichment entry point ───────────────────────────────────────────────

def enrich_movers(movers: list[dict]) -> list[dict]:
    """
    Takes list of mover dicts, returns them enriched with:
      - play_description (str)
      - one_liner, funding_stage, funding_total_usd, last_round,
        investors, company_type, confidence
    Uses a persistent cache keyed by app_id so each app is only enriched once.
    """
    if not movers:
        return movers

    cache      = load_cache()
    enriched   = []
    new_entries = 0

    print(f"\n► Enriching {len(movers)} movers…")

    for i, mover in enumerate(movers):
        app_id    = mover.get("app_id", "")
        title     = mover.get("title", "")
        developer = mover.get("developer", "")

        if app_id in cache:
            print(f"  [{i+1}/{len(movers)}] {title[:35]} — cache hit")
            mover = {**mover, **cache[app_id]}
        else:
            print(f"  [{i+1}/{len(movers)}] {title[:35]} — fetching…")

            # Step 1: Play Store description
            description = fetch_play_description(app_id)
            time.sleep(1.5)   # be polite to Play Store

            # Step 2: Claude enrichment
            enrichment = enrich_via_claude(app_id, title, developer, description)
            time.sleep(0.5)   # small buffer between Claude calls

            entry = {"play_description": description, **enrichment}
            cache[app_id] = entry
            new_entries += 1
            mover = {**mover, **entry}

        enriched.append(mover)

    if new_entries > 0:
        save_cache(cache)
        print(f"  Cache updated — {new_entries} new entries saved")

    return enriched
