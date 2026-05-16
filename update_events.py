"""
update_events.py — StreamX Live Events Auto-Updater
════════════════════════════════════════════════════
Runs every 20 minutes via GitHub Actions.
Fetches live sports/events streams from free public sources,
validates them, manages auto-expiry, and writes events.json.

Sources used (all FREE, no API key needed):
  1. StreamedSU    — streamed.su/api (free sports streams)
  2. SportFree     — sportfree.tv public schedule
  3. DaddyLiveHD   — daddylive.dad schedule endpoint
  4. VIPLeague     — vipleague.st (scraped schedule)
  5. Manual seed   — MANUAL_EVENTS in this file (always available)

Run:  python update_events.py
"""

import json
import os
import re
import time
import logging
import concurrent.futures
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ═══════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════
BASE_DIR    = os.getcwd()
OUTPUT_FILE = os.path.join(BASE_DIR, "events.json")
LOG_LEVEL   = logging.INFO

# Stream validation timeout (seconds)
VALIDATE_TIMEOUT   = 8
VALIDATE_WORKERS   = 12
MAX_STREAMS_PER_EVENT = 4

# How many minutes past end_time before we hard-delete from list
EXPIRY_GRACE_MINUTES = 30

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 Chrome/124.0.0.0 Mobile Safari/537.36",
    "VLC/3.0.20 LibVLC/3.0.20",
]

# ─── SPORT ICONS (for UI badge colour / icon) ───────────────────
SPORT_META = {
    "Cricket":     {"icon": "sports_cricket",  "color": "#00C853"},
    "Football":    {"icon": "sports_soccer",   "color": "#2962FF"},
    "Basketball":  {"icon": "sports_basketball","color": "#FF6D00"},
    "Tennis":      {"icon": "sports_tennis",   "color": "#FFD600"},
    "MMA":         {"icon": "sports_mma",      "color": "#D50000"},
    "Boxing":      {"icon": "sports_boxing",   "color": "#AA00FF"},
    "WWE":         {"icon": "sports_kabaddi",  "color": "#D50000"},
    "Formula 1":   {"icon": "directions_car",  "color": "#FF1744"},
    "Rugby":       {"icon": "sports_rugby",    "color": "#00BFA5"},
    "Baseball":    {"icon": "sports_baseball", "color": "#FF6F00"},
    "Hockey":      {"icon": "sports_hockey",   "color": "#1565C0"},
    "Golf":        {"icon": "golf_course",     "color": "#388E3C"},
    "Cycling":     {"icon": "directions_bike", "color": "#0288D1"},
    "Olympics":    {"icon": "emoji_events",    "color": "#FFD600"},
    "Other":       {"icon": "live_tv",         "color": "#E53935"},
}

# ═══════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("EventUpdater")


# ═══════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def fmt_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def headers(extra: dict = None) -> dict:
    import random
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if extra:
        h.update(extra)
    return h


def safe_get(url: str, timeout: int = 12, **kwargs) -> Optional[requests.Response]:
    try:
        r = requests.get(url, headers=headers(), timeout=timeout,
                         allow_redirects=True, **kwargs)
        if r.status_code == 200:
            return r
    except Exception as e:
        log.debug(f"GET failed {url}: {e}")
    return None


# ═══════════════════════════════════════════════════════════════════
#  STREAM VALIDATOR
# ═══════════════════════════════════════════════════════════════════

VALID_CONTENT_TYPES = {
    "video/", "application/x-mpegurl",
    "application/vnd.apple.mpegurl",
    "application/octet-stream",
    "audio/mpegurl", "audio/x-mpegurl",
}

VALID_EXTENSIONS = (".m3u8", ".ts", ".mp4", ".mpd", ".m3u")


def is_valid_stream(url: str) -> bool:
    """Returns True if the URL serves real video/stream content."""
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    if not url.startswith(("http://", "https://", "rtmp://", "rtsp://")):
        return False
    if url.startswith(("rtmp://", "rtsp://")):
        return True  # can't HEAD-check, accept if well-formed
    try:
        with requests.get(
            url, headers=headers(), stream=True,
            timeout=(5, VALIDATE_TIMEOUT), allow_redirects=True
        ) as r:
            if r.status_code not in (200, 206):
                return False
            ct = r.headers.get("Content-Type", "").lower()
            if any(ct.startswith(v) for v in VALID_CONTENT_TYPES):
                return True
            if any(url.lower().endswith(ext) for ext in VALID_EXTENSIONS):
                chunk = next(r.iter_content(512), None)
                return chunk is not None and len(chunk) > 0
            return False
    except Exception:
        return False


def validate_streams(stream_list: list[dict]) -> list[dict]:
    """
    Takes list of {"name": ..., "url": ...} and returns only working ones.
    Stops after MAX_STREAMS_PER_EVENT working streams.
    """
    results: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=VALIDATE_WORKERS) as ex:
        future_map = {
            ex.submit(is_valid_stream, s["url"]): s
            for s in stream_list if s.get("url")
        }
        for future in concurrent.futures.as_completed(future_map):
            stream = future_map[future]
            try:
                if future.result():
                    results.append(stream)
                    log.info(f"    ✅ Valid: {stream['name']} → {stream['url'][:60]}…")
                    if len(results) >= MAX_STREAMS_PER_EVENT:
                        for f in future_map:
                            f.cancel()
                        break
                else:
                    log.debug(f"    ❌ Dead: {stream['url'][:60]}")
            except Exception:
                pass
    return results


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 1 — StreamedSU (streamed.su)
#  Free sports streams, no auth needed
# ═══════════════════════════════════════════════════════════════════

def fetch_streamedsu() -> list[dict]:
    """
    Fetches today's live/upcoming events from streamed.su public API.
    Returns list of raw event dicts ready for normalize_event().
    """
    log.info("📡 [StreamedSU] Fetching schedule…")
    events = []
    try:
        r = safe_get("https://streamed.su/api/matches/all", timeout=15)
        if not r:
            return []
        data = r.json()
        now = now_utc()

        for match in data:
            try:
                title   = match.get("title", "")
                sport   = classify_sport(title, match.get("category", ""))
                date_ms = match.get("date", 0)
                if not date_ms:
                    continue

                start = datetime.fromtimestamp(date_ms / 1000, tz=timezone.utc)
                end   = start + timedelta(hours=3)

                # Skip events ended > grace period ago
                if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
                    continue

                # Skip events more than 24h in future
                if start > now + timedelta(hours=24):
                    continue

                streams_raw = []
                sources = match.get("sources", [])
                for i, src in enumerate(sources[:6]):
                    src_id  = src.get("source", "")
                    src_key = src.get("id", "")
                    if src_id and src_key:
                        # Construct stream URL for this source
                        stream_url = f"https://streamed.su/api/stream/{src_id}/{src_key}"
                        streams_raw.append({
                            "name": f"Server {i+1} ({src_id.upper()})",
                            "url":  stream_url
                        })

                if not streams_raw:
                    continue

                events.append({
                    "event_id":  f"streamed_{match.get('id', title.lower().replace(' ','_')[:30])}",
                    "title":     title,
                    "sport":     sport,
                    "start_time": fmt_iso(start),
                    "end_time":   fmt_iso(end),
                    "is_live":    start <= now <= end,
                    "streams":    streams_raw,
                    "source":     "StreamedSU",
                })
            except Exception as e:
                log.debug(f"  StreamedSU row error: {e}")

        log.info(f"  → {len(events)} candidates from StreamedSU")
    except Exception as e:
        log.warning(f"  StreamedSU failed: {e}")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 2 — DaddyLiveHD (daddylive.dad)
#  Has a public schedule JSON endpoint
# ═══════════════════════════════════════════════════════════════════

def fetch_daddylive() -> list[dict]:
    """
    Fetches DaddyLiveHD schedule from their public schedule.json
    """
    log.info("📡 [DaddyLiveHD] Fetching schedule…")
    events = []
    schedule_url = "https://daddylive.dad/schedule/schedule-generated.json"
    try:
        r = safe_get(schedule_url, timeout=15)
        if not r:
            return []
        data = r.json()
        now = now_utc()

        for date_key, categories in data.items():
            if not isinstance(categories, dict):
                continue
            for cat_name, matches in categories.items():
                if not isinstance(matches, list):
                    continue
                for match in matches:
                    try:
                        title    = match.get("event", "") or match.get("title", "")
                        time_str = match.get("time", "")
                        sport    = classify_sport(title, cat_name)
                        channels = match.get("channels", [])

                        if not title or not channels:
                            continue

                        # Parse time — DaddyLive uses "HH:MM" UTC for today/tomorrow
                        try:
                            base_date = datetime.strptime(date_key, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        except Exception:
                            base_date = now.replace(hour=0, minute=0, second=0, microsecond=0)

                        try:
                            h, m = map(int, time_str.split(":"))
                            start = base_date.replace(hour=h, minute=m, second=0)
                        except Exception:
                            start = base_date

                        end = start + timedelta(hours=3)

                        if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
                            continue
                        if start > now + timedelta(hours=24):
                            continue

                        streams_raw = []
                        for i, ch in enumerate(channels[:6]):
                            ch_id   = ch.get("channel_id", "")
                            ch_name = ch.get("channel_name", f"Server {i+1}")
                            if ch_id:
                                stream_url = f"https://daddylive.dad/stream/stream-{ch_id}.php"
                                streams_raw.append({
                                    "name": f"{ch_name} (S{i+1})",
                                    "url":  stream_url,
                                })

                        if not streams_raw:
                            continue

                        events.append({
                            "event_id":   f"daddy_{date_key}_{title.lower()[:20].replace(' ','_')}",
                            "title":      title,
                            "sport":      sport,
                            "start_time": fmt_iso(start),
                            "end_time":   fmt_iso(end),
                            "is_live":    start <= now <= end,
                            "streams":    streams_raw,
                            "source":     "DaddyLiveHD",
                        })
                    except Exception as e:
                        log.debug(f"  DaddyLive row error: {e}")

        log.info(f"  → {len(events)} candidates from DaddyLiveHD")
    except Exception as e:
        log.warning(f"  DaddyLiveHD failed: {e}")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 3 — SportFree scraper (sportfree.tv)
# ═══════════════════════════════════════════════════════════════════

def fetch_sportfree() -> list[dict]:
    """
    Scrapes SportFree.tv schedule page for live matches.
    """
    log.info("📡 [SportFree] Scraping schedule…")
    events = []
    try:
        r = safe_get("https://sportfree.tv/", timeout=15)
        if not r:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        now  = now_utc()

        # Find match containers — adjust selectors if site changes
        match_divs = soup.find_all("div", class_=re.compile(r"match|event|game", re.I))
        if not match_divs:
            match_divs = soup.find_all("li", class_=re.compile(r"match|event", re.I))

        for div in match_divs[:30]:
            try:
                title = (
                    div.find("h2") or div.find("h3") or
                    div.find(class_=re.compile("title|name", re.I))
                )
                if not title:
                    continue
                title_text = title.get_text(strip=True)
                if len(title_text) < 4:
                    continue

                sport = classify_sport(title_text, "")

                links = div.find_all("a", href=re.compile(r"stream|watch|live", re.I))
                streams_raw = []
                for i, a in enumerate(links[:4]):
                    href = a.get("href", "")
                    if not href.startswith("http"):
                        href = "https://sportfree.tv" + href
                    streams_raw.append({
                        "name": f"Stream {i+1}",
                        "url":  href,
                    })

                if not streams_raw:
                    continue

                start = now
                end   = now + timedelta(hours=3)

                events.append({
                    "event_id":   f"sportfree_{hash(title_text) & 0xFFFFFF}",
                    "title":      title_text,
                    "sport":      sport,
                    "start_time": fmt_iso(start),
                    "end_time":   fmt_iso(end),
                    "is_live":    True,
                    "streams":    streams_raw,
                    "source":     "SportFree",
                })
            except Exception as e:
                log.debug(f"  SportFree row error: {e}")

        log.info(f"  → {len(events)} candidates from SportFree")
    except Exception as e:
        log.warning(f"  SportFree failed: {e}")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 4 — Manual / Curated Seeds
#  Add known reliable event streams here manually.
#  These stay active until end_time passes.
# ═══════════════════════════════════════════════════════════════════
#
#  HOW TO ADD A MANUAL EVENT:
#  1. Copy one block below
#  2. Fill in event_id (unique), title, sport, start_time, end_time (ISO UTC)
#  3. Add 2-4 stream URLs from reliable free sources
#  4. Commit → GitHub Actions will push events.json automatically
#
MANUAL_EVENTS: list[dict] = [
    # ── EXAMPLE (will be auto-expired after end_time) ─────────
    # {
    #     "event_id":   "ipl_2026_final",
    #     "title":      "IPL 2026 Final — KKR vs MI",
    #     "sport":      "Cricket",
    #     "start_time": "2026-05-25T14:00:00Z",
    #     "end_time":   "2026-05-25T19:00:00Z",
    #     "streams": [
    #         {"name": "Server 1 (HD)",  "url": "https://example.m3u8"},
    #         {"name": "Server 2 (SD)",  "url": "https://backup.m3u8"},
    #     ],
    #     "source": "Manual",
    # },
]


# ═══════════════════════════════════════════════════════════════════
#  SPORT CLASSIFIER
# ═══════════════════════════════════════════════════════════════════

_SPORT_KEYWORDS: dict[str, list[str]] = {
    "Cricket":    ["cricket", "ipl", "t20", "odi", "test match", "bpl", "psl", "cpl", "bcci", "icc"],
    "Football":   ["football", "soccer", "premier league", "la liga", "bundesliga", "serie a",
                   "ligue 1", "champions league", "europa", "mls", "world cup", "copa", "fa cup",
                   "bundesliga", "eredivisie"],
    "Basketball": ["basketball", "nba", "wnba", "euroleague", "fiba"],
    "Tennis":     ["tennis", "atp", "wta", "grand slam", "wimbledon", "roland garros",
                   "us open", "australian open"],
    "MMA":        ["mma", "ufc", "one fc", "bellator", "pfl"],
    "Boxing":     ["boxing", "wbc", "wba", "ibf", "wbo"],
    "WWE":        ["wwe", "aew", "nxt", "smackdown", "raw", "summerslam", "wrestlemania",
                   "royal rumble", "survivor series"],
    "Formula 1":  ["formula 1", "f1", "grand prix", "motogp", "indycar", "nascar"],
    "Rugby":      ["rugby", "six nations", "super rugby", "premiership rugby"],
    "Baseball":   ["baseball", "mlb", "world series"],
    "Hockey":     ["hockey", "nhl", "iihf"],
    "Golf":       ["golf", "pga", "masters", "open championship", "ryder cup"],
    "Cycling":    ["cycling", "tour de france", "giro", "vuelta"],
    "Olympics":   ["olympic", "paralympic"],
}


def classify_sport(title: str, category: str) -> str:
    combined = (title + " " + category).lower()
    for sport, kws in _SPORT_KEYWORDS.items():
        if any(kw in combined for kw in kws):
            return sport
    return "Other"


# ═══════════════════════════════════════════════════════════════════
#  DE-DUPLICATE
# ═══════════════════════════════════════════════════════════════════

def deduplicate(events: list[dict]) -> list[dict]:
    """Remove duplicate events by event_id, keep first occurrence."""
    seen = set()
    result = []
    for ev in events:
        eid = ev.get("event_id", "")
        if eid and eid not in seen:
            seen.add(eid)
            result.append(ev)
    return result


# ═══════════════════════════════════════════════════════════════════
#  ENRICH EVENTS — add sport meta, validate streams
# ═══════════════════════════════════════════════════════════════════

def enrich_event(ev: dict) -> Optional[dict]:
    """
    Validates streams, adds sport metadata.
    Returns None if no working stream found.
    """
    sport = ev.get("sport", "Other")
    meta  = SPORT_META.get(sport, SPORT_META["Other"])

    log.info(f"🔍 Validating: [{sport}] {ev['title']}")
    working = validate_streams(ev.get("streams", []))

    if not working:
        log.warning(f"  ⚠️  No working streams — skipping: {ev['title']}")
        return None

    return {
        "event_id":   ev["event_id"],
        "title":      ev["title"],
        "sport":      sport,
        "sport_icon": meta["icon"],
        "sport_color": meta["color"],
        "start_time": ev["start_time"],
        "end_time":   ev["end_time"],
        "is_live":    ev.get("is_live", False),
        "streams":    working,
        "source":     ev.get("source", "Unknown"),
    }


# ═══════════════════════════════════════════════════════════════════
#  AUTO-EXPIRY
# ═══════════════════════════════════════════════════════════════════

def is_expired(ev: dict) -> bool:
    end = parse_iso(ev.get("end_time", ""))
    if not end:
        return False
    grace = end + timedelta(minutes=EXPIRY_GRACE_MINUTES)
    return now_utc() > grace


def update_live_flag(ev: dict) -> dict:
    start = parse_iso(ev.get("start_time", ""))
    end   = parse_iso(ev.get("end_time", ""))
    now   = now_utc()
    if start and end:
        ev["is_live"] = start <= now <= end
    return ev


# ═══════════════════════════════════════════════════════════════════
#  LOAD / SAVE events.json
# ═══════════════════════════════════════════════════════════════════

def load_existing() -> list[dict]:
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, encoding="utf-8") as f:
                data = json.load(f)
            return data.get("active_events", [])
        except Exception:
            pass
    return []


def save_events(active: list[dict]):
    payload = {
        "last_updated":  fmt_iso(now_utc()),
        "active_events": sorted(active, key=lambda e: e.get("start_time", "")),
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log.info(f"💾 Saved {len(active)} active events → {OUTPUT_FILE}")


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    log.info("═" * 60)
    log.info("🚀 StreamX Live Events Updater — START")
    log.info(f"   UTC Time: {fmt_iso(now_utc())}")
    log.info("═" * 60)

    # ── Step 1: Load existing events, remove expired ──────────────
    existing = load_existing()
    still_valid = [e for e in existing if not is_expired(e)]
    expired_count = len(existing) - len(still_valid)
    if expired_count:
        log.info(f"🗑  Removed {expired_count} expired events")
    existing_ids = {e["event_id"] for e in still_valid}

    # ── Step 2: Collect new candidates from all sources ───────────
    raw_candidates: list[dict] = []

    # Source 1 — StreamedSU
    raw_candidates.extend(fetch_streamedsu())

    # Source 2 — DaddyLiveHD
    raw_candidates.extend(fetch_daddylive())

    # Source 3 — SportFree
    raw_candidates.extend(fetch_sportfree())

    # Source 4 — Manual seeds
    for ev in MANUAL_EVENTS:
        if not is_expired(ev):
            raw_candidates.append(ev)

    # ── Step 3: De-duplicate ─────────────────────────────────────
    raw_candidates = deduplicate(raw_candidates)

    # ── Step 4: Filter out events we already have (by event_id) ──
    new_candidates = [e for e in raw_candidates if e["event_id"] not in existing_ids]
    log.info(f"🆕 {len(new_candidates)} new candidates to validate")

    # ── Step 5: Validate streams for new events ───────────────────
    enriched_new: list[dict] = []
    for ev in new_candidates:
        result = enrich_event(ev)
        if result:
            enriched_new.append(result)

    # ── Step 6: Re-validate existing events (update is_live flag) ─
    for ev in still_valid:
        update_live_flag(ev)

    # ── Step 7: Merge & save ──────────────────────────────────────
    all_active = still_valid + enriched_new
    all_active = deduplicate(all_active)  # safety

    save_events(all_active)

    log.info("═" * 60)
    log.info(f"✅ Done! Active events: {len(all_active)}")
    log.info(f"   🔴 Live now  : {sum(1 for e in all_active if e.get('is_live'))}")
    log.info(f"   🕐 Upcoming  : {sum(1 for e in all_active if not e.get('is_live'))}")
    log.info("═" * 60)


if __name__ == "__main__":
    main()
