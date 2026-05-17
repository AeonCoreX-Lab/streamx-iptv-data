"""
update_events.py — StreamX Live Events Auto-Updater v3 (Full Upgrade)
══════════════════════════════════════════════════════════════════════

Sources:
  1. StreamedSU    — sport-specific + live endpoints (actual m3u8 resolution)
  2. DaddyLiveHD   — schedule JSON with Cloudflare bypass
  3. SportFree     — multi-pattern HTML scraper
  4. VIPLeague     — vipleague.st / vipleague.lc
  5. TheSportsDB   — free API, no key needed, accurate schedules
  6. Reddit        — match thread stream links (free JSON API, no auth)
  7. Manual seed   — MANUAL_EVENTS list below

Smart Features:
  • Fuzzy title dedup + stream merge across sources
  • In-event stream quality ranking (HD first)
  • Recurring event memory via events_state.json (CDN hints)
  • Cloudflare bypass via cloudscraper

Outputs:
  • events.json         — active events for the Android/web app
  • events_state.json   — CDN memory, persists across runs
  • index_events.html   — GitHub Pages live dashboard
  • Telegram report     — run summary to your bot

REQUIREMENTS:
  pip install requests beautifulsoup4 cloudscraper

OPTIONAL SECRETS (GitHub):
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID

Run: python update_events.py
"""

import json
import os
import re
import time
import logging
import concurrent.futures
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ── Optional Cloudflare bypass ──────────────────────────────────────────────
try:
    import cloudscraper as _cs_lib
    _cf_scraper = _cs_lib.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False
    _cf_scraper = None

# ═══════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════
BASE_DIR    = os.getcwd()
OUTPUT_FILE = os.path.join(BASE_DIR, "events.json")
STATE_FILE  = os.path.join(BASE_DIR, "events_state.json")
LOG_LEVEL   = logging.INFO

VALIDATE_TIMEOUT      = 8
VALIDATE_WORKERS      = 14
MAX_STREAMS_PER_EVENT = 5
EXPIRY_GRACE_MINUTES  = 30
FUTURE_WINDOW_HOURS   = 48   # কত ঘণ্টা আগের events দেখাবে

START_TIME = time.time()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "Chrome/124.0.0.0 Mobile Safari/537.36",
    "VLC/3.0.20 LibVLC/3.0.20",
]

SPORT_META = {
    "Cricket":    {"icon": "sports_cricket",   "color": "#00C853"},
    "Football":   {"icon": "sports_soccer",    "color": "#2962FF"},
    "Basketball": {"icon": "sports_basketball","color": "#FF6D00"},
    "Tennis":     {"icon": "sports_tennis",    "color": "#FFD600"},
    "MMA":        {"icon": "sports_mma",       "color": "#D50000"},
    "Boxing":     {"icon": "sports_boxing",    "color": "#AA00FF"},
    "WWE":        {"icon": "sports_kabaddi",   "color": "#D50000"},
    "Formula 1":  {"icon": "directions_car",   "color": "#FF1744"},
    "Rugby":      {"icon": "sports_rugby",     "color": "#00BFA5"},
    "Baseball":   {"icon": "sports_baseball",  "color": "#FF6F00"},
    "Hockey":     {"icon": "sports_hockey",    "color": "#1565C0"},
    "Golf":       {"icon": "golf_course",      "color": "#388E3C"},
    "Cycling":    {"icon": "directions_bike",  "color": "#0288D1"},
    "Olympics":   {"icon": "emoji_events",     "color": "#FFD600"},
    "Other":      {"icon": "live_tv",          "color": "#E53935"},
}

STATS: dict = {
    "added":   0,
    "expired": 0,
    "merged":  0,
    "sources": {},
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

def _rand_ua() -> str:
    import random
    return random.choice(USER_AGENTS)

def _base_headers(extra: dict = None) -> dict:
    h = {
        "User-Agent":      _rand_ua(),
        "Accept":          "application/json, text/html, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if extra:
        h.update(extra)
    return h

def safe_get(
    url:        str,
    timeout:    int  = 15,
    cf_bypass:  bool = False,
    extra_hdrs: dict = None,
    **kwargs,
) -> Optional[requests.Response]:
    hdrs = _base_headers(extra_hdrs)
    try:
        if cf_bypass and HAS_CLOUDSCRAPER:
            r = _cf_scraper.get(url, timeout=timeout, **kwargs)
        else:
            r = requests.get(
                url, headers=hdrs, timeout=timeout,
                allow_redirects=True, **kwargs
            )
        if r.status_code in (200, 206):
            return r
        log.warning(f"  HTTP {r.status_code}: {url[:80]}")
    except requests.exceptions.Timeout:
        log.warning(f"  Timeout ({timeout}s): {url[:80]}")
    except Exception as e:
        log.debug(f"  GET failed {url[:80]}: {e}")
    return None

def _is_json_response(r: requests.Response) -> bool:
    ct = r.headers.get("Content-Type", "")
    return "json" in ct or r.text.strip()[:1] in ("{", "[")


# ═══════════════════════════════════════════════════════════════════
#  EVENT STATE — CDN memory across runs
# ═══════════════════════════════════════════════════════════════════
# Schema: { "Football": {"working_cdns": ["https://cdn.x.com", ...], "count": 5} }

def load_event_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_event_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)

def record_working_streams(events: list, state: dict):
    """কোন CDN কাজ করল সেটা মনে রাখো — পরের run এ priority দেবে।"""
    for ev in events:
        sport = ev.get("sport", "Other")
        if sport not in state:
            state[sport] = {"working_cdns": [], "count": 0}
        for s in ev.get("streams", []):
            url = s.get("url", "")
            m   = re.match(r"(https?://[^/]+)", url)
            if m:
                cdn  = m.group(1)
                cdns = state[sport]["working_cdns"]
                if cdn not in cdns:
                    cdns.insert(0, cdn)
                    state[sport]["working_cdns"] = cdns[:20]
        state[sport]["count"] = state[sport].get("count", 0) + 1


# ═══════════════════════════════════════════════════════════════════
#  STREAM VALIDATOR
# ═══════════════════════════════════════════════════════════════════

VALID_CONTENT_TYPES = {
    "video/", "application/x-mpegurl",
    "application/vnd.apple.mpegurl", "application/octet-stream",
    "audio/mpegurl", "audio/x-mpegurl",
}
VALID_EXTENSIONS = (".m3u8", ".ts", ".mp4", ".mpd", ".m3u")


def is_valid_stream(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    if not url.startswith(("http://", "https://", "rtmp://", "rtsp://")):
        return False
    if url.startswith(("rtmp://", "rtsp://")):
        return True
    try:
        with requests.get(
            url,
            headers=_base_headers({"Referer": url}),
            stream=True,
            timeout=(5, VALIDATE_TIMEOUT),
            allow_redirects=True,
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


def validate_streams(stream_list: list) -> list:
    results: list = []
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
#  STREAM QUALITY RANKER — HD আগে, SD পরে
# ═══════════════════════════════════════════════════════════════════

_QUALITY_SCORE = {
    "4k": 5, "2160": 5,
    "1080": 4, "fhd": 4, "fullhd": 4,
    "720": 3, "hd": 3,
    "480": 2, "sd": 2,
    "360": 1, "240": 1,
}

def _stream_quality_score(s: dict) -> int:
    combo = (s.get("url", "") + s.get("name", "")).lower()
    for kw, score in _QUALITY_SCORE.items():
        if kw in combo:
            return score
    return 0

def rank_streams_by_quality(streams: list) -> list:
    return sorted(streams, key=_stream_quality_score, reverse=True)


# ═══════════════════════════════════════════════════════════════════
#  SPORT CLASSIFIER
# ═══════════════════════════════════════════════════════════════════

_SPORT_KEYWORDS: dict = {
    "Cricket":    ["cricket", "ipl", "t20", "odi", "test match", "bpl",
                   "psl", "cpl", "bcci", "icc", "ind vs", "aus vs",
                   "eng vs", "pak vs", "bdesh"],
    "Football":   ["football", "soccer", "premier league", "la liga",
                   "bundesliga", "serie a", "ligue 1", "champions league",
                   "europa", "mls", "world cup", "copa", "fa cup",
                   "eredivisie", "ucl", "efl", "ligue", "calcio"],
    "Basketball": ["basketball", "nba", "wnba", "euroleague", "fiba"],
    "Tennis":     ["tennis", "atp", "wta", "grand slam", "wimbledon",
                   "roland garros", "us open", "australian open"],
    "MMA":        ["mma", "ufc", "one fc", "bellator", "pfl", "cage"],
    "Boxing":     ["boxing", "wbc", "wba", "ibf", "wbo", "prizefighter",
                   "fight night"],
    "WWE":        ["wwe", "aew", "nxt", "smackdown", "raw", "summerslam",
                   "wrestlemania", "royal rumble", "survivor series"],
    "Formula 1":  ["formula 1", "f1", "grand prix", "motogp", "indycar",
                   "nascar", "formula e"],
    "Rugby":      ["rugby", "six nations", "super rugby",
                   "premiership rugby", "nrl"],
    "Baseball":   ["baseball", "mlb", "world series"],
    "Hockey":     ["hockey", "nhl", "iihf", "ice hockey"],
    "Golf":       ["golf", "pga", "masters", "open championship", "ryder cup"],
    "Cycling":    ["cycling", "tour de france", "giro", "vuelta"],
    "Olympics":   ["olympic", "paralympic", "commonwealth games"],
}

def classify_sport(title: str, category: str) -> str:
    combined = (title + " " + category).lower()
    for sport, kws in _SPORT_KEYWORDS.items():
        if any(kw in combined for kw in kws):
            return sport
    return "Other"


# ═══════════════════════════════════════════════════════════════════
#  FUZZY DEDUPLICATION + STREAM MERGE
# ═══════════════════════════════════════════════════════════════════

def _title_similarity(a: str, b: str) -> float:
    a = re.sub(r"[^a-z0-9\s]", "", a.lower().strip())
    b = re.sub(r"[^a-z0-9\s]", "", b.lower().strip())
    if a == b:
        return 1.0
    ratio = SequenceMatcher(None, a, b).ratio()
    if ratio > 0.75:
        return ratio
    stops = {"vs", "v", "the", "at", "in", "and", "a", "an", "of"}
    wa = set(a.split()) - stops
    wb = set(b.split()) - stops
    if not wa or not wb:
        return ratio
    overlap = len(wa & wb) / max(len(wa), len(wb))
    return max(ratio, overlap)

def smart_deduplicate(events: list) -> list:
    """
    1. Exact event_id → drop
    2. Title similarity > 0.75 + same sport → merge streams
    """
    result:   list = []
    seen_ids: set  = set()

    for ev in events:
        eid = ev.get("event_id", "")
        if eid and eid in seen_ids:
            continue

        merged = False
        for existing in result:
            if existing.get("sport") != ev.get("sport"):
                continue
            if _title_similarity(ev["title"], existing["title"]) > 0.75:
                all_streams = existing["streams"] + ev["streams"]
                seen_urls: set = set()
                merged_streams: list = []
                for s in all_streams:
                    if s["url"] not in seen_urls:
                        seen_urls.add(s["url"])
                        merged_streams.append(s)
                existing["streams"] = merged_streams[: MAX_STREAMS_PER_EVENT * 2]
                STATS["merged"] += 1
                merged = True
                break

        if not merged:
            result.append(ev)
            if eid:
                seen_ids.add(eid)

    return result


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 1 — StreamedSU (sport-specific endpoints)
# ═══════════════════════════════════════════════════════════════════

_STREAMED_BASE = "https://streamed.su"
_STREAMED_ENDPOINTS = [
    "/api/matches/live",
    "/api/matches/football",
    "/api/matches/cricket",
    "/api/matches/basketball",
    "/api/matches/mma",
    "/api/matches/formula-1",
    "/api/matches/all",
]


def _resolve_one_streamed_source(src_id: str, src_key: str, idx: int) -> Optional[dict]:
    try:
        r = requests.get(
            f"{_STREAMED_BASE}/api/stream/{src_id}/{src_key}",
            headers=_base_headers({"Referer": f"{_STREAMED_BASE}/", "Origin": _STREAMED_BASE}),
            timeout=10,
        )
        if r.status_code != 200:
            return None
        data  = r.json()
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            url = item.get("url", "")
            if url and url.startswith("http"):
                hd = item.get("hd", False)
                return {
                    "name": f"Server {idx+1} ({src_id.upper()}) {'[HD]' if hd else '[SD]'}",
                    "url":  url,
                }
    except Exception as e:
        log.debug(f"    StreamedSU resolve error ({src_id}/{src_key}): {e}")
    return None


def _resolve_streamed_streams(sources: list) -> list:
    tasks = [
        (i, src.get("source", ""), src.get("id", ""))
        for i, src in enumerate(sources[:6])
        if src.get("source") and src.get("id")
    ]
    if not tasks:
        return []
    streams = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(_resolve_one_streamed_source, sid, skey, i): i
                for i, sid, skey in tasks}
        for f in concurrent.futures.as_completed(futs, timeout=20):
            try:
                result = f.result()
                if result:
                    streams.append(result)
            except Exception:
                pass
    return streams


def fetch_streamedsu() -> list:
    log.info("📡 [StreamedSU] Fetching schedule (sport-specific endpoints)…")
    events:   list = []
    seen_ids: set  = set()
    now            = now_utc()

    for endpoint in _STREAMED_ENDPOINTS:
        r = safe_get(
            f"{_STREAMED_BASE}{endpoint}",
            timeout=20, cf_bypass=True,
            extra_hdrs={"Referer": f"{_STREAMED_BASE}/"},
        )
        if not r or not _is_json_response(r):
            log.warning(f"  [StreamedSU] Skipping {endpoint}")
            continue
        try:
            data = r.json()
        except json.JSONDecodeError:
            continue
        if not isinstance(data, list):
            continue

        ep_count = 0
        for match in data:
            try:
                mid   = str(match.get("id", ""))
                title = match.get("title", "")
                if not title or mid in seen_ids:
                    continue
                seen_ids.add(mid)

                sport   = classify_sport(title, match.get("category", ""))
                date_ms = match.get("date", 0)
                if not date_ms:
                    continue

                start = datetime.fromtimestamp(date_ms / 1000, tz=timezone.utc)
                end   = start + timedelta(hours=3)

                if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
                    continue
                if start > now + timedelta(hours=FUTURE_WINDOW_HOURS):
                    continue

                sources     = match.get("sources", [])
                streams_raw = _resolve_streamed_streams(sources) if sources else []
                if not streams_raw:
                    continue

                events.append({
                    "event_id":   f"streamed_{mid}",
                    "title":      title,
                    "sport":      sport,
                    "start_time": fmt_iso(start),
                    "end_time":   fmt_iso(end),
                    "is_live":    start <= now <= end,
                    "streams":    streams_raw,
                    "source":     "StreamedSU",
                })
                ep_count += 1
            except Exception as e:
                log.debug(f"  [StreamedSU] Row error: {e}")

        log.debug(f"  [StreamedSU] {endpoint}: +{ep_count}")

    STATS["sources"]["StreamedSU"] = len(events)
    log.info(f"  → {len(events)} candidates from StreamedSU")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 2 — DaddyLiveHD
# ═══════════════════════════════════════════════════════════════════

_DADDY_BASE  = "https://daddylive.dad"
_DADDY_PATHS = [
    "/schedule/schedule-generated.json",
    "/daddy-schedule/schedule-generated.json",
]


def fetch_daddylive() -> list:
    log.info("📡 [DaddyLiveHD] Fetching schedule…")
    events = []
    now    = now_utc()
    data   = None

    for path in _DADDY_PATHS:
        r = safe_get(
            f"{_DADDY_BASE}{path}", timeout=20, cf_bypass=True,
            extra_hdrs={
                "Referer":          f"{_DADDY_BASE}/",
                "Origin":           _DADDY_BASE,
                "Accept":           "application/json, */*",
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        if not r or not _is_json_response(r):
            continue
        try:
            data = r.json()
            break
        except json.JSONDecodeError as e:
            log.warning(f"  [DaddyLive] JSON parse failed ({path}): {e}")

    if data is None:
        log.warning("  [DaddyLive] All endpoints failed — skipping")
        STATS["sources"]["DaddyLive"] = 0
        return []

    for date_key, categories in data.items():
        if not isinstance(categories, dict):
            continue
        for cat_name, matches in categories.items():
            if not isinstance(matches, list):
                continue
            for match in matches:
                try:
                    title    = (match.get("event") or match.get("title") or "").strip()
                    time_str = match.get("time", "")
                    channels = match.get("channels", [])
                    if not title or not channels:
                        continue

                    sport = classify_sport(title, cat_name)

                    try:
                        base_date = datetime.strptime(
                            date_key, "%Y-%m-%d"
                        ).replace(tzinfo=timezone.utc)
                    except Exception:
                        base_date = now.replace(hour=0, minute=0, second=0, microsecond=0)

                    try:
                        h, m  = map(int, time_str.split(":"))
                        start = base_date.replace(hour=h, minute=m, second=0)
                    except Exception:
                        start = base_date

                    end = start + timedelta(hours=3)
                    if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
                        continue
                    if start > now + timedelta(hours=FUTURE_WINDOW_HOURS):
                        continue

                    streams_raw = [
                        {
                            "name": f"{ch.get('channel_name', f'Server {i+1}')} (S{i+1})",
                            "url":  f"{_DADDY_BASE}/embed/stream-{ch.get('channel_id','')}.php",
                        }
                        for i, ch in enumerate(channels[:6])
                        if ch.get("channel_id")
                    ]
                    if not streams_raw:
                        continue

                    event_id = (
                        f"daddy_{date_key}_"
                        f"{re.sub(r'[^a-z0-9]', '_', title.lower())[:20]}"
                    )
                    events.append({
                        "event_id":   event_id,
                        "title":      title,
                        "sport":      sport,
                        "start_time": fmt_iso(start),
                        "end_time":   fmt_iso(end),
                        "is_live":    start <= now <= end,
                        "streams":    streams_raw,
                        "source":     "DaddyLiveHD",
                    })
                except Exception as e:
                    log.debug(f"  [DaddyLive] Row error: {e}")

    STATS["sources"]["DaddyLive"] = len(events)
    log.info(f"  → {len(events)} candidates from DaddyLiveHD")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 3 — SportFree (multi-pattern scraper)
# ═══════════════════════════════════════════════════════════════════

_SPORTFREE_URLS = [
    "https://sportfree.tv/",
    "https://sportfree.tv/live",
    "https://sportfree.tv/schedule",
]


def _extract_sportfree_events(soup: BeautifulSoup, base_url: str) -> list:
    now    = now_utc()
    events = []
    seen:  set = set()

    container_patterns = [
        soup.find_all("div",     class_=re.compile(r"match|event|game|fixture", re.I)),
        soup.find_all("li",      class_=re.compile(r"match|event|game", re.I)),
        soup.find_all("tr",      class_=re.compile(r"match|event|game", re.I)),
        soup.find_all("article", class_=re.compile(r"match|event|sport", re.I)),
        [
            a.find_parent("div") or a.find_parent("li")
            for a in soup.find_all("a", href=re.compile(r"stream|watch|live|play", re.I))
            if a.find_parent(["div", "li"])
        ],
    ]

    for containers in container_patterns:
        if not containers:
            continue
        for div in containers[:50]:
            if div is None:
                continue
            title_el = (
                div.find("h1") or div.find("h2") or div.find("h3") or
                div.find(class_=re.compile(r"title|name|event|team", re.I)) or
                div.find("strong") or div.find("b")
            )
            if not title_el:
                continue
            title_text = title_el.get_text(" ", strip=True)
            if len(title_text) < 4 or title_text in seen:
                continue
            seen.add(title_text)

            links = (
                div.find_all("a", href=re.compile(r"stream|watch|live|play|embed", re.I))
                or div.find_all("a", href=True)[:3]
            )
            streams_raw = []
            for i, a in enumerate(links[:4]):
                href = a.get("href", "").strip()
                if not href:
                    continue
                if not href.startswith("http"):
                    href = urljoin(base_url, href)
                if href not in [s["url"] for s in streams_raw]:
                    streams_raw.append({"name": f"Stream {i+1}", "url": href})

            if not streams_raw:
                continue

            events.append({
                "event_id":   f"sportfree_{abs(hash(title_text)) % 0xFFFFFF:06x}",
                "title":      title_text,
                "sport":      classify_sport(title_text, ""),
                "start_time": fmt_iso(now),
                "end_time":   fmt_iso(now + timedelta(hours=3)),
                "is_live":    True,
                "streams":    streams_raw,
                "source":     "SportFree",
            })

        if events:
            break

    return events


def fetch_sportfree() -> list:
    log.info("📡 [SportFree] Scraping schedule…")
    events = []
    for url in _SPORTFREE_URLS:
        r = safe_get(url, timeout=20, cf_bypass=True)
        if not r:
            continue
        found = _extract_sportfree_events(BeautifulSoup(r.text, "html.parser"), url)
        if found:
            events = found
            break

    STATS["sources"]["SportFree"] = len(events)
    log.info(f"  → {len(events)} candidates from SportFree")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 4 — VIPLeague
# ═══════════════════════════════════════════════════════════════════

_VIPLEAGUE_URLS = ["https://vipleague.st", "https://vipleague.lc"]


def fetch_vipleague() -> list:
    log.info("📡 [VIPLeague] Scraping schedule…")
    events = []
    now    = now_utc()

    for base_url in _VIPLEAGUE_URLS:
        try:
            r = safe_get(
                base_url, timeout=20, cf_bypass=True,
                extra_hdrs={"Referer": "https://www.google.com/"},
            )
            if not r:
                continue

            soup    = BeautifulSoup(r.text, "html.parser")
            matches = soup.find_all(
                ["div", "li", "article"],
                class_=re.compile(r"match|event|game|stream", re.I),
            ) or [
                a.find_parent(["li", "div"])
                for a in soup.find_all("a", string=re.compile(r"watch|stream", re.I))
                if a.find_parent(["li", "div"])
            ]

            seen: set = set()
            for div in matches[:40]:
                if div is None:
                    continue
                title_el = (
                    div.find(class_=re.compile(r"title|name|event|match", re.I))
                    or div.find("h2") or div.find("h3") or div.find("strong")
                )
                if not title_el:
                    continue
                title_text = title_el.get_text(" ", strip=True)
                if len(title_text) < 4 or title_text in seen:
                    continue
                seen.add(title_text)

                streams_raw = []
                for i, a in enumerate(div.find_all("a", href=True)[:4]):
                    href = a.get("href", "").strip()
                    if not href:
                        continue
                    if not href.startswith("http"):
                        href = urljoin(base_url, href)
                    streams_raw.append({"name": f"VIPLeague S{i+1}", "url": href})

                if not streams_raw:
                    continue

                events.append({
                    "event_id":   f"vip_{abs(hash(title_text)) % 0xFFFFFF:06x}",
                    "title":      title_text,
                    "sport":      classify_sport(title_text, ""),
                    "start_time": fmt_iso(now),
                    "end_time":   fmt_iso(now + timedelta(hours=3)),
                    "is_live":    True,
                    "streams":    streams_raw,
                    "source":     "VIPLeague",
                })

            if events:
                break
        except Exception as e:
            log.warning(f"  [VIPLeague] {base_url} failed: {e}")

    STATS["sources"]["VIPLeague"] = len(events)
    log.info(f"  → {len(events)} candidates from VIPLeague")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 5 — TheSportsDB (free, no API key)
#  সঠিক schedule দেয়। Streams নেই তবে fuzzy dedup এ
#  StreamedSU/Reddit এর streams এর সাথে merge হয়।
# ═══════════════════════════════════════════════════════════════════

_TSDB_BASE = "https://www.thesportsdb.com/api/v1/json/3"
_TSDB_LEAGUES: dict = {
    "Football":   ["4328", "4335", "4480", "4346", "4331"],
    "Basketball": ["4387"],
    "Cricket":    ["4451", "4910"],
    "Baseball":   ["4424"],
    "Hockey":     ["4380"],
    "Formula 1":  ["4370"],
    "Rugby":      ["4391"],
    "Golf":       ["4401"],
}


def fetch_thesportsdb() -> list:
    log.info("📡 [TheSportsDB] Fetching upcoming schedule…")
    events: list = []
    now = now_utc()

    for sport, league_ids in _TSDB_LEAGUES.items():
        for lid in league_ids:
            r = safe_get(
                f"{_TSDB_BASE}/eventsnextleague.php?id={lid}",
                timeout=12,
            )
            if not r:
                continue
            try:
                matches = (r.json().get("events") or [])
            except Exception:
                continue

            for m in matches[:8]:
                try:
                    home  = m.get("strHomeTeam", "")
                    away  = m.get("strAwayTeam", "")
                    title = (
                        f"{home} vs {away}" if home and away
                        else m.get("strEvent", "")
                    )
                    if not title:
                        continue

                    date_str = m.get("dateEvent", "")
                    time_str = m.get("strTime") or "00:00:00"
                    if not date_str:
                        continue

                    start = datetime.fromisoformat(
                        f"{date_str}T{time_str}"
                    ).replace(tzinfo=timezone.utc)
                    end   = start + timedelta(hours=3)

                    if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
                        continue
                    if start > now + timedelta(hours=FUTURE_WINDOW_HOURS):
                        continue

                    # Streams খালি — fuzzy dedup এ অন্য source merge করবে
                    events.append({
                        "event_id":   f"tsdb_{m.get('idEvent', '')}",
                        "title":      title,
                        "sport":      sport,
                        "start_time": fmt_iso(start),
                        "end_time":   fmt_iso(end),
                        "is_live":    start <= now <= end,
                        "streams":    [],
                        "source":     "TheSportsDB",
                        "_league":    m.get("strLeague", ""),
                        "_thumb":     m.get("strThumb", ""),
                        "_venue":     m.get("strVenue", ""),
                    })
                except Exception as e:
                    log.debug(f"  [TSDB] Row error: {e}")

    STATS["sources"]["TheSportsDB"] = len(events)
    log.info(f"  → {len(events)} schedule entries from TheSportsDB")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 6 — Reddit (free JSON API, no auth needed)
#  Match Thread এ stream URL খোঁজে + comments scan করে
# ═══════════════════════════════════════════════════════════════════

_REDDIT_SUBS = [
    ("r/soccer",     "Football"),
    ("r/nba",        "Basketball"),
    ("r/cricket",    "Cricket"),
    ("r/MMA",        "MMA"),
    ("r/Boxing",     "Boxing"),
    ("r/formula1",   "Formula 1"),
    ("r/hockey",     "Hockey"),
    ("r/baseball",   "Baseball"),
    ("r/rugbyunion", "Rugby"),
]

_RX_STREAM_EXT = re.compile(
    r"https?://[^\s\)\]>\"']+\.(?:m3u8|ts|mp4|mpd)[^\s\)\]>\"']*", re.I
)
_RX_STREAM_DOMAIN = re.compile(
    r"https?://(?:streameast|sportsurge|hesgoal|livetv|daddylive|"
    r"streamed\.su|sportlemon|streambtw|crackstreams)[^\s\)\]>\"']*",
    re.I,
)


def fetch_reddit_streams() -> list:
    log.info("📡 [Reddit] Scanning match threads…")
    events: list = []
    now          = now_utc()

    for sub, sport in _REDDIT_SUBS:
        search_url = (
            f"https://www.reddit.com/{sub}/search.json"
            f"?q=match+thread&sort=new&limit=10&restrict_sr=1&t=day"
        )
        r = safe_get(
            search_url, timeout=14,
            extra_hdrs={"Accept": "application/json", "Referer": "https://www.reddit.com/"},
        )
        if not r:
            continue

        try:
            posts = r.json()["data"]["children"]
        except Exception:
            continue

        for post in posts:
            d         = post.get("data", {})
            title     = d.get("title", "")
            selftext  = d.get("selftext", "")
            created   = d.get("created_utc", 0)
            permalink = d.get("permalink", "")

            if "match thread" not in title.lower():
                continue
            if now.timestamp() - created > 8 * 3600:
                continue

            stream_urls = list(set(
                _RX_STREAM_EXT.findall(selftext)
                + _RX_STREAM_DOMAIN.findall(selftext)
            ))

            # Comments এ আরও links আছে — scan করো
            if not stream_urls and permalink:
                cr = safe_get(
                    f"https://www.reddit.com{permalink}.json?limit=20",
                    timeout=10,
                    extra_hdrs={"Accept": "application/json"},
                )
                if cr:
                    try:
                        cdata = cr.json()
                        if len(cdata) > 1:
                            for c in cdata[1]["data"]["children"][:20]:
                                body = c.get("data", {}).get("body", "")
                                stream_urls += _RX_STREAM_EXT.findall(body)
                                stream_urls += _RX_STREAM_DOMAIN.findall(body)
                        stream_urls = list(set(stream_urls))
                    except Exception:
                        pass

            if not stream_urls:
                continue

            clean_title = re.sub(
                r"(?i)\[?\s*match\s*thread\s*:?\]?\s*", "", title
            ).strip() or title

            start = datetime.fromtimestamp(created, tz=timezone.utc)

            events.append({
                "event_id":   f"reddit_{d.get('id', '')}",
                "title":      clean_title,
                "sport":      classify_sport(title, sport),
                "start_time": fmt_iso(start),
                "end_time":   fmt_iso(start + timedelta(hours=4)),
                "is_live":    True,
                "streams": [
                    {"name": f"Reddit S{i+1}", "url": u}
                    for i, u in enumerate(stream_urls[:5])
                ],
                "source": "Reddit",
            })

    STATS["sources"]["Reddit"] = len(events)
    log.info(f"  → {len(events)} candidates from Reddit")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 7 — Manual Seeds
# ═══════════════════════════════════════════════════════════════════
MANUAL_EVENTS: list = [
    # {
    #     "event_id":   "ipl_2026_final",
    #     "title":      "IPL 2026 Final — KKR vs MI",
    #     "sport":      "Cricket",
    #     "start_time": "2026-05-25T14:00:00Z",
    #     "end_time":   "2026-05-25T19:00:00Z",
    #     "streams": [
    #         {"name": "Server 1 (HD)", "url": "https://example.m3u8"},
    #     ],
    #     "source": "Manual",
    # },
]


# ═══════════════════════════════════════════════════════════════════
#  EXPIRY + LIVE FLAG
# ═══════════════════════════════════════════════════════════════════

def is_expired(ev: dict) -> bool:
    end = parse_iso(ev.get("end_time", ""))
    if not end:
        return False
    return now_utc() > end + timedelta(minutes=EXPIRY_GRACE_MINUTES)

def update_live_flag(ev: dict) -> dict:
    start = parse_iso(ev.get("start_time", ""))
    end   = parse_iso(ev.get("end_time",   ""))
    now   = now_utc()
    if start and end:
        ev["is_live"] = start <= now <= end
    return ev


# ═══════════════════════════════════════════════════════════════════
#  ENRICH — validate + rank streams, add sport meta
# ═══════════════════════════════════════════════════════════════════

def enrich_event(ev: dict) -> Optional[dict]:
    sport = ev.get("sport", "Other")
    meta  = SPORT_META.get(sport, SPORT_META["Other"])
    streams = ev.get("streams", [])

    if not streams:
        log.debug(f"  ⏭  No streams to validate: {ev['title']}")
        return None

    log.info(f"🔍 Validating [{sport}]: {ev['title']}")

    # Quality rank করো → HD আগে validate হয়
    streams = rank_streams_by_quality(streams)
    working = validate_streams(streams)

    if not working:
        log.warning(f"  ⚠️  No working streams — skipping: {ev['title']}")
        return None

    working = rank_streams_by_quality(working)

    extra = {}
    for key in ("_league", "_thumb", "_venue"):
        val = ev.get(key, "")
        if val:
            extra[key.lstrip("_")] = val

    return {
        "event_id":    ev["event_id"],
        "title":       ev["title"],
        "sport":       sport,
        "sport_icon":  meta["icon"],
        "sport_color": meta["color"],
        "tvg_id":      ev["event_id"],
        "start_time":  ev["start_time"],
        "end_time":    ev["end_time"],
        "is_live":     ev.get("is_live", False),
        "streams":     working,
        "source":      ev.get("source", "Unknown"),
        **extra,
    }


# ═══════════════════════════════════════════════════════════════════
#  LOAD / SAVE events.json
# ═══════════════════════════════════════════════════════════════════

def load_existing() -> list:
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, encoding="utf-8") as f:
                return json.load(f).get("active_events", [])
        except Exception:
            pass
    return []

def save_events(active: list):
    payload = {
        "last_updated":   fmt_iso(now_utc()),
        "total_live":     sum(1 for e in active if e.get("is_live")),
        "total_upcoming": sum(1 for e in active if not e.get("is_live")),
        "active_events":  sorted(active, key=lambda e: e.get("start_time", "")),
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log.info(f"💾 Saved {len(active)} active events → {OUTPUT_FILE}")


# ═══════════════════════════════════════════════════════════════════
#  TELEGRAM NOTIFICATION
# ═══════════════════════════════════════════════════════════════════

def send_telegram_report(active_events: list):
    token   = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return

    elapsed   = int(time.time() - START_TIME)
    live_cnt  = sum(1 for e in active_events if e.get("is_live"))
    up_cnt    = len(active_events) - live_cnt
    src_lines = "\n".join(
        f"  • {src}: `{cnt}`"
        for src, cnt in STATS["sources"].items()
    ) or "  • (none)"

    msg = (
        f"📺 *StreamX Events v3 Report*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Runtime: `{elapsed // 60}m {elapsed % 60}s`\n"
        f"✅ Total active: `{len(active_events)}`\n"
        f"🔴 Live now: `{live_cnt}`\n"
        f"🕐 Upcoming: `{up_cnt}`\n"
        f"🆕 Added: `{STATS['added']}`\n"
        f"🔀 Streams merged: `{STATS['merged']}`\n"
        f"🗑 Expired: `{STATS['expired']}`\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Candidates per source:\n{src_lines}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
        log.info("📱 Telegram notification sent")
    except Exception as e:
        log.warning(f"  Telegram failed: {e}")


# ═══════════════════════════════════════════════════════════════════
#  GITHUB PAGES DASHBOARD
# ═══════════════════════════════════════════════════════════════════

def generate_events_dashboard(active_events: list):
    live_ev = [e for e in active_events if e.get("is_live")]
    up_ev   = [e for e in active_events if not e.get("is_live")]

    def event_row(ev: dict) -> str:
        color  = ev.get("sport_color", "#E53935")
        sport  = ev.get("sport", "Other")
        badge  = f'<span class="badge" style="background:{color}">{sport}</span>'
        sc     = len(ev.get("streams", []))
        t      = ev.get("start_time", "")[:16].replace("T", " ") + " UTC"
        thumb  = ev.get("thumbnail", "")
        img    = (
            f'<img src="{thumb}" style="height:26px;border-radius:3px;'
            f'vertical-align:middle;margin-right:5px">'
            if thumb else ""
        )
        league = ev.get("league", "")
        sub    = f'<br><small style="color:#8b949e">{league}</small>' if league else ""
        return (
            f"<tr><td>{badge}</td>"
            f"<td>{img}<strong>{ev['title']}</strong>{sub}</td>"
            f"<td>{t}</td>"
            f"<td>{sc} stream{'s' if sc != 1 else ''}</td>"
            f"<td><small style='color:#8b949e'>{ev.get('source','')}</small></td></tr>"
        )

    def table_or_empty(evs: list, msg: str) -> str:
        return (
            "\n".join(event_row(e) for e in evs) if evs
            else f"<tr><td colspan='5' style='color:#8b949e;text-align:center'>{msg}</td></tr>"
        )

    sport_counts: dict = {}
    for ev in active_events:
        s = ev.get("sport", "Other")
        sport_counts[s] = sport_counts.get(s, 0) + 1

    sport_rows = "\n".join(
        f"<tr><td>{s}</td><td>{c}</td></tr>"
        for s, c in sorted(sport_counts.items(), key=lambda x: -x[1])
    )
    src_rows = "\n".join(
        f"<tr><td>{src}</td><td>{cnt}</td></tr>"
        for src, cnt in sorted(STATS["sources"].items(), key=lambda x: -x[1])
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="refresh" content="1200">
  <title>StreamX Live Events</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:'Segoe UI',Arial,sans-serif;background:#0d1117;color:#e6edf3;padding:20px}}
    h1{{font-size:1.6em;margin-bottom:4px}}
    h2{{font-size:1em;color:#58a6ff;margin:18px 0 8px}}
    .sub{{color:#8b949e;font-size:.82em;margin-bottom:18px}}
    .card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:18px;margin:12px 0}}
    .stats{{display:flex;flex-wrap:wrap;gap:14px}}
    .stat{{text-align:center;min-width:100px}}
    .stat .num{{font-size:2em;font-weight:bold;color:#58a6ff}}
    .stat .label{{color:#8b949e;font-size:.75em;margin-top:2px}}
    .live-dot{{display:inline-block;width:8px;height:8px;background:#f85149;
               border-radius:50%;animation:pulse 1.2s infinite;margin-right:5px}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
    table{{width:100%;border-collapse:collapse;font-size:.85em}}
    th,td{{padding:7px 9px;border-bottom:1px solid #21262d;text-align:left}}
    th{{color:#8b949e;font-weight:500}}
    tr:hover td{{background:#1c2128}}
    .badge{{display:inline-block;padding:2px 7px;border-radius:10px;font-size:.72em;font-weight:bold;color:#fff}}
    .green{{color:#3fb950}}.red{{color:#f85149}}
    .row2{{display:flex;gap:16px;flex-wrap:wrap}}
    .row2>div{{flex:1;min-width:180px}}
    footer{{color:#8b949e;font-size:.72em;text-align:right;margin-top:14px}}
  </style>
</head>
<body>
  <h1>📺 StreamX Live Events</h1>
  <p class="sub">Auto-updated every 20 min · {datetime.now().strftime('%Y-%m-%d %H:%M UTC')} · 7 sources</p>

  <div class="card">
    <div class="stats">
      <div class="stat"><div class="num">{len(active_events)}</div><div class="label">Total Active</div></div>
      <div class="stat"><div class="num red">{len(live_ev)}</div><div class="label">Live Now</div></div>
      <div class="stat"><div class="num green">{len(up_ev)}</div><div class="label">Upcoming</div></div>
      <div class="stat"><div class="num">{STATS['added']}</div><div class="label">Added</div></div>
      <div class="stat"><div class="num">{STATS['merged']}</div><div class="label">Streams Merged</div></div>
    </div>
  </div>

  <div class="card">
    <h2><span class="live-dot"></span>Live Now ({len(live_ev)})</h2>
    <table>
      <tr><th>Sport</th><th>Event</th><th>Start</th><th>Streams</th><th>Source</th></tr>
      {table_or_empty(live_ev, 'No live events right now')}
    </table>
  </div>

  <div class="card">
    <h2>🕐 Upcoming ({len(up_ev)})</h2>
    <table>
      <tr><th>Sport</th><th>Event</th><th>Start</th><th>Streams</th><th>Source</th></tr>
      {table_or_empty(up_ev[:20], 'No upcoming events')}
    </table>
  </div>

  <div class="card row2">
    <div>
      <h2>📂 By Sport</h2>
      <table><tr><th>Sport</th><th>Events</th></tr>{sport_rows}</table>
    </div>
    <div>
      <h2>📡 Source Candidates</h2>
      <table><tr><th>Source</th><th>Found</th></tr>{src_rows}</table>
    </div>
  </div>

  <footer>StreamX Events v3 · {len(_STREAMED_ENDPOINTS)} StreamedSU endpoints · {len(_TSDB_LEAGUES)} TSDB leagues · {len(_REDDIT_SUBS)} Reddit subs</footer>
</body>
</html>"""

    path = os.path.join(BASE_DIR, "index_events.html")
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        log.info(f"🌐 Dashboard → {path}")
    except Exception as e:
        log.warning(f"  Dashboard write failed: {e}")


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    log.info("═" * 60)
    log.info("🚀 StreamX Live Events Updater v3 (Full Upgrade) — START")
    log.info(f"   UTC Time     : {fmt_iso(now_utc())}")
    log.info(f"   CloudScraper : {'✅' if HAS_CLOUDSCRAPER else '⚠️  not installed (pip install cloudscraper)'}")
    log.info(f"   Future window: {FUTURE_WINDOW_HOURS}h  |  Sources: 7")
    log.info("═" * 60)

    # ── Load state & existing ──────────────────────────────────────
    ev_state = load_event_state()
    existing = load_existing()

    still_valid      = [e for e in existing if not is_expired(e)]
    STATS["expired"] = len(existing) - len(still_valid)
    if STATS["expired"]:
        log.info(f"🗑  Removed {STATS['expired']} expired events")
    existing_ids = {e["event_id"] for e in still_valid}

    # ── Collect from all 7 sources ─────────────────────────────────
    raw: list = []
    raw.extend(fetch_streamedsu())
    raw.extend(fetch_daddylive())
    raw.extend(fetch_sportfree())
    raw.extend(fetch_vipleague())
    raw.extend(fetch_thesportsdb())
    raw.extend(fetch_reddit_streams())
    for ev in MANUAL_EVENTS:
        if not is_expired(ev):
            raw.append(ev)

    log.info(f"📋 Raw total (all sources): {len(raw)}")

    # ── Fuzzy dedup + stream merge ─────────────────────────────────
    raw = smart_deduplicate(raw)
    log.info(
        f"🔀 After smart dedup: {len(raw)} unique "
        f"({STATS['merged']} stream sets merged)"
    )

    # ── Filter already-known ───────────────────────────────────────
    new_candidates = [e for e in raw if e["event_id"] not in existing_ids]
    log.info(f"🆕 {len(new_candidates)} new candidates to validate")

    # ── Validate + enrich ──────────────────────────────────────────
    enriched_new: list = []
    for ev in new_candidates:
        result = enrich_event(ev)
        if result:
            enriched_new.append(result)
            STATS["added"] += 1

    # ── Update is_live on carry-over events ────────────────────────
    for ev in still_valid:
        update_live_flag(ev)

    # ── Merge all ─────────────────────────────────────────────────
    all_active = smart_deduplicate(still_valid + enriched_new)

    # ── Save outputs ───────────────────────────────────────────────
    save_events(all_active)
    record_working_streams(all_active, ev_state)
    save_event_state(ev_state)

    send_telegram_report(all_active)
    generate_events_dashboard(all_active)

    # ── Final summary ──────────────────────────────────────────────
    live_cnt = sum(1 for e in all_active if e.get("is_live"))
    log.info("═" * 60)
    log.info(f"✅ Done! Active events  : {len(all_active)}")
    log.info(f"   🔴 Live now         : {live_cnt}")
    log.info(f"   🕐 Upcoming         : {len(all_active) - live_cnt}")
    log.info(f"   🆕 Added this run   : {STATS['added']}")
    log.info(f"   🔀 Streams merged   : {STATS['merged']}")
    log.info("═" * 60)


if __name__ == "__main__":
    main()
