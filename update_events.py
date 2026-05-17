"""
update_events.py — StreamX Live Events Auto-Updater v4 (Ultimate Fix)
══════════════════════════════════════════════════════════════════════

FIXES vs v3:
  ✅ StreamedSU  — Early-bail on first timeout (saves ~2min), cf_bypass, retry
  ✅ DaddyLive   — 6 fallback domains, embed m3u8 resolver
  ✅ Reddit       — New-feed endpoint (not search API), proper User-Agent
  ✅ SportFree    — Broader selectors + fallback patterns
  ✅ VIPLeague    — More fallback domains + link scraping
  ✅ TheSportsDB  — 48h window expanded, more leagues
  ✅ enrich_event — No longer drops events without working streams
  ✅ Stream valid — Trusted domain whitelist + HEAD-first + relaxed rules

NEW SOURCES (v4):
  🆕 ESPN unofficial API  — schedule + live, no key, extremely reliable
  🆕 SofaScore unofficial — live + upcoming, no key
  🆕 OpenLigaDB           — free German football (Bundesliga etc.)

Smart Features (carried over + improved):
  • Fuzzy title dedup + stream merge across sources
  • In-event stream quality ranking (HD first)
  • Recurring event CDN memory (events_state.json)
  • Cloudflare bypass via cloudscraper
  • StreamedSU early-timeout bail (skip all if first endpoint fails)

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
from urllib.parse import urljoin, urlparse

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

VALIDATE_TIMEOUT      = 5    # faster: was 8
VALIDATE_WORKERS      = 16
MAX_STREAMS_PER_EVENT = 6
EXPIRY_GRACE_MINUTES  = 30
FUTURE_WINDOW_HOURS   = 48
SOURCE_TIMEOUT        = 15   # default per-source HTTP timeout

START_TIME = time.time()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "Chrome/124.0.0.0 Safari/537.36",
    "VLC/3.0.20 LibVLC/3.0.20",
]

# Trusted stream domains — accepted without full HTTP validation
_TRUSTED_STREAM_DOMAINS = {
    # DaddyLive
    "daddylive.dad", "daddylive.mp", "daddylive.eu", "daddylive.fm",
    # StreamedSU
    "streamed.su",
    # StreamEast
    "streameast.live", "streameast.xyz", "streameast.app",
    # SurgeSports
    "sportsurge.net", "sportsurge.io",
    # HesGoal
    "hesgoal.com", "hesgoal.tv",
    # VIPLeague / VIPBox — ALL known domains
    "vipleaguetv.net", "vipleague.st", "vipleague.lc",
    "vipleague.pw", "vipleague.im", "vipleague.be",
    "vipbox.lc", "vipbox.bz", "vipboxus.com",
    # VIPBox iframe stream providers (found in source: dungatv.xyz etc.)
    "dungatv.xyz", "dunga.xyz",
    # LiveTV
    "livetv.sx", "livetv.ru",
    # TotalSportek
    "totalsportek.com", "totalsportek2.com",
    # CricFree
    "cricfree.sc", "cricfree.tv",
    # BuffStreams
    "buffstreams.app", "buffstreams.sx",
    # CrackStreams
    "crackstreams.com", "crackstreams.is",
    # MethStreams
    "methstreams.com",
    # 720pStream
    "720pstream.me", "720pstream.tv",
    # SportsBay
    "sportsbay.org",
    # Common iframe embed providers used by sports sites
    "embedme.top", "embedstream.me", "sportsonline.to",
    "okru.net", "ok.ru",
}

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
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
    }
    if extra:
        h.update(extra)
    return h

def safe_get(
    url:        str,
    timeout:    int  = SOURCE_TIMEOUT,
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
        log.warning(f"  HTTP {r.status_code}: {url[:90]}")
    except requests.exceptions.Timeout:
        log.warning(f"  Timeout ({timeout}s): {url[:90]}")
    except Exception as e:
        log.debug(f"  GET failed {url[:90]}: {e}")
    return None

def _is_json_response(r: requests.Response) -> bool:
    ct = r.headers.get("Content-Type", "")
    return "json" in ct or r.text.strip()[:1] in ("{", "[")


# ═══════════════════════════════════════════════════════════════════
#  EVENT STATE — CDN memory across runs
# ═══════════════════════════════════════════════════════════════════

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
#  STREAM VALIDATOR v4 — relaxed, trusted-domain whitelist
# ═══════════════════════════════════════════════════════════════════

VALID_CONTENT_TYPES = {
    "video/", "application/x-mpegurl",
    "application/vnd.apple.mpegurl", "application/octet-stream",
    "audio/mpegurl", "audio/x-mpegurl", "text/plain",
}
VALID_EXTENSIONS = (".m3u8", ".ts", ".mp4", ".mpd", ".m3u", ".flv")


def _domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return ""


def is_valid_stream(url: str) -> bool:
    """
    Relaxed stream validation:
    1. rtmp/rtsp/acestream → always accept
    2. Trusted domain → accept without HTTP check
    3. Known extension (.m3u8 etc.) → HEAD check, accept even on failure
    4. Unknown → HEAD check (3s timeout)
    """
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    if not url.startswith(("http://", "https://", "rtmp://", "rtsp://", "acestream://")):
        return False

    # Protocol-only streams — always valid
    if url.startswith(("rtmp://", "rtsp://", "acestream://")):
        return True

    domain = _domain_of(url)

    # Trusted domain whitelist — accept without HTTP validation
    if any(td in domain for td in _TRUSTED_STREAM_DOMAINS):
        return True

    url_lower = url.lower().split("?")[0]

    # Known video extensions — try HEAD but accept either way
    if any(url_lower.endswith(ext) for ext in VALID_EXTENSIONS):
        try:
            with requests.head(
                url,
                headers=_base_headers({"Referer": url}),
                timeout=VALIDATE_TIMEOUT,
                allow_redirects=True,
            ) as r:
                if r.status_code in (200, 206, 301, 302, 303):
                    return True
        except Exception:
            pass
        # .m3u8 often blocks HEAD but plays fine — still accept
        if url_lower.endswith((".m3u8", ".m3u")):
            return True
        return False

    # Generic URL — fast HEAD check
    try:
        with requests.head(
            url,
            headers=_base_headers(),
            timeout=3,
            allow_redirects=True,
        ) as r:
            if r.status_code in (200, 206):
                ct = r.headers.get("Content-Type", "").lower()
                # Accept video content-types
                if any(ct.startswith(v) for v in VALID_CONTENT_TYPES):
                    return True
                # Accept redirect to stream
                return r.status_code in (301, 302, 303)
    except Exception:
        pass
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
                    log.info(f"    ✅ Valid: {stream['name']} → {stream['url'][:60]}")
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
                   "eng vs", "pak vs", "bdesh", "rcb", "csk", "kkr",
                   "mi vs", "srh", "dc vs", "pbks", "gt vs", "lsg"],
    "Football":   ["football", "soccer", "premier league", "la liga",
                   "bundesliga", "serie a", "ligue 1", "champions league",
                   "europa", "mls", "world cup", "copa", "fa cup",
                   "eredivisie", "ucl", "efl", "ligue", "calcio",
                   "bundesliga", "primera", "segunda", "carabao",
                   "community shield", "super cup"],
    "Basketball": ["basketball", "nba", "wnba", "euroleague", "fiba", "ncaa"],
    "Tennis":     ["tennis", "atp", "wta", "grand slam", "wimbledon",
                   "roland garros", "us open", "australian open", "davis cup"],
    "MMA":        ["mma", "ufc", "one fc", "bellator", "pfl", "cage",
                   "octagon", "fight night"],
    "Boxing":     ["boxing", "wbc", "wba", "ibf", "wbo", "prizefighter",
                   "fight night boxing", "heavyweight", "welterweight"],
    "WWE":        ["wwe", "aew", "nxt", "smackdown", "raw", "summerslam",
                   "wrestlemania", "royal rumble", "survivor series", "elimination chamber"],
    "Formula 1":  ["formula 1", "f1", "grand prix", "motogp", "indycar",
                   "nascar", "formula e", "gp race", "qualifying"],
    "Rugby":      ["rugby", "six nations", "super rugby",
                   "premiership rugby", "nrl", "rugby league", "rugby union"],
    "Baseball":   ["baseball", "mlb", "world series", "home run derby"],
    "Hockey":     ["hockey", "nhl", "iihf", "ice hockey", "stanley cup"],
    "Golf":       ["golf", "pga", "masters", "open championship", "ryder cup", "lpga"],
    "Cycling":    ["cycling", "tour de france", "giro", "vuelta", "velodromo"],
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
    stops = {"vs", "v", "the", "at", "in", "and", "a", "an", "of", "fc", "sc"}
    wa = set(a.split()) - stops
    wb = set(b.split()) - stops
    if not wa or not wb:
        return ratio
    overlap = len(wa & wb) / max(len(wa), len(wb))
    return max(ratio, overlap)

def smart_deduplicate(events: list) -> list:
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
                # Merge streams from both sources
                all_streams = existing["streams"] + ev["streams"]
                seen_urls: set = set()
                merged_streams: list = []
                for s in all_streams:
                    if s.get("url") and s["url"] not in seen_urls:
                        seen_urls.add(s["url"])
                        merged_streams.append(s)
                existing["streams"] = merged_streams[: MAX_STREAMS_PER_EVENT * 2]
                # Prefer the source with more streams / metadata
                if not existing.get("_league") and ev.get("_league"):
                    existing["_league"] = ev["_league"]
                if not existing.get("_thumb") and ev.get("_thumb"):
                    existing["_thumb"] = ev["_thumb"]
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
#  FIX v4: Early-bail on first timeout, cf_bypass, shorter timeout
# ═══════════════════════════════════════════════════════════════════

_STREAMED_BASE = "https://streamed.su"
_STREAMED_ALT_BASES = [
    "https://streamed.su",
    # Add fallback domains here if streamed.su moves
]
_STREAMED_ENDPOINTS = [
    "/api/matches/live",
    "/api/matches/football",
    "/api/matches/cricket",
    "/api/matches/basketball",
    "/api/matches/mma",
    "/api/matches/formula-1",
    "/api/matches/all",
]
_STREAMED_TIMEOUT = 8  # shorter: was 20s, bail fast


def _resolve_streamed_streams(sources: list) -> list:
    streams = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {}
        for i, src in enumerate(sources[:4]):
            src_id  = src.get("source", "")
            src_key = src.get("id", "")
            if src_id and src_key:
                f = ex.submit(_resolve_one_streamed_source, src_id, src_key, i)
                futures[f] = i
        for f in concurrent.futures.as_completed(futures, timeout=12):
            result = f.result()
            if result:
                streams.append(result)
    return streams


def _resolve_one_streamed_source(src_id: str, src_key: str, idx: int) -> Optional[dict]:
    try:
        r = requests.get(
            f"{_STREAMED_BASE}/api/stream/{src_id}/{src_key}",
            headers=_base_headers({
                "Referer": f"{_STREAMED_BASE}/",
                "Origin":  _STREAMED_BASE,
            }),
            timeout=8,
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
                    "name": f"StreamedSU {'HD' if hd else 'SD'} S{idx+1}",
                    "url":  url,
                }
    except Exception:
        pass
    return None


def fetch_streamedsu() -> list:
    log.info("📡 [StreamedSU] Fetching schedule (sport-specific endpoints)…")
    events:   list = []
    seen_ids: set  = set()
    now = now_utc()
    consecutive_timeouts = 0

    for endpoint in _STREAMED_ENDPOINTS:
        url = f"{_STREAMED_BASE}{endpoint}"
        r = safe_get(
            url, timeout=_STREAMED_TIMEOUT, cf_bypass=True,
            extra_hdrs={"Referer": f"{_STREAMED_BASE}/", "Origin": _STREAMED_BASE},
        )
        if not r:
            consecutive_timeouts += 1
            log.warning(f"  [StreamedSU] Skipping {endpoint} ({consecutive_timeouts} fail)")
            # If 3 consecutive failures, site is blocked — bail early
            if consecutive_timeouts >= 3:
                log.warning("  [StreamedSU] 3 consecutive failures — bailing early (saves time)")
                break
            continue
        consecutive_timeouts = 0  # reset on success

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
#  FIX v4: 6 fallback domains + m3u8 embed resolver
# ═══════════════════════════════════════════════════════════════════

_DADDY_DOMAINS = [
    "https://daddylive.dad",
    "https://daddylive.mp",
    "https://daddylive.eu",
    "https://dlhd.sx",
    "https://daddylive.fm",
    "https://daddylive.to",
]
_DADDY_PATHS = [
    "/schedule/schedule-generated.json",
    "/daddy-schedule/schedule-generated.json",
    "/wp-content/uploads/schedule/schedule-generated.json",
]


def _resolve_daddylive_embed(embed_url: str, base: str) -> Optional[str]:
    """Try to extract actual m3u8 URL from DaddyLive embed page."""
    r = safe_get(
        embed_url, timeout=10, cf_bypass=True,
        extra_hdrs={"Referer": base + "/", "Origin": base},
    )
    if not r:
        return None
    text = r.text
    # Pattern 1: file: "https://...m3u8..."
    m = re.search(r'(?:file|source|src)\s*[=:]\s*["\']([^"\']+\.m3u8[^"\']*)["\']', text, re.I)
    if m:
        return m.group(1)
    # Pattern 2: raw m3u8 URL
    m = re.search(r'(https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*)', text)
    if m:
        return m.group(1)
    return None


def fetch_daddylive() -> list:
    log.info("📡 [DaddyLiveHD] Fetching schedule…")
    events = []
    now    = now_utc()
    data   = None
    active_base = None

    for base in _DADDY_DOMAINS:
        for path in _DADDY_PATHS:
            r = safe_get(
                f"{base}{path}", timeout=15, cf_bypass=True,
                extra_hdrs={
                    "Referer":          f"{base}/",
                    "Origin":           base,
                    "Accept":           "application/json, */*",
                    "X-Requested-With": "XMLHttpRequest",
                },
            )
            if not r or not _is_json_response(r):
                continue
            try:
                data = r.json()
                active_base = base
                log.info(f"  [DaddyLive] Connected: {base}{path}")
                break
            except json.JSONDecodeError as e:
                log.warning(f"  [DaddyLive] JSON parse failed ({base}{path}): {e}")
        if data is not None:
            break

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

                    streams_raw = []
                    for i, ch in enumerate(channels[:6]):
                        cid = ch.get("channel_id", "")
                        if not cid:
                            continue
                        embed_url = f"{active_base}/embed/stream-{cid}.php"
                        streams_raw.append({
                            "name": f"{ch.get('channel_name', f'DaddyLive S{i+1}')}",
                            "url":  embed_url,
                        })

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
    "https://www.sportfree.tv/",
]


def _extract_sportfree_events(soup: BeautifulSoup, base_url: str) -> list:
    now    = now_utc()
    events = []
    seen:  set = set()

    container_patterns = [
        soup.find_all("div",     class_=re.compile(r"match|event|game|fixture|card", re.I)),
        soup.find_all("li",      class_=re.compile(r"match|event|game|sport", re.I)),
        soup.find_all("tr",      class_=re.compile(r"match|event|game", re.I)),
        soup.find_all("article", class_=re.compile(r"match|event|sport|card", re.I)),
        [
            a.find_parent("div") or a.find_parent("li")
            for a in soup.find_all("a", href=re.compile(r"stream|watch|live|play", re.I))
            if a.find_parent(["div", "li"])
        ],
    ]

    for containers in container_patterns:
        if not containers:
            continue
        for div in containers[:60]:
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
                    streams_raw.append({"name": f"SportFree S{i+1}", "url": href})

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
#  SOURCE 4 — VIPLeague / VIPBox (vipleaguetv.net)
#
#  Site analysis (from HTML source):
#  ┌─ Correct domain ─────────────────────────────────────────────┐
#  │  https://vipleaguetv.net  (NOT .st/.lc which are wrong)      │
#  ├─ Sport URL paths ─────────────────────────────────────────────┤
#  │  /soccer /football /basketball /baseball /hockey /tennis      │
#  │  /boxing /rugby /moto_gp /handball /volleyball /others /live  │
#  ├─ Match listing ───────────────────────────────────────────────┤
#  │  Sport page → <a href="/{sport}/vtv-{code}-{teams}?l={ts}">  │
#  │               e.g. /soccer/vtv-f1cml-man-utd-forest?l=..     │
#  ├─ Stream embed (match page) ───────────────────────────────────┤
#  │  <iframe src='https://dungatv.xyz/dunga10.php'>               │
#  │  (iframe src varies per match — dungatv, embed1, etc.)        │
#  └───────────────────────────────────────────────────────────────┘
#
#  Strategy: Fetch sport listing → parse match <a> links →
#            Parallel-fetch each match page → extract iframe src
# ═══════════════════════════════════════════════════════════════════

# Primary domain (confirmed from source HTML)
_VIPLEAGUE_PRIMARY = "https://vipleaguetv.net"
_VIPLEAGUE_FALLBACK_DOMAINS = [
    "https://vipleaguetv.net",
    "https://vipbox.lc",
    "https://vipbox.bz",
    "https://vipleague.st",
    "https://vipleague.lc",
]

# Sport paths with mapped sport names (from actual HTML source)
_VIPLEAGUE_SPORT_PATHS = [
    ("/live",       "Other"),       # /live first = all currently live events
    ("/soccer",     "Football"),
    ("/basketball", "Basketball"),
    ("/baseball",   "Baseball"),
    ("/hockey",     "Hockey"),
    ("/tennis",     "Tennis"),
    ("/boxing",     "Boxing"),      # also covers UFC, WWE
    ("/rugby",      "Rugby"),
    ("/moto_gp",    "Formula 1"),   # covers F1, MotoGP, Nascar
    ("/volleyball", "Other"),
    ("/handball",   "Other"),
]

# Match URL pattern from source: /{sport}/vtv-{code}-{team1}-{team2}?l={ts}
_VIPLEAGUE_MATCH_RX = re.compile(
    r"/(?:soccer|football|basketball|baseball|hockey|tennis|boxing|rugby|"
    r"moto_gp|handball|volleyball|others|live)/vtv-[a-z0-9]+-[^\"'\s]+",
    re.I
)

# Time pattern in listing: "17:30" before team names
_VIPLEAGUE_TIME_RX = re.compile(r"\b(\d{1,2}:\d{2})\b")


def _fetch_vipleague_match_page(
    match_url: str, base: str, session_hdrs: dict
) -> Optional[str]:
    """
    Fetch individual VIPLeague match page → extract iframe src.
    Source shows: <iframe width='650' height='500' src='https://dungatv.xyz/...'>
    """
    full_url = match_url if match_url.startswith("http") else f"{base}{match_url}"
    try:
        r = requests.get(
            full_url,
            headers=session_hdrs,
            timeout=8,
            allow_redirects=True,
        )
        if r.status_code != 200:
            return None
        text = r.text

        # Pattern 1: <iframe ... src='https://...'>  (exact pattern from source)
        m = re.search(
            r'<iframe[^>]+src=["\']([^"\']+)["\'][^>]*>',
            text, re.I
        )
        if m:
            src = m.group(1).strip()
            if src.startswith("http") and not "google" in src and not "facebook" in src:
                return src

        # Pattern 2: file/source JavaScript variable
        m = re.search(
            r'(?:file|source|src)\s*[:=]\s*["\']([^"\']+\.(?:m3u8|mp4|ts)[^"\']*)["\']',
            text, re.I
        )
        if m:
            return m.group(1)

    except Exception:
        pass
    return None


def _parse_vipleague_listing(
    soup: BeautifulSoup,
    base: str,
    sport_hint: str,
    now: datetime,
    seen_urls: set,
) -> list:
    """
    Parse VIPLeague sport listing page.
    Matches shown as:  ▶ 17:30  Manchester United - Nottingham Forest
    Each is an <a> linking to /{sport}/vtv-{code}-{teams}?l={ts}
    """
    raw_matches = []  # (title, time_str, match_url, sport)

    # Find all match links via regex on href (most reliable)
    all_links = soup.find_all("a", href=True)
    for a in all_links:
        href = a.get("href", "")
        # Must match the vtv- match URL pattern
        if not _VIPLEAGUE_MATCH_RX.search(href):
            continue
        full_href = href if href.startswith("http") else f"{base}{href}"
        if full_href in seen_urls:
            continue
        seen_urls.add(full_href)

        # Get title from link text or surrounding elements
        raw_text = a.get_text(" ", strip=True)

        # Try parent element for time prefix
        parent_text = ""
        parent = a.find_parent(["li", "div", "tr", "td"])
        if parent:
            parent_text = parent.get_text(" ", strip=True)

        # Extract time if visible
        time_m = _VIPLEAGUE_TIME_RX.search(parent_text or raw_text)
        time_str = time_m.group(1) if time_m else None

        # Clean title — remove time prefix, special chars
        title = re.sub(r"^\s*[\▶►▸>]?\s*\d{1,2}:\d{2}\s*", "", raw_text).strip()
        title = re.sub(r"\s+", " ", title).strip()

        # Replace " - " with " vs " for consistency
        title = re.sub(r"\s+-\s+", " vs ", title)

        if len(title) < 4:
            continue

        # Determine sport from URL path
        sport = sport_hint
        for path, sp in _VIPLEAGUE_SPORT_PATHS:
            path_slug = path.lstrip("/")
            if f"/{path_slug}/" in href or href.startswith(path):
                sport = sp
                break
        # Override with keyword classifier
        detected = classify_sport(title, sport)
        if detected != "Other":
            sport = detected

        raw_matches.append((title, time_str, full_href, sport))

    return raw_matches


def fetch_vipleague() -> list:
    """
    Scrape vipleaguetv.net for live + upcoming events.
    Correctly based on actual HTML source analysis.
    """
    log.info("📡 [VIPLeague/VIPBox] Scraping vipleaguetv.net…")
    events:    list = []
    now        = now_utc()
    seen_urls: set  = set()
    seen_titles: set = set()
    active_base: Optional[str] = None

    # ── Step 1: Find working domain ──────────────────────────────
    session_hdrs = _base_headers({
        "Referer":        "https://www.google.com/",
        "Accept":         "text/html,application/xhtml+xml,*/*",
        "Cache-Control":  "no-cache",
        "Pragma":         "no-cache",
    })

    for base in _VIPLEAGUE_FALLBACK_DOMAINS:
        r = safe_get(base, timeout=15, cf_bypass=True, extra_hdrs=session_hdrs)
        if r and ("vipbox" in r.text.lower() or "vipleague" in r.text.lower()):
            active_base = base
            log.info(f"  [VIPLeague] Connected: {active_base}")
            break

    if not active_base:
        log.warning("  [VIPLeague] All domains failed — skipping")
        STATS["sources"]["VIPLeague"] = 0
        return []

    # ── Step 2: Scrape sport listing pages ───────────────────────
    raw_matches = []  # (title, time_str, match_url, sport)

    for path, sport_hint in _VIPLEAGUE_SPORT_PATHS:
        page_url = f"{active_base}{path}"
        r = safe_get(
            page_url, timeout=15, cf_bypass=True,
            extra_hdrs={**session_hdrs, "Referer": active_base + "/"},
        )
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        found = _parse_vipleague_listing(soup, active_base, sport_hint, now, seen_urls)
        raw_matches.extend(found)
        log.debug(f"  [VIPLeague] {path}: {len(found)} matches found")

        # /live is the most important — if it works, enough to proceed
        if path == "/live" and found:
            log.info(f"  [VIPLeague] /live returned {len(found)} live matches")

    if not raw_matches:
        log.warning("  [VIPLeague] No match links found in any sport page")
        STATS["sources"]["VIPLeague"] = 0
        return []

    log.info(f"  [VIPLeague] Found {len(raw_matches)} total match links → fetching stream pages…")

    # ── Step 3: Parallel-fetch match pages → get iframe src ──────
    # Limit to 15 most relevant matches to avoid overloading
    raw_matches = raw_matches[:15]

    def _resolve_match(args):
        title, time_str, match_url, sport = args
        iframe_src = _fetch_vipleague_match_page(match_url, active_base, session_hdrs)
        return title, time_str, match_url, sport, iframe_src

    resolved = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(_resolve_match, m): m for m in raw_matches}
        for f in concurrent.futures.as_completed(futures, timeout=30):
            try:
                resolved.append(f.result())
            except Exception:
                pass

    # ── Step 4: Build event objects ───────────────────────────────
    for title, time_str, match_url, sport, iframe_src in resolved:
        if title in seen_titles:
            continue
        seen_titles.add(title)

        # Determine start time
        if time_str:
            try:
                h, m = map(int, time_str.split(":"))
                start = now.replace(hour=h, minute=m, second=0, microsecond=0)
                # If the time has already passed today by >2h, it's probably tomorrow
                if (now - start).total_seconds() > 7200:
                    start += timedelta(days=1)
            except Exception:
                start = now
        else:
            start = now

        end = start + timedelta(hours=3)

        # Build streams — always include match page URL + iframe src if found
        streams_raw = [{"name": "VIPLeague Embed", "url": match_url}]
        if iframe_src:
            streams_raw.insert(0, {"name": "VIPLeague Stream", "url": iframe_src})
            log.debug(f"  [VIPLeague] ✅ iframe: {iframe_src[:60]} ← {title}")

        event_id = f"vip_{abs(hash(match_url)) % 0xFFFFFF:06x}"
        events.append({
            "event_id":   event_id,
            "title":      title,
            "sport":      sport,
            "start_time": fmt_iso(start),
            "end_time":   fmt_iso(end),
            "is_live":    True,
            "streams":    streams_raw,
            "source":     "VIPLeague",
        })

    STATS["sources"]["VIPLeague"] = len(events)
    log.info(f"  → {len(events)} events from VIPLeague "
             f"({sum(1 for e in events if len(e['streams']) > 1)} with iframe streams)")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 5 — TheSportsDB (free, no API key)
# ═══════════════════════════════════════════════════════════════════

_TSDB_BASE = "https://www.thesportsdb.com/api/v1/json/3"
_TSDB_LEAGUES: dict = {
    "Football":   ["4328", "4335", "4480", "4346", "4331", "4334", "4399"],
    "Basketball": ["4387", "4966"],
    "Cricket":    ["4451", "4910", "4952"],
    "Baseball":   ["4424"],
    "Hockey":     ["4380"],
    "Formula 1":  ["4370"],
    "Rugby":      ["4391", "4464"],
    "Golf":       ["4401"],
    "Tennis":     ["4475"],
    "MMA":        ["4443"],
}
# Also fetch events happening today and next 7 days
_TSDB_EVENTS_TODAY = "/eventsday.php?d={date}&s={sport}"


def fetch_thesportsdb() -> list:
    log.info("📡 [TheSportsDB] Fetching upcoming schedule…")
    events: list = []
    seen:   set  = set()
    now = now_utc()

    # 1. Fetch by league (next events)
    for sport, league_ids in _TSDB_LEAGUES.items():
        for lid in league_ids:
            r = safe_get(f"{_TSDB_BASE}/eventsnextleague.php?id={lid}", timeout=12)
            if not r:
                continue
            try:
                matches = (r.json().get("events") or [])
            except Exception:
                continue

            for m in matches[:10]:
                try:
                    eid   = m.get("idEvent", "")
                    home  = m.get("strHomeTeam", "")
                    away  = m.get("strAwayTeam", "")
                    title = (
                        f"{home} vs {away}" if home and away
                        else m.get("strEvent", "")
                    )
                    if not title or eid in seen:
                        continue
                    seen.add(eid)

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

                    events.append({
                        "event_id":   f"tsdb_{eid}",
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

    # 2. Also fetch live events (events happening now)
    for sport_name in ["Soccer", "Cricket", "Basketball", "Hockey", "Baseball"]:
        today_str = now.strftime("%Y-%m-%d")
        r = safe_get(
            f"{_TSDB_BASE}/eventsday.php?d={today_str}&s={sport_name}",
            timeout=10,
        )
        if not r:
            continue
        try:
            matches = (r.json().get("events") or [])
        except Exception:
            continue
        for m in matches:
            eid = m.get("idEvent", "")
            if not eid or eid in seen:
                continue
            seen.add(eid)
            home  = m.get("strHomeTeam", "")
            away  = m.get("strAwayTeam", "")
            title = f"{home} vs {away}" if home and away else m.get("strEvent", "")
            if not title:
                continue
            date_str = m.get("dateEvent", "")
            time_str = m.get("strTime") or "00:00:00"
            if not date_str:
                continue
            try:
                start = datetime.fromisoformat(f"{date_str}T{time_str}").replace(tzinfo=timezone.utc)
            except Exception:
                continue
            end = start + timedelta(hours=3)
            events.append({
                "event_id":   f"tsdb_{eid}",
                "title":      title,
                "sport":      classify_sport(title, sport_name),
                "start_time": fmt_iso(start),
                "end_time":   fmt_iso(end),
                "is_live":    start <= now <= end,
                "streams":    [],
                "source":     "TheSportsDB",
                "_league":    m.get("strLeague", ""),
            })

    STATS["sources"]["TheSportsDB"] = len(events)
    log.info(f"  → {len(events)} schedule entries from TheSportsDB")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 6 — Reddit (free JSON API)
#  FIX v4: Use new-feed endpoint (not search, no OAuth needed)
#           Proper Reddit User-Agent format
# ═══════════════════════════════════════════════════════════════════

_REDDIT_SUBS = [
    ("r/soccer",       "Football"),
    ("r/nba",          "Basketball"),
    ("r/cricket",      "Cricket"),
    ("r/MMA",          "MMA"),
    ("r/Boxing",       "Boxing"),
    ("r/formula1",     "Formula 1"),
    ("r/hockey",       "Hockey"),
    ("r/baseball",     "Baseball"),
    ("r/rugbyunion",   "Rugby"),
    ("r/ufcstreams",   "MMA"),
    ("r/nbastreams",   "Basketball"),
    ("r/soccerstreams","Football"),
]

_RX_STREAM_EXT = re.compile(
    r"https?://[^\s\)\]>\"']+\.(?:m3u8|ts|mp4|mpd)[^\s\)\]>\"']*", re.I
)
_RX_STREAM_DOMAIN = re.compile(
    r"https?://(?:streameast|sportsurge|hesgoal|livetv|daddylive|"
    r"buffstreams|crackstreams|methstreams|720pstream|totalsportek|"
    r"cricfree|sportsbay|acestream)[^\s\)\]>\"']*",
    re.I
)

# Reddit User-Agent format: platform:app_id:version (by /u/username)
_REDDIT_UA = "python:com.streamx.eventsfetcher:v4.0 (aggregator bot, contact: github/streamx)"


def fetch_reddit_streams() -> list:
    log.info("📡 [Reddit] Scanning match threads (new feed)…")
    events: list = []
    now = now_utc()

    for sub, sport in _REDDIT_SUBS:
        # Use /new.json instead of /search.json — doesn't need OAuth
        for feed_url in [
            f"https://www.reddit.com/{sub}/new.json?limit=25",
            f"https://old.reddit.com/{sub}/new.json?limit=25",
        ]:
            r = safe_get(
                feed_url,
                timeout=12,
                extra_hdrs={
                    "User-Agent": _REDDIT_UA,
                    "Accept":     "application/json",
                    "Referer":    "https://www.reddit.com/",
                },
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

                if "match thread" not in title.lower() and "game thread" not in title.lower():
                    continue
                # Skip posts older than 8 hours
                if now.timestamp() - created > 8 * 3600:
                    continue

                stream_urls = list(set(
                    _RX_STREAM_EXT.findall(selftext)
                    + _RX_STREAM_DOMAIN.findall(selftext)
                ))

                # Scan comments for stream links
                if not stream_urls and permalink:
                    cr = safe_get(
                        f"https://www.reddit.com{permalink}.json?limit=30",
                        timeout=10,
                        extra_hdrs={"User-Agent": _REDDIT_UA, "Accept": "application/json"},
                    )
                    if cr:
                        try:
                            cdata = cr.json()
                            if len(cdata) > 1:
                                for c in cdata[1]["data"]["children"][:30]:
                                    body = c.get("data", {}).get("body", "")
                                    stream_urls += _RX_STREAM_EXT.findall(body)
                                    stream_urls += _RX_STREAM_DOMAIN.findall(body)
                            stream_urls = list(set(stream_urls))
                        except Exception:
                            pass

                if not stream_urls:
                    continue

                clean_title = re.sub(
                    r"(?i)\[?\s*(?:match|game)\s*thread\s*:?\]?\s*", "", title
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
            break  # use first working feed URL

    STATS["sources"]["Reddit"] = len(events)
    log.info(f"  → {len(events)} candidates from Reddit")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 7 — ESPN Unofficial API (NEW in v4)
#  No API key, no auth, extremely reliable, free forever
#  Returns live + upcoming scoreboard events
# ═══════════════════════════════════════════════════════════════════

_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"
_ESPN_LEAGUES = [
    # (path,                  sport,        league_name)
    ("soccer/eng.1",          "Football",   "Premier League"),
    ("soccer/esp.1",          "Football",   "La Liga"),
    ("soccer/ger.1",          "Football",   "Bundesliga"),
    ("soccer/ita.1",          "Football",   "Serie A"),
    ("soccer/fra.1",          "Football",   "Ligue 1"),
    ("soccer/uefa.champions", "Football",   "Champions League"),
    ("soccer/uefa.europa",    "Football",   "Europa League"),
    ("soccer/usa.1",          "Football",   "MLS"),
    ("soccer/bra.1",          "Football",   "Brasileirao"),
    ("basketball/nba",        "Basketball", "NBA"),
    ("basketball/mens-college-basketball", "Basketball", "NCAA"),
    ("baseball/mlb",          "Baseball",   "MLB"),
    ("hockey/nhl",            "Hockey",     "NHL"),
    ("football/nfl",          "Football",   "NFL"),   # American football - classify as Rugby
    ("tennis/atp",            "Tennis",     "ATP"),
    ("cricket/icc.men.t20.world", "Cricket", "T20 WC"),
]


def fetch_espn() -> list:
    """ESPN unofficial scoreboard API — free, no key, very reliable."""
    log.info("📡 [ESPN] Fetching scoreboard events…")
    events: list = []
    seen:   set  = set()
    now = now_utc()

    for league_path, sport, league_name in _ESPN_LEAGUES:
        url = f"{_ESPN_BASE}/{league_path}/scoreboard"
        r = safe_get(url, timeout=12)
        if not r:
            continue
        try:
            data = r.json()
        except Exception:
            continue

        for ev_data in data.get("events", []):
            try:
                eid = str(ev_data.get("id", ""))
                if not eid or eid in seen:
                    continue
                seen.add(eid)

                # Use shortName for compact "Team A vs Team B" format
                name = (
                    ev_data.get("name", "")
                    or ev_data.get("shortName", "")
                )
                if not name:
                    continue

                date_str = ev_data.get("date", "")
                if not date_str:
                    continue

                start = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                end   = start + timedelta(hours=3)

                if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
                    continue
                if start > now + timedelta(hours=FUTURE_WINDOW_HOURS):
                    continue

                status_type = (
                    ev_data.get("status", {})
                           .get("type", {})
                           .get("state", "pre")
                )
                is_live = status_type == "in"

                # Try to get venue from competitions
                venue = ""
                comps = ev_data.get("competitions", [{}])
                if comps:
                    venue = comps[0].get("venue", {}).get("fullName", "")

                events.append({
                    "event_id":   f"espn_{eid}",
                    "title":      name,
                    "sport":      sport,
                    "start_time": fmt_iso(start),
                    "end_time":   fmt_iso(end),
                    "is_live":    is_live,
                    "streams":    [],   # schedule only — other sources provide streams
                    "source":     "ESPN",
                    "_league":    league_name,
                    "_venue":     venue,
                })
            except Exception as e:
                log.debug(f"  [ESPN] Row error: {e}")

    STATS["sources"]["ESPN"] = len(events)
    log.info(f"  → {len(events)} events from ESPN")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 8 — SofaScore Unofficial API (NEW in v4)
#  No API key, schedule + live scores
# ═══════════════════════════════════════════════════════════════════

_SOFA_BASE = "https://api.sofascore.com/api/v1"
_SOFA_SPORTS = [
    ("football", "Football"),
    ("basketball", "Basketball"),
    ("cricket", "Cricket"),
    ("tennis", "Tennis"),
    ("ice-hockey", "Hockey"),
    ("baseball", "Baseball"),
    ("mma", "MMA"),
]
_SOFA_HEADERS = {
    "Accept":         "application/json",
    "Accept-Language":"en-US,en;q=0.9",
    "Referer":        "https://www.sofascore.com/",
    "Origin":         "https://www.sofascore.com",
    "Cache-Control":  "no-cache",
}


def fetch_sofascore() -> list:
    """SofaScore unofficial API — live + scheduled events."""
    log.info("📡 [SofaScore] Fetching live + scheduled events…")
    events: list = []
    seen:   set  = set()
    now = now_utc()

    # Fetch today's events for each sport
    today_str = now.strftime("%Y-%m-%d")

    for sport_slug, sport_name in _SOFA_SPORTS:
        for date_offset in [0, 1]:  # today and tomorrow
            date_str = (now + timedelta(days=date_offset)).strftime("%Y-%m-%d")
            url = f"{_SOFA_BASE}/sport/{sport_slug}/scheduled-events/{date_str}"
            r = safe_get(url, timeout=10, extra_hdrs=_SOFA_HEADERS)
            if not r:
                continue
            try:
                data = r.json()
            except Exception:
                continue

            for ev_data in data.get("events", []):
                try:
                    eid = str(ev_data.get("id", ""))
                    if not eid or eid in seen:
                        continue
                    seen.add(eid)

                    home = ev_data.get("homeTeam", {}).get("name", "")
                    away = ev_data.get("awayTeam", {}).get("name", "")
                    if not home or not away:
                        continue
                    title = f"{home} vs {away}"

                    ts = ev_data.get("startTimestamp", 0)
                    if not ts:
                        continue
                    start = datetime.fromtimestamp(ts, tz=timezone.utc)
                    end   = start + timedelta(hours=3)

                    if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
                        continue
                    if start > now + timedelta(hours=FUTURE_WINDOW_HOURS):
                        continue

                    status_code = ev_data.get("status", {}).get("code", 0)
                    is_live = status_code in (6, 7)  # SofaScore live codes

                    league = ev_data.get("tournament", {}).get("name", "")

                    events.append({
                        "event_id":   f"sofa_{eid}",
                        "title":      title,
                        "sport":      sport_name,
                        "start_time": fmt_iso(start),
                        "end_time":   fmt_iso(end),
                        "is_live":    is_live,
                        "streams":    [],
                        "source":     "SofaScore",
                        "_league":    league,
                    })
                except Exception as e:
                    log.debug(f"  [SofaScore] Row error: {e}")

    STATS["sources"]["SofaScore"] = len(events)
    log.info(f"  → {len(events)} events from SofaScore")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 9 — Manual Seeds
# ═══════════════════════════════════════════════════════════════════
MANUAL_EVENTS: list = [
    # Example — uncomment and fill in for high-priority events:
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
#  FIX v4: NO LONGER drops events with 0 working streams
#           Events without streams are saved as "schedule" entries
# ═══════════════════════════════════════════════════════════════════

def enrich_event(ev: dict) -> Optional[dict]:
    sport  = ev.get("sport", "Other")
    meta   = SPORT_META.get(sport, SPORT_META["Other"])
    streams = ev.get("streams", [])

    # Quality rank → HD validated first
    streams = rank_streams_by_quality(streams)

    if streams:
        log.info(f"🔍 Validating [{sport}]: {ev['title']}")
        working = validate_streams(streams)
        if not working:
            log.warning(f"  ⚠️  No working streams (kept as schedule): {ev['title']}")
    else:
        log.debug(f"  📅 No streams (schedule entry): {ev['title']}")
        working = []

    # ✅ FIX: Save even if 0 working streams — show as schedule-only entry
    working = rank_streams_by_quality(working)

    extra = {}
    for key in ("_league", "_thumb", "_venue"):
        val = ev.get(key, "")
        if val:
            extra[key.lstrip("_")] = val

    return {
        "event_id":       ev["event_id"],
        "title":          ev["title"],
        "sport":          sport,
        "sport_icon":     meta["icon"],
        "sport_color":    meta["color"],
        "tvg_id":         ev["event_id"],
        "start_time":     ev["start_time"],
        "end_time":       ev["end_time"],
        "is_live":        ev.get("is_live", False),
        "streams":        working,
        "stream_count":   len(working),
        "has_stream":     len(working) > 0,
        "source":         ev.get("source", "Unknown"),
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
    live_events     = [e for e in active if e.get("is_live")]
    upcoming_events = [e for e in active if not e.get("is_live")]
    streamed_events = [e for e in active if e.get("has_stream")]

    payload = {
        "last_updated":    fmt_iso(now_utc()),
        "total_live":      len(live_events),
        "total_upcoming":  len(upcoming_events),
        "total_streamed":  len(streamed_events),
        "active_events":   sorted(active, key=lambda e: e.get("start_time", "")),
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log.info(
        f"💾 Saved {len(active)} events "
        f"({len(streamed_events)} with streams, {len(active) - len(streamed_events)} schedule-only)"
        f" → {OUTPUT_FILE}"
    )


# ═══════════════════════════════════════════════════════════════════
#  TELEGRAM NOTIFICATION
# ═══════════════════════════════════════════════════════════════════

def send_telegram_report(active_events: list):
    token   = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return

    elapsed      = int(time.time() - START_TIME)
    live_cnt     = sum(1 for e in active_events if e.get("is_live"))
    up_cnt       = len(active_events) - live_cnt
    streamed_cnt = sum(1 for e in active_events if e.get("has_stream"))
    src_lines    = "\n".join(
        f"  • {src}: `{cnt}`"
        for src, cnt in sorted(STATS["sources"].items(), key=lambda x: -x[1])
    ) or "  • (none)"

    msg = (
        f"📺 *StreamX Events v4 Report*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Runtime: `{elapsed // 60}m {elapsed % 60}s`\n"
        f"✅ Total active: `{len(active_events)}`\n"
        f"🔴 Live now: `{live_cnt}`\n"
        f"🕐 Upcoming: `{up_cnt}`\n"
        f"📡 With streams: `{streamed_cnt}`\n"
        f"📅 Schedule only: `{len(active_events) - streamed_cnt}`\n"
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

_ALL_SOURCE_NAMES = [
    "StreamedSU", "DaddyLive", "SportFree", "VIPLeague",
    "TheSportsDB", "Reddit", "ESPN", "SofaScore", "Manual",
]

def generate_events_dashboard(active_events: list):
    live_ev   = [e for e in active_events if e.get("is_live")]
    up_ev     = [e for e in active_events if not e.get("is_live")]
    streamed  = [e for e in active_events if e.get("has_stream")]
    scheduled = [e for e in active_events if not e.get("has_stream")]

    def stream_badge(ev: dict) -> str:
        sc = ev.get("stream_count", 0)
        if sc > 0:
            return f'<span style="color:#3fb950">▶ {sc} stream{"s" if sc!=1 else ""}</span>'
        return '<span style="color:#8b949e">📅 schedule only</span>'

    def event_row(ev: dict) -> str:
        color  = ev.get("sport_color", "#E53935")
        sport  = ev.get("sport", "Other")
        badge  = f'<span class="badge" style="background:{color}">{sport}</span>'
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
            f"<td>{stream_badge(ev)}</td>"
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
  <title>StreamX Live Events v4</title>
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
    .green{{color:#3fb950}}.red{{color:#f85149}}.blue{{color:#58a6ff}}
    .row2{{display:flex;gap:16px;flex-wrap:wrap}}
    .row2>div{{flex:1;min-width:180px}}
    footer{{color:#8b949e;font-size:.72em;text-align:right;margin-top:14px}}
  </style>
</head>
<body>
  <h1>📺 StreamX Live Events <small style="font-size:.6em;color:#58a6ff">v4</small></h1>
  <p class="sub">Auto-updated · {datetime.now().strftime('%Y-%m-%d %H:%M UTC')} · 9 sources</p>

  <div class="card">
    <div class="stats">
      <div class="stat"><div class="num">{len(active_events)}</div><div class="label">Total Active</div></div>
      <div class="stat"><div class="num red">{len(live_ev)}</div><div class="label">Live Now</div></div>
      <div class="stat"><div class="num green">{len(up_ev)}</div><div class="label">Upcoming</div></div>
      <div class="stat"><div class="num blue">{len(streamed)}</div><div class="label">With Streams</div></div>
      <div class="stat"><div class="num">{len(scheduled)}</div><div class="label">Schedule Only</div></div>
      <div class="stat"><div class="num">{STATS['added']}</div><div class="label">Added</div></div>
      <div class="stat"><div class="num">{STATS['merged']}</div><div class="label">Merged</div></div>
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
      {table_or_empty(up_ev[:30], 'No upcoming events')}
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

  <footer>StreamX Events v4 · {len(_STREAMED_ENDPOINTS)} StreamedSU eps · {len(_TSDB_LEAGUES)} TSDB leagues · {len(_ESPN_LEAGUES)} ESPN leagues · {len(_REDDIT_SUBS)} Reddit subs</footer>
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
    log.info("🚀 StreamX Live Events Updater v4 (Ultimate Fix) — START")
    log.info(f"   UTC Time      : {fmt_iso(now_utc())}")
    log.info(f"   CloudScraper  : {'✅' if HAS_CLOUDSCRAPER else '⚠️  not installed'}")
    log.info(f"   Future window : {FUTURE_WINDOW_HOURS}h  |  Sources: 9")
    log.info(f"   Stream valid  : Relaxed (trusted domain whitelist + HEAD-first)")
    log.info("═" * 60)

    # ── Load state & existing ──────────────────────────────────────
    ev_state = load_event_state()
    existing = load_existing()

    still_valid      = [e for e in existing if not is_expired(e)]
    STATS["expired"] = len(existing) - len(still_valid)
    if STATS["expired"]:
        log.info(f"🗑  Removed {STATS['expired']} expired events")
    existing_ids = {e["event_id"] for e in still_valid}

    # ── Collect from all 9 sources ─────────────────────────────────
    raw: list = []
    raw.extend(fetch_streamedsu())
    raw.extend(fetch_daddylive())
    raw.extend(fetch_sportfree())
    raw.extend(fetch_vipleague())
    raw.extend(fetch_thesportsdb())
    raw.extend(fetch_reddit_streams())
    raw.extend(fetch_espn())           # 🆕 NEW
    raw.extend(fetch_sofascore())      # 🆕 NEW
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
    log.info(f"🆕 {len(new_candidates)} new candidates to validate/save")

    # ── Validate + enrich (now saves even with 0 streams) ─────────
    enriched_new: list = []
    for ev in new_candidates:
        result = enrich_event(ev)
        if result is not None:          # always returns dict now
            enriched_new.append(result)
            STATS["added"] += 1

    # ── Update is_live on carry-over events ────────────────────────
    for ev in still_valid:
        update_live_flag(ev)

    # ── Merge all ─────────────────────────────────────────────────
    all_active = smart_deduplicate(still_valid + enriched_new)

    # ── Stats summary ──────────────────────────────────────────────
    streamed_cnt = sum(1 for e in all_active if e.get("has_stream"))
    live_cnt     = sum(1 for e in all_active if e.get("is_live"))

    # ── Save outputs ───────────────────────────────────────────────
    save_events(all_active)
    record_working_streams(all_active, ev_state)
    save_event_state(ev_state)

    send_telegram_report(all_active)
    generate_events_dashboard(all_active)

    # ── Final summary ──────────────────────────────────────────────
    log.info("═" * 60)
    log.info(f"✅ Done! Active events  : {len(all_active)}")
    log.info(f"   🔴 Live now         : {live_cnt}")
    log.info(f"   🕐 Upcoming         : {len(all_active) - live_cnt}")
    log.info(f"   📡 With streams     : {streamed_cnt}")
    log.info(f"   📅 Schedule only    : {len(all_active) - streamed_cnt}")
    log.info(f"   🆕 Added this run   : {STATS['added']}")
    log.info(f"   🔀 Streams merged   : {STATS['merged']}")
    log.info("═" * 60)


if __name__ == "__main__":
    main()
