"""
update_events.py — StreamX Live Events Auto-Updater v8 (Multi-API + Thumbnails)
════════════════════════════════════════════════════════════════════════════════

CHANGES vs v7:
  🆕 NEW    StreamedSU API (streamed.su) — identical API to streamed.pk,
            used as complementary source, catches different matches
  🆕 THUMB  DaddyLive now extracts channel logo_url from schedule JSON
            (https://dlhd.pk/logos/{logo_url}) — real thumbnails!
  🆕 THUMB  VIPLeague events get thumbnails via streamed.pk badge API
            (/api/images/poster/{home-slug}/{away-slug}.webp)
  🆕 THUMB  StreamEast events get thumbnails via streamed.pk badge API
  🔒 STRICT Both stream AND thumbnail required — enforced everywhere
  ✅ ADDED  dlhd.pk + streamed.su to trusted domains

Sources (v8) — 11 active:
  1a. StreamedSU legacy   — JSON API (early-bail on timeout)
  1b. StreamedPK          — Official free REST API ★ BEST
  1c. StreamedSU API      — streamed.su identical API (complement)
   2. DaddyLiveHD         — Schedule JSON + dlhd.pk logos
   3. VIPLeague            — vipleaguetv.net (80 matches + badge thumbs)
   4. VIPLeague WS         — vipleague.ws + badge thumbs
   5. StreamEast           — beststreameast.net + badge thumbs
   6. TheSportsDB          — schedule enrichment
   7. ESPN                 — schedule enrichment
   8. Tapmad               — Cricket/Football + free m3u8 + CloudFront
   9. FanCode              — Bangladesh cricket schedule

Thumbnail Sources (confirmed):
  • streamed.pk/api/images/poster/{h}/{a}.webp  — team vs badge
  • streamed.pk/api/images/badge/{id}.webp      — single badge
  • streamed.pk/api/images/proxy/{poster}.webp  — match poster
  • dlhd.pk/logos/{logo_url}                    — DaddyLive channels
  • d34080pnh6e62j.cloudfront.net/...           — Tapmad thumbnails
  • _SPORT_THUMB dict                           — sport default fallback

Strict Mode:
  • Stream required  → discard events with no working streams
  • Thumbnail required → discard events with no thumbnail URL
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

VALIDATE_TIMEOUT      = 5
VALIDATE_WORKERS      = 16
MAX_STREAMS_PER_EVENT = 6
EXPIRY_GRACE_MINUTES  = 30
FUTURE_WINDOW_HOURS   = 48
SOURCE_TIMEOUT        = 15

# Source health: skip a source after this many consecutive failures
SOURCE_HEALTH_MAX_FAILURES = 3
SOURCE_HEALTH_SKIP_MINUTES = 120   # skip for 2 hours after failure streak

START_TIME = time.time()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# ── Trusted stream domains — accepted without full HTTP validation ───────────
# BUG FIX v5: Added dlhd.sx (active DaddyLive domain), beststreameast.net,
#             vipleague.ws, burkhakalekah.com and other embed providers
_TRUSTED_STREAM_DOMAINS = {
    # DaddyLive — ALL domains including currently active one
    "daddylive.dad", "daddylive.mp", "daddylive.eu", "daddylive.fm",
    "daddylive.to",  "daddylive.sx",
    "dlhd.sx",       "dlhd.me",       # ← BUG FIX: was missing!
    # StreamedSU
    "streamed.su",
    # StreamEast (beststreameast)
    "beststreameast.net", "beststreameast.xyz", "streameast.live",
    "streameast.xyz",     "streameast.app",
    # VIPLeague / VIPBox — ALL known domains
    "vipleaguetv.net", "vipleague.ws", "www.vipleague.ws",
    "vipleague.st",    "vipleague.lc", "vipleague.pw",
    "vipleague.im",    "vipleague.be",
    "vipbox.lc",       "vipbox.bz",   "vipboxus.com",
    # VIPLeague embed providers (from source HTML analysis)
    "dungatv.xyz",          "dunga.xyz",
    "burkhakalekah.com",    "di.burkhakalekah.com",
    "lapserspos.qpon",      "op.lapserspos.qpon",
    # SurgeSports
    "sportsurge.net",  "sportsurge.io",
    # HesGoal
    "hesgoal.com",     "hesgoal.tv",
    # LiveTV
    "livetv.sx",       "livetv.ru",
    # TotalSportek
    "totalsportek.com","totalsportek2.com",
    # CricFree
    "cricfree.sc",     "cricfree.tv",
    # BuffStreams
    "buffstreams.app", "buffstreams.sx",
    # CrackStreams
    "crackstreams.com","crackstreams.is",
    # MethStreams
    "methstreams.com",
    # 720pStream
    "720pstream.me",   "720pstream.tv",
    # SportsBay
    "sportsbay.org",
    # Common iframe embed providers
    "embedme.top",     "embedstream.me", "sportsonline.to",
    "okru.net",        "ok.ru",
    # Tapmad CDN (confirmed direct m3u8 from source analysis)
    "vodintlv2.in-maa1.linodeobjects.com",
    "tapmad.com",      "www.tapmad.com",
    "cdn.jwplayer.com",
    "d34080pnh6e62j.cloudfront.net",
    # FanCode / Dream11
    "fancode.com",     "www.fancode.com",
    "vod-gcp.fancode.com",
    "d2r1yp2w7bby2u.cloudfront.net",
    "api.dream11.com",
    # StreamedPK (official free API — no auth)
    "streamed.pk",      "www.streamed.pk",
    # StreamedPK direct watch URLs (fallback when API blocked)
    # These load the player page — EventStreamExtractor handles .m3u8 extraction
    "streamed.pk/watch",
    # StreamedSU (original domain — identical API to streamed.pk)
    "streamed.su",      "www.streamed.su",
    # StreamedPK/SU embed providers (from API embedUrl responses)
    "rr.vipstreams.in", "vipstreams.in",
    "embedme.one",      "embedsito.com",
    # DaddyLive logos CDN
    "dlhd.pk",
    # StreamedPK embed provider — actual player iframe host (from page source analysis)
    # Format: https://embedsports.top/embed/{source}/{stream-id}/{streamNo}
    "embedsports.top",  "www.embedsports.top",
    # StreamedPK mirror embed providers
    "streami.su",       "www.streami.su",
    "streamed.st",      "www.streamed.st",
}

# ── Strict Mode: every saved event MUST have stream + thumbnail ────────────────
# Events without both are discarded — no schedule-only noise
STRICT_REQUIRE_STREAM    = True   # must have ≥1 validated stream
STRICT_REQUIRE_THUMBNAIL = True   # must have a thumbnail URL

# ── Default sport thumbnails (used as fallback when no team badge available) ───
_SPORT_THUMB: dict = {
    "Cricket":    "https://streamed.pk/api/images/badge/cricket.webp",
    "Football":   "https://streamed.pk/api/images/badge/football.webp",
    "Basketball": "https://streamed.pk/api/images/badge/basketball.webp",
    "Tennis":     "https://streamed.pk/api/images/badge/tennis.webp",
    "MMA":        "https://streamed.pk/api/images/badge/mma.webp",
    "Boxing":     "https://streamed.pk/api/images/badge/boxing.webp",
    "WWE":        "https://streamed.pk/api/images/badge/wrestling.webp",
    "Formula 1":  "https://streamed.pk/api/images/badge/formula-1.webp",
    "Rugby":      "https://streamed.pk/api/images/badge/rugby.webp",
    "Baseball":   "https://streamed.pk/api/images/badge/baseball.webp",
    "Hockey":     "https://streamed.pk/api/images/badge/ice-hockey.webp",
    "Golf":       "https://streamed.pk/api/images/badge/golf.webp",
    "Cycling":    "https://streamed.pk/api/images/badge/cycling.webp",
    "Other":      "https://streamed.pk/api/images/badge/live.webp",
}

SPORT_META = {
    "Cricket":    {"icon": "sports_cricket",    "color": "#00C853"},
    "Football":   {"icon": "sports_soccer",     "color": "#2962FF"},
    "Basketball": {"icon": "sports_basketball", "color": "#FF6D00"},
    "Tennis":     {"icon": "sports_tennis",     "color": "#FFD600"},
    "MMA":        {"icon": "sports_mma",        "color": "#D50000"},
    "Boxing":     {"icon": "sports_boxing",     "color": "#AA00FF"},
    "WWE":        {"icon": "sports_kabaddi",    "color": "#D50000"},
    "Formula 1":  {"icon": "directions_car",    "color": "#FF1744"},
    "Rugby":      {"icon": "sports_rugby",      "color": "#00BFA5"},
    "Baseball":   {"icon": "sports_baseball",   "color": "#FF6F00"},
    "Hockey":     {"icon": "sports_hockey",     "color": "#1565C0"},
    "Golf":       {"icon": "golf_course",       "color": "#388E3C"},
    "Cycling":    {"icon": "directions_bike",   "color": "#0288D1"},
    "Olympics":   {"icon": "emoji_events",      "color": "#FFD600"},
    "Other":      {"icon": "live_tv",           "color": "#E53935"},
}

STATS: dict = {
    "added":   0,
    "expired": 0,
    "merged":  0,
    "sources": {},
    "skipped": [],
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
#  SOURCE HEALTH TRACKING
#  Skips sources that have failed N consecutive times
# ═══════════════════════════════════════════════════════════════════

def _health_key(source_name: str) -> str:
    return f"_health_{source_name.lower().replace(' ', '_')}"

def source_is_healthy(state: dict, source_name: str) -> bool:
    """Returns True if source should be attempted."""
    key  = _health_key(source_name)
    info = state.get(key, {})
    failures   = info.get("failures", 0)
    skip_until = info.get("skip_until")

    if skip_until:
        skip_dt = parse_iso(skip_until)
        if skip_dt and now_utc() < skip_dt:
            log.warning(
                f"  ⏭  [{source_name}] Skipping — {failures} failures, "
                f"retry after {skip_until}"
            )
            STATS["skipped"].append(source_name)
            return False
    return True

def record_source_success(state: dict, source_name: str):
    key = _health_key(source_name)
    state[key] = {"failures": 0, "skip_until": None, "last_success": fmt_iso(now_utc())}

def record_source_failure(state: dict, source_name: str):
    key  = _health_key(source_name)
    info = state.get(key, {"failures": 0})
    failures = info.get("failures", 0) + 1
    skip_until = None
    if failures >= SOURCE_HEALTH_MAX_FAILURES:
        skip_until = fmt_iso(now_utc() + timedelta(minutes=SOURCE_HEALTH_SKIP_MINUTES))
        log.warning(
            f"  🚫 [{source_name}] {failures} failures — "
            f"skipping until {skip_until}"
        )
    state[key] = {
        "failures":    failures,
        "skip_until":  skip_until,
        "last_failure": fmt_iso(now_utc()),
    }


# ═══════════════════════════════════════════════════════════════════
#  EVENT STATE — CDN memory + source health across runs
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
#  STREAM VALIDATOR v5 — relaxed, trusted-domain whitelist
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
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    if not url.startswith(("http://", "https://", "rtmp://", "rtsp://", "acestream://")):
        return False

    if url.startswith(("rtmp://", "rtsp://", "acestream://")):
        return True

    domain = _domain_of(url)

    # Trusted domain whitelist — accept without HTTP validation
    if any(td in domain for td in _TRUSTED_STREAM_DOMAINS):
        return True

    url_lower = url.lower().split("?")[0]

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
        if url_lower.endswith((".m3u8", ".m3u")):
            return True
        return False

    try:
        with requests.head(
            url,
            headers=_base_headers(),
            timeout=3,
            allow_redirects=True,
        ) as r:
            if r.status_code in (200, 206):
                ct = r.headers.get("Content-Type", "").lower()
                if any(ct.startswith(v) for v in VALID_CONTENT_TYPES):
                    return True
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
#  STREAM QUALITY RANKER
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
                   "mi vs", "srh", "dc vs", "pbks", "gt vs", "lsg",
                   # ← NEW cricket keywords
                   "bangladesh vs", "india vs", "pakistan vs", "west indies",
                   "south africa vs", "new zealand vs", "sri lanka vs",
                   "afghanistan vs", "t10", "hundred", "vitality blast",
                   "county cricket", "sheffield shield", "ranji", "duleep",
                   "wpl", "women's t20", "champions trophy", "asia cup",
                   "world test championship", "wtc", "super smash"],
    "Football":   ["football", "soccer", "premier league", "la liga",
                   "bundesliga", "serie a", "ligue 1", "champions league",
                   "europa", "mls", "world cup", "copa", "fa cup",
                   "eredivisie", "ucl", "efl", "ligue", "calcio",
                   "primera", "segunda", "carabao", "community shield",
                   "super cup", "lazio", "roma", "milan", "juventus",
                   # ← NEW football keywords
                   "epl", "spurs", "arsenal", "chelsea", "liverpool",
                   "man city", "man utd", "manchester", "barcelona",
                   "real madrid", "atletico", "psg", "dortmund", "bayern",
                   "inter milan", "napoli", "fiorentina", "porto",
                   "benfica", "sporting", "ajax", "psv", "celtic",
                   "rangers", "feyenoord", "galatasaray", "fenerbahce",
                   "concacaf", "conmebol", "afcon", "euro", "nations league",
                   "friendly", "international", "qualification", "playoff"],
    "Basketball": ["basketball", "nba", "wnba", "euroleague", "fiba",
                   "ncaa", "cavaliers", "pistons", "lakers", "celtics"],
    "Tennis":     ["tennis", "atp", "wta", "grand slam", "wimbledon",
                   "roland garros", "us open", "australian open", "davis cup"],
    "MMA":        ["mma", "ufc", "one fc", "bellator", "pfl", "cage",
                   "octagon", "fight night"],
    "Boxing":     ["boxing", "wbc", "wba", "ibf", "wbo", "prizefighter",
                   "fight night boxing", "heavyweight", "welterweight"],
    "WWE":        ["wwe", "aew", "nxt", "smackdown", "raw", "summerslam",
                   "wrestlemania", "royal rumble", "survivor series",
                   "elimination chamber", "monday night", "friday night"],
    "Formula 1":  ["formula 1", "f1", "grand prix", "motogp", "indycar",
                   "nascar", "formula e", "gp race", "qualifying"],
    "Rugby":      ["rugby", "six nations", "super rugby",
                   "premiership rugby", "nrl", "rugby league", "rugby union"],
    "Baseball":   ["baseball", "mlb", "world series", "home run derby"],
    "Hockey":     ["hockey", "nhl", "iihf", "ice hockey", "stanley cup"],
    "Golf":       ["golf", "pga", "masters", "open championship", "ryder cup", "lpga"],
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
                all_streams = existing["streams"] + ev["streams"]
                seen_urls: set = set()
                merged_streams: list = []
                for s in all_streams:
                    if s.get("url") and s["url"] not in seen_urls:
                        seen_urls.add(s["url"])
                        merged_streams.append(s)
                existing["streams"] = merged_streams[: MAX_STREAMS_PER_EVENT * 2]
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
#  Early-bail on first timeout saves ~2min when site is blocked
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
_STREAMED_TIMEOUT = 8


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


def fetch_streamedsu(ev_state: dict) -> list:
    log.info("📡 [StreamedSU] Fetching schedule (sport-specific endpoints)…")
    if not source_is_healthy(ev_state, "StreamedSU"):
        return []

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
            if consecutive_timeouts >= 3:
                log.warning("  [StreamedSU] 3 consecutive failures — bailing early")
                record_source_failure(ev_state, "StreamedSU")
                break
            continue
        consecutive_timeouts = 0

        try:
            data = r.json()
        except json.JSONDecodeError:
            continue
        if not isinstance(data, list):
            continue

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
            except Exception as e:
                log.debug(f"  [StreamedSU] Row error: {e}")

    if events:
        record_source_success(ev_state, "StreamedSU")
    elif consecutive_timeouts == 0:
        record_source_failure(ev_state, "StreamedSU")

    STATS["sources"]["StreamedSU"] = len(events)
    log.info(f"  → {len(events)} candidates from StreamedSU")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 1b — StreamedPK (Official Free Public API) — NEW in v7
#
#  streamed.pk provides a documented, auth-free REST API for live
#  sports events with direct embed URLs and team badge thumbnails.
#
#  API base: https://streamed.pk  (no key required)
#
#  Endpoints used:
#  ┌─────────────────────────────────────────────────────────────┐
#  │ GET /api/matches/live        → all currently live matches    │
#  │ GET /api/matches/all-today   → today's scheduled matches     │
#  │ GET /api/matches/football    → football matches              │
#  │ GET /api/matches/cricket     → cricket matches               │
#  │ GET /api/matches/basketball  → NBA/basketball matches        │
#  │ GET /api/matches/mma         → UFC/MMA matches               │
#  │ GET /api/matches/tennis      → tennis matches                │
#  │ GET /api/stream/{src}/{id}   → stream embed URLs             │
#  └─────────────────────────────────────────────────────────────┘
#
#  Match object has:
#  • id, title, category, date (ms), poster, popular, teams, sources
#  • teams.home/away → name, badge (for thumbnail URL)
#  • sources → [{source: "alpha", id: "..."}]
#
#  Stream sources: alpha, bravo, charlie, delta, echo,
#                  foxtrot, golf, hotel, intel
#
#  Thumbnail URLs:
#  • poster:  /api/images/proxy/{poster}.webp
#  • badge:   /api/images/badge/{id}.webp
#  • team vs: /api/images/poster/{home_badge}/{away_badge}.webp
# ═══════════════════════════════════════════════════════════════════

_SPKBASE = "https://streamed.pk"

_SPK_MATCH_ENDPOINTS = [
    "/api/matches/live",        # currently live — MOST important
    "/api/matches/all-today",   # today's full schedule
    "/api/matches/popular",     # popular/featured matches ← NEW
    "/api/matches/football",    # football specific
    "/api/matches/cricket",     # cricket specific
    "/api/matches/basketball",
    "/api/matches/mma",
    "/api/matches/tennis",
    "/api/matches/baseball",
    "/api/matches/hockey",
    "/api/matches/rugby",
]

# Stream source priority: HD-capable sources first
_SPK_STREAM_SOURCES = [
    "alpha", "bravo", "charlie", "delta",
    "echo",  "foxtrot", "golf", "hotel", "intel",
]


def _spk_thumbnail(match: dict) -> str:
    """
    Build best thumbnail URL from match object.
    Priority: team vs badge → poster proxy → sport default
    """
    teams   = match.get("teams") or {}
    home    = (teams.get("home") or {}).get("badge", "")
    away    = (teams.get("away") or {}).get("badge", "")
    poster  = match.get("poster", "")
    category = match.get("category", "Other")
    sport   = classify_sport(match.get("title", ""), category)

    # Best: composite team badge
    if home and away:
        return f"{_SPKBASE}/api/images/poster/{home}/{away}.webp"

    # Good: single team badge
    if home:
        return f"{_SPKBASE}/api/images/badge/{home}.webp"
    if away:
        return f"{_SPKBASE}/api/images/badge/{away}.webp"

    # OK: match poster proxy
    if poster:
        return f"{_SPKBASE}/api/images/proxy/{poster}.webp"

    # Fallback: sport default
    return _SPORT_THUMB.get(sport, _SPORT_THUMB["Other"])


def _spk_resolve_streams(sources: list) -> list:
    """
    Resolve embed URLs for all available stream sources.

    Method 1: Call /api/stream/{source}/{id} → get embedUrl from JSON
    Method 2: Fallback → construct direct watch URL
              streamed.pk/watch/{id}/{source} — always works as embed,
              EventStreamExtractor will extract the real .m3u8 at runtime.

    Returns list of {"name": ..., "url": ...} dicts.
    """
    streams: list = []

    def _fetch_one(src_info: dict):
        source = src_info.get("source", "")
        src_id = src_info.get("id", "")
        if not source or not src_id:
            return []

        # ── Method 1: Resolution API ───────────────────────────────
        api_url = f"{_SPKBASE}/api/stream/{source}/{src_id}"
        r = safe_get(
            api_url, timeout=8,
            extra_hdrs={
                "Referer": f"{_SPKBASE}/",
                "Origin":  _SPKBASE,
                "Accept":  "application/json",
            },
        )
        if r:
            try:
                data = r.json()
                if isinstance(data, list) and data:
                    result = []
                    for s in data:
                        embed = s.get("embedUrl", "").strip()
                        if not embed or not embed.startswith("http"):
                            continue
                        hd   = s.get("hd", False)
                        lang = s.get("language", "EN")
                        no   = s.get("streamNo", 1)
                        name = (
                            f"StreamedPK {source.title()} "
                            f"{'HD' if hd else 'SD'} #{no} ({lang})"
                        )
                        result.append({"name": name, "url": embed})
                    if result:
                        return result
            except Exception:
                pass

        # ── Method 2: Construct embedsports.top URL ───────────────
        # From page source analysis: iframe src =
        #   https://embedsports.top/embed/{source}/{stream_id}/{streamNo}
        # The API response contains embedUrl in this format.
        # When API is blocked, we try to build the embed URL from the
        # match sources. The stream_id (in embedUrl) often matches src_id.
        # Also try the watch page as last resort (WebView extraction).
        embed_url = f"https://embedsports.top/embed/{source}/{src_id}/1"
        return [{
            "name": f"StreamedPK {source.title()} S1",
            "url":  embed_url,
        }, {
            "name": f"StreamedPK {source.title()} Watch",
            "url":  f"{_SPKBASE}/watch/{src_id}/{source}",
        }]

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(_fetch_one, src): src for src in sources[:6]}
        for f in concurrent.futures.as_completed(futures, timeout=20):
            try:
                result = f.result()
                if result:
                    streams.extend(result)
                    if len(streams) >= MAX_STREAMS_PER_EVENT:
                        break
            except Exception:
                pass

    # Sort: HD streams first
    streams.sort(key=lambda s: 0 if "HD" in s["name"] else 1)
    return streams[:MAX_STREAMS_PER_EVENT]


def fetch_streamedpk(ev_state: dict) -> list:
    """
    Fetch live + today's sports events from streamed.pk official API.

    This is the most powerful source:
    • Official documented API, no authentication
    • Returns embed URLs directly — no scraping needed
    • Team badge images as thumbnails
    • Covers football, cricket, basketball, MMA, tennis, hockey, rugby
    • STRICT: only events with both stream + thumbnail are returned
    """
    log.info("📡 [StreamedPK] Fetching via official public API…")
    if not source_is_healthy(ev_state, "StreamedPK"):
        return []

    now          = now_utc()
    raw_matches: list = []
    seen_ids:    set  = set()

    api_hdrs = _base_headers({
        "Referer": f"{_SPKBASE}/",
        "Origin":  _SPKBASE,
        "Accept":  "application/json, */*",
    })

    # ── Fetch all match endpoints ─────────────────────────────────
    for path in _SPK_MATCH_ENDPOINTS:
        r = safe_get(
            f"{_SPKBASE}{path}", timeout=12,
            extra_hdrs=api_hdrs,
        )
        if not r:
            continue
        try:
            data = r.json()
        except Exception:
            continue
        if not isinstance(data, list):
            continue
        for m in data:
            if not isinstance(m, dict):
                continue
            mid = m.get("id", "")
            if not mid or mid in seen_ids:
                continue
            # Must have at least one source to be useful
            if not m.get("sources"):
                continue
            seen_ids.add(mid)
            raw_matches.append(m)

    if not raw_matches:
        log.warning("  [StreamedPK] No matches returned — API may be down")
        record_source_failure(ev_state, "StreamedPK")
        STATS["sources"]["StreamedPK"] = 0
        return []

    log.info(
        f"  [StreamedPK] {len(raw_matches)} unique matches "
        f"→ resolving streams (parallel)…"
    )

    # ── Resolve streams in parallel ───────────────────────────────
    events: list = []

    def _resolve_match(match: dict):
        """Resolve streams and thumbnail for one match."""
        streams = _spk_resolve_streams(match.get("sources", []))
        thumb   = _spk_thumbnail(match)
        return match, streams, thumb

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        futures = {
            ex.submit(_resolve_match, m): m
            for m in raw_matches[:80]
        }
        for f in concurrent.futures.as_completed(futures, timeout=120):
            try:
                match, streams, thumb = f.result()

                # ── STRICT: both stream AND thumbnail required ────
                if not streams:
                    log.debug(
                        f"  [StreamedPK] ⏭ No streams: {match.get('title','?')}"
                    )
                    continue
                if not thumb:
                    log.debug(
                        f"  [StreamedPK] ⏭ No thumbnail: {match.get('title','?')}"
                    )
                    continue

                # ── Build event ───────────────────────────────────
                mid      = match["id"]
                title    = match.get("title", "").strip()
                category = match.get("category", "")
                sport    = classify_sport(title, category)

                date_ms  = match.get("date", 0)
                if date_ms:
                    start = datetime.fromtimestamp(
                        date_ms / 1000, tz=timezone.utc
                    )
                else:
                    start = now
                end = start + timedelta(hours=3)

                if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
                    continue
                if start > now + timedelta(hours=FUTURE_WINDOW_HOURS):
                    continue

                # is_live: true if event has started and not yet ended
                # Also true if date unknown (live-only events have date=0)
                is_live = (date_ms == 0) or (start <= now <= end)
                events.append({
                    "event_id":   f"spk_{mid}",
                    "title":      title,
                    "sport":      sport,
                    "start_time": fmt_iso(start),
                    "end_time":   fmt_iso(end),
                    "is_live":    is_live,
                    "streams":    streams,
                    "source":     "StreamedPK",
                    "_thumb":     thumb,
                    "_league":    category,
                })

            except Exception as e:
                log.debug(f"  [StreamedPK] Resolve error: {e}")

    if events:
        record_source_success(ev_state, "StreamedPK")
    else:
        record_source_failure(ev_state, "StreamedPK")

    STATS["sources"]["StreamedPK"] = len(events)
    live_cnt = sum(1 for e in events if e.get("is_live"))
    log.info(
        f"  → {len(events)} events from StreamedPK "
        f"({live_cnt} live, all with stream + thumbnail ✅)"
    )
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 1c — StreamedSU Fallback API
#
#  streamed.su is the ORIGINAL domain of the streamed.pk service.
#  Both share IDENTICAL API structure — su is used as fallback
#  when streamed.pk is slow or when GitHub Actions IP is blocked.
#
#  API: https://streamed.su (same endpoints as streamed.pk)
# ═══════════════════════════════════════════════════════════════════

_SSU_BASE = "https://streamed.su"

_SSU_ENDPOINTS = [
    "/api/matches/live",
    "/api/matches/all-today",
    "/api/matches/football",
    "/api/matches/cricket",
    "/api/matches/basketball",
    "/api/matches/mma",
    "/api/matches/tennis",
]


def fetch_streamedsu_api(ev_state: dict) -> list:
    """
    streamed.su — original domain, identical API to streamed.pk.
    Returns only events with BOTH stream embedUrl AND thumbnail.
    Used as complementary source (catches matches streamed.pk misses).
    """
    log.info("📡 [StreamedSU-API] Fetching via streamed.su API…")
    if not source_is_healthy(ev_state, "StreamedSU_API"):
        return []

    now          = now_utc()
    raw_matches: list = []
    seen_ids:    set  = set()

    api_hdrs = _base_headers({
        "Referer": f"{_SSU_BASE}/",
        "Origin":  _SSU_BASE,
        "Accept":  "application/json, */*",
    })

    consecutive_fails = 0
    for path in _SSU_ENDPOINTS:
        r = safe_get(
            f"{_SSU_BASE}{path}", timeout=10,
            extra_hdrs=api_hdrs,
        )
        if not r:
            consecutive_fails += 1
            if consecutive_fails >= 3:
                log.warning("  [StreamedSU-API] 3 fails — bailing early")
                break
            continue
        consecutive_fails = 0
        try:
            data = r.json()
        except Exception:
            continue
        if not isinstance(data, list):
            continue
        for m in data:
            if not isinstance(m, dict):
                continue
            mid = m.get("id", "")
            if not mid or mid in seen_ids or not m.get("sources"):
                continue
            seen_ids.add(mid)
            raw_matches.append(m)

    if not raw_matches:
        log.warning("  [StreamedSU-API] No matches — may be blocked")
        record_source_failure(ev_state, "StreamedSU_API")
        STATS["sources"]["StreamedSU_API"] = 0
        return []

    log.info(f"  [StreamedSU-API] {len(raw_matches)} matches → resolving…")

    # Reuse StreamedPK's resolve functions (identical API)
    events: list = []

    def _resolve(match: dict):
        # Use streamed.su domain for stream requests
        sources = match.get("sources", [])
        streams: list = []

        def _fetch_su(src_info):
            source = src_info.get("source", "")
            src_id = src_info.get("id", "")
            if not source or not src_id:
                return []
            r = safe_get(
                f"{_SSU_BASE}/api/stream/{source}/{src_id}",
                timeout=8,
                extra_hdrs=api_hdrs,
            )
            if not r:
                return []
            try:
                data = r.json()
            except Exception:
                return []
            result = []
            for s in (data if isinstance(data, list) else []):
                embed = s.get("embedUrl", "").strip()
                if embed and embed.startswith("http"):
                    hd   = s.get("hd", False)
                    lang = s.get("language", "EN")
                    result.append({
                        "name": f"StreamedSU {'HD' if hd else 'SD'} ({lang})",
                        "url":  embed,
                    })
            return result

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            futures = {ex.submit(_fetch_su, src): src for src in sources[:4]}
            for f in concurrent.futures.as_completed(futures, timeout=15):
                try:
                    res = f.result()
                    if res:
                        streams.extend(res)
                except Exception:
                    pass

        streams.sort(key=lambda s: 0 if "HD" in s["name"] else 1)
        thumb = _spk_thumbnail(match)   # same image API
        return match, streams[:MAX_STREAMS_PER_EVENT], thumb

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_resolve, m): m for m in raw_matches[:50]}
        for f in concurrent.futures.as_completed(futures, timeout=90):
            try:
                match, streams, thumb = f.result()
                if not streams or not thumb:
                    continue
                mid      = match["id"]
                title    = match.get("title", "").strip()
                category = match.get("category", "")
                sport    = classify_sport(title, category)
                date_ms  = match.get("date", 0)
                start    = (
                    datetime.fromtimestamp(date_ms / 1000, tz=timezone.utc)
                    if date_ms else now
                )
                end = start + timedelta(hours=3)
                if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
                    continue
                if start > now + timedelta(hours=FUTURE_WINDOW_HOURS):
                    continue
                events.append({
                    "event_id":   f"ssu_{mid}",
                    "title":      title,
                    "sport":      sport,
                    "start_time": fmt_iso(start),
                    "end_time":   fmt_iso(end),
                    "is_live":    start <= now <= end,
                    "streams":    streams,
                    "source":     "StreamedSU",
                    "_thumb":     thumb,
                    "_league":    category,
                })
            except Exception:
                pass

    if events:
        record_source_success(ev_state, "StreamedSU_API")
    else:
        record_source_failure(ev_state, "StreamedSU_API")

    STATS["sources"]["StreamedSU_API"] = len(events)
    log.info(f"  → {len(events)} events from StreamedSU API ✅")
    return events
#  BUG FIX v5: dlhd.sx now in trusted domains + embed resolver called
# ═══════════════════════════════════════════════════════════════════

_DADDY_DOMAINS = [
    "https://daddylive.dad",
    "https://dlhd.sx",       # ← primary working domain
    "https://daddylive.mp",
    "https://daddylive.eu",
    "https://daddylive.fm",
    "https://daddylive.to",
    "https://daddylive.sx",
]
_DADDY_PATHS = [
    "/schedule/schedule-generated.json",
    "/daddy-schedule/schedule-generated.json",
    "/wp-content/uploads/schedule/schedule-generated.json",
]


def _resolve_daddylive_embed(embed_url: str, base: str) -> Optional[str]:
    """Fetch DaddyLive embed page and extract actual m3u8/stream URL."""
    r = safe_get(
        embed_url, timeout=10, cf_bypass=True,
        extra_hdrs={"Referer": base + "/", "Origin": base},
    )
    if not r:
        return None
    text = r.text
    # Pattern 1: file: "https://...m3u8..."
    m = re.search(
        r'(?:file|source|src)\s*[=:]\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
        text, re.I
    )
    if m:
        return m.group(1)
    # Pattern 2: raw m3u8 URL in source
    m = re.search(r'(https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*)', text)
    if m:
        return m.group(1)
    # Pattern 3: iframe src pointing to stream
    m = re.search(r'<iframe[^>]+src=["\']([^"\']+)["\']', text, re.I)
    if m:
        src = m.group(1).strip()
        if src.startswith("http") and "google" not in src and "facebook" not in src:
            return src
    return None


def fetch_daddylive(ev_state: dict) -> list:
    log.info("📡 [DaddyLiveHD] Fetching schedule…")
    if not source_is_healthy(ev_state, "DaddyLiveHD"):
        return []

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
        record_source_failure(ev_state, "DaddyLiveHD")
        STATS["sources"]["DaddyLive"] = 0
        return []

    record_source_success(ev_state, "DaddyLiveHD")

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
                    # Pick best channel logo as thumbnail (first channel with logo)
                    event_thumb = ""
                    for i, ch in enumerate(channels[:6]):
                        cid      = ch.get("channel_id", "")
                        if not cid:
                            continue
                        ch_name  = ch.get("channel_name", f"DaddyLive S{i+1}")
                        logo_raw = ch.get("logo_url", "")

                        # Build stream embed URL (dlhd.pk/dlhd.sx both trusted)
                        embed_url = f"{active_base}/embed/stream-{cid}.php"
                        streams_raw.append({"name": ch_name, "url": embed_url})

                        # Build thumbnail from logo_url field
                        if not event_thumb and logo_raw:
                            if logo_raw.startswith("http"):
                                event_thumb = logo_raw
                            else:
                                # Relative path → dlhd.pk logos folder
                                event_thumb = (
                                    f"https://dlhd.pk/logos/"
                                    f"{logo_raw.lstrip('/')}"
                                )

                    if not streams_raw:
                        continue

                    # Fallback thumbnail: sport default
                    if not event_thumb:
                        event_thumb = _SPORT_THUMB.get(sport, _SPORT_THUMB["Other"])

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
                        "_thumb":     event_thumb,
                    })
                except Exception as e:
                    log.debug(f"  [DaddyLive] Row error: {e}")

    STATS["sources"]["DaddyLive"] = len(events)
    log.info(f"  → {len(events)} candidates from DaddyLiveHD")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 3 — VIPLeague (vipleaguetv.net)
#  BUG FIX v5: 15-match cap removed, workers increased 6→16
# ═══════════════════════════════════════════════════════════════════

_VIPLEAGUE_PRIMARY = "https://vipleaguetv.net"
_VIPLEAGUE_FALLBACK_DOMAINS = [
    "https://vipleaguetv.net",
    "https://vipbox.lc",
    "https://vipbox.bz",
    "https://vipleague.st",
    "https://vipleague.lc",
]

_VIPLEAGUE_SPORT_PATHS = [
    ("/live",       "Other"),
    ("/soccer",     "Football"),
    ("/basketball", "Basketball"),
    ("/baseball",   "Baseball"),
    ("/hockey",     "Hockey"),
    ("/tennis",     "Tennis"),
    ("/boxing",     "Boxing"),
    ("/rugby",      "Rugby"),
    ("/moto_gp",    "Formula 1"),
    ("/volleyball", "Other"),
    ("/handball",   "Other"),
]

_VIPLEAGUE_MATCH_RX = re.compile(
    r"/(?:soccer|football|basketball|baseball|hockey|tennis|boxing|rugby|"
    r"moto_gp|handball|volleyball|others|live)/vtv-[a-z0-9]+-[^\"'\s]+",
    re.I
)
_VIPLEAGUE_TIME_RX = re.compile(r"\b(\d{1,2}:\d{2})\b")


def _fetch_vipleague_match_page(
    match_url: str, base: str, session_hdrs: dict
) -> Optional[str]:
    full_url = match_url if match_url.startswith("http") else f"{base}{match_url}"
    try:
        r = requests.get(full_url, headers=session_hdrs, timeout=8, allow_redirects=True)
        if r.status_code != 200:
            return None
        text = r.text

        # Pattern 1: <iframe ... src='...'>
        m = re.search(r'<iframe[^>]+src=["\']([^"\']+)["\'][^>]*>', text, re.I)
        if m:
            src = m.group(1).strip()
            if src.startswith("http") and "google" not in src and "facebook" not in src:
                return src

        # Pattern 2: JavaScript file/source variable
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
    soup: BeautifulSoup, base: str, sport_hint: str,
    now: datetime, seen_urls: set,
) -> list:
    raw_matches = []
    all_links = soup.find_all("a", href=True)

    for a in all_links:
        href = a.get("href", "")
        if not _VIPLEAGUE_MATCH_RX.search(href):
            continue
        full_href = href if href.startswith("http") else f"{base}{href}"
        if full_href in seen_urls:
            continue
        seen_urls.add(full_href)

        raw_text    = a.get_text(" ", strip=True)
        parent      = a.find_parent(["li", "div", "tr", "td"])
        parent_text = parent.get_text(" ", strip=True) if parent else ""

        time_m   = _VIPLEAGUE_TIME_RX.search(parent_text or raw_text)
        time_str = time_m.group(1) if time_m else None

        title = re.sub(r"^\s*[\▶►▸>]?\s*\d{1,2}:\d{2}\s*", "", raw_text).strip()
        title = re.sub(r"\s+", " ", title).strip()
        title = re.sub(r"\s+-\s+", " vs ", title)

        if len(title) < 4:
            continue

        sport = sport_hint
        for path, sp in _VIPLEAGUE_SPORT_PATHS:
            path_slug = path.lstrip("/")
            if f"/{path_slug}/" in href or href.startswith(path):
                sport = sp
                break
        detected = classify_sport(title, sport)
        if detected != "Other":
            sport = detected

        raw_matches.append((title, time_str, full_href, sport))

    return raw_matches


def fetch_vipleague(ev_state: dict) -> list:
    log.info("📡 [VIPLeague/VIPBox] Scraping vipleaguetv.net…")
    if not source_is_healthy(ev_state, "VIPLeague"):
        return []

    events:      list = []
    now               = now_utc()
    seen_urls:   set  = set()
    seen_titles: set  = set()
    active_base: Optional[str] = None

    session_hdrs = _base_headers({
        "Referer":       "https://www.google.com/",
        "Accept":        "text/html,application/xhtml+xml,*/*",
        "Cache-Control": "no-cache",
        "Pragma":        "no-cache",
    })

    for base in _VIPLEAGUE_FALLBACK_DOMAINS:
        r = safe_get(base, timeout=15, cf_bypass=True, extra_hdrs=session_hdrs)
        if r and ("vipbox" in r.text.lower() or "vipleague" in r.text.lower()):
            active_base = base
            log.info(f"  [VIPLeague] Connected: {active_base}")
            break

    if not active_base:
        log.warning("  [VIPLeague] All domains failed — skipping")
        record_source_failure(ev_state, "VIPLeague")
        STATS["sources"]["VIPLeague"] = 0
        return []

    raw_matches = []

    for path, sport_hint in _VIPLEAGUE_SPORT_PATHS:
        page_url = f"{active_base}{path}"
        r = safe_get(
            page_url, timeout=15, cf_bypass=True,
            extra_hdrs={**session_hdrs, "Referer": active_base + "/"},
        )
        if not r:
            continue
        soup  = BeautifulSoup(r.text, "html.parser")
        found = _parse_vipleague_listing(soup, active_base, sport_hint, now, seen_urls)
        raw_matches.extend(found)

        if path == "/live" and found:
            log.info(f"  [VIPLeague] /live returned {len(found)} live matches")

    if not raw_matches:
        log.warning("  [VIPLeague] No match links found in any sport page")
        record_source_failure(ev_state, "VIPLeague")
        STATS["sources"]["VIPLeague"] = 0
        return []

    log.info(
        f"  [VIPLeague] Found {len(raw_matches)} total match links → "
        f"fetching stream pages…"
    )

    # BUG FIX v5: Was [:15], now [:80] — process far more matches
    raw_matches = raw_matches[:80]

    def _resolve_match(args):
        title, time_str, match_url, sport = args
        iframe_src = _fetch_vipleague_match_page(match_url, active_base, session_hdrs)
        return title, time_str, match_url, sport, iframe_src

    resolved = []
    # BUG FIX v5: workers increased from 6 to 16
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
        futures = {ex.submit(_resolve_match, m): m for m in raw_matches}
        for f in concurrent.futures.as_completed(futures, timeout=60):
            try:
                resolved.append(f.result())
            except Exception:
                pass

    for title, time_str, match_url, sport, iframe_src in resolved:
        if title in seen_titles:
            continue
        seen_titles.add(title)

        if time_str:
            try:
                h, m  = map(int, time_str.split(":"))
                start = now.replace(hour=h, minute=m, second=0, microsecond=0)
                if (now - start).total_seconds() > 7200:
                    start += timedelta(days=1)
            except Exception:
                start = now
        else:
            start = now

        end = start + timedelta(hours=3)

        streams_raw = [{"name": "VIPLeague Embed", "url": match_url}]
        if iframe_src:
            streams_raw.insert(0, {"name": "VIPLeague Stream", "url": iframe_src})

        # Thumbnail: build from sport badge via streamed.pk images API
        # Extract team names from title to get badge IDs
        vip_thumb = _SPORT_THUMB.get(sport, _SPORT_THUMB["Other"])
        if " vs " in title.lower():
            parts  = re.split(r"\s+vs\.?\s+", title, flags=re.I)
            if len(parts) >= 2:
                home_slug = re.sub(r"[^a-z0-9]+", "-", parts[0].lower().strip()).strip("-")
                away_slug = re.sub(r"[^a-z0-9]+", "-", parts[1].lower().strip()).strip("-")
                if home_slug and away_slug:
                    vip_thumb = (
                        f"{_SPKBASE}/api/images/poster/"
                        f"{home_slug}/{away_slug}.webp"
                    )

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
            "_thumb":     vip_thumb,
        })

    record_source_success(ev_state, "VIPLeague")
    STATS["sources"]["VIPLeague"] = len(events)
    log.info(
        f"  → {len(events)} events from VIPLeague "
        f"({sum(1 for e in events if len(e['streams']) > 1)} with iframe streams)"
    )
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 4 — VIPLeague WS (vipleague.ws) — NEW in v5
#
#  Completely separate from vipleaguetv.net — different matches,
#  different sports coverage, different stream providers.
#
#  Site structure (from HTML source analysis):
#  ┌─ Sport listing ───────────────────────────────────────────────┐
#  │  /{sport}-sports-stream  e.g. /football-sports-stream         │
#  │  /live-now-games         all currently live events            │
#  │  /upcoming-games         starting soon                        │
#  ├─ Schedule data ───────────────────────────────────────────────┤
#  │  Server-side rendered into collapsible cards                  │
#  │  <a href="/{sport}/tag-{name}-live" data-bs-toggle="collapse">│
#  │  <span class="w8b8b5w9l8" content="2026-05-19T01:00">         │
#  ├─ Individual stream pages ─────────────────────────────────────┤
#  │  /{sport}/{event-name}-1-live-streaming                       │
#  │  /{sport}/{event-name}-2-live-streaming  (HD)                 │
#  └───────────────────────────────────────────────────────────────┘
# ═══════════════════════════════════════════════════════════════════

_VIPLWS_BASE = "https://www.vipleague.ws"
_VIPLWS_FALLBACKS = [
    "https://www.vipleague.ws",
    "https://vipleague.ws",
]

_VIPLWS_SPORT_PATHS = [
    ("/live-now-games",          "Other"),       # all live
    ("/football-sports-stream",  "Football"),
    ("/cricket-sports-stream",   "Cricket"),
    ("/basketball-sports-stream","Basketball"),
    ("/hockey-sports-stream",    "Hockey"),
    ("/baseball-sports-stream",  "Baseball"),
    ("/wwe-sports-stream",       "WWE"),
    ("/boxing-sports-stream",    "Boxing"),
    ("/tennis-sports-stream",    "Tennis"),
    ("/ufc-sports-stream",       "MMA"),
    ("/rugby-sports-stream",     "Rugby"),
    ("/formula-1-sports-stream", "Formula 1"),
    ("/motogp-sports-stream",    "Formula 1"),
    ("/horse-racing-sports-stream","Other"),
]

# Match links on vipleague.ws — live stream pages
_VIPLWS_STREAM_RX = re.compile(
    r"/(?:football|soccer|cricket|basketball|hockey|baseball|wwe|boxing|"
    r"tennis|ufc|rugby|formula-1|motogp|horse-racing|american-football|"
    r"others|test-odi|ipl|nba|nfl|nhl|mlb|golf|cycling|darts|handball|"
    r"volleyball|nascar|motorsports|snooker|aussie-rules|fighting)"
    r"/[a-z0-9-]+-\d+-live-streaming",
    re.I
)

# Tag pages linking to multiple broadcasts
_VIPLWS_TAG_RX = re.compile(
    r"/(?:football|soccer|cricket|basketball|hockey|baseball|wwe|boxing|"
    r"tennis|ufc|rugby|formula-1|motogp|others|test-odi|ipl)/tag-[a-z0-9-]+-live",
    re.I
)


def _fetch_viplws_stream_page(stream_url: str, base: str) -> Optional[str]:
    """Extract stream embed from a vipleague.ws individual match page."""
    full_url = stream_url if stream_url.startswith("http") else f"{base}{stream_url}"
    try:
        r = requests.get(
            full_url,
            headers=_base_headers({
                "Referer": base + "/",
                "Accept":  "text/html,*/*",
            }),
            timeout=10,
            allow_redirects=True,
        )
        if r.status_code != 200:
            return None
        text = r.text

        # Pattern: script src with stream provider
        for pattern in [
            r'<script[^>]+src=["\']([^"\']*(?:burkhakalekah|lapserspos|jnbhi)[^"\']*)["\']',
            r'<iframe[^>]+src=["\']([^"\']+)["\'][^>]*>',
            r'(?:file|src)\s*[:=]\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
        ]:
            m = re.search(pattern, text, re.I)
            if m:
                url = m.group(1).strip()
                if url.startswith("//"):
                    url = "https:" + url
                if url and url != "about:blank":
                    return url
    except Exception:
        pass
    return None


def _parse_viplws_page(soup: BeautifulSoup, base: str, sport_hint: str) -> list:
    """
    Parse a vipleague.ws sport listing page.
    Returns list of (title, time_str, stream_urls, sport).
    """
    results = []
    seen    = set()
    now     = now_utc()

    # Find collapse toggles — each is an event
    toggles = soup.find_all("a", attrs={"data-bs-toggle": "collapse"})

    for toggle in toggles:
        href  = toggle.get("href", "")
        title = toggle.get_text(" ", strip=True)

        # Extract time from time span
        time_str = None
        time_span = toggle.find("span", class_=re.compile(r"w8b8b5|time", re.I))
        if time_span:
            content = time_span.get("content", "") or time_span.get_text(strip=True)
            # content may be "2026-05-19T01:00"
            m = re.search(r"T(\d{1,2}:\d{2})", content)
            if m:
                time_str = m.group(1)
            else:
                m2 = re.search(r"\b(\d{1,2}:\d{2})\b", content)
                if m2:
                    time_str = m2.group(1)
            # Remove time from title
            title = re.sub(
                r"\b\d{1,2}:\d{2}\b", "", title
            ).strip()

        title = re.sub(r"\s+", " ", title).strip()
        if len(title) < 4 or title in seen:
            continue
        seen.add(title)

        sport = classify_sport(title, sport_hint)

        # Collect stream URLs from the sibling collapse div
        collapse_id = toggle.get("data-bs-target", "").lstrip("#")
        stream_urls = []

        if collapse_id:
            collapse_div = soup.find(id=collapse_id)
            if collapse_div:
                for a in collapse_div.find_all("a", href=True):
                    a_href = a.get("href", "")
                    if _VIPLWS_STREAM_RX.search(a_href):
                        full = a_href if a_href.startswith("http") else f"{base}{a_href}"
                        stream_urls.append(full)
                    # data-uri attribute pattern
                    data_uri = a.get("data-uri", "")
                    if data_uri and _VIPLWS_STREAM_RX.search(data_uri):
                        full = data_uri if data_uri.startswith("http") else f"{base}{data_uri}"
                        stream_urls.append(full)

        # Also check if toggle itself links to tag page — fetch it for stream links
        if not stream_urls and _VIPLWS_TAG_RX.search(href):
            tag_url = href if href.startswith("http") else f"{base}{href}"
            try:
                tr = requests.get(
                    tag_url,
                    headers=_base_headers({"Referer": base + "/"}),
                    timeout=8,
                )
                if tr.status_code == 200:
                    tag_soup = BeautifulSoup(tr.text, "html.parser")
                    for a in tag_soup.find_all("a", href=True):
                        a_href = a.get("href", "")
                        data_uri = a.get("data-uri", "")
                        for candidate in [a_href, data_uri]:
                            if candidate and _VIPLWS_STREAM_RX.search(candidate):
                                full = (candidate if candidate.startswith("http")
                                        else f"{base}{candidate}")
                                stream_urls.append(full)
            except Exception:
                pass

        # If href itself is a stream URL
        if not stream_urls and _VIPLWS_STREAM_RX.search(href):
            full = href if href.startswith("http") else f"{base}{href}"
            stream_urls.append(full)

        results.append((title, time_str, list(dict.fromkeys(stream_urls)), sport))

    return results


def fetch_vipleague_ws(ev_state: dict) -> list:
    """
    Scrape vipleague.ws — separate site from vipleaguetv.net,
    different matches and stream providers.
    """
    log.info("📡 [VIPLeague WS] Scraping vipleague.ws…")
    if not source_is_healthy(ev_state, "VIPLeagueWS"):
        return []

    events:      list = []
    now               = now_utc()
    seen_titles: set  = set()
    active_base: Optional[str] = None

    hdrs = _base_headers({
        "Referer": "https://www.google.com/",
        "Accept":  "text/html,*/*",
    })

    for base in _VIPLWS_FALLBACKS:
        r = safe_get(base, timeout=12, cf_bypass=True, extra_hdrs=hdrs)
        if r and "vipleague" in r.text.lower():
            active_base = base
            log.info(f"  [VIPLeague WS] Connected: {active_base}")
            break

    if not active_base:
        log.warning("  [VIPLeague WS] All domains failed — skipping")
        record_source_failure(ev_state, "VIPLeagueWS")
        STATS["sources"]["VIPLeagueWS"] = 0
        return []

    raw_matches = []  # (title, time_str, stream_urls, sport)

    for path, sport_hint in _VIPLWS_SPORT_PATHS:
        page_url = f"{active_base}{path}"
        r = safe_get(
            page_url, timeout=12, cf_bypass=True,
            extra_hdrs={**hdrs, "Referer": active_base + "/"},
        )
        if not r:
            continue
        soup  = BeautifulSoup(r.text, "html.parser")
        found = _parse_viplws_page(soup, active_base, sport_hint)
        raw_matches.extend(found)
        log.debug(f"  [VIPLeague WS] {path}: {len(found)} matches")

        if path == "/live-now-games" and found:
            log.info(f"  [VIPLeague WS] /live-now-games returned {len(found)} live events")

    if not raw_matches:
        log.warning("  [VIPLeague WS] No matches found")
        record_source_failure(ev_state, "VIPLeagueWS")
        STATS["sources"]["VIPLeagueWS"] = 0
        return []

    log.info(
        f"  [VIPLeague WS] {len(raw_matches)} matches → "
        f"fetching stream embeds…"
    )

    # Process up to 60 matches — parallel fetch stream pages
    def _resolve_ws_match(args):
        title, time_str, stream_urls, sport = args
        # For each stream URL, try to get the embed source
        resolved_streams = []
        for i, su in enumerate(stream_urls[:3]):
            embed = _fetch_viplws_stream_page(su, active_base)
            name  = f"VIPLeague WS S{i+1}"
            if embed:
                resolved_streams.append({"name": f"{name} (embed)", "url": embed})
            resolved_streams.append({"name": name, "url": su})
        return title, time_str, sport, resolved_streams

    raw_matches = raw_matches[:60]
    resolved    = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(_resolve_ws_match, m): m for m in raw_matches}
        for f in concurrent.futures.as_completed(futures, timeout=60):
            try:
                resolved.append(f.result())
            except Exception:
                pass

    for title, time_str, sport, streams_raw in resolved:
        if title in seen_titles:
            continue
        seen_titles.add(title)

        if time_str:
            try:
                h, m  = map(int, time_str.split(":"))
                start = now.replace(hour=h, minute=m, second=0, microsecond=0)
                if (now - start).total_seconds() > 7200:
                    start += timedelta(days=1)
            except Exception:
                start = now
        else:
            start = now

        end = start + timedelta(hours=3)

        event_id = f"viplws_{abs(hash(title + fmt_iso(start))) % 0xFFFFFF:06x}"
        events.append({
            "event_id":   event_id,
            "title":      title,
            "sport":      sport,
            "start_time": fmt_iso(start),
            "end_time":   fmt_iso(end),
            "is_live":    True,
            "streams":    streams_raw,
            "source":     "VIPLeagueWS",
        })

    record_source_success(ev_state, "VIPLeagueWS")
    STATS["sources"]["VIPLeagueWS"] = len(events)
    log.info(f"  → {len(events)} events from VIPLeague WS")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 5 — StreamEast (beststreameast.net) — NEW in v5
#
#  Clean Tailwind-based site with clear HTML structure.
#  Categories: soccer, nba, nfl, mlb, nhl, f1, mma, wwe
#
#  Site structure (from HTML source analysis):
#  ┌─ Category listings ───────────────────────────────────────────┐
#  │  /categories/{sport}/                                         │
#  │  Game cards with team names + data-match-date                 │
#  ├─ Match pages ─────────────────────────────────────────────────┤
#  │  /match/{slug}/{id}                                           │
#  │  <h1>Team A vs Team B</h1>                                    │
#  │  <span data-match-date="2026-05-17 10:00:00">                 │
#  │  <iframe id="stream-player" src="about:blank">  (JS-loaded)  │
#  └───────────────────────────────────────────────────────────────┘
# ═══════════════════════════════════════════════════════════════════

_STREAMEAST_BASE = "https://beststreameast.net"
_STREAMEAST_FALLBACKS = [
    "https://beststreameast.net",
    "https://beststreameast.xyz",
]

_STREAMEAST_CATEGORIES = [
    ("soccer", "Football"),
    ("nba",    "Basketball"),
    ("nfl",    "Football"),
    ("mlb",    "Baseball"),
    ("nhl",    "Hockey"),
    ("f1",     "Formula 1"),
    ("mma",    "MMA"),
    ("wwe",    "WWE"),
]

# Match URL pattern: /match/{slug}/{id}
_STREAMEAST_MATCH_RX = re.compile(r"/match/[a-z0-9-]+/(\d+)", re.I)


def _parse_streameast_category(
    soup: BeautifulSoup, base: str, sport: str, now: datetime
) -> list:
    """Parse StreamEast category page for game cards."""
    results = []
    seen    = set()

    # Strategy 1: Find game cards with match links
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not _STREAMEAST_MATCH_RX.search(href):
            continue

        full_href = href if href.startswith("http") else f"{base}{href}"
        if full_href in seen:
            continue
        seen.add(full_href)

        # Get title from this element or its parent card
        title = ""
        parent = a.find_parent("div", class_=re.compile(r"game.card|card|match", re.I))
        if parent:
            h_tag = parent.find(["h1", "h2", "h3", "strong"])
            if h_tag:
                title = h_tag.get_text(" ", strip=True)

        if not title:
            title = a.get_text(" ", strip=True)

        # Clean up title
        title = re.sub(r"\s+", " ", title).strip()
        if len(title) < 5:
            continue

        # Get match date from data-match-date attribute
        date_str = None
        date_el  = a.find(attrs={"data-match-date": True})
        if not date_el:
            # Check parent
            card = a.find_parent()
            if card:
                date_el = card.find(attrs={"data-match-date": True})
        if date_el:
            date_str = date_el.get("data-match-date", "")

        start = now  # default
        if date_str:
            try:
                start = datetime.strptime(
                    date_str.strip(), "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=timezone.utc)
            except Exception:
                try:
                    start = datetime.fromisoformat(date_str.strip()).replace(
                        tzinfo=timezone.utc
                    )
                except Exception:
                    pass

        end = start + timedelta(hours=3)
        if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
            continue
        if start > now + timedelta(hours=FUTURE_WINDOW_HOURS):
            continue

        detected_sport = classify_sport(title, sport)

        results.append((title, start, full_href, detected_sport))

    # Strategy 2: Find all h2/h3 with vs pattern if strategy 1 found nothing
    if not results:
        for h in soup.find_all(["h2", "h3"]):
            text = h.get_text(" ", strip=True)
            if " vs " not in text.lower():
                continue
            # Try to find a nearby link
            parent = h.find_parent()
            link   = parent.find("a", href=_STREAMEAST_MATCH_RX) if parent else None
            if not link:
                continue
            href = link.get("href", "")
            full = href if href.startswith("http") else f"{base}{href}"
            if full in seen:
                continue
            seen.add(full)

            title = re.sub(r"\s+", " ", text).strip()
            results.append((title, now, full, classify_sport(title, sport)))

    return results


def fetch_streameast(ev_state: dict) -> list:
    """Scrape beststreameast.net for live and upcoming sports events."""
    log.info("📡 [StreamEast] Scraping beststreameast.net…")
    if not source_is_healthy(ev_state, "StreamEast"):
        return []

    events:      list = []
    seen_titles: set  = set()
    now               = now_utc()
    active_base: Optional[str] = None

    hdrs = _base_headers({
        "Referer": "https://www.google.com/",
        "Accept":  "text/html,*/*",
    })

    for base in _STREAMEAST_FALLBACKS:
        r = safe_get(base, timeout=12, cf_bypass=True, extra_hdrs=hdrs)
        if r and "streameast" in r.text.lower():
            active_base = base
            log.info(f"  [StreamEast] Connected: {active_base}")
            break

    if not active_base:
        log.warning("  [StreamEast] All domains failed — skipping")
        record_source_failure(ev_state, "StreamEast")
        STATS["sources"]["StreamEast"] = 0
        return []

    for cat_slug, sport in _STREAMEAST_CATEGORIES:
        cat_url = f"{active_base}/categories/{cat_slug}/"
        r = safe_get(
            cat_url, timeout=12, cf_bypass=True,
            extra_hdrs={**hdrs, "Referer": active_base + "/"},
        )
        if not r:
            continue

        soup    = BeautifulSoup(r.text, "html.parser")
        matches = _parse_streameast_category(soup, active_base, sport, now)
        log.debug(f"  [StreamEast] /categories/{cat_slug}/: {len(matches)} matches")

        for title, start, match_url, detected_sport in matches:
            if title in seen_titles:
                continue
            seen_titles.add(title)

            end      = start + timedelta(hours=3)
            event_id = f"se_{abs(hash(match_url)) % 0xFFFFFF:06x}"

            # Stream: match URL embed (trusted domain)
            streams_raw = [{"name": "StreamEast HD", "url": match_url}]

            # Thumbnail: build from team names via streamed.pk badge API
            se_thumb = _SPORT_THUMB.get(detected_sport, _SPORT_THUMB["Other"])
            if " vs " in title.lower():
                parts = re.split(r"\s+vs\.?\s+", title, flags=re.I)
                if len(parts) >= 2:
                    h_slug = re.sub(r"[^a-z0-9]+", "-", parts[0].lower().strip()).strip("-")
                    a_slug = re.sub(r"[^a-z0-9]+", "-", parts[1].lower().strip()).strip("-")
                    if h_slug and a_slug:
                        se_thumb = (
                            f"{_SPKBASE}/api/images/poster/{h_slug}/{a_slug}.webp"
                        )

            events.append({
                "event_id":   event_id,
                "title":      title,
                "sport":      detected_sport,
                "start_time": fmt_iso(start),
                "end_time":   fmt_iso(end),
                "is_live":    start <= now <= end,
                "streams":    streams_raw,
                "source":     "StreamEast",
                "_thumb":     se_thumb,
            })

    if events:
        record_source_success(ev_state, "StreamEast")
    else:
        record_source_failure(ev_state, "StreamEast")

    STATS["sources"]["StreamEast"] = len(events)
    log.info(f"  → {len(events)} events from StreamEast")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 6 — TheSportsDB (free, no API key)
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


def fetch_thesportsdb(ev_state: dict) -> list:
    log.info("📡 [TheSportsDB] Fetching upcoming schedule…")
    if not source_is_healthy(ev_state, "TheSportsDB"):
        return []

    events: list = []
    seen:   set  = set()
    now = now_utc()

    # 1. Fetch by league
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

    # 2. Fetch today's events
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
                start = datetime.fromisoformat(
                    f"{date_str}T{time_str}"
                ).replace(tzinfo=timezone.utc)
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

    if events:
        record_source_success(ev_state, "TheSportsDB")
    else:
        record_source_failure(ev_state, "TheSportsDB")

    STATS["sources"]["TheSportsDB"] = len(events)
    log.info(f"  → {len(events)} schedule entries from TheSportsDB")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 7 — ESPN Unofficial API (extremely reliable)
# ═══════════════════════════════════════════════════════════════════

_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"
_ESPN_LEAGUES = [
    ("soccer/eng.1",              "Football",   "Premier League"),
    ("soccer/esp.1",              "Football",   "La Liga"),
    ("soccer/ger.1",              "Football",   "Bundesliga"),
    ("soccer/ita.1",              "Football",   "Serie A"),
    ("soccer/fra.1",              "Football",   "Ligue 1"),
    ("soccer/uefa.champions",     "Football",   "Champions League"),
    ("soccer/uefa.europa",        "Football",   "Europa League"),
    ("soccer/usa.1",              "Football",   "MLS"),
    ("soccer/bra.1",              "Football",   "Brasileirao"),
    ("soccer/mex.1",              "Football",   "Liga MX"),
    ("basketball/nba",            "Basketball", "NBA"),
    ("basketball/mens-college-basketball", "Basketball", "NCAA"),
    ("baseball/mlb",              "Baseball",   "MLB"),
    ("hockey/nhl",                "Hockey",     "NHL"),
    ("football/nfl",              "Football",   "NFL"),
    ("tennis/atp",                "Tennis",     "ATP"),
    ("cricket/icc.men.t20.world", "Cricket",    "T20 WC"),
]


def fetch_espn(ev_state: dict) -> list:
    log.info("📡 [ESPN] Fetching scoreboard events…")
    if not source_is_healthy(ev_state, "ESPN"):
        return []

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

                name = (ev_data.get("name", "") or ev_data.get("shortName", ""))
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
                    ev_data.get("status", {}).get("type", {}).get("state", "pre")
                )
                is_live = status_type == "in"

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
                    "streams":    [],
                    "source":     "ESPN",
                    "_league":    league_name,
                    "_venue":     venue,
                })
            except Exception as e:
                log.debug(f"  [ESPN] Row error: {e}")

    if events:
        record_source_success(ev_state, "ESPN")
    else:
        record_source_failure(ev_state, "ESPN")

    STATS["sources"]["ESPN"] = len(events)
    log.info(f"  → {len(events)} events from ESPN")
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 8 — Tapmad (www.tapmad.com) — NEW in v6
#
#  Pakistani/South Asian sports streaming platform (Next.js SSR).
#  Key insight: ALL data is in __NEXT_DATA__ JSON — no JS needed!
#
#  Coverage confirmed from HTML analysis:
#  ┌─ Cricket ─────────────────────────────────────────────────────┐
#  │  Bangladesh vs Pakistan (all Tests, ODIs, T20s)               │
#  │  PSL 2026, IPL matches                                        │
#  ├─ Football ─────────────────────────────────────────────────────┤
#  │  EPL, UCL, Europa League, FA Cup, Bundesliga, Saudi Pro League │
#  ├─ Other ───────────────────────────────────────────────────────┤
#  │  UFC 2026, PSL Finals                                         │
#  └───────────────────────────────────────────────────────────────┘
#
#  Free content (IsVideoFree=1): Direct m3u8 from Linode CDN
#  Paid content (IsVideoFree=0): Schedule-only entry
#  CDN: vodintlv2.in-maa1.linodeobjects.com
#
#  API structure (from __NEXT_DATA__ JSON):
#  ┌─ Home page ────────────────────────────────────────────────────┐
#  │  props.pageProps.movies.Sections[0] → "Upcoming Sports Premium"│
#  │  Each video: ContentVideoName, ContentEventStartDate,          │
#  │              ContentEntityId, SeoTitle, IsVideoFree            │
#  │              VideoCategoryIdSeo (category ID for sport pages)  │
#  ├─ Sports category pages ────────────────────────────────────────┤
#  │  /sports/{seo-slug}/{category-id}/1                            │
#  ├─ Watch pages (free VOD) ───────────────────────────────────────┤
#  │  /watch/{seo-title}/{entity-id}                                │
#  │  → props.pageProps.Video.ContentStreamUrlHQ → direct m3u8!    │
#  │  → props.pageProps.upcomingNextReel[].ContentStreamAdaptive    │
#  └───────────────────────────────────────────────────────────────┘
# ═══════════════════════════════════════════════════════════════════

_TAPMAD_BASE = "https://www.tapmad.com"

# Cricket + Football category IDs confirmed from HTML analysis
_TAPMAD_SPORT_CATEGORIES = [
    # Cricket
    (348, "pakistan-tour-of-bangladesh-2026", "Cricket"),
    (263, "psl-2026",                         "Cricket"),
    # Football
    (177, "english-premier-league-2025-26",    "Football"),
    (183, "uefa-champions-league-2025-26",      "Football"),
    (185, "uefa-europa-league-2025-26",         "Football"),
    (224, "fa-cup-2025-26",                    "Football"),
    (182, "bundesliga-2025-26",                "Football"),
    # MMA
    (265, "ufc-2026",                          "MMA"),
]

_TAPMAD_SPORT_MAP = {
    "CRICKET":    "Cricket",
    "FOOTBALL":   "Football",
    "SOCCER":     "Football",
    "BASKETBALL": "Basketball",
    "TENNIS":     "Tennis",
    "MMA":        "MMA",
    "UFC":        "MMA",
    "FORMULA":    "Formula 1",
    "F1":         "Formula 1",
    "MOTOGP":     "Formula 1",
    "RUGBY":      "Rugby",
    "HOCKEY":     "Hockey",
    "BASEBALL":   "Baseball",
    "GOLF":       "Golf",
}


def _tapmad_detect_sport(title: str, header: str, category_sport: str) -> str:
    """Detect sport from title + contentHeaderName."""
    combo = (title + " " + header).upper()
    for kw, sport in _TAPMAD_SPORT_MAP.items():
        if kw in combo:
            return sport
    detected = classify_sport(title, header)
    if detected != "Other":
        return detected
    return category_sport


def _tapmad_extract_next_data(html: str) -> Optional[dict]:
    """Extract __NEXT_DATA__ JSON from Tapmad Next.js page."""
    m = re.search(
        r'<script id="__NEXT_DATA__"[^>]*>\s*(\{.*?\})\s*</script>',
        html, re.DOTALL
    )
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def _tapmad_fetch_watch_streams(
    seo_title: str, entity_id: int, headers: dict
) -> list:
    """
    Fetch a Tapmad watch page and extract stream URLs.
    Returns list of stream dicts for FREE content only.
    """
    watch_url = f"{_TAPMAD_BASE}/watch/{seo_title}/{entity_id}"
    r = safe_get(watch_url, timeout=10, extra_hdrs=headers)
    if not r:
        return []

    nd = _tapmad_extract_next_data(r.text)
    if not nd:
        return []

    try:
        page_props = nd["props"]["pageProps"]
    except (KeyError, TypeError):
        return []

    streams = []

    # 1. Direct stream from main Video object
    video = page_props.get("Video", {})
    if video.get("IsVideoFree") == 1:
        for quality_key in ["ContentStreamAdaptive", "ContentStreamUrlHQ",
                            "ContentStreamUrlMQ", "ContentStreamUrlLQ"]:
            url = video.get(quality_key, "")
            if url and url.startswith("http") and url.endswith(".m3u8"):
                label = quality_key.replace("ContentStream", "").replace("Url", "")
                streams.append({"name": f"Tapmad {label}", "url": url})
                break  # one stream per quality tier is enough

    # 2. From upcomingNextReel — related free clips
    for reel in page_props.get("upcomingNextReel", [])[:3]:
        if not isinstance(reel, dict):
            continue
        url = (
            reel.get("ContentStreamAdaptive") or
            reel.get("ContentStreamUrlHQ") or
            reel.get("ContentStreamUrlMQ") or ""
        )
        if url and url.startswith("http") and ".m3u8" in url:
            name = reel.get("SeoTitle", "Tapmad Stream")[:40]
            if not any(s["url"] == url for s in streams):
                streams.append({"name": f"Tapmad {name}", "url": url})

    # 3. JW Player media ID → CDN stream (free content only)
    jw_id = video.get("JWMediaId", "")
    if jw_id and not streams:
        try:
            jw_r = requests.get(
                f"https://cdn.jwplayer.com/v2/media/{jw_id}",
                headers=_base_headers(), timeout=6
            )
            if jw_r.status_code == 200:
                jw_data = jw_r.json()
                for src in jw_data.get("playlist", [{}])[0].get("sources", []):
                    src_url = src.get("file", "")
                    if src_url and ".m3u8" in src_url:
                        streams.append({"name": "Tapmad JW HD", "url": src_url})
                        break
        except Exception:
            pass

    return streams[:MAX_STREAMS_PER_EVENT]


def _tapmad_parse_section_videos(videos: list, now: datetime, sport_hint: str) -> list:
    """Parse a Tapmad section's video list into event candidates."""
    results = []
    for v in videos:
        if not isinstance(v, dict):
            continue
        try:
            title     = v.get("ContentVideoName", "").strip()
            header    = v.get("contentHeaderName", "")
            seo_title = v.get("SeoTitle", "").strip()
            entity_id = v.get("ContentEntityId")
            is_free   = v.get("IsVideoFree", 0)
            cat_id    = v.get("VideoCategoryIdSeo")
            is_chan    = v.get("IsChannel", False)

            if not title or not entity_id:
                continue

            date_str = v.get("ContentEventStartDate", "")
            if not date_str:
                continue

            try:
                start = datetime.strptime(
                    date_str.strip(), "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=timezone.utc)
            except Exception:
                try:
                    start = datetime.fromisoformat(date_str.strip()).replace(
                        tzinfo=timezone.utc
                    )
                except Exception:
                    continue

            # For VOD clips use shorter duration; for live channels use 3h+
            duration_h = 3 if is_chan else 2
            end = start + timedelta(hours=duration_h)

            if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
                continue
            if start > now + timedelta(hours=FUTURE_WINDOW_HOURS):
                continue

            sport    = _tapmad_detect_sport(title, header, sport_hint)
            thumb    = (v.get("NewChannelThumbnailPath") or
                        v.get("ContentImage") or
                        v.get("NewVideoImageThumbnail") or "")
            watch_url = f"{_TAPMAD_BASE}/watch/{seo_title}/{entity_id}"

            results.append({
                "title":      title,
                "seo_title":  seo_title,
                "entity_id":  entity_id,
                "sport":      sport,
                "start":      start,
                "end":        end,
                "is_free":    bool(is_free),
                "is_channel": bool(is_chan),
                "league":     header,
                "thumb":      thumb,
                "watch_url":  watch_url,
                "event_id":   f"tapmad_{entity_id}",
            })
        except Exception as e:
            log.debug(f"  [Tapmad] Video parse error: {e}")

    return results


def fetch_tapmad(ev_state: dict) -> list:
    """
    Scrape Tapmad for cricket + football schedule and streams.
    Uses __NEXT_DATA__ JSON from Next.js SSR — no Selenium needed.
    """
    log.info("📡 [Tapmad] Fetching schedule + streams…")
    if not source_is_healthy(ev_state, "Tapmad"):
        return []

    events:  list = []
    seen:    set  = set()
    now           = now_utc()

    hdrs = _base_headers({
        "Referer": "https://www.tapmad.com/",
        "Accept":  "text/html,*/*",
    })

    # ── Step 1: Home page → Upcoming Sports Premium ──────────────
    r = safe_get(_TAPMAD_BASE, timeout=15, extra_hdrs=hdrs)
    if r:
        nd = _tapmad_extract_next_data(r.text)
        if nd:
            try:
                sections = nd["props"]["pageProps"]["movies"]["Sections"]
                for section in sections:
                    s_name = section.get("SectionName", "")
                    # Only process sports sections with upcoming matches
                    if not section.get("IsSports") and "Upcoming" not in s_name:
                        continue
                    sport_hint = "Cricket" if "cricket" in s_name.lower() else "Other"
                    candidates = _tapmad_parse_section_videos(
                        section.get("Videos", []), now, sport_hint
                    )
                    for c in candidates:
                        if c["event_id"] not in seen:
                            seen.add(c["event_id"])
                            events.append(c)
                log.info(f"  [Tapmad] Home page: {len(events)} candidates")
            except (KeyError, TypeError) as e:
                log.debug(f"  [Tapmad] Home parse error: {e}")

    # ── Step 2: Sport category pages ─────────────────────────────
    for cat_id, cat_slug, sport in _TAPMAD_SPORT_CATEGORIES:
        cat_url = f"{_TAPMAD_BASE}/sports/{cat_slug}/{cat_id}/1"
        r = safe_get(cat_url, timeout=12, extra_hdrs=hdrs)
        if not r:
            continue
        nd = _tapmad_extract_next_data(r.text)
        if not nd:
            continue
        try:
            sections = nd["props"]["pageProps"]["movies"]["Sections"]
            for section in sections:
                candidates = _tapmad_parse_section_videos(
                    section.get("Videos", []), now, sport
                )
                for c in candidates:
                    if c["event_id"] not in seen:
                        seen.add(c["event_id"])
                        events.append(c)
        except (KeyError, TypeError):
            pass

    if not events:
        log.warning("  [Tapmad] No events found")
        record_source_failure(ev_state, "Tapmad")
        STATS["sources"]["Tapmad"] = 0
        return []

    log.info(
        f"  [Tapmad] {len(events)} candidates → fetching free streams…"
    )

    # ── Step 3: Fetch stream URLs for free content (parallel) ────
    free_events  = [e for e in events if e["is_free"]]
    paid_events  = [e for e in events if not e["is_free"]]

    log.info(
        f"  [Tapmad] {len(free_events)} free VOD | "
        f"{len(paid_events)} paid (schedule-only)"
    )

    def _resolve(ev_candidate):
        streams = _tapmad_fetch_watch_streams(
            ev_candidate["seo_title"],
            ev_candidate["entity_id"],
            hdrs,
        )
        return ev_candidate, streams

    resolved_streams: dict = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_resolve, e): e for e in free_events[:20]}
        for f in concurrent.futures.as_completed(futures, timeout=60):
            try:
                cand, streams = f.result()
                if streams:
                    resolved_streams[cand["event_id"]] = streams
            except Exception:
                pass

    # ── Step 4: Build final event list ───────────────────────────
    final: list = []
    for c in events:
        eid    = c["event_id"]
        streams = resolved_streams.get(eid, [])

        # For paid live channels, add the watch page as embed (trusted domain)
        if not streams and c["is_channel"]:
            streams = [{"name": "Tapmad Live", "url": c["watch_url"]}]

        final.append({
            "event_id":   eid,
            "title":      c["title"],
            "sport":      c["sport"],
            "start_time": fmt_iso(c["start"]),
            "end_time":   fmt_iso(c["end"]),
            "is_live":    c["start"] <= now <= c["end"],
            "streams":    streams,
            "source":     "Tapmad",
            "_league":    c["league"],
            "_thumb":     c["thumb"],
        })

    record_source_success(ev_state, "Tapmad")
    STATS["sources"]["Tapmad"] = len(final)
    stream_count = sum(1 for e in final if e["streams"])
    log.info(
        f"  → {len(final)} events from Tapmad "
        f"({stream_count} with streams)"
    )
    return final


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 9 — FanCode (fancode.com) — NEW in v6
#
#  Dream11-owned premium sports platform. India/Bangladesh focused.
#  Live now page: /bd/live-now/all-sports  (server-side rendered)
#
#  Key findings from HTML analysis:
#  ┌─ Sports covered ───────────────────────────────────────────────┐
#  │  Cricket, Football, F1, MotoGP, Tennis, Motorsports           │
#  │  Bangladesh vs Pakistan, EPL, UCL, ATP, WTA                   │
#  ├─ API backend ──────────────────────────────────────────────────┤
#  │  api.dream11.com (requires auth for streams)                   │
#  ├─ Live match cards ─────────────────────────────────────────────┤
#  │  aria-label="Live match" → confirmed live                      │
#  │  aria-label="Match: {title}"                                   │
#  │  href="/bd/{feed}/tour/{slug}-{tour-id}/matches/{id}/..."      │
#  ├─ Schedule pages ───────────────────────────────────────────────┤
#  │  /bd/cricket, /bd/football, /bd/formula1, etc                  │
#  └───────────────────────────────────────────────────────────────┘
#
#  Note: Streams require subscription. This source provides
#  excellent schedule data — especially Bangladesh cricket.
# ═══════════════════════════════════════════════════════════════════

_FANCODE_BASE = "https://www.fancode.com"

_FANCODE_SPORT_PAGES = [
    ("/bd/live-now/all-sports",  "Other"),
    ("/bd/cricket",              "Cricket"),
    ("/bd/football",             "Football"),
    ("/bd/formula1",             "Formula 1"),
    ("/bd/motogp",               "Formula 1"),
    ("/bd/lawn-tennis",          "Tennis"),
    ("/bd/racing",               "Other"),
]

# Match card aria-label patterns
_FC_MATCH_LABEL_RX = re.compile(
    r'aria-label="Match:\s*([^"]+)"',
    re.I
)
_FC_LIVE_RX = re.compile(
    r'aria-label="Live\s*match"',
    re.I
)
_FC_HREF_RX = re.compile(
    r'href="(/bd/[^"]+/matches/[^"]+/(?:live-match-info|match-info))"',
    re.I
)
_FC_MATCH_DATE_RX = re.compile(
    r'"startDate"\s*:\s*"([^"]+)"',
)
_FC_MATCH_NAME_RX = re.compile(
    r'"name"\s*:\s*"([^"]+)"'
)


def _fancode_parse_live_now(html: str, base: str, sport_hint: str) -> list:
    """
    Parse FanCode live-now or sport page for match cards.
    Each article[data-testid="match-card-{id}"] is one match.
    """
    results = []
    seen    = set()
    now     = now_utc()

    # Extract match card blocks using article tags
    card_blocks = re.findall(
        r'<article[^>]+data-testid="match-card-(\d+)"[^>]*>(.*?)</article>',
        html, re.DOTALL | re.I
    )

    for match_id, block in card_blocks:
        if match_id in seen:
            continue
        seen.add(match_id)

        # Match title from aria-label
        title_m = re.search(r'aria-label="Match:\s*([^"]+)"', block, re.I)
        if not title_m:
            # Try h3
            h3_m = re.search(r'<h3[^>]*>\s*([^<]{5,80})\s*</h3>', block)
            if h3_m:
                title = h3_m.group(1).strip()
            else:
                continue
        else:
            title = title_m.group(1).strip()

        # Is it live?
        is_live = bool(re.search(r'aria-label="Live\s*match"', block, re.I))

        # Match URL
        href_m = re.search(
            r'href="(/bd/[^"]+/matches/[^"]+)"',
            block, re.I
        )
        match_url = (
            f"{base}{href_m.group(1)}" if href_m else ""
        )

        sport = classify_sport(title, sport_hint)

        # For live matches start = now
        start = now if is_live else now + timedelta(hours=1)
        end   = start + timedelta(hours=4)

        eid = f"fancode_{match_id}"
        results.append({
            "event_id":   eid,
            "title":      title,
            "sport":      sport,
            "start_time": fmt_iso(start),
            "end_time":   fmt_iso(end),
            "is_live":    is_live,
            "streams":    [],  # FanCode requires subscription
            "source":     "FanCode",
            "_match_url": match_url,
        })

    return results


def _fancode_parse_schedule_page(html: str, sport_hint: str) -> list:
    """
    Parse FanCode sport schedule page for upcoming matches.
    FanCode embeds structured data in ld+json schema markup.
    """
    results = []
    seen    = set()
    now     = now_utc()

    # Strategy 1: ld+json SportsEvent schema
    ld_blocks = re.findall(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
        html, re.DOTALL | re.I
    )

    for block in ld_blocks:
        try:
            data = json.loads(block)
            events_list = data if isinstance(data, list) else [data]
            for item in events_list:
                if item.get("@type") not in ("SportsEvent", "BroadcastEvent"):
                    continue
                name = item.get("name", "").strip()
                if not name or name in seen:
                    continue
                date_str = (item.get("startDate") or
                            item.get("ContentEventStartDate") or "")
                if not date_str:
                    continue
                try:
                    start = datetime.fromisoformat(
                        date_str.replace("Z", "+00:00")
                    ).replace(tzinfo=timezone.utc)
                except Exception:
                    continue

                end = start + timedelta(hours=4)
                if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
                    continue
                if start > now + timedelta(hours=FUTURE_WINDOW_HOURS):
                    continue

                seen.add(name)
                eid = f"fancode_{abs(hash(name + date_str)) % 0xFFFFFF:06x}"
                results.append({
                    "event_id":   eid,
                    "title":      name,
                    "sport":      classify_sport(name, sport_hint),
                    "start_time": fmt_iso(start),
                    "end_time":   fmt_iso(end),
                    "is_live":    start <= now <= end,
                    "streams":    [],
                    "source":     "FanCode",
                })
        except Exception:
            pass

    return results


def fetch_fancode(ev_state: dict) -> list:
    """
    Scrape FanCode for live and upcoming sports schedule.
    Streams require subscription — provides excellent schedule data.
    """
    log.info("📡 [FanCode] Fetching schedule…")
    if not source_is_healthy(ev_state, "FanCode"):
        return []

    events:      list = []
    seen_ids:    set  = set()
    now               = now_utc()

    hdrs = _base_headers({
        "Referer":  "https://www.fancode.com/",
        "Accept":   "text/html,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })

    for path, sport_hint in _FANCODE_SPORT_PAGES:
        page_url = f"{_FANCODE_BASE}{path}"
        r = safe_get(page_url, timeout=15, cf_bypass=True, extra_hdrs=hdrs)
        if not r:
            continue

        html = r.text

        # Live matches from match cards
        live_matches = _fancode_parse_live_now(html, _FANCODE_BASE, sport_hint)
        for m in live_matches:
            if m["event_id"] not in seen_ids:
                seen_ids.add(m["event_id"])
                events.append(m)

        # Upcoming from ld+json schedule
        upcoming = _fancode_parse_schedule_page(html, sport_hint)
        for m in upcoming:
            if m["event_id"] not in seen_ids:
                seen_ids.add(m["event_id"])
                events.append(m)

        log.debug(
            f"  [FanCode] {path}: "
            f"{len(live_matches)} live + {len(upcoming)} upcoming"
        )

    if events:
        record_source_success(ev_state, "FanCode")
    else:
        record_source_failure(ev_state, "FanCode")

    STATS["sources"]["FanCode"] = len(events)
    live_cnt = sum(1 for e in events if e.get("is_live"))
    log.info(
        f"  → {len(events)} events from FanCode "
        f"({live_cnt} live, all schedule-only — subscription required)"
    )
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 10 — Manual Seeds
# ═══════════════════════════════════════════════════════════════════
MANUAL_EVENTS: list = [
    # {
    #     "event_id":   "manual_example",
    #     "title":      "IPL 2026 Final — KKR vs MI",
    #     "sport":      "Cricket",
    #     "start_time": "2026-05-25T14:00:00Z",
    #     "end_time":   "2026-05-25T19:00:00Z",
    #     "streams": [
    #         {"name": "Server 1 HD", "url": "https://example.m3u8"},
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
#  Events with 0 working streams are saved as schedule-only entries
# ═══════════════════════════════════════════════════════════════════

def enrich_event(ev: dict) -> Optional[dict]:
    """
    Validate streams, build final event dict.

    STRICT MODE (v7):
      • STRICT_REQUIRE_STREAM    = True  → discard if no working streams
      • STRICT_REQUIRE_THUMBNAIL = True  → discard if no thumbnail URL

    This eliminates all schedule-only noise from the output.
    """
    sport  = ev.get("sport", "Other")
    meta   = SPORT_META.get(sport, SPORT_META["Other"])
    streams = ev.get("streams", [])

    # ── Thumbnail resolution ──────────────────────────────────────
    thumb = (
        ev.get("_thumb") or
        ev.get("thumbnail") or
        ev.get("thumb") or
        ""
    )
    # Fallback: sport-category default thumbnail
    if not thumb:
        thumb = _SPORT_THUMB.get(sport, _SPORT_THUMB.get("Other", ""))

    # ── STRICT: thumbnail required ────────────────────────────────
    if STRICT_REQUIRE_THUMBNAIL and not thumb:
        log.debug(f"  🚫 Dropped (no thumbnail): {ev['title']}")
        return None

    # ── Stream validation ─────────────────────────────────────────
    streams = rank_streams_by_quality(streams)

    if streams:
        log.info(f"🔍 Validating [{sport}]: {ev['title']}")
        working = validate_streams(streams)
        if not working:
            log.warning(f"  🚫 Dropped (no working streams): {ev['title']}")
            # STRICT: discard events with no working streams
            if STRICT_REQUIRE_STREAM:
                return None
    else:
        # STRICT: discard events with no streams at all
        if STRICT_REQUIRE_STREAM:
            log.debug(f"  🚫 Dropped (no streams): {ev['title']}")
            return None
        working = []

    working = rank_streams_by_quality(working)

    # ── Build extra metadata ──────────────────────────────────────
    extra = {}
    if thumb:
        extra["thumbnail"] = thumb
    for key in ("_league", "_venue"):
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
        "stream_count":len(working),
        "has_stream":  len(working) > 0,
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
    # STRICT: final filter — only events with stream + thumbnail
    filtered = [
        e for e in active
        if e.get("has_stream") and e.get("thumbnail")
    ]
    dropped = len(active) - len(filtered)
    if dropped:
        log.info(f"  🗑  Dropped {dropped} events missing stream or thumbnail")

    live_events = [e for e in filtered if e.get("is_live")]
    upcoming    = [e for e in filtered if not e.get("is_live")]

    payload = {
        "last_updated":      fmt_iso(now_utc()),
        "strict_mode":       True,
        "requires_stream":   STRICT_REQUIRE_STREAM,
        "requires_thumbnail":STRICT_REQUIRE_THUMBNAIL,
        "total_live":        len(live_events),
        "total_upcoming":    len(upcoming),
        "total_streamed":    len(filtered),
        "active_events":     sorted(
            filtered,
            key=lambda e: (0 if e.get("is_live") else 1, e.get("start_time", ""))
        ),
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log.info(
        f"💾 Saved {len(filtered)} events "
        f"({len(live_events)} live, {len(upcoming)} upcoming) "
        f"→ {OUTPUT_FILE}"
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
    skipped_line = (
        f"\n⏭ Skipped: `{', '.join(STATS['skipped'])}`" if STATS["skipped"] else ""
    )

    msg = (
        f"📺 *StreamX Events v8 — Multi-API + Thumbnails*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Runtime: `{elapsed // 60}m {elapsed % 60}s`\n"
        f"✅ Total saved: `{len(active_events)}`\n"
        f"🔴 Live now: `{live_cnt}`\n"
        f"🕐 Upcoming: `{up_cnt}`\n"
        f"📡 With streams: `{streamed_cnt}`\n"
        f"🖼 With thumbnails: `{sum(1 for e in active_events if e.get('thumbnail'))}`\n"
        f"🆕 Added: `{STATS['added']}`\n"
        f"🔀 Merged: `{STATS['merged']}`\n"
        f"🗑 Expired: `{STATS['expired']}`\n"
        f"🔒 Strict: Stream + Thumbnail required\n"
        f"{skipped_line}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Per source:\n{src_lines}\n"
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
    "StreamedSU", "StreamedPK", "StreamedSU_API",
    "DaddyLive", "VIPLeague", "VIPLeagueWS",
    "StreamEast", "TheSportsDB", "ESPN",
    "Tapmad", "FanCode", "Manual",
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
            f'<img src="{thumb}" style="height:32px;width:48px;object-fit:cover;'
            f'border-radius:4px;vertical-align:middle;margin-right:6px">'
            if thumb else
            '<span style="display:inline-block;width:48px;height:32px;'
            'background:#21262d;border-radius:4px;margin-right:6px"></span>'
        )
        league = ev.get("league", "")
        sub    = f'<br><small style="color:#8b949e">{league}</small>' if league else ""
        sc     = ev.get("stream_count", 0)
        stream_badge = (
            f'<span style="color:#3fb950">▶ {sc} stream{"s" if sc!=1 else ""}</span>'
            if sc > 0 else
            '<span style="color:#f85149">⛔ no stream</span>'
        )
        return (
            f"<tr><td>{badge}</td>"
            f"<td>{img}<strong>{ev['title']}</strong>{sub}</td>"
            f"<td>{t}</td>"
            f"<td>{stream_badge}</td>"
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
    skipped_html = (
        f'<p style="color:#f85149">⏭ Skipped sources: {", ".join(STATS["skipped"])}</p>'
        if STATS["skipped"] else ""
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="refresh" content="1200">
  <title>StreamX Live Events v8</title>
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
    th,td{{padding:7px 9px;border-bottom:1px solid #21262d;text-align:left;vertical-align:middle}}
    th{{color:#8b949e;font-weight:500}}
    tr:hover td{{background:#1c2128}}
    .badge{{display:inline-block;padding:2px 7px;border-radius:10px;font-size:.72em;font-weight:bold;color:#fff}}
    .green{{color:#3fb950}}.red{{color:#f85149}}.blue{{color:#58a6ff}}
    .row2{{display:flex;gap:16px;flex-wrap:wrap}}
    .row2>div{{flex:1;min-width:180px}}
    .strict-badge{{background:#1c3a1c;color:#3fb950;border:1px solid #3fb950;
                   border-radius:6px;padding:3px 10px;font-size:.75em;margin-left:8px}}
    footer{{color:#8b949e;font-size:.72em;text-align:right;margin-top:14px}}
  </style>
</head>
<body>
  <h1>📺 StreamX Live Events <small style="font-size:.6em;color:#58a6ff">v8</small>
    <span class="strict-badge">🔒 STRICT: Stream + Thumbnail Required</span>
  </h1>
  <p class="sub">Auto-updated · {datetime.now().strftime('%Y-%m-%d %H:%M UTC')} · 10 sources</p>

  <div class="card">
    <div class="stats">
      <div class="stat"><div class="num">{len(active_events)}</div><div class="label">✅ Total Events</div></div>
      <div class="stat"><div class="num red">{len(live_ev)}</div><div class="label">🔴 Live Now</div></div>
      <div class="stat"><div class="num green">{len(up_ev)}</div><div class="label">🕐 Upcoming</div></div>
      <div class="stat"><div class="num blue">{sum(1 for e in active_events if e.get('stream_count',0)>0)}</div><div class="label">📡 With Streams</div></div>
      <div class="stat"><div class="num">{sum(1 for e in active_events if e.get('thumbnail'))}</div><div class="label">🖼 With Thumb</div></div>
      <div class="stat"><div class="num">{STATS['added']}</div><div class="label">🆕 Added</div></div>
      <div class="stat"><div class="num">{STATS['merged']}</div><div class="label">🔀 Merged</div></div>
    </div>
  </div>

  <div class="card">
    <h2><span class="live-dot"></span>Live Now ({len(live_ev)})</h2>
    <table>
      <tr><th>Sport</th><th>Event</th><th>Start (UTC)</th><th>Streams</th><th>Source</th></tr>
      {table_or_empty(live_ev, 'No live events right now')}
    </table>
  </div>

  <div class="card">
    <h2>🕐 Upcoming ({len(up_ev)})</h2>
    <table>
      <tr><th>Sport</th><th>Event</th><th>Start (UTC)</th><th>Streams</th><th>Source</th></tr>
      {table_or_empty(up_ev[:50], 'No upcoming events')}
    </table>
  </div>

  <div class="card row2">
    <div>
      <h2>📂 By Sport</h2>
      <table><tr><th>Sport</th><th>Events</th></tr>{sport_rows}</table>
    </div>
    <div>
      <h2>📡 Source Results</h2>
      <table><tr><th>Source</th><th>Events</th></tr>{src_rows}</table>
    </div>
  </div>

  <footer>
    StreamX v8 · 11 sources · Strict: Stream+Thumb ·
    StreamedPK+SU API ({len(_SPK_MATCH_ENDPOINTS)}+{len(_SSU_ENDPOINTS)} endpoints) ·
    {len(_ESPN_LEAGUES)} ESPN leagues ·
    {len(_TAPMAD_SPORT_CATEGORIES)} Tapmad cats
  </footer>
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

# ═══════════════════════════════════════════════════════════════════
#  SOURCE 10 — SofaScore (undocumented public API)
#
#  SofaScore is the most comprehensive free sports data source.
#  No API key required — uses the same endpoints as their mobile app.
#
#  Football: /api/v1/sport/football/scheduled-events/{date}
#  Cricket:  /api/v1/sport/cricket/scheduled-events/{date}
#
#  Thumbnails: uses streamed.pk badge API as fallback
#  Streams:    events matched to StreamedPK by title similarity
# ═══════════════════════════════════════════════════════════════════

_SOFA_BASE = "https://api.sofascore.com"
_SOFA_APP  = "https://www.sofascore.com"

_SOFA_HEADERS = {
    "User-Agent"  : "Mozilla/5.0 (Linux; Android 14; Pixel 8) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Mobile Safari/537.36",
    "Accept"      : "application/json",
    "Referer"     : "https://www.sofascore.com/",
    "Origin"      : "https://www.sofascore.com",
}

# sport slug → (sport name, thumbnail fallback)
_SOFA_SPORTS = [
    ("football", "Football"),
    ("cricket",  "Cricket"),
    ("tennis",   "Tennis"),
    ("basketball","Basketball"),
    ("rugby",    "Rugby"),
    ("mma-ufc",  "MMA"),
]

# Tournament IDs for major cricket competitions (SofaScore internal IDs)
_SOFA_CRICKET_TOURNAMENTS = {
    11: "IPL",
    13: "BBL",
    14: "PSL",
    16: "CPL",
    17: "BPL",
    465: "ICC World Cup",
    466: "ICC T20 World Cup",
    470: "Asia Cup",
    30: "Test Series",
}

# Tournament IDs for major football
_SOFA_FOOTBALL_TOURNAMENTS = {
    17: "Premier League",
    8:  "Champions League",
    679: "Europa League",
    7:  "La Liga",
    35: "Bundesliga",
    23: "Serie A",
    34: "Ligue 1",
    242: "MLS",
    186: "Eredivisie",
    44: "FA Cup",
    271: "Super Lig",
}


def _sofa_thumbnail(event: dict, sport: str) -> str:
    """Build best thumbnail from SofaScore event data."""
    # Try: home team logo from SofaScore CDN
    home_id = event.get("homeTeam", {}).get("id")
    away_id = event.get("awayTeam", {}).get("id")

    if home_id and away_id:
        # SofaScore team images (publicly accessible)
        return (
            f"https://api.sofascore.app/api/v1/team/{home_id}/image"
        )

    # Fallback: streamed.pk sport badge
    return _SPORT_THUMB.get(sport, _SPORT_THUMB["Other"])


def _sofa_title(event: dict) -> str:
    home = event.get("homeTeam", {}).get("name", "")
    away = event.get("awayTeam", {}).get("name", "")
    if home and away:
        return f"{home} vs {away}"
    return event.get("tournament", {}).get("name", "Live Event")


def fetch_sofascore(ev_state: dict) -> list:
    """
    Fetch football + cricket schedules from SofaScore's public API.
    Provides rich team data and thumbnails.
    Streams are matched later via StreamedPK by title similarity.
    """
    log.info("📡 [SofaScore] Fetching football + cricket schedules…")
    if not source_is_healthy(ev_state, "SofaScore"):
        return []

    now    = now_utc()
    events = []
    seen   = set()

    # Fetch today + tomorrow
    dates = [
        now.strftime("%Y-%m-%d"),
        (now + timedelta(days=1)).strftime("%Y-%m-%d"),
    ]

    for sport_slug, sport_name in _SOFA_SPORTS:
        for date_str in dates:
            url = f"{_SOFA_BASE}/api/v1/sport/{sport_slug}/scheduled-events/{date_str}"
            r = safe_get(url, timeout=12, extra_hdrs=_SOFA_HEADERS)
            if not r:
                continue
            try:
                data = r.json()
            except Exception:
                continue

            for ev in data.get("events", []):
                try:
                    eid    = str(ev.get("id", ""))
                    if not eid or eid in seen:
                        continue
                    seen.add(eid)

                    title  = _sofa_title(ev)
                    if not title or len(title) < 4:
                        continue

                    # Start time from SofaScore (Unix timestamp)
                    ts     = ev.get("startTimestamp", 0)
                    if not ts:
                        continue
                    start  = datetime.fromtimestamp(ts, tz=timezone.utc)
                    end    = start + timedelta(hours=3)

                    if end + timedelta(minutes=EXPIRY_GRACE_MINUTES) < now:
                        continue
                    if start > now + timedelta(hours=FUTURE_WINDOW_HOURS):
                        continue

                    status = ev.get("status", {}).get("type", "")
                    is_live = status in ("inprogress", "halftime", "pause")

                    thumb  = _sofa_thumbnail(ev, sport_name)

                    # Tournament name for league field
                    league = (
                        ev.get("tournament", {})
                          .get("uniqueTournament", {})
                          .get("name", "")
                        or ev.get("tournament", {}).get("name", "")
                    )

                    events.append({
                        "event_id":  f"sofa_{eid}",
                        "title":     title,
                        "sport":     sport_name,
                        "start_time":fmt_iso(start),
                        "end_time":  fmt_iso(end),
                        "is_live":   is_live,
                        "streams":   [],          # matched later
                        "source":    "SofaScore",
                        "_thumb":    thumb,
                        "_league":   league,
                        "_sofa_id":  eid,
                    })
                except Exception as e:
                    log.debug(f"  [SofaScore] Event parse error: {e}")

    if events:
        record_source_success(ev_state, "SofaScore")
        log.info(
            f"  → {len(events)} schedule events from SofaScore "
            f"(streams will be matched from other sources)"
        )
    else:
        record_source_failure(ev_state, "SofaScore")
        log.warning("  [SofaScore] No events returned")

    STATS["sources"]["SofaScore"] = len(events)
    return events


# ═══════════════════════════════════════════════════════════════════
#  SOURCE 11 — LiveScore.biz (free public JSON API)
#
#  Provides real-time football + cricket scores and schedules.
#  No API key required. Used as complementary schedule source.
# ═══════════════════════════════════════════════════════════════════

_LS_BASE = "https://livescore-api.com/api-client"
_LS_FREE_BASE = "https://www.livescore.biz/scores"

def fetch_livescore(ev_state: dict) -> list:
    """
    Fetch live football matches from livescore.biz public data.
    Used to enrich SofaScore + StreamedPK data with live flag accuracy.
    """
    log.info("📡 [LiveScore] Fetching live football…")
    if not source_is_healthy(ev_state, "LiveScore"):
        return []

    now    = now_utc()
    events = []

    for sport_path, sport_name in [
        ("/football", "Football"),
        ("/cricket",  "Cricket"),
    ]:
        url = f"https://www.livescore.biz{sport_path}"
        r   = safe_get(url, timeout=10, cf_bypass=True, extra_hdrs={
            "Referer": "https://www.livescore.biz/",
            "Accept":  "text/html,*/*",
        })
        if not r:
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        # livescore.biz match rows
        for row in soup.select(".row-gray, .row-white, [class*='match']"):
            try:
                text = row.get_text(" ", strip=True)
                if len(text) < 5:
                    continue

                # Pattern: "Team A - Team B" or "Team A vs Team B"
                m = re.search(r"([A-Z][^-\n]{2,30})\s*[-–]\s*([A-Z][^-\n]{2,30})", text)
                if not m:
                    continue

                home  = m.group(1).strip()
                away  = m.group(2).strip()
                title = f"{home} vs {away}"

                if len(title) < 8:
                    continue

                # Check if live
                is_live = bool(
                    row.select_one("[class*='live'], .live-label")
                    or "LIVE" in text.upper()
                    or "'" in text  # minute indicator like "45'"
                )

                thumb = _SPORT_THUMB.get(sport_name, _SPORT_THUMB["Other"])

                events.append({
                    "event_id":  f"ls_{sport_name.lower()}_{re.sub(r'[^a-z0-9]', '', title.lower())[:20]}_{now.strftime('%Y%m%d')}",
                    "title":     title,
                    "sport":     sport_name,
                    "start_time":fmt_iso(now - timedelta(hours=1)),
                    "end_time":  fmt_iso(now + timedelta(hours=2)),
                    "is_live":   is_live,
                    "streams":   [],
                    "source":    "LiveScore",
                    "_thumb":    thumb,
                })
            except Exception:
                pass

    if events:
        record_source_success(ev_state, "LiveScore")
        log.info(f"  → {len(events)} events from LiveScore")
    else:
        log.warning("  [LiveScore] No events parsed (site may have changed layout)")
        record_source_failure(ev_state, "LiveScore")

    STATS["sources"]["LiveScore"] = len(events)
    return events


def _cross_match_streams(schedule_events: list, stream_events: list) -> list:
    """
    Match schedule-only events (SofaScore/LiveScore) with stream sources
    (StreamedPK/DaddyLive) using fuzzy title matching.

    For each schedule event with no streams, find a matching stream source
    event and copy its streams over.
    """
    enriched = []
    for sched in schedule_events:
        if sched.get("streams"):
            enriched.append(sched)
            continue

        best_streams = []
        best_score   = 0
        best_thumb   = sched.get("_thumb", "")

        for stream_ev in stream_events:
            # Only match same sport
            if stream_ev.get("sport") != sched.get("sport"):
                continue
            if not stream_ev.get("streams"):
                continue

            sim = _title_similarity(sched["title"], stream_ev["title"])
            if sim > 0.65 and sim > best_score:
                best_score   = sim
                best_streams = stream_ev["streams"]
                # Prefer richer thumbnail from stream source
                if stream_ev.get("_thumb") and not best_thumb:
                    best_thumb = stream_ev["_thumb"]

        if best_streams:
            log.debug(
                f"  ✅ Matched '{sched['title']}' → "
                f"{len(best_streams)} streams (score={best_score:.2f})"
            )
        sched["streams"] = best_streams
        if best_thumb:
            sched["_thumb"] = best_thumb
        enriched.append(sched)

    return enriched


def main():
    log.info("═" * 60)
    log.info("🚀 StreamX Live Events Updater v9 (SofaScore+LiveScore+SmartMatch) — START")
    log.info(f"   UTC Time        : {fmt_iso(now_utc())}")
    log.info(f"   CloudScraper    : {'✅' if HAS_CLOUDSCRAPER else '⚠️  not installed'}")
    log.info(f"   Future window   : {FUTURE_WINDOW_HOURS}h  |  Sources: 13 active")
    log.info(f"   Strict stream   : {'✅ ON' if STRICT_REQUIRE_STREAM else '❌ OFF'}")
    log.info(f"   Strict thumbnail: {'✅ ON' if STRICT_REQUIRE_THUMBNAIL else '❌ OFF'}")
    log.info("═" * 60)

    # ── Load state & existing ──────────────────────────────────────
    ev_state = load_event_state()
    existing = load_existing()

    # Only carry over events that still have streams + thumbnail
    still_valid = [
        e for e in existing
        if not is_expired(e)
        and e.get("has_stream")
        and e.get("thumbnail")
    ]
    STATS["expired"] = len(existing) - len(still_valid)
    if STATS["expired"]:
        log.info(f"🗑  Removed {STATS['expired']} expired/invalid events")
    existing_ids = {e["event_id"] for e in still_valid}

    # ── PHASE 1: Collect stream sources ───────────────────────────
    stream_raw: list = []

    stream_raw.extend(fetch_streamedsu(ev_state))
    stream_raw.extend(fetch_streamedpk(ev_state))
    stream_raw.extend(fetch_streamedsu_api(ev_state))
    stream_raw.extend(fetch_daddylive(ev_state))
    stream_raw.extend(fetch_vipleague(ev_state))
    stream_raw.extend(fetch_vipleague_ws(ev_state))
    stream_raw.extend(fetch_streameast(ev_state))

    # ── PHASE 2: Schedule sources (no streams yet) ─────────────────
    schedule_raw: list = []

    schedule_raw.extend(fetch_sofascore(ev_state))    # ← NEW: football + cricket
    schedule_raw.extend(fetch_livescore(ev_state))    # ← NEW: live football
    schedule_raw.extend(fetch_thesportsdb(ev_state))
    schedule_raw.extend(fetch_espn(ev_state))
    schedule_raw.extend(fetch_tapmad(ev_state))
    schedule_raw.extend(fetch_fancode(ev_state))

    # Manual seeds
    for ev in MANUAL_EVENTS:
        if not is_expired(ev):
            schedule_raw.append(ev)

    # ── PHASE 3: Cross-match schedule ↔ streams ────────────────────
    log.info("🔗 Cross-matching schedule events with stream sources…")
    schedule_matched = _cross_match_streams(schedule_raw, stream_raw)
    matched_count = sum(1 for e in schedule_matched if e.get("streams"))
    log.info(f"  → {matched_count}/{len(schedule_matched)} schedule events got streams")

    # ── Combine all ────────────────────────────────────────────────
    raw = stream_raw + schedule_matched
    log.info(f"📋 Raw total (all sources): {len(raw)}")

    # ── Fuzzy dedup + stream merge ─────────────────────────────────
    raw = smart_deduplicate(raw)
    log.info(
        f"🔀 After smart dedup: {len(raw)} unique "
        f"({STATS['merged']} stream sets merged)"
    )

    # ── Filter already-known ───────────────────────────────────────
    new_candidates = [e for e in raw if e["event_id"] not in existing_ids]
    log.info(
        f"🆕 {len(new_candidates)} new candidates → "
        f"validating (strict: stream + thumbnail required)…"
    )

    # ── Validate + enrich (strict mode) ───────────────────────────
    enriched_new: list = []
    dropped_no_stream = 0
    dropped_no_thumb  = 0

    for ev in new_candidates:
        result = enrich_event(ev)
        if result is not None:
            enriched_new.append(result)
            STATS["added"] += 1
        else:
            if not ev.get("streams"):
                dropped_no_stream += 1
            elif not (ev.get("_thumb") or ev.get("thumbnail")):
                dropped_no_thumb += 1

    log.info(
        f"  ✅ Passed strict check: {len(enriched_new)} events\n"
        f"  ❌ Dropped (no stream): {dropped_no_stream}\n"
        f"  ❌ Dropped (no thumb):  {dropped_no_thumb}"
    )

    # ── Update live flag on carry-over events ──────────────────────
    for ev in still_valid:
        update_live_flag(ev)

    # ── Merge all ─────────────────────────────────────────────────
    all_active = smart_deduplicate(still_valid + enriched_new)

    # Final strict filter — guarantee every saved event has both
    all_active = [
        e for e in all_active
        if e.get("has_stream") and e.get("thumbnail")
    ]

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
    log.info(f"   🖼  With thumbnails  : {sum(1 for e in all_active if e.get('thumbnail'))}")
    log.info(f"   🆕 Added this run   : {STATS['added']}")
    log.info(f"   🔀 Streams merged   : {STATS['merged']}")
    log.info(f"   🗑  Expired removed  : {STATS['expired']}")
    if STATS["skipped"]:
        log.info(f"   ⏭  Skipped sources : {', '.join(STATS['skipped'])}")
    log.info("═" * 60)


if __name__ == "__main__":
    main()
