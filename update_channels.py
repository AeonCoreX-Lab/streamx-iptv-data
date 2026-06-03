"""
update_channels.py — StreamX IPTV Channel Auto-Updater v7
══════════════════════════════════════════════════════════
v7 Upgrades (on top of v6):
  9.  Gemini AI Category Classifier — gemini-1.5-flash (free tier)
      Groq fallback — llama-3.1-8b-instant (free tier)
      Rule-based fallback — works with no API key at all
      Classifies channels iptv-org marked as "general" or uncategorized
      Results cached in state → never re-classifies same channel
  10. Scheduled State Cleanup
      Permanently dead channels older than 30 days → purged from state
      State file stays lean across months of runs

OPTIONAL SECRETS (GitHub):
  TELEGRAM_BOT_TOKEN  — Telegram bot token
  TELEGRAM_CHAT_ID    — Telegram chat/channel id
  GEMINI_API_KEY      — Google AI Studio free key (15 RPM, 1M TPD)
  GROQ_API_KEY        — Groq free key (fallback when Gemini unavailable)

Run: python update_channels.py
"""

import json, os, time, shutil, logging, tempfile, random, hashlib, re
import glob
import requests
import concurrent.futures
from datetime import datetime
from urllib.parse import urlparse, quote

# ═══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
BASE_DIR     = os.getcwd()
CATEGORY_DIR = os.path.join(BASE_DIR, "categories")
BACKUP_DIR   = os.path.join(BASE_DIR, "backups")
REPORT_DIR   = os.path.join(BASE_DIR, "reports")
PLAYLIST_DIR = os.path.join(BASE_DIR, "playlists")
CACHE_DIR    = os.path.join(BASE_DIR, ".cache")
STATE_FILE   = os.path.join(BASE_DIR, "updater_state.json")
LOGO_MAP_FILE = os.path.join(BASE_DIR, "channel_logo_mapping.json")
LOGOS_DIR     = os.path.join(BASE_DIR, "logos")

MAX_BACKUPS_TO_KEEP     = 3
MAX_REPORTS_TO_KEEP     = 5
MAX_STREAMS_PER_CHANNEL = 3
API_CACHE_TTL_SECONDS   = 3600
STREAM_CHECK_WORKERS    = 20
LOGO_SEARCH_WORKERS     = 6
MAX_RUNTIME_SECONDS     = 5 * 3600 + 30 * 60
RECHECK_INTERVAL        = 12 * 3600
STATE_SAVE_EVERY        = 50

# Dead channel config (v6)
DEAD_FAIL_THRESHOLD     = 3
RECOVERY_CHECK_INTERVAL = 6 * 3600

# ── v7: AI Category Classifier ────────────────────────────────────────────────
# Gemini free tier: 15 RPM, 1M tokens/day — set GEMINI_API_KEY in GitHub Secrets
GEMINI_API_KEY   = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL     = "gemini-1.5-flash"
GEMINI_ENDPOINT  = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent"
)
# Groq free fallback: llama-3.1-8b-instant — set GROQ_API_KEY in GitHub Secrets
GROQ_API_KEY     = os.environ.get("GROQ_API_KEY", "")
GROQ_ENDPOINT    = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL       = "llama-3.1-8b-instant"

AI_BATCH_SIZE    = 15    # channels per API call (fits in one prompt)
AI_RPM_DELAY     = 4.5   # seconds between batches (stay under 15 RPM)
AI_MAX_RETRIES   = 2     # retries on transient error

# Categories the AI can assign — must match CATEGORY_RULES category_name values
AI_VALID_GENRES  = [
    "Sports", "News", "Entertainment", "Kids", "Music",
    "Informative", "Religious", "Lifestyle", "Business",
    "Weather", "Movies", "Other",
]

# ── v7: Scheduled State Cleanup ───────────────────────────────────────────────
# Permanently dead channels older than this → removed from state entirely
DEAD_CLEANUP_DAYS = 30

STREAMS_API  = "https://iptv-org.github.io/api/streams.json"
CHANNELS_API = "https://iptv-org.github.io/api/channels.json"
DEFAULT_LOGO = "https://raw.githubusercontent.com/iptv-org/api/master/data/categories/no-logo.png"

# ═══════════════════════════════════════════════════════════════════════════════
#  📂  CATEGORY RULES — v6: 30+ countries + 10 genres
#
#  type "country" → filter by ISO 3166-1 alpha-2 country code
#  type "genre"   → filter by iptv-org category names (list)
#  type "multi_country" → filter by list of country codes (regional groupings)
# ═══════════════════════════════════════════════════════════════════════════════
CATEGORY_RULES = {

    # ── Asia ──────────────────────────────────────────────────────────────────
    "bangladesh.json":   {"type": "country",       "filter": "BD",  "category_name": "Bangladesh"},
    "india.json":        {"type": "country",       "filter": "IN",  "category_name": "India"},
    "pakistan.json":     {"type": "country",       "filter": "PK",  "category_name": "Pakistan"},
    "indonesia.json":    {"type": "country",       "filter": "ID",  "category_name": "Indonesia"},
    "malaysia.json":     {"type": "country",       "filter": "MY",  "category_name": "Malaysia"},
    "japan.json":        {"type": "country",       "filter": "JP",  "category_name": "Japan"},
    "south_korea.json":  {"type": "country",       "filter": "KR",  "category_name": "South Korea"},
    "china.json":        {"type": "country",       "filter": "CN",  "category_name": "China"},
    "turkey.json":       {"type": "country",       "filter": "TR",  "category_name": "Turkey"},

    # ── Middle East ───────────────────────────────────────────────────────────
    "uae.json":          {"type": "country",       "filter": "AE",  "category_name": "UAE"},
    "saudi_arabia.json": {"type": "country",       "filter": "SA",  "category_name": "Saudi Arabia"},
    "egypt.json":        {"type": "country",       "filter": "EG",  "category_name": "Egypt"},
    "iran.json":         {"type": "country",       "filter": "IR",  "category_name": "Iran"},
    "iraq.json":         {"type": "country",       "filter": "IQ",  "category_name": "Iraq"},

    # ── Europe ────────────────────────────────────────────────────────────────
    "uk.json":           {"type": "country",       "filter": "GB",  "category_name": "UK"},
    "germany.json":      {"type": "country",       "filter": "DE",  "category_name": "Germany"},
    "france.json":       {"type": "country",       "filter": "FR",  "category_name": "France"},
    "italy.json":        {"type": "country",       "filter": "IT",  "category_name": "Italy"},
    "spain.json":        {"type": "country",       "filter": "ES",  "category_name": "Spain"},
    "netherlands.json":  {"type": "country",       "filter": "NL",  "category_name": "Netherlands"},
    "portugal.json":     {"type": "country",       "filter": "PT",  "category_name": "Portugal"},
    "russia.json":       {"type": "country",       "filter": "RU",  "category_name": "Russia"},
    "poland.json":       {"type": "country",       "filter": "PL",  "category_name": "Poland"},
    "ukraine.json":      {"type": "country",       "filter": "UA",  "category_name": "Ukraine"},

    # ── Americas ──────────────────────────────────────────────────────────────
    "usa.json":          {"type": "country",       "filter": "US",  "category_name": "USA"},
    "canada.json":       {"type": "country",       "filter": "CA",  "category_name": "Canada"},
    "brazil.json":       {"type": "country",       "filter": "BR",  "category_name": "Brazil"},
    "mexico.json":       {"type": "country",       "filter": "MX",  "category_name": "Mexico"},
    "argentina.json":    {"type": "country",       "filter": "AR",  "category_name": "Argentina"},
    "colombia.json":     {"type": "country",       "filter": "CO",  "category_name": "Colombia"},

    # ── Africa ────────────────────────────────────────────────────────────────
    "nigeria.json":      {"type": "country",       "filter": "NG",  "category_name": "Nigeria"},
    "kenya.json":        {"type": "country",       "filter": "KE",  "category_name": "Kenya"},
    "south_africa.json": {"type": "country",       "filter": "ZA",  "category_name": "South Africa"},
    "ghana.json":        {"type": "country",       "filter": "GH",  "category_name": "Ghana"},
    "ethiopia.json":     {"type": "country",       "filter": "ET",  "category_name": "Ethiopia"},

    # ── Oceania ───────────────────────────────────────────────────────────────
    "australia.json":    {"type": "country",       "filter": "AU",  "category_name": "Australia"},
    "new_zealand.json":  {"type": "country",       "filter": "NZ",  "category_name": "New Zealand"},

    # ── Genre categories ──────────────────────────────────────────────────────
    "sports.json":       {"type": "genre",  "filter": ["sports"],
                          "category_name": "Sports"},
    "news.json":         {"type": "genre",  "filter": ["news"],
                          "category_name": "News"},
    "entertainment.json":{"type": "genre",  "filter": ["entertainment", "general"],
                          "category_name": "Entertainment"},
    "kids.json":         {"type": "genre",  "filter": ["kids", "animation"],
                          "category_name": "Kids"},
    "music.json":        {"type": "genre",  "filter": ["music"],
                          "category_name": "Music"},
    "informative.json":  {"type": "genre",  "filter": ["documentary", "education", "science", "history"],
                          "category_name": "Informative"},
    "religious.json":    {"type": "genre",  "filter": ["religious"],
                          "category_name": "Religious"},
    "lifestyle.json":    {"type": "genre",  "filter": ["lifestyle", "fashion", "travel", "food", "cooking", "auto", "shop"],
                          "category_name": "Lifestyle"},
    "business.json":     {"type": "genre",  "filter": ["business", "finance"],
                          "category_name": "Business"},
    "weather.json":      {"type": "genre",  "filter": ["weather"],
                          "category_name": "Weather"},
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "VLC/3.0.20 LibVLC/3.0.20",
    "Kodi/20.0 (Linux; Android 12)",
]

STATS = {
    "checked": 0, "skipped_state": 0, "repaired": 0,
    "logo_fixed": 0, "added": 0, "removed_dead": 0,
    "recovered": 0,       # v6: channels recovered from dead
    "ai_classified": 0,   # v7: channels classified by AI
    "ai_fallback": 0,     # v7: channels classified by rule-based fallback
    "state_cleaned": 0,   # v7: stale dead entries purged from state
    "files_updated": 0, "m3u_generated": 0,
}

EPG_SOURCES = [
    "https://iptv-org.github.io/epg/guides/bd.xml",
    "https://iptv-org.github.io/epg/guides/in.xml",
    "https://iptv-org.github.io/epg/guides/us.xml",
    "https://iptv-org.github.io/epg/guides/gb.xml",
    "https://iptv-org.github.io/epg/guides/ae.xml",
    "https://iptv-org.github.io/epg/guides/pk.xml",
    "https://iptv-org.github.io/epg/guides/au.xml",
    "https://iptv-org.github.io/epg/guides/de.xml",
    "https://iptv-org.github.io/epg/guides/fr.xml",
    "https://iptv-org.github.io/epg/guides/it.xml",
    "https://iptv-org.github.io/epg/guides/es.xml",
    "https://iptv-org.github.io/epg/guides/br.xml",
    "https://iptv-org.github.io/epg/guides/tr.xml",
    "https://iptv-org.github.io/epg/guides/sa.xml",
]

# ═══════════════════════════════════════════════════════════════════════════════
#  📝  LOGGING
# ═══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger()
START_TIME = time.time()

# ═══════════════════════════════════════════════════════════════════════════════
#  🛠️  UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def time_remaining() -> float:
    return MAX_RUNTIME_SECONDS - (time.time() - START_TIME)

def safe_str(v, default="") -> str:
    return str(v).strip() if v is not None else default

def _headers() -> dict:
    return {"User-Agent": random.choice(USER_AGENTS)}

# ═══════════════════════════════════════════════════════════════════════════════
#  💾  STATE
#  Schema per channel_id (v6):
#  {
#    "stream_ok":          true | false,
#    "last_checked":       <epoch>,
#    "fail_count":         <int>,
#    "pass_count":         <int>,
#    "health_score":       <float 0-1>,
#    "quality":            "HD"|"SD"|...,
#    "logo_searched":      true | false,
#    "permanently_dead":   true | false,   ← v6: removed after fail ≥ 3
#    "dead_since":         <epoch>,        ← v6: when it became permanently dead
#    "dead_stream_url":    <str>,          ← v6: the URL that failed (to detect new URLs)
#    "last_recovery_check":<epoch>,        ← v6: last time we checked iptv-org for recovery
#    "category":           <str>,          ← v6: which category it belongs to (for re-add)
#    "country":            <str>,          ← v6: country code (for re-add)
#  }
# ═══════════════════════════════════════════════════════════════════════════════

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_state(state: dict):
    with tempfile.NamedTemporaryFile('w', delete=False,
                                     suffix='.tmp', encoding='utf-8') as f:
        json.dump(state, f)
        tmp = f.name
    shutil.move(tmp, STATE_FILE)

# ═══════════════════════════════════════════════════════════════════════════════
#  🌐  CACHED API FETCH
# ═══════════════════════════════════════════════════════════════════════════════
os.makedirs(CACHE_DIR, exist_ok=True)

def _cache_path(url: str) -> str:
    key = hashlib.md5(url.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{key}.json")

def fetch_json_cached(url: str):
    path = _cache_path(url)
    if os.path.exists(path):
        age = time.time() - os.path.getmtime(path)
        if age < API_CACHE_TTL_SECONDS:
            logger.info(f"📦 Cache hit ({int(age)}s old): {url.split('/')[-1]}")
            with open(path, encoding="utf-8") as f:
                return json.load(f)
    logger.info(f"📡 Fetching: {url}")
    r = requests.get(url, timeout=60, headers=_headers())
    r.raise_for_status()
    data = r.json()
    with open(path, 'w', encoding="utf-8") as f:
        json.dump(data, f)
    return data

# ═══════════════════════════════════════════════════════════════════════════════
#  ✅  STREAM VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════
VIDEO_CONTENT_TYPES = {
    'video/',
    'application/x-mpegurl',
    'application/vnd.apple.mpegurl',
    'application/octet-stream',
    'audio/mpegurl',
    'audio/x-mpegurl',
}

def is_valid_stream(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    if not url.startswith(('http://', 'https://', 'rtmp://', 'rtsp://')):
        return False
    if url.startswith(('rtmp://', 'rtsp://')):
        return True
    try:
        with requests.get(
            url, headers=_headers(), stream=True,
            timeout=(5, 8), allow_redirects=True
        ) as r:
            if r.status_code not in (200, 206):
                return False
            ct = r.headers.get('Content-Type', '').lower()
            if any(ct.startswith(v) for v in VIDEO_CONTENT_TYPES):
                return True
            if any(url.lower().endswith(ext) for ext in ('.m3u8', '.ts', '.mp4', '.mpd')):
                chunk = next(r.iter_content(512), None)
                return chunk is not None and len(chunk) > 0
            return False
    except Exception:
        return False

def get_working_streams(channel_id: str, streams_by_id: dict) -> list:
    candidates = streams_by_id.get(channel_id, [])[:8]
    if not candidates:
        return []
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=STREAM_CHECK_WORKERS) as ex:
        future_map = {
            ex.submit(is_valid_stream, s.get('url', '')): s.get('url', '')
            for s in candidates
        }
        for future in concurrent.futures.as_completed(future_map):
            url = future_map[future]
            try:
                if future.result() and url:
                    results.append(url)
                    if len(results) >= MAX_STREAMS_PER_CHANNEL:
                        for f in future_map:
                            f.cancel()
                        break
            except Exception:
                pass
    return results

# ═══════════════════════════════════════════════════════════════════════════════
#  🤖  v7: AI-POWERED CATEGORY CLASSIFIER
#
#  Priority chain:
#    1. Gemini 1.5 Flash (free: 15 RPM, 1M tokens/day) — GEMINI_API_KEY
#    2. Groq llama-3.1-8b-instant (free) — GROQ_API_KEY
#    3. Rule-based keyword classifier — always works, no API needed
#
#  When used:
#    - New channel with iptv-org category = "general" or empty
#    - Channel not yet in state["ai_category"]
#    - Batched in groups of AI_BATCH_SIZE to minimise API calls
#
#  Result stored in state: state[channel_id]["ai_category"] = "Sports"
#  Never re-classifies same channel → free tier limits respected
# ═══════════════════════════════════════════════════════════════════════════════

# Rule-based fallback — comprehensive keyword → genre map
_RULE_KEYWORDS: list = [
    # Sports
    (["sport", "espn", "fox sport", "sky sport", "dazn", "bein", "eurosport",
      "nba", "nfl", "nhl", "mlb", "nascar", "ufc", "tennis", "golf",
      "cricket", "football", "soccer", "rugby", "boxing", "wrestling",
      "olympic", "racing", "motorsport", "f1", "serie a", "laliga",
      "premier league", "bundesliga", "ligue 1", "champions", "supersport",
      "willow", "tsn", "sportsnet"],                     "Sports"),
    # News
    (["news", "cnn", "bbc", "al jazeera", "france 24", "dw news", "euronews",
      "sky news", "fox news", "msnbc", "abc news", "nbc news", "cbs news",
      "i24", "times now", "ndtv", "wion", "cgtn", "trt world",
      "channel news", "republic", "mirror now"],         "News"),
    # Kids
    (["cartoon", "nickelodeon", "nick jr", "disney", "boomerang", "toonami",
      "baby tv", "kidz", "kids", "junior", "junior channel", "children",
      "cbeebies", "pbs kids", "boing", "duck tv", "tiny pop"],   "Kids"),
    # Music
    (["music", "mtv", "vh1", "vevo", "hits", "radio", "fm ", "sound",
      "melody", "beat", "channel v", "9x", "zee music", "colors music",
      "star vijay music", "b4u music", "magic music"],   "Music"),
    # Religious
    (["islam", "islamic", "quran", "muslim", "peace tv", "hidayat",
      "madani", "noor", "faith", "christian", "gospel", "church", "bible",
      "prayer", "god", "divine", "trinity", "sanatan", "dharma",
      "hindu", "jain", "sikh"],                          "Religious"),
    # Informative
    (["documentary", "national geographic", "nat geo", "discovery",
      "history", "science", "nature", "animal planet", "earth",
      "exploration", "knowledge", "learn", "education", "edutainment",
      "smithsonian", "dw dokument"],                     "Informative"),
    # Business
    (["business", "cnbc", "bloomberg", "finance", "money", "economy",
      "market", "trade", "investing", "stock"],          "Business"),
    # Lifestyle
    (["lifestyle", "fashion", "travel", "cooking", "food", "recipe",
      "home", "diy", "health", "wellness", "beauty", "style",
      "living", "tlc", "e! entertainment", "hgtv", "food network",
      "travel channel", "bravo"],                        "Lifestyle"),
    # Weather
    (["weather", "meteo", "clima", "accuweather", "the weather"],  "Weather"),
    # Entertainment / general TV
    (["entertainment", "general", "comedy", "drama", "movie", "film",
      "series", "show", "zee", "star plus", "colors", "sony",
      "channel one", "tv1", "tv2", "tv3", "tv4", "tv5",
      "canal", "tele", "prime", "max", "hbo"],           "Entertainment"),
]

def _rule_classify(name: str, iptv_cats: list) -> str:
    """Fast keyword-based classifier — no API needed."""
    text = (name + " " + " ".join(iptv_cats)).lower()
    for keywords, genre in _RULE_KEYWORDS:
        if any(kw in text for kw in keywords):
            return genre
    return "Entertainment"   # safe default


def _build_ai_prompt(batch: list) -> str:
    """
    Build a compact prompt for Gemini / Groq.
    batch = [{"idx": 1, "name": "ESPN", "country": "US", "cats": ["sports"]}]
    """
    valid = ", ".join(AI_VALID_GENRES)
    lines = [
        f"Classify each TV channel into exactly ONE of: {valid}",
        "Return ONLY a JSON object like {\"1\": \"Sports\", \"2\": \"News\", ...}",
        "No explanation, no markdown, just the JSON.\n",
        "Channels (index | name | country | iptv-org hint):",
    ]
    for item in batch:
        hint = ", ".join(item["cats"]) if item["cats"] else "unknown"
        lines.append(f"{item['idx']} | {item['name']} | {item['country']} | {hint}")
    return "\n".join(lines)


def _call_gemini(prompt: str) -> dict:
    """Call Gemini 1.5 Flash API. Returns {idx_str: genre} or {}."""
    if not GEMINI_API_KEY:
        return {}
    try:
        r = requests.post(
            GEMINI_ENDPOINT,
            params={"key": GEMINI_API_KEY},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.1,
                    "maxOutputTokens": 256,
                },
            },
            timeout=20,
        )
        if r.status_code == 429:
            logger.warning("  🤖 Gemini rate-limited — waiting 60s")
            time.sleep(60)
            return {}
        if r.status_code != 200:
            logger.debug(f"  🤖 Gemini error {r.status_code}: {r.text[:100]}")
            return {}
        text = (
            r.json()
            .get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
            .strip()
        )
        # Strip markdown code fences if present
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip())
        return json.loads(text)
    except Exception as e:
        logger.debug(f"  🤖 Gemini parse error: {e}")
        return {}


def _call_groq(prompt: str) -> dict:
    """Call Groq llama-3.1-8b-instant API. Returns {idx_str: genre} or {}."""
    if not GROQ_API_KEY:
        return {}
    try:
        r = requests.post(
            GROQ_ENDPOINT,
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":    GROQ_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens":  256,
            },
            timeout=20,
        )
        if r.status_code == 429:
            logger.warning("  🤖 Groq rate-limited — waiting 30s")
            time.sleep(30)
            return {}
        if r.status_code != 200:
            logger.debug(f"  🤖 Groq error {r.status_code}: {r.text[:100]}")
            return {}
        text = (
            r.json()
            .get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip())
        return json.loads(text)
    except Exception as e:
        logger.debug(f"  🤖 Groq parse error: {e}")
        return {}


def ai_classify_batch(channels: list, state: dict) -> dict:
    """
    Classify a list of channels using AI (Gemini → Groq → rule-based).

    channels = [{"id": ..., "name": ..., "country": ..., "cats": [...]}]
    Returns {channel_id: genre_string}
    """
    if not channels:
        return {}

    results: dict = {}

    # Split into batches of AI_BATCH_SIZE
    for batch_start in range(0, len(channels), AI_BATCH_SIZE):
        batch     = channels[batch_start: batch_start + AI_BATCH_SIZE]
        indexed   = [
            {"idx": i + 1, **ch}
            for i, ch in enumerate(batch)
        ]
        prompt    = _build_ai_prompt(indexed)
        api_result: dict = {}

        # Try Gemini first
        if GEMINI_API_KEY:
            for attempt in range(AI_MAX_RETRIES):
                api_result = _call_gemini(prompt)
                if api_result:
                    break
                time.sleep(2)
            if api_result:
                logger.info(
                    f"  🤖 Gemini classified {len(api_result)}/{len(indexed)} channels"
                )

        # Groq fallback
        if not api_result and GROQ_API_KEY:
            for attempt in range(AI_MAX_RETRIES):
                api_result = _call_groq(prompt)
                if api_result:
                    break
                time.sleep(2)
            if api_result:
                logger.info(
                    f"  🤖 Groq classified {len(api_result)}/{len(indexed)} channels"
                )

        # Map results back to channel IDs
        for item in indexed:
            cid   = item["id"]
            genre = api_result.get(str(item["idx"]), "")
            # Validate — must be a known genre
            if genre not in AI_VALID_GENRES:
                genre = ""
            if genre:
                results[cid] = genre
                STATS["ai_classified"] += 1
            else:
                # Rule-based fallback for this channel
                results[cid] = _rule_classify(item["name"], item["cats"])
                STATS["ai_fallback"] += 1

        # Rate limit delay between batches
        if batch_start + AI_BATCH_SIZE < len(channels):
            time.sleep(AI_RPM_DELAY)

    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  🧹  v7: SCHEDULED STATE CLEANUP
#
#  Permanently dead channels accumulate in state.json over time.
#  After DEAD_CLEANUP_DAYS (30 days), they're purged entirely.
#  This keeps the state file lean and speeds up loads.
# ═══════════════════════════════════════════════════════════════════════════════

def cleanup_stale_dead_state(state: dict) -> int:
    """
    Remove permanently_dead channels older than DEAD_CLEANUP_DAYS from state.
    Called once per run before main processing.
    Returns number of entries removed.
    """
    now       = time.time()
    threshold = DEAD_CLEANUP_DAYS * 24 * 3600
    to_remove = [
        cid for cid, s in state.items()
        if s.get("permanently_dead")
        and (now - s.get("dead_since", now)) > threshold
    ]
    for cid in to_remove:
        del state[cid]
    if to_remove:
        logger.info(
            f"🧹 State cleanup: removed {len(to_remove)} entries "
            f"dead >{DEAD_CLEANUP_DAYS} days"
        )
    STATS["state_cleaned"] = len(to_remove)
    return len(to_remove)


# ═══════════════════════════════════════════════════════════════════════════════
#
#  Logic:
#    1. Channel fails DEAD_FAIL_THRESHOLD times → permanently_dead = True
#    2. Removed from categories JSON permanently
#    3. Every RECOVERY_CHECK_INTERVAL: check iptv-org for this channel_id
#    4. If iptv-org has a DIFFERENT URL (new stream) → validate it
#    5. If valid → re-add to correct category JSON with fresh state
# ═══════════════════════════════════════════════════════════════════════════════

def check_dead_channel_recovery(
    state: dict,
    streams_by_id: dict,
    channel_info_map: dict,
    logo_mapping: dict,
) -> list:
    """
    Scan permanently_dead channels for new streams in iptv-org.
    Returns list of recovered channel dicts ready to be re-added.
    """
    now         = time.time()
    recovered   = []
    dead_ids    = [
        cid for cid, s in state.items()
        if s.get("permanently_dead") and
           (now - s.get("last_recovery_check", 0)) > RECOVERY_CHECK_INTERVAL
    ]

    if not dead_ids:
        return []

    logger.info(f"🔍 Checking {len(dead_ids)} permanently dead channels for recovery…")

    def _check_one(cid: str):
        ch_state   = state.get(cid, {})
        old_url    = ch_state.get("dead_stream_url", "")
        api_streams = streams_by_id.get(cid, [])

        # Update recovery check timestamp regardless of outcome
        state[cid] = {**state.get(cid, {}), "last_recovery_check": now}

        if not api_streams:
            return None  # iptv-org still has no streams for this channel

        # Check if iptv-org has a NEW URL (different from the one that died)
        new_urls = [
            s["url"] for s in api_streams
            if s.get("url", "").strip() and s["url"].strip() != old_url
        ]
        if not new_urls:
            return None  # same dead URL, no recovery

        # Validate the new URLs
        for url in new_urls[:3]:
            if is_valid_stream(url):
                details = channel_info_map.get(cid, {})
                logo    = get_channel_logo(
                    cid, details.get("name", ""), details.get("website", ""),
                    logo_mapping
                )
                quality = detect_quality(url)
                logger.info(f"  🟢 RECOVERED: {details.get('name', cid)} — new URL found!")
                return {
                    "id":         safe_str(details.get("id", cid)),
                    "name":       safe_str(details.get("name"), "Unknown Channel"),
                    "logoUrl":    safe_str(logo, DEFAULT_LOGO),
                    "streamUrls": [url],
                    "country":    safe_str(details.get("country", "")),
                    "category":   ch_state.get("category", ""),
                    "languages":  details.get("languages", []),
                    "_recovered_quality": quality,
                }
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_check_one, cid): cid for cid in dead_ids}
        for future in concurrent.futures.as_completed(futures, timeout=120):
            try:
                result = future.result()
                if result:
                    recovered.append(result)
            except Exception:
                pass

    if recovered:
        logger.info(f"  ✅ {len(recovered)} channels recovered from dead!")
    return recovered


# ═══════════════════════════════════════════════════════════════════════════════
#  📊  HEALTH SCORING
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_score(passes: int, fails: int) -> float:
    total = passes + fails
    if total == 0:
        return 0.5
    return round(passes / total, 2)

# ═══════════════════════════════════════════════════════════════════════════════
#  🔍  QUALITY DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

_QUALITY_ORDER = [
    (2160, "4K"),
    (1080, "FHD"),
    (720,  "HD"),
    (480,  "SD"),
    (0,    "SD"),
]

def detect_quality(url: str) -> str:
    if not url or not url.startswith("http"):
        return "Unknown"
    try:
        with requests.get(url, stream=True, timeout=(4, 6), headers=_headers()) as r:
            if r.status_code in (200, 206):
                content = b""
                for chunk in r.iter_content(2048):
                    content += chunk
                    if len(content) >= 2048:
                        break
                text = content.decode("utf-8", errors="ignore")
                m = re.search(r"RESOLUTION=\d+x(\d+)", text)
                if m:
                    height = int(m.group(1))
                    for res, label in _QUALITY_ORDER:
                        if height >= res:
                            return label
    except Exception:
        pass
    url_lower = url.lower()
    for pattern, label in [
        ("2160", "4K"), ("4k", "4K"),
        ("1080", "FHD"), ("fhd", "FHD"), ("fullhd", "FHD"),
        ("720",  "HD"),  ("hd",  "HD"),
        ("480",  "SD"),  ("360", "SD"), ("sd", "SD"),
    ]:
        if pattern in url_lower:
            return label
    return "Unknown"

# ═══════════════════════════════════════════════════════════════════════════════
#  🖼️  LOGO RESOLUTION — v6 Priority:
#    1. logos/{country_code}/{channel_id}.png  (local folder, exact match)
#    2. logos/{any_folder}/{channel_id}.png    (any subfolder scan)
#    3. channel_logo_mapping.json              (pre-built mapping)
#    4. iptv-org API logo field
#    5. Online: Clearbit → Wikimedia → Google Favicon
#    6. Default placeholder
# ═══════════════════════════════════════════════════════════════════════════════

def load_logo_mapping() -> dict:
    """
    Load logo mapping from JSON + scan logos/ folder directly.
    Returns flat dict: {channel_id: logo_url_or_path}
    """
    mapping = {}

    # Load from JSON mapping file
    if os.path.exists(LOGO_MAP_FILE):
        try:
            with open(LOGO_MAP_FILE, encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict) and 'flat' in data:
                mapping = data.get('flat', {})
            elif isinstance(data, dict):
                mapping = data
            logger.info(f"🗺️  Logo mapping: {len(mapping)} from JSON")
        except Exception as e:
            logger.warning(f"⚠️  Logo mapping load failed: {e}")

    # v6: Scan logos/ folder directly — channel_id = filename without extension
    # Structure: logos/{country_code}/{channel_id}.png
    #            logos/{channel_id}.png  (flat fallback)
    logos_from_folder = 0
    if os.path.exists(LOGOS_DIR):
        for entry in os.listdir(LOGOS_DIR):
            entry_path = os.path.join(LOGOS_DIR, entry)
            if os.path.isdir(entry_path):
                # Country subfolder: logos/{country}/{channel_id}.png
                for fname in os.listdir(entry_path):
                    if fname.lower().endswith('.png'):
                        cid      = fname[:-4]  # remove .png
                        rel_path = f"logos/{entry}/{fname}"
                        if cid not in mapping:  # don't override JSON mapping
                            mapping[cid] = rel_path
                            logos_from_folder += 1
            elif entry.lower().endswith('.png'):
                # Flat: logos/{channel_id}.png
                cid = entry[:-4]
                if cid not in mapping:
                    mapping[cid] = f"logos/{entry}"
                    logos_from_folder += 1

    if logos_from_folder:
        logger.info(f"🗺️  Logo folder scan: +{logos_from_folder} logos (total: {len(mapping)})")

    return mapping


def _logo_raw_to_url(raw_path: str) -> str:
    """Convert relative logos/ path to raw GitHub URL."""
    if raw_path.startswith("http"):
        return raw_path
    if raw_path.startswith("logos/"):
        repo = "AeonCoreX-Lab/streamx-iptv-data"
        return f"https://raw.githubusercontent.com/{repo}/main/{raw_path}"
    return raw_path


def get_channel_logo(
    cid: str,
    name: str,
    website: str = "",
    mapping: dict = None,
    api_logo: str = "",
    country: str = "",
) -> str:
    """
    v6 Smart logo resolver with full priority chain.
    """
    if mapping is None:
        mapping = load_logo_mapping()

    # Priority 1: logos/{country}/{channel_id}.png — exact match in country folder
    if cid and country and os.path.exists(LOGOS_DIR):
        country_lower = country.lower()
        for subdir in [country_lower, country_lower.upper(), country]:
            local = os.path.join(LOGOS_DIR, subdir, f"{cid}.png")
            if os.path.exists(local):
                return _logo_raw_to_url(f"logos/{subdir}/{cid}.png")

    # Priority 2: logos/ any subfolder — channel_id match
    if cid and cid in mapping:
        return _logo_raw_to_url(mapping[cid])

    # Priority 3: iptv-org API logo field
    if api_logo and api_logo.startswith("http") and api_logo != DEFAULT_LOGO:
        return api_logo

    # Priority 4: Online search
    online = _find_logo_online(name, website)
    if online:
        return online

    return DEFAULT_LOGO


def _find_logo_online(name: str, website: str = "") -> str:
    logo = _try_clearbit(name, website)
    if logo:
        return logo
    logo = _try_wikimedia(name)
    if logo:
        return logo
    logo = _try_google_favicon(website)
    if logo:
        return logo
    return ""


def _clean_slug(name: str) -> str:
    s = name.lower()
    s = re.sub(r'\b(hd|sd|\+\d|[0-9]+k|channel|tv|network|media|plus|premier|news)\b', '', s)
    s = re.sub(r'[^a-z0-9]', '', s)
    return s.strip()


def _try_clearbit(name: str, website: str = "") -> str:
    domains = []
    if website:
        parsed = urlparse(website).netloc
        if parsed:
            domains.append(parsed.replace('www.', ''))
    slug = _clean_slug(name)
    if slug:
        domains += [f"{slug}.com", f"{slug}tv.com", f"watch{slug}.com"]
    seen = set()
    for domain in domains:
        if domain in seen or not domain:
            continue
        seen.add(domain)
        url = f"https://logo.clearbit.com/{domain}"
        try:
            r = requests.get(url, headers=_headers(), timeout=6, allow_redirects=True)
            if r.status_code == 200 and 'image' in r.headers.get('Content-Type', ''):
                return url
        except Exception:
            pass
    return ""


def _try_wikimedia(name: str) -> str:
    try:
        base = "https://en.wikipedia.org/w/api.php"
        r = requests.get(base, params={
            "action": "query", "list": "search",
            "srsearch": f"{name} TV channel", "srlimit": 1, "format": "json"
        }, headers=_headers(), timeout=7)
        results = r.json().get("query", {}).get("search", [])
        if not results:
            return ""
        title = results[0]["title"]
        r2 = requests.get(base, params={
            "action": "query", "titles": title,
            "prop": "pageimages", "pithumbsize": 300, "format": "json"
        }, headers=_headers(), timeout=7)
        pages = r2.json().get("query", {}).get("pages", {})
        for page in pages.values():
            thumb = page.get("thumbnail", {}).get("source", "")
            if thumb:
                return thumb
    except Exception:
        pass
    return ""


def _try_google_favicon(website: str) -> str:
    if not website:
        return ""
    domain = urlparse(website).netloc.replace('www.', '')
    if not domain:
        return ""
    url = f"https://www.google.com/s2/favicons?sz=128&domain={domain}"
    try:
        r = requests.get(url, headers=_headers(), timeout=5)
        if r.status_code == 200 and len(r.content) > 500:
            return url
    except Exception:
        pass
    return ""

# ═══════════════════════════════════════════════════════════════════════════════
#  🧠  SMART PRIORITY QUEUE
# ═══════════════════════════════════════════════════════════════════════════════

def prioritize_channels(channels: list, state: dict) -> tuple:
    now = time.time()
    buckets = {0: [], 1: [], 2: []}
    skipped = 0
    for ch in channels:
        cid      = ch.get('id', '')
        ch_state = state.get(cid, {})
        last_chk = ch_state.get('last_checked', 0)
        age      = now - last_chk
        if not ch_state:
            buckets[0].append(ch)
        elif not ch_state.get('stream_ok', True):
            buckets[1].append(ch)
        elif age > RECHECK_INTERVAL:
            buckets[2].append(ch)
        else:
            skipped += 1
    ordered = buckets[0] + buckets[1] + buckets[2]
    logger.info(
        f"  📋 Queue → new: {len(buckets[0])} | "
        f"was-dead: {len(buckets[1])} | "
        f"recheck: {len(buckets[2])} | "
        f"skip (healthy): {skipped}"
    )
    return ordered, skipped

# ═══════════════════════════════════════════════════════════════════════════════
#  🗄️  FILE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def cleanup_old(directory: str, prefix: str, suffix: str, keep: int):
    if not os.path.exists(directory):
        return
    files = sorted(
        f for f in os.listdir(directory)
        if f.startswith(prefix) and f.endswith(suffix)
    )
    for old in files[:-keep] if keep > 0 else files:
        try:
            os.remove(os.path.join(directory, old))
        except Exception:
            pass

def create_backup(filepath: str):
    if not os.path.exists(filepath):
        return
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(BACKUP_DIR, f"{os.path.basename(filepath)}_{ts}.bak")
    try:
        shutil.copy2(filepath, dest)
    except Exception as e:
        logger.warning(f"⚠️  Backup failed: {e}")

def atomic_save_json(filepath: str, data: dict):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with tempfile.NamedTemporaryFile(
        'w', dir=os.path.dirname(filepath),
        delete=False, encoding='utf-8'
    ) as tmp:
        json.dump(data, tmp, indent=2, ensure_ascii=False)
        tmp_name = tmp.name
    shutil.move(tmp_name, filepath)
    logger.info(f"💾 Saved: {os.path.basename(filepath)}")
    STATS["files_updated"] += 1

def load_json(filepath: str) -> dict:
    if os.path.exists(filepath):
        try:
            with open(filepath, encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {"channels": []}

# ═══════════════════════════════════════════════════════════════════════════════
#  🎵  M3U GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def generate_epg_playlist_header() -> str:
    url_attrs = " ".join(f'url-tvg="{u}"' for u in EPG_SOURCES)
    return f'#EXTM3U {url_attrs} refresh="3600"\n'

def _build_m3u(channels: list, state: dict = None) -> str:
    if state:
        channels = sorted(
            channels,
            key=lambda ch: state.get(ch.get('id', ''), {}).get('health_score', 0.5),
            reverse=True,
        )
    lines = [generate_epg_playlist_header().strip()]
    for ch in channels:
        urls = [u for u in (ch.get('streamUrls') or []) if u and isinstance(u, str) and u.strip()]
        if not urls:
            continue
        name    = safe_str(ch.get('name'),     'Unknown')
        logo    = safe_str(ch.get('logoUrl'),  '')
        cid     = safe_str(ch.get('id'),       '')
        group   = safe_str(ch.get('category'), 'Uncategorized')
        quality = ""
        if state and cid:
            quality = state.get(cid, {}).get("quality", "")
        if quality and quality not in ("Unknown", ""):
            group_tagged = f"{group} [{quality}]"
            name_tagged  = f"{name} [{quality}]"
        else:
            group_tagged = group
            name_tagged  = name
        lines.append(
            f'#EXTINF:-1 tvg-id="{cid}" tvg-logo="{logo}" '
            f'group-title="{group_tagged}",{name_tagged}'
        )
        lines.append(urls[0].strip())
    return '\n'.join(lines)

def generate_m3u(json_data: dict, filename: str, state: dict = None):
    os.makedirs(PLAYLIST_DIR, exist_ok=True)
    path = os.path.join(PLAYLIST_DIR, filename.replace(".json", ".m3u"))
    try:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(_build_m3u(json_data.get('channels', []), state))
        logger.info(f"🎵 M3U: {filename.replace('.json', '.m3u')}")
        STATS["m3u_generated"] += 1
    except Exception as e:
        logger.error(f"❌ M3U failed ({filename}): {e}")

def generate_master_m3u(all_channels: list, state: dict = None):
    os.makedirs(PLAYLIST_DIR, exist_ok=True)
    path = os.path.join(PLAYLIST_DIR, "all_channels.m3u")
    try:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(_build_m3u(all_channels, state))
        logger.info(f"🌟 Master M3U: {len(all_channels)} channels")
    except Exception as e:
        logger.error(f"❌ Master M3U failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
#  📄  REPORT
# ═══════════════════════════════════════════════════════════════════════════════

def write_report(state: dict):
    os.makedirs(REPORT_DIR, exist_ok=True)
    cleanup_old(REPORT_DIR, "report_", ".txt", MAX_REPORTS_TO_KEEP)
    elapsed       = int(time.time() - START_TIME)
    ts            = datetime.now().strftime("%Y-%m-%d_%H-%M")
    path          = os.path.join(REPORT_DIR, f"report_{ts}.txt")
    total_healthy = sum(1 for v in state.values() if v.get('stream_ok'))
    total_dead    = sum(1 for v in state.values() if not v.get('stream_ok'))
    total_perm    = sum(1 for v in state.values() if v.get('permanently_dead'))
    scores        = [v.get("health_score", 0) for v in state.values()]
    avg_score     = round(sum(scores) / len(scores), 2) if scores else 0.0
    excellent     = sum(1 for s in scores if s >= 0.9)
    good          = sum(1 for s in scores if 0.7 <= s < 0.9)
    poor          = sum(1 for s in scores if s < 0.7)
    quality_counts: dict = {}
    for v in state.values():
        q = v.get("quality", "Unknown")
        quality_counts[q] = quality_counts.get(q, 0) + 1
    quality_lines = "   ".join(f"{q}: {c}" for q, c in sorted(quality_counts.items()))
    content = f"""
╔══════════════════════════════════════════════╗
║      IPTV AUTO-UPDATER  —  RUN REPORT v7     ║
║      {datetime.now().strftime("%Y-%m-%d  %H:%M:%S")}                   ║
╚══════════════════════════════════════════════╝

⏱  Runtime              : {elapsed // 3600}h {(elapsed % 3600) // 60}m {elapsed % 60}s
─────────────────────────────────────────────
✅  Channels checked     : {STATS['checked']}
⏭  Skipped (recent)     : {STATS['skipped_state']}
🩹  Streams repaired     : {STATS['repaired']}
🗑  Dead removed (perm)  : {STATS['removed_dead']}
🟢  Recovered from dead  : {STATS['recovered']}
🖼  Logos fixed          : {STATS['logo_fixed']}
🆕  New channels added   : {STATS['added']}
─────────────────────────────────────────────
🤖  AI classified        : {STATS['ai_classified']} (Gemini/Groq)
📐  Rule-based fallback  : {STATS['ai_fallback']}
🧹  State entries purged : {STATS['state_cleaned']}
─────────────────────────────────────────────
💾  JSON files saved     : {STATS['files_updated']}
🎵  M3U files created    : {STATS['m3u_generated']} + 1 master
─────────────────────────────────────────────
📦  State summary:
   ✅ Healthy            : {total_healthy}
   ❌ Dead (temp)        : {total_dead - total_perm}
   🪦 Permanently dead   : {total_perm}
─────────────────────────────────────────────
📊  Health Score (avg)   : {avg_score}
   ⭐ Excellent (≥0.9)   : {excellent}
   👍 Good (0.7-0.9)     : {good}
   ⚠️  Poor (<0.7)        : {poor}
─────────────────────────────────────────────
🔍  Quality Breakdown    : {quality_lines or 'n/a'}
═══════════════════════════════════════════════
"""
    try:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
    except Exception as e:
        logger.error(f"❌ Report write failed: {e}")
    print(content)

# ═══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM NOTIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

def send_telegram_report(state: dict):
    token   = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    elapsed       = int(time.time() - START_TIME)
    total_healthy = sum(1 for v in state.values() if v.get('stream_ok'))
    total_dead    = sum(1 for v in state.values() if not v.get('stream_ok'))
    total_perm    = sum(1 for v in state.values() if v.get('permanently_dead'))
    scores        = [v.get("health_score", 0) for v in state.values()]
    avg_score     = round(sum(scores) / len(scores), 2) if scores else 0.0
    ai_mode = "Gemini" if GEMINI_API_KEY else ("Groq" if GROQ_API_KEY else "Rules")
    msg = (
        f"📺 *IPTV Auto-Updater v7 Report*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Runtime: `{elapsed // 3600}h {(elapsed % 3600) // 60}m`\n"
        f"✅ Healthy: `{total_healthy}`\n"
        f"❌ Dead: `{total_dead - total_perm}`\n"
        f"🪦 Permanently dead: `{total_perm}`\n"
        f"🟢 Recovered: `{STATS['recovered']}`\n"
        f"🆕 Added: `{STATS['added']}`\n"
        f"🩹 Repaired: `{STATS['repaired']}`\n"
        f"🗑 Removed: `{STATS['removed_dead']}`\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 AI classified ({ai_mode}): `{STATS['ai_classified']}`\n"
        f"📐 Rule-based: `{STATS['ai_fallback']}`\n"
        f"🧹 State cleaned: `{STATS['state_cleaned']}`\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Avg health: `{avg_score}`\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
        logger.info("📱 Telegram notification sent")
    except Exception as e:
        logger.warning(f"  Telegram failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
#  🌐  DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════

def generate_dashboard(all_channels: list, state: dict):
    total      = len(all_channels)
    healthy    = sum(1 for v in state.values() if v.get('stream_ok'))
    dead       = sum(1 for v in state.values() if not v.get('stream_ok'))
    perm_dead  = sum(1 for v in state.values() if v.get('permanently_dead'))
    cat_counts: dict = {}
    for ch in all_channels:
        cat = ch.get('category', 'Unknown')
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    cat_rows = "\n".join(
        f'<tr><td>{cat}</td><td>{count}</td></tr>'
        for cat, count in sorted(cat_counts.items())
    )
    quality_counts: dict = {}
    for v in state.values():
        q = v.get("quality", "Unknown")
        quality_counts[q] = quality_counts.get(q, 0) + 1
    quality_rows = "\n".join(
        f'<tr><td>{q}</td><td>{c}</td></tr>'
        for q, c in sorted(quality_counts.items(), key=lambda x: -x[1])
    )
    scores    = [v.get("health_score", 0) for v in state.values()]
    avg_score = round(sum(scores) / len(scores), 2) if scores else 0.0
    excellent = sum(1 for s in scores if s >= 0.9)
    good      = sum(1 for s in scores if 0.7 <= s < 0.9)
    poor      = sum(1 for s in scores if s < 0.7)
    top_channels = sorted(
        [
            (ch.get('name', 'Unknown'), state.get(ch.get('id', ''), {}).get('health_score', 0))
            for ch in all_channels if ch.get('id') in state
        ],
        key=lambda x: -x[1],
    )[:10]
    top_rows = "\n".join(
        f'<tr><td>{name}</td>'
        f'<td><div class="score-bar"><div style="width:{int(score*100)}%;'
        f'background:{"#3fb950" if score >= 0.7 else "#f0883e"}"></div></div>'
        f'&nbsp;{score}</td></tr>'
        for name, score in top_channels
    )
    elapsed = int(time.time() - START_TIME)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="3600">
  <title>StreamX IPTV Dashboard v6</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:'Segoe UI',Arial,sans-serif;background:#0d1117;color:#e6edf3;padding:20px}}
    h1{{font-size:1.6em;margin-bottom:4px}} h2{{font-size:1em;color:#58a6ff;margin:18px 0 10px}}
    .subtitle{{color:#8b949e;font-size:.82em;margin-bottom:20px}}
    .card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:20px;margin:12px 0}}
    .stats{{display:flex;flex-wrap:wrap;gap:16px}}
    .stat{{text-align:center;min-width:110px}}
    .stat .num{{font-size:2em;font-weight:bold;color:#58a6ff}}
    .stat .label{{color:#8b949e;font-size:.78em;margin-top:2px}}
    .green{{color:#3fb950}}.red{{color:#f85149}}.orange{{color:#f0883e}}.purple{{color:#bc8cff}}
    table{{width:100%;border-collapse:collapse;font-size:.86em}}
    th,td{{padding:7px 10px;border-bottom:1px solid #21262d;text-align:left}}
    th{{color:#8b949e;font-weight:500}} tr:hover td{{background:#1c2128}}
    .score-bar{{display:inline-block;width:80px;height:8px;background:#21262d;border-radius:4px;vertical-align:middle}}
    .score-bar div{{height:100%;border-radius:4px}}
    .footer{{color:#8b949e;font-size:.75em;text-align:right;margin-top:16px}}
    .badge{{display:inline-block;padding:1px 6px;border-radius:10px;font-size:.72em;margin-left:4px;color:#fff}}
    .badge-epg{{background:#1f6feb}} .badge-v7{{background:#bc8cff}} .badge-ai{{background:#3fb950}}
  </style>
</head>
<body>
  <h1>📺 StreamX IPTV Dashboard <span class="badge badge-v7">v7</span></h1>
  <p class="subtitle">
    Auto-updated · {datetime.now().strftime('%Y-%m-%d %H:%M UTC')} ·
    Runtime: {elapsed//3600}h {(elapsed%3600)//60}m ·
    {len(CATEGORY_RULES)} categories
    <span class="badge badge-epg">EPG ✓</span>
    <span class="badge badge-ai">🤖 AI ✓</span>
  </p>
  <div class="card">
    <div class="stats">
      <div class="stat"><div class="num">{total}</div><div class="label">Total Channels</div></div>
      <div class="stat"><div class="num green">{healthy}</div><div class="label">Healthy</div></div>
      <div class="stat"><div class="num red">{dead - perm_dead}</div><div class="label">Temp Dead</div></div>
      <div class="stat"><div class="num purple">{perm_dead}</div><div class="label">Perm Dead 🪦</div></div>
      <div class="stat"><div class="num green">{STATS['recovered']}</div><div class="label">Recovered 🟢</div></div>
      <div class="stat"><div class="num">{STATS['added']}</div><div class="label">Added</div></div>
      <div class="stat"><div class="num orange">{avg_score}</div><div class="label">Avg Health</div></div>
    </div>
  </div>
  <div class="card">
    <h2>🤖 AI Classifier (v7)</h2>
    <div class="stats">
      <div class="stat"><div class="num green">{STATS['ai_classified']}</div><div class="label">AI Classified</div></div>
      <div class="stat"><div class="num">{STATS['ai_fallback']}</div><div class="label">Rule-based</div></div>
      <div class="stat"><div class="num orange">{STATS['state_cleaned']}</div><div class="label">State Cleaned 🧹</div></div>
      <div class="stat"><div class="num purple">{len(ai_cache) if 'ai_cache' in dir() else '—'}</div><div class="label">AI Cache Size</div></div>
    </div>
  </div>
  <div class="card" style="display:flex;gap:20px;flex-wrap:wrap">
    <div style="flex:1;min-width:200px">
      <h2>📂 By Category ({len(cat_counts)} categories)</h2>
      <table><tr><th>Category</th><th>Channels</th></tr>{cat_rows}</table>
    </div>
    <div style="flex:1;min-width:200px">
      <h2>🔍 Stream Quality</h2>
      <table><tr><th>Quality</th><th>Channels</th></tr>{quality_rows}</table>
    </div>
  </div>
  <div class="card">
    <h2>📊 Health Score Distribution</h2>
    <div class="stats" style="margin-bottom:12px">
      <div class="stat"><div class="num green">{excellent}</div><div class="label">Excellent (≥0.9)</div></div>
      <div class="stat"><div class="num">{good}</div><div class="label">Good (0.7–0.9)</div></div>
      <div class="stat"><div class="num orange">{poor}</div><div class="label">Poor (&lt;0.7)</div></div>
    </div>
    <h2>⭐ Top 10 Most Reliable</h2>
    <table><tr><th>Channel</th><th>Health Score</th></tr>{top_rows}</table>
  </div>
  <p class="footer">StreamX IPTV v7 · {len(CATEGORY_RULES)} categories · {len(EPG_SOURCES)} EPG feeds · AI: {ai_mode if 'ai_mode' in dir() else 'Rule-based'}</p>
</body></html>"""
    path = os.path.join(BASE_DIR, "index_channels.html")
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info(f"🌐 Dashboard generated")
    except Exception as e:
        logger.warning(f"  Dashboard failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def update_channels():
    logger.info(f"🚀 IPTV Auto-Updater v7  —  {len(CATEGORY_RULES)} categories")
    logger.info(f"⏱  Max runtime: {MAX_RUNTIME_SECONDS // 3600}h {(MAX_RUNTIME_SECONDS % 3600) // 60}m")
    ai_mode = "Gemini" if GEMINI_API_KEY else ("Groq" if GROQ_API_KEY else "Rule-based")
    logger.info(f"🤖 AI Classifier: {ai_mode} | 🧹 State cleanup: {DEAD_CLEANUP_DAYS}-day threshold")

    try:
        api_streams  = fetch_json_cached(STREAMS_API)
        api_channels = fetch_json_cached(CHANNELS_API)
    except Exception as e:
        logger.critical(f"❌ API fetch failed: {e}")
        return

    channel_info_map: dict = {c['id']: c for c in api_channels}

    streams_by_id: dict = {}
    for s in api_streams:
        if s.get('status') in ('error', 'offline'):
            continue
        cid = s.get('channel', '')
        url = s.get('url', '').strip()
        if cid and url:
            streams_by_id.setdefault(cid, []).append(s)

    logger.info(f"📊 API: {len(channel_info_map)} channels | {len(streams_by_id)} with streams")

    state        = load_state()
    logo_mapping = load_logo_mapping()
    logger.info(f"📂 State: {len(state)} tracked | Logo mapping: {len(logo_mapping)}")
    perm_dead_count = sum(1 for v in state.values() if v.get('permanently_dead'))
    logger.info(f"🪦 Permanently dead in state: {perm_dead_count}")

    # ── v7: Scheduled state cleanup (run every time — fast) ───────────────────
    cleanup_stale_dead_state(state)

    # ── v7: AI classifier cache — load which channels are already classified ──
    # state[cid]["ai_category"] = "Sports"  → skip re-classification
    ai_cache: dict = {
        cid: s["ai_category"]
        for cid, s in state.items()
        if s.get("ai_category")
    }
    logger.info(f"🤖 AI cache: {len(ai_cache)} channels already classified")

    os.makedirs(CATEGORY_DIR, exist_ok=True)
    cleanup_old(BACKUP_DIR, "", ".bak", MAX_BACKUPS_TO_KEEP * len(CATEGORY_RULES))

    # ── v6: Check dead channels for recovery BEFORE processing ────────────────
    if time_remaining() > 600:
        recovered_channels = check_dead_channel_recovery(
            state, streams_by_id, channel_info_map, logo_mapping
        )
        # Re-add recovered channels to their original categories
        for ch in recovered_channels:
            cat_name = ch.get("category", "")
            country  = ch.get("country", "")
            quality  = ch.pop("_recovered_quality", "Unknown")

            # Find matching category file
            target_file = None
            for fname, rules in CATEGORY_RULES.items():
                if rules["type"] == "country" and rules["filter"] == country:
                    target_file = fname
                    break
                if rules["type"] == "genre" and rules["category_name"] == cat_name:
                    target_file = fname
                    break
                if rules["category_name"] == cat_name:
                    target_file = fname
                    break

            if not target_file:
                continue

            filepath = os.path.join(CATEGORY_DIR, target_file)
            cat_data = load_json(filepath)
            existing_ids = {c['id'] for c in cat_data.get('channels', [])}

            if ch['id'] not in existing_ids:
                ch['category'] = CATEGORY_RULES.get(target_file, {}).get('category_name', cat_name)
                cat_data.setdefault('channels', []).append(ch)
                create_backup(filepath)
                atomic_save_json(filepath, cat_data)
                # Update state: no longer permanently dead
                state[ch['id']] = {
                    "stream_ok":       True,
                    "last_checked":    time.time(),
                    "fail_count":      0,
                    "pass_count":      1,
                    "health_score":    calculate_score(1, 0),
                    "quality":         quality,
                    "logo_searched":   ch['logoUrl'] != DEFAULT_LOGO,
                    "permanently_dead": False,
                    "category":        ch['category'],
                    "country":         country,
                }
                STATS["recovered"] += 1
                logger.info(f"  🟢 Re-added: {ch['name']} → {target_file}")

    all_channels_collection: list = []
    global_process_count = 0

    for filename, rules in CATEGORY_RULES.items():

        if time_remaining() < 300:
            logger.warning(f"⏰ <5m remaining — stopping at {filename}")
            save_state(state)
            break

        filepath = os.path.join(CATEGORY_DIR, filename)
        logger.info(
            f"\n{'═' * 60}\n"
            f"📂  {filename}  ({rules.get('category_name', '')})  |  "
            f"⏱  {int(time_remaining() // 60)}m left"
        )

        current_data  = load_json(filepath)
        existing      = current_data.get('channels', [])
        existing_ids  = {ch['id'] for ch in existing}
        data_modified = False

        channels_to_keep_map = {ch['id']: ch for ch in existing}

        # ── PART 1: Maintain existing channels ────────────────────────────────
        to_process, skipped_count = prioritize_channels(existing, state)
        STATS["skipped_state"] += skipped_count

        for ch in to_process:

            if time_remaining() < 120:
                logger.warning("⏰ 2m left — stopping, saving state.")
                save_state(state)
                break

            STATS["checked"] += 1
            ch_id    = ch.get('id', '')
            ch_state = state.get(ch_id, {})
            now      = time.time()
            details  = channel_info_map.get(ch_id, {})

            ch['streamUrls'] = [
                u for u in (ch.get('streamUrls') or [])
                if u and isinstance(u, str) and u.strip()
            ]

            # ── Stream check ──────────────────────────────────────────────────
            working = get_working_streams(ch_id, streams_by_id)

            if working:
                if working != ch['streamUrls']:
                    ch['streamUrls'] = working
                    data_modified = True
                    STATS["repaired"] += 1
                    logger.info(f"  🩹 Repaired: {ch.get('name')}")

                pass_count   = ch_state.get('pass_count', 0) + 1
                fail_count   = ch_state.get('fail_count', 0)
                new_score    = calculate_score(pass_count, fail_count)
                curr_quality = ch_state.get('quality', '')
                if not curr_quality or curr_quality == 'Unknown':
                    curr_quality = detect_quality(working[0])

                state[ch_id] = {
                    **ch_state,
                    "stream_ok":       True,
                    "last_checked":    now,
                    "fail_count":      0,
                    "pass_count":      pass_count,
                    "health_score":    new_score,
                    "quality":         curr_quality,
                    "permanently_dead": False,
                    "category":        rules['category_name'],
                    "country":         safe_str(details.get('country', '')),
                }

            else:
                # ── v6: Dead channel tracking ──────────────────────────────────
                fail_count = ch_state.get('fail_count', 0) + 1
                pass_count = ch_state.get('pass_count', 0)
                new_score  = calculate_score(pass_count, fail_count)

                # Get current stream URL to detect future changes
                dead_url = (ch.get('streamUrls') or [""])[0]

                state[ch_id] = {
                    **ch_state,
                    "stream_ok":          False,
                    "last_checked":       now,
                    "fail_count":         fail_count,
                    "pass_count":         pass_count,
                    "health_score":       new_score,
                    "dead_stream_url":    dead_url,
                    "category":           rules['category_name'],
                    "country":            safe_str(details.get('country', '')),
                }

                if fail_count >= DEAD_FAIL_THRESHOLD:
                    # ── PERMANENT DELETION ─────────────────────────────────────
                    logger.warning(
                        f"  🪦 PERMANENTLY removed ({fail_count}× dead): {ch.get('name')}"
                    )
                    STATS["removed_dead"] += 1
                    data_modified = True
                    channels_to_keep_map.pop(ch_id, None)

                    # Mark as permanently dead — recovery monitor will watch it
                    state[ch_id] = {
                        **state[ch_id],
                        "permanently_dead":    True,
                        "dead_since":          now,
                        "last_recovery_check": 0,  # check on next run
                    }

                    global_process_count += 1
                    if global_process_count % STATE_SAVE_EVERY == 0:
                        save_state(state)
                    continue

                else:
                    logger.info(
                        f"  ⚠️   No stream (fail #{fail_count}/{DEAD_FAIL_THRESHOLD}, "
                        f"score={new_score}): {ch.get('name')}"
                    )

            # ── v6: Smart Logo Fix ─────────────────────────────────────────────
            curr_logo = ch.get('logoUrl', '')
            website   = details.get('website', '')
            api_logo  = details.get('logo', '')
            country   = safe_str(details.get('country', ''))

            best_logo = get_channel_logo(
                ch_id, ch.get('name', ''), website,
                logo_mapping, api_logo, country
            )
            if best_logo and best_logo != curr_logo and best_logo != DEFAULT_LOGO:
                ch['logoUrl'] = best_logo
                data_modified = True
                STATS["logo_fixed"] += 1

            channels_to_keep_map[ch_id] = ch

            global_process_count += 1
            if global_process_count % STATE_SAVE_EVERY == 0:
                save_state(state)
                logger.info(
                    f"  💾 State saved — {global_process_count} processed | "
                    f"{int(time_remaining() // 60)}m left"
                )

        current_data['channels'] = list(channels_to_keep_map.values())

        # ── PART 2: Discover & add new channels ───────────────────────────────
        if time_remaining() > 300:
            new_candidates = []
            for ch_id in streams_by_id:
                if ch_id in existing_ids:
                    continue
                # Skip permanently dead channels
                if state.get(ch_id, {}).get("permanently_dead"):
                    continue
                if ch_id in state and state[ch_id].get("stream_ok") is False:
                    continue
                details = channel_info_map.get(ch_id)
                if not details:
                    continue

                match = False
                if rules['type'] == 'country':
                    match = details.get('country') == rules['filter']
                elif rules['type'] == 'genre':
                    cats  = {c.lower() for c in details.get('categories', [])}
                    match = bool(cats & {f.lower() for f in rules['filter']})

                if match:
                    new_candidates.append(ch_id)

            if new_candidates:
                logger.info(f"  ⚡ {len(new_candidates)} new candidates for {filename}")

                # ── v7: AI classify channels with "general"/empty iptv-org category ──
                # Only for genre-type categories — country categories don't need it
                if rules["type"] == "genre" and (GEMINI_API_KEY or GROQ_API_KEY):
                    to_classify = []
                    for cid in new_candidates:
                        if cid in ai_cache:
                            continue  # already classified
                        details  = channel_info_map.get(cid, {})
                        iptv_cats = details.get("categories", [])
                        # Only AI-classify channels that iptv-org marks as "general"
                        # or has no useful category info
                        if not iptv_cats or set(c.lower() for c in iptv_cats) <= {"general", ""}:
                            to_classify.append({
                                "id":      cid,
                                "name":    details.get("name", cid),
                                "country": details.get("country", ""),
                                "cats":    iptv_cats,
                            })

                    if to_classify:
                        logger.info(
                            f"  🤖 AI classifying {len(to_classify)} uncategorized channels…"
                        )
                        batch_results = ai_classify_batch(to_classify, state)
                        for cid, genre in batch_results.items():
                            ai_cache[cid] = genre
                            # Persist in state
                            state.setdefault(cid, {})["ai_category"] = genre

                def _process_new(cid):
                    if time_remaining() < 120:
                        return None
                    details = channel_info_map.get(cid, {})
                    urls    = get_working_streams(cid, streams_by_id)
                    if not urls:
                        state[cid] = {
                            "stream_ok":    False,
                            "last_checked": time.time(),
                            "fail_count":   1,
                            "pass_count":   0,
                            "health_score": calculate_score(0, 1),
                            "logo_searched": False,
                            "quality":      "Unknown",
                            "category":     rules['category_name'],
                            "country":      safe_str(details.get('country', '')),
                        }
                        return None

                    website  = details.get('website', '')
                    api_logo = details.get('logo', '')
                    country  = safe_str(details.get('country', ''))

                    # v6: Logo priority — logos folder → API → online
                    logo = get_channel_logo(
                        cid, details.get('name', ''), website,
                        logo_mapping, api_logo, country
                    )
                    quality = detect_quality(urls[0]) if urls else "Unknown"

                    # v7: Use AI category if iptv-org says "general" or empty
                    iptv_cats    = details.get("categories", [])
                    is_generic   = not iptv_cats or set(
                        c.lower() for c in iptv_cats
                    ) <= {"general", ""}
                    ai_cat       = ai_cache.get(cid, "")
                    final_cat    = (
                        ai_cat if (is_generic and ai_cat)
                        else rules['category_name']
                    )

                    return {
                        "id":         safe_str(details.get('id')),
                        "name":       safe_str(details.get('name'), 'Unknown Channel'),
                        "logoUrl":    safe_str(logo, DEFAULT_LOGO),
                        "streamUrls": [u for u in urls if u and isinstance(u, str)],
                        "category":   final_cat,
                        "languages":  details.get('languages', []),
                        "_quality":   quality,
                        "_country":   country,
                    }

                new_channels: list = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
                    futures = {ex.submit(_process_new, cid): cid for cid in new_candidates}
                    for future in concurrent.futures.as_completed(futures):
                        if time_remaining() < 120:
                            break
                        result = future.result()
                        if result:
                            quality = result.pop("_quality", "Unknown")
                            country = result.pop("_country", "")
                            new_channels.append(result)
                            state[result['id']] = {
                                "stream_ok":       True,
                                "last_checked":    time.time(),
                                "fail_count":      0,
                                "pass_count":      1,
                                "health_score":    calculate_score(1, 0),
                                "logo_searched":   result['logoUrl'] != DEFAULT_LOGO,
                                "quality":         quality,
                                "permanently_dead": False,
                                "category":        rules['category_name'],
                                "country":         country,
                            }
                            STATS["added"] += 1
                            logger.info(f"  ✅ [NEW] {result['name']} [{quality}]")

                if new_channels:
                    new_channels.sort(key=lambda x: x['name'])
                    current_data['channels'].extend(new_channels)
                    data_modified = True
                    logger.info(f"  📥 Added {len(new_channels)} new to {filename}")

        # ── Save JSON + generate M3U ───────────────────────────────────────────
        if data_modified:
            create_backup(filepath)
            atomic_save_json(filepath, current_data)

        generate_m3u(current_data, filename, state)
        all_channels_collection.extend(current_data.get('channels', []))
        save_state(state)
        logger.info(
            f"  ✔  {filename}: {len(current_data['channels'])} channels | "
            f"{int(time_remaining() // 60)}m left"
        )

    # ── Final outputs ──────────────────────────────────────────────────────────
    if all_channels_collection:
        generate_master_m3u(all_channels_collection, state)

    save_state(state)
    write_report(state)
    send_telegram_report(state)
    generate_dashboard(all_channels_collection, state)

    total_tracked = len(state)
    total_healthy = sum(1 for v in state.values() if v.get('stream_ok'))
    total_dead    = sum(1 for v in state.values() if not v.get('stream_ok'))
    perm_dead     = sum(1 for v in state.values() if v.get('permanently_dead'))
    scores        = [v.get("health_score", 0) for v in state.values()]
    avg_score     = round(sum(scores) / len(scores), 2) if scores else 0.0

    logger.info(
        f"\n{'═' * 60}\n"
        f"📊  FINAL SUMMARY\n"
        f"   ✅  Healthy      : {total_healthy}\n"
        f"   ❌  Dead (temp)  : {total_dead - perm_dead}\n"
        f"   🪦  Perm dead    : {perm_dead}\n"
        f"   🟢  Recovered    : {STATS['recovered']}\n"
        f"   📦  Total tracked: {total_tracked}\n"
        f"   ⭐  Avg score    : {avg_score}\n"
        f"{'═' * 60}"
    )
    logger.info("🎉  All done!")


if __name__ == "__main__":
    update_channels()
