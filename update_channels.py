"""
update_channels.py — StreamX IPTV Channel Auto-Updater v5
══════════════════════════════════════════════════════════
Upgrades included in this version:
  1. Stream Health Scoring   — reliability score per channel, M3U sorted by score
  2. Stream Quality Detection — HD/FHD/4K/SD auto-detect, tagged in M3U
  3. Telegram Notification   — post run report to Telegram bot
  4. GitHub Pages Dashboard  — generate index.html for live stats
  5. EPG (TV Guide) Support  — url-tvg header in every M3U playlist

OPTIONAL SECRETS (GitHub):
  TELEGRAM_BOT_TOKEN  — your bot token
  TELEGRAM_CHAT_ID    — your chat / channel id

Run: python update_channels.py
"""

import json, os, time, shutil, logging, tempfile, random, hashlib, re
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

MAX_BACKUPS_TO_KEEP     = 3
MAX_REPORTS_TO_KEEP     = 5
MAX_STREAMS_PER_CHANNEL = 3
API_CACHE_TTL_SECONDS   = 3600         # re-fetch API only if > 1h old
STREAM_CHECK_WORKERS    = 20           # parallel stream checkers
LOGO_SEARCH_WORKERS     = 6
MAX_RUNTIME_SECONDS     = 5 * 3600 + 30 * 60   # 5h 30m — safe under GitHub 6h kill
RECHECK_INTERVAL        = 12 * 3600            # recheck healthy streams every 12h
STATE_SAVE_EVERY        = 50                   # flush state every N channels

STREAMS_API  = "https://iptv-org.github.io/api/streams.json"
CHANNELS_API = "https://iptv-org.github.io/api/channels.json"
DEFAULT_LOGO = "https://raw.githubusercontent.com/iptv-org/api/master/data/categories/no-logo.png"

CATEGORY_RULES = {
    "bangladesh.json": {"type": "country", "filter": "BD",  "category_name": "Bangladesh"},
    "india.json":      {"type": "country", "filter": "IN",  "category_name": "India"},
    "usa.json":        {"type": "country", "filter": "US",  "category_name": "USA"},
    "uk.json":         {"type": "country", "filter": "GB",  "category_name": "UK"},
    "uae.json":        {"type": "country", "filter": "AE",  "category_name": "UAE"},
    "sports.json":     {"type": "genre",   "filter": ["sports"],                            "category_name": "Sports"},
    "kids.json":       {"type": "genre",   "filter": ["kids", "animation"],                 "category_name": "Kids"},
    "music.json":      {"type": "genre",   "filter": ["music"],                             "category_name": "Music"},
    "informative.json":{"type": "genre",   "filter": ["documentary","education","science"],  "category_name": "Informative"},
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
    "files_updated": 0, "m3u_generated": 0,
}

# ── Upgrade 5: EPG source URLs ────────────────────────────────────────────────
EPG_SOURCES = [
    "https://iptv-org.github.io/epg/guides/bd.xml",
    "https://iptv-org.github.io/epg/guides/in.xml",
    "https://iptv-org.github.io/epg/guides/us.xml",
    "https://iptv-org.github.io/epg/guides/gb.xml",
    "https://iptv-org.github.io/epg/guides/ae.xml",
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
#  💾  STATE  — persists across GitHub Actions runs
#
#  Schema per channel_id (v5 — adds health_score + pass_count + quality):
#  {
#    "stream_ok":     true | false,
#    "last_checked":  <epoch float>,
#    "fail_count":    <int>,
#    "pass_count":    <int>,          ← NEW (Upgrade 1)
#    "health_score":  <float 0-1>,    ← NEW (Upgrade 1)
#    "quality":       "HD"|"SD"|...,  ← NEW (Upgrade 2)
#    "logo_searched": true | false
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
#  ✅  STREAM VALIDATION  — checks actual video content, not just HTTP 200
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
#  📊  UPGRADE 1 — Stream Health Scoring
#  Tracks pass/fail ratio per channel. M3U sorted by score (best first).
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_score(passes: int, fails: int) -> float:
    """
    Reliability score in [0.0, 1.0].
    Recent checks have more weight via simple ratio; as data grows
    the score reflects long-term reliability.
    Returns 0.5 for brand-new channels (neutral prior).
    """
    total = passes + fails
    if total == 0:
        return 0.5
    return round(passes / total, 2)

# ═══════════════════════════════════════════════════════════════════════════════
#  🔍  UPGRADE 2 — Stream Quality Detection
#  Reads M3U8 RESOLUTION tag or URL hints to classify quality.
# ═══════════════════════════════════════════════════════════════════════════════

_QUALITY_ORDER = [
    (2160, "4K"),
    (1080, "FHD"),
    (720,  "HD"),
    (480,  "SD"),
    (0,    "SD"),   # fallback
]

def detect_quality(url: str) -> str:
    """
    Detect stream quality: '4K' | 'FHD' | 'HD' | 'SD' | 'Unknown'.
    1. Reads first 2KB of the stream to look for M3U8 RESOLUTION tag.
    2. Falls back to URL keyword hints.
    """
    if not url or not url.startswith("http"):
        return "Unknown"

    # ── Try reading M3U8 RESOLUTION tag ─────────────────────────────────────
    try:
        with requests.get(
            url, stream=True, timeout=(4, 6), headers=_headers()
        ) as r:
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

    # ── URL keyword hints ────────────────────────────────────────────────────
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
#  🖼️  LOGO SEARCH  — 3 free sources, no API key required
# ═══════════════════════════════════════════════════════════════════════════════

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

def find_logo_online(name: str, website: str = "") -> str:
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

# ═══════════════════════════════════════════════════════════════════════════════
#  🧠  SMART PRIORITY QUEUE
# ═══════════════════════════════════════════════════════════════════════════════

def prioritize_channels(channels: list, state: dict) -> tuple:
    """
    Returns (to_process, to_skip_count).

    Priority order:
      0 → never checked (brand new)
      1 → last check showed dead stream
      2 → healthy but last checked > RECHECK_INTERVAL ago
      skip → healthy + checked recently
    """
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
#  🎵  M3U GENERATOR  (v5 — EPG header + quality tags + health sort)
# ═══════════════════════════════════════════════════════════════════════════════

def generate_epg_playlist_header() -> str:
    """
    Upgrade 5: M3U header with url-tvg EPG sources.
    IPTV players (TiviMate, IPTV Smarters, etc.) load the guide
    from these XML feeds automatically.
    """
    url_attrs = " ".join(f'url-tvg="{u}"' for u in EPG_SOURCES)
    return f'#EXTM3U {url_attrs} refresh="3600"\n'


def _build_m3u(channels: list, state: dict = None) -> str:
    """
    Build M3U playlist content.
    - Upgrade 1: sort channels by health_score (best first)
    - Upgrade 2: append quality tag to group-title and name
    - Upgrade 5: EPG header with url-tvg attributes
    """
    # Upgrade 1: sort by health score (descending) before writing
    if state:
        channels = sorted(
            channels,
            key=lambda ch: state.get(ch.get('id', ''), {}).get('health_score', 0.5),
            reverse=True,
        )

    lines = [generate_epg_playlist_header().strip()]

    for ch in channels:
        urls = [
            u for u in (ch.get('streamUrls') or [])
            if u and isinstance(u, str) and u.strip()
        ]
        if not urls:
            continue

        name    = safe_str(ch.get('name'),     'Unknown')
        logo    = safe_str(ch.get('logoUrl'),  '')
        cid     = safe_str(ch.get('id'),       '')
        group   = safe_str(ch.get('category'), 'Uncategorized')

        # Upgrade 2: add quality tag from state
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
        logger.info(f"🌟 Master M3U written: {len(all_channels)} channels")
    except Exception as e:
        logger.error(f"❌ Master M3U failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
#  📄  REPORT
# ═══════════════════════════════════════════════════════════════════════════════

def write_report(state: dict):
    os.makedirs(REPORT_DIR, exist_ok=True)
    cleanup_old(REPORT_DIR, "report_", ".txt", MAX_REPORTS_TO_KEEP)

    elapsed  = int(time.time() - START_TIME)
    ts       = datetime.now().strftime("%Y-%m-%d_%H-%M")
    path     = os.path.join(REPORT_DIR, f"report_{ts}.txt")

    total_healthy = sum(1 for v in state.values() if v.get('stream_ok'))
    total_dead    = sum(1 for v in state.values() if not v.get('stream_ok'))
    total_tracked = len(state)

    # Upgrade 1: health score distribution
    scores = [v.get("health_score", 0) for v in state.values()]
    avg_score = round(sum(scores) / len(scores), 2) if scores else 0.0
    excellent = sum(1 for s in scores if s >= 0.9)
    good      = sum(1 for s in scores if 0.7 <= s < 0.9)
    poor      = sum(1 for s in scores if s < 0.7)

    # Upgrade 2: quality breakdown
    quality_counts: dict = {}
    for v in state.values():
        q = v.get("quality", "Unknown")
        quality_counts[q] = quality_counts.get(q, 0) + 1

    quality_lines = "   ".join(
        f"{q}: {c}" for q, c in sorted(quality_counts.items())
    )

    content = f"""
╔══════════════════════════════════════════════╗
║      IPTV AUTO-UPDATER  —  RUN REPORT v5     ║
║      {datetime.now().strftime("%Y-%m-%d  %H:%M:%S")}                   ║
╚══════════════════════════════════════════════╝

⏱  Runtime              : {elapsed // 3600}h {(elapsed % 3600) // 60}m {elapsed % 60}s
─────────────────────────────────────────────
✅  Channels checked     : {STATS['checked']}
⏭  Skipped (recent)     : {STATS['skipped_state']}
🩹  Streams repaired     : {STATS['repaired']}
🗑  Dead channels removed: {STATS['removed_dead']}
🖼  Logos fixed          : {STATS['logo_fixed']}
🆕  New channels added   : {STATS['added']}
─────────────────────────────────────────────
💾  JSON files saved     : {STATS['files_updated']}
🎵  M3U files created    : {STATS['m3u_generated']} + 1 master
─────────────────────────────────────────────
📦  Total tracked (state): {total_tracked}
   ✅ Healthy            : {total_healthy}
   ❌ Dead / no stream   : {total_dead}
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
        logger.info(f"📄 Report: {path}")
    except Exception as e:
        logger.error(f"❌ Report write failed: {e}")

    print(content)

# ═══════════════════════════════════════════════════════════════════════════════
#  📱  UPGRADE 3 — Telegram Notification
#  Set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID in GitHub Secrets.
# ═══════════════════════════════════════════════════════════════════════════════

def send_telegram_report(state: dict):
    token   = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.debug("  Telegram: no credentials — skipping")
        return

    elapsed       = int(time.time() - START_TIME)
    total_healthy = sum(1 for v in state.values() if v.get('stream_ok'))
    total_dead    = sum(1 for v in state.values() if not v.get('stream_ok'))

    # Health score stats
    scores    = [v.get("health_score", 0) for v in state.values()]
    avg_score = round(sum(scores) / len(scores), 2) if scores else 0.0

    msg = (
        f"📺 *IPTV Auto-Updater Report*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Runtime: `{elapsed // 3600}h {(elapsed % 3600) // 60}m`\n"
        f"✅ Healthy streams: `{total_healthy}`\n"
        f"❌ Dead channels: `{total_dead}`\n"
        f"🆕 New added: `{STATS['added']}`\n"
        f"🩹 Repaired: `{STATS['repaired']}`\n"
        f"🗑 Removed: `{STATS['removed_dead']}`\n"
        f"🖼 Logos fixed: `{STATS['logo_fixed']}`\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Avg health score: `{avg_score}`\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
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
#  🌐  UPGRADE 4 — GitHub Pages Status Dashboard
#  Generates index.html — serve it on GitHub Pages for live stats.
# ═══════════════════════════════════════════════════════════════════════════════

def generate_dashboard(all_channels: list, state: dict):
    total   = len(all_channels)
    healthy = sum(1 for v in state.values() if v.get('stream_ok'))
    dead    = sum(1 for v in state.values() if not v.get('stream_ok'))

    # Category breakdown
    cat_counts: dict = {}
    for ch in all_channels:
        cat = ch.get('category', 'Unknown')
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    cat_rows = "\n".join(
        f'<tr><td>{cat}</td><td>{count}</td></tr>'
        for cat, count in sorted(cat_counts.items())
    )

    # Quality breakdown (Upgrade 2)
    quality_counts: dict = {}
    for v in state.values():
        q = v.get("quality", "Unknown")
        quality_counts[q] = quality_counts.get(q, 0) + 1
    quality_rows = "\n".join(
        f'<tr><td>{q}</td><td>{c}</td></tr>'
        for q, c in sorted(quality_counts.items(), key=lambda x: -x[1])
    )

    # Health score stats (Upgrade 1)
    scores    = [v.get("health_score", 0) for v in state.values()]
    avg_score = round(sum(scores) / len(scores), 2) if scores else 0.0
    excellent = sum(1 for s in scores if s >= 0.9)
    good      = sum(1 for s in scores if 0.7 <= s < 0.9)
    poor      = sum(1 for s in scores if s < 0.7)

    # Top 10 healthiest channels
    top_channels = sorted(
        [
            (ch.get('name', 'Unknown'), state.get(ch.get('id', ''), {}).get('health_score', 0))
            for ch in all_channels
            if ch.get('id') in state
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
  <title>StreamX IPTV Status Dashboard</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:'Segoe UI',Arial,sans-serif;background:#0d1117;color:#e6edf3;padding:20px}}
    h1{{font-size:1.6em;margin-bottom:4px}}
    h2{{font-size:1em;color:#58a6ff;margin:18px 0 10px}}
    .subtitle{{color:#8b949e;font-size:.82em;margin-bottom:20px}}
    .card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:20px;margin:12px 0}}
    .stats{{display:flex;flex-wrap:wrap;gap:16px}}
    .stat{{text-align:center;min-width:110px}}
    .stat .num{{font-size:2em;font-weight:bold;color:#58a6ff}}
    .stat .label{{color:#8b949e;font-size:.78em;margin-top:2px}}
    .green{{color:#3fb950}}.red{{color:#f85149}}.orange{{color:#f0883e}}
    table{{width:100%;border-collapse:collapse;font-size:.86em}}
    th,td{{padding:7px 10px;border-bottom:1px solid #21262d;text-align:left}}
    th{{color:#8b949e;font-weight:500}}
    tr:hover td{{background:#1c2128}}
    .score-bar{{display:inline-block;width:80px;height:8px;background:#21262d;
               border-radius:4px;vertical-align:middle}}
    .score-bar div{{height:100%;border-radius:4px}}
    .footer{{color:#8b949e;font-size:.75em;text-align:right;margin-top:16px}}
    .epg-tag{{display:inline-block;background:#1f6feb;color:#fff;padding:1px 6px;
              border-radius:10px;font-size:.72em;margin-left:4px}}
  </style>
</head>
<body>
  <h1>📺 StreamX IPTV Dashboard</h1>
  <p class="subtitle">
    Auto-updated · {datetime.now().strftime('%Y-%m-%d %H:%M UTC')} ·
    Runtime: {elapsed//3600}h {(elapsed%3600)//60}m
    <span class="epg-tag">EPG ✓</span>
  </p>

  <div class="card">
    <div class="stats">
      <div class="stat">
        <div class="num">{total}</div>
        <div class="label">Total Channels</div>
      </div>
      <div class="stat">
        <div class="num green">{healthy}</div>
        <div class="label">Healthy Streams</div>
      </div>
      <div class="stat">
        <div class="num red">{dead}</div>
        <div class="label">Dead Channels</div>
      </div>
      <div class="stat">
        <div class="num">{STATS['added']}</div>
        <div class="label">Added This Run</div>
      </div>
      <div class="stat">
        <div class="num orange">{avg_score}</div>
        <div class="label">Avg Health Score</div>
      </div>
    </div>
  </div>

  <div class="card" style="display:flex;gap:20px;flex-wrap:wrap">
    <div style="flex:1;min-width:200px">
      <h2>📂 By Category</h2>
      <table>
        <tr><th>Category</th><th>Channels</th></tr>
        {cat_rows}
      </table>
    </div>
    <div style="flex:1;min-width:200px">
      <h2>🔍 Stream Quality (Upgrade 2)</h2>
      <table>
        <tr><th>Quality</th><th>Channels</th></tr>
        {quality_rows}
      </table>
    </div>
  </div>

  <div class="card">
    <h2>📊 Health Score Distribution (Upgrade 1)</h2>
    <div class="stats" style="margin-bottom:12px">
      <div class="stat">
        <div class="num green">{excellent}</div>
        <div class="label">Excellent (≥0.9)</div>
      </div>
      <div class="stat">
        <div class="num">{good}</div>
        <div class="label">Good (0.7–0.9)</div>
      </div>
      <div class="stat">
        <div class="num orange">{poor}</div>
        <div class="label">Poor (&lt;0.7)</div>
      </div>
    </div>
    <h2>⭐ Top 10 Most Reliable Channels</h2>
    <table>
      <tr><th>Channel</th><th>Health Score</th></tr>
      {top_rows}
    </table>
  </div>

  <p class="footer">
    StreamX IPTV Auto-Updater v5 · EPG sources: {len(EPG_SOURCES)} feeds
  </p>
</body>
</html>"""

    path = os.path.join(BASE_DIR, "index.html")
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info(f"🌐 Dashboard generated: {path}")
    except Exception as e:
        logger.warning(f"  Dashboard write failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def update_channels():
    logger.info("🚀 IPTV Ultimate Updater v5  —  Large Scale Mode (4300+ channels)")
    logger.info(f"⏱  Max runtime: {MAX_RUNTIME_SECONDS // 3600}h "
                f"{(MAX_RUNTIME_SECONDS % 3600) // 60}m")
    logger.info("📊 Upgrades: Health Scoring | Quality Detect | Telegram | Dashboard | EPG")

    # ── Fetch & cache remote API data ─────────────────────────────────────────
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

    logger.info(f"📊 API: {len(channel_info_map)} channels | "
                f"{len(streams_by_id)} channels have streams")

    state = load_state()
    logger.info(f"📂 State: {len(state)} channels already tracked")

    os.makedirs(CATEGORY_DIR, exist_ok=True)
    cleanup_old(BACKUP_DIR, "", ".bak", MAX_BACKUPS_TO_KEEP * len(CATEGORY_RULES))

    all_channels_collection: list = []
    global_process_count = 0

    for filename, rules in CATEGORY_RULES.items():

        # ── File-level time gate ───────────────────────────────────────────────
        if time_remaining() < 300:
            logger.warning(
                f"⏰ Less than 5m remaining — skipping {filename} and all after it"
            )
            save_state(state)
            break

        filepath = os.path.join(CATEGORY_DIR, filename)
        logger.info(
            f"\n{'═' * 60}\n"
            f"📂  {filename}   |   ⏱  {int(time_remaining() // 60)}m left"
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
                logger.warning("⏰ 2m left — stopping mid-file, saving state.")
                save_state(state)
                break

            STATS["checked"] += 1
            ch_id    = ch.get('id', '')
            ch_state = state.get(ch_id, {})
            now      = time.time()

            if ch_id not in channel_info_map:
                continue

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

                # ── Upgrade 1: Update health score (pass) ─────────────────────
                pass_count = ch_state.get('pass_count', 0) + 1
                fail_count = ch_state.get('fail_count', 0)
                new_score  = calculate_score(pass_count, fail_count)

                # ── Upgrade 2: Detect quality (only if not already detected) ──
                current_quality = ch_state.get('quality', '')
                if not current_quality or current_quality == 'Unknown':
                    current_quality = detect_quality(working[0])

                state[ch_id] = {
                    **ch_state,
                    "stream_ok":    True,
                    "last_checked": now,
                    "fail_count":   0,
                    "pass_count":   pass_count,
                    "health_score": new_score,
                    "quality":      current_quality,
                }

            else:
                fail_count  = ch_state.get('fail_count', 0) + 1
                pass_count  = ch_state.get('pass_count', 0)
                new_score   = calculate_score(pass_count, fail_count)

                state[ch_id] = {
                    **ch_state,
                    "stream_ok":    False,
                    "last_checked": now,
                    "fail_count":   fail_count,
                    "pass_count":   pass_count,
                    "health_score": new_score,
                }

                if fail_count >= 3:
                    logger.warning(
                        f"  🗑  Removing ({fail_count}× dead): {ch.get('name')}"
                    )
                    STATS["removed_dead"] += 1
                    data_modified = True
                    channels_to_keep_map.pop(ch_id, None)

                    global_process_count += 1
                    if global_process_count % STATE_SAVE_EVERY == 0:
                        save_state(state)
                    continue

                else:
                    logger.info(
                        f"  ⚠️   No stream (fail #{fail_count}, "
                        f"score={new_score}): {ch.get('name')}"
                    )

            # ── Logo fix (done only once per channel lifetime) ────────────────
            if not ch_state.get('logo_searched'):
                current_logo = ch.get('logoUrl', '')
                if not current_logo or current_logo == DEFAULT_LOGO:
                    api_logo = channel_info_map.get(ch_id, {}).get('logo', '')
                    website  = channel_info_map.get(ch_id, {}).get('website', '')
                    if api_logo:
                        ch['logoUrl'] = api_logo
                        data_modified = True
                        STATS["logo_fixed"] += 1
                        logger.info(f"  🖼  Logo (API): {ch.get('name')}")
                    else:
                        found = find_logo_online(ch.get('name', ''), website)
                        if found:
                            ch['logoUrl'] = found
                            data_modified = True
                            STATS["logo_fixed"] += 1
                            logger.info(f"  🖼  Logo (web): {ch.get('name')}")

                state[ch_id] = {
                    **state.get(ch_id, {}),
                    "logo_searched": True,
                }

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
                if ch_id in state:
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
                logger.info(
                    f"  ⚡ {len(new_candidates)} new candidates for {filename}"
                )

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
                        }
                        return None

                    website = details.get('website', '')
                    logo    = (
                        details.get('logo')
                        or find_logo_online(details.get('name', ''), website)
                        or DEFAULT_LOGO
                    )

                    # Upgrade 2: detect quality for new channels
                    quality = detect_quality(urls[0]) if urls else "Unknown"

                    return {
                        "id":         safe_str(details.get('id')),
                        "name":       safe_str(details.get('name'), 'Unknown Channel'),
                        "logoUrl":    safe_str(logo, DEFAULT_LOGO),
                        "streamUrls": [u for u in urls if u and isinstance(u, str)],
                        "category":   rules['category_name'],
                        "languages":  details.get('languages', []),
                        **({"genre": rules['category_name']}
                           if rules['type'] == 'genre' else {}),
                        "_quality":   quality,  # temp field, stored in state
                    }

                new_channels: list = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
                    futures = {
                        ex.submit(_process_new, cid): cid
                        for cid in new_candidates
                    }
                    for future in concurrent.futures.as_completed(futures):
                        if time_remaining() < 120:
                            break
                        result = future.result()
                        if result:
                            quality = result.pop("_quality", "Unknown")
                            new_channels.append(result)
                            state[result['id']] = {
                                "stream_ok":    True,
                                "last_checked": time.time(),
                                "fail_count":   0,
                                "pass_count":   1,
                                "health_score": calculate_score(1, 0),
                                "logo_searched": result['logoUrl'] != DEFAULT_LOGO,
                                "quality":      quality,
                            }
                            STATS["added"] += 1
                            logger.info(
                                f"  ✅ [NEW] {result['name']} [{quality}]"
                            )

                if new_channels:
                    new_channels.sort(key=lambda x: x['name'])
                    current_data['channels'].extend(new_channels)
                    data_modified = True
                    logger.info(f"  📥 Added {len(new_channels)} new channels")

        # ── Save JSON + generate M3U for this category ────────────────────────
        if data_modified:
            create_backup(filepath)
            atomic_save_json(filepath, current_data)

        # Pass state to M3U generator for sorting + quality tags (Upgrades 1+2)
        generate_m3u(current_data, filename, state)
        all_channels_collection.extend(current_data.get('channels', []))

        save_state(state)
        logger.info(
            f"  ✔  Done: {filename} "
            f"({len(current_data['channels'])} channels) | "
            f"{int(time_remaining() // 60)}m left"
        )

    # ── Final outputs ─────────────────────────────────────────────────────────
    if all_channels_collection:
        generate_master_m3u(all_channels_collection, state)

    save_state(state)
    write_report(state)

    # Upgrade 3 — Telegram
    send_telegram_report(state)

    # Upgrade 4 — Dashboard
    generate_dashboard(all_channels_collection, state)

    # Progress summary
    total_tracked = len(state)
    total_healthy = sum(1 for v in state.values() if v.get('stream_ok'))
    total_dead    = sum(1 for v in state.values() if not v.get('stream_ok'))
    scores        = [v.get("health_score", 0) for v in state.values()]
    avg_score     = round(sum(scores) / len(scores), 2) if scores else 0.0

    logger.info(
        f"\n{'═' * 60}\n"
        f"📊  OVERALL PROGRESS\n"
        f"   ✅  Healthy      : {total_healthy}\n"
        f"   ❌  Dead         : {total_dead}\n"
        f"   📦  Total tracked: {total_tracked}\n"
        f"   ⭐  Avg score    : {avg_score}\n"
        f"{'═' * 60}"
    )
    logger.info("🎉  All done! Check the 'playlists/' folder for M3U files.")


if __name__ == "__main__":
    update_channels()
