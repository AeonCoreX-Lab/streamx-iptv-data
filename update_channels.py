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
API_CACHE_TTL_SECONDS   = 3600          # re-fetch API only if > 1h old
STREAM_CHECK_WORKERS    = 20            # parallel stream checkers
LOGO_SEARCH_WORKERS     = 6
MAX_RUNTIME_SECONDS     = 5 * 3600 + 30 * 60   # 5h 30m — safe under GitHub 6h kill
RECHECK_INTERVAL        = 12 * 3600             # recheck healthy streams every 12h
STATE_SAVE_EVERY        = 50                    # flush state every N channels

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
# ═══════════════════════════════════════════════════════════════════════════════

def load_state() -> dict:
    """
    Schema per channel_id:
    {
      "stream_ok":     true | false,
      "last_checked":  <epoch float>,
      "fail_count":    <int>,
      "logo_searched": true | false
    }
    """
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

    # RTMP/RTSP — can't easily HEAD-check; accept if URL is well-formed
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
#  🖼️  LOGO SEARCH  — 3 free sources, no API key required
# ═══════════════════════════════════════════════════════════════════════════════

def _clean_slug(name: str) -> str:
    """'BBC News HD' → 'bbcnews',  'Star Sports 1' → 'starsports'"""
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
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
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

def _build_m3u(channels: list) -> str:
    lines = ["#EXTM3U"]
    for ch in channels:
        urls = [
            u for u in (ch.get('streamUrls') or [])
            if u and isinstance(u, str) and u.strip()
        ]
        if not urls:
            continue
        name  = safe_str(ch.get('name'),     'Unknown')
        logo  = safe_str(ch.get('logoUrl'),  '')
        cid   = safe_str(ch.get('id'),       '')
        group = safe_str(ch.get('category'), 'Uncategorized')
        lines.append(
            f'#EXTINF:-1 tvg-id="{cid}" tvg-logo="{logo}" '
            f'group-title="{group}",{name}'
        )
        lines.append(urls[0].strip())
    return '\n'.join(lines)

def generate_m3u(json_data: dict, filename: str):
    os.makedirs(PLAYLIST_DIR, exist_ok=True)
    path = os.path.join(PLAYLIST_DIR, filename.replace(".json", ".m3u"))
    try:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(_build_m3u(json_data.get('channels', [])))
        logger.info(f"🎵 M3U: {filename.replace('.json', '.m3u')}")
        STATS["m3u_generated"] += 1
    except Exception as e:
        logger.error(f"❌ M3U failed ({filename}): {e}")

def generate_master_m3u(all_channels: list):
    os.makedirs(PLAYLIST_DIR, exist_ok=True)
    path = os.path.join(PLAYLIST_DIR, "all_channels.m3u")
    try:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(_build_m3u(all_channels))
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

    content = f"""
╔══════════════════════════════════════════════╗
║      IPTV AUTO-UPDATER  —  RUN REPORT        ║
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
#  🚀  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def update_channels():
    logger.info("🚀 IPTV Ultimate Updater v4  —  Large Scale Mode (4300+ channels)")
    logger.info(f"⏱  Max runtime: {MAX_RUNTIME_SECONDS // 3600}h "
                f"{(MAX_RUNTIME_SECONDS % 3600) // 60}m")

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

        # channels_to_keep_map keeps ALL existing channels by default
        # (including ones we skip); only explicitly deleted ones are removed
        channels_to_keep_map = {ch['id']: ch for ch in existing}

        # ── PART 1: Maintain existing channels ────────────────────────────────
        to_process, skipped_count = prioritize_channels(existing, state)
        STATS["skipped_state"] += skipped_count

        for ch in to_process:

            # Per-channel time gate
            if time_remaining() < 120:
                logger.warning("⏰ 2m left — stopping mid-file, saving state.")
                save_state(state)
                break

            STATS["checked"] += 1
            ch_id    = ch.get('id', '')
            ch_state = state.get(ch_id, {})
            now      = time.time()

            # Manual / unknown channel — keep untouched
            if ch_id not in channel_info_map:
                continue

            # Clean out any None / empty stream entries
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

                state[ch_id] = {
                    **ch_state,
                    "stream_ok":    True,
                    "last_checked": now,
                    "fail_count":   0,
                }

            else:
                fail_count = ch_state.get('fail_count', 0) + 1
                state[ch_id] = {
                    **ch_state,
                    "stream_ok":    False,
                    "last_checked": now,
                    "fail_count":   fail_count,
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
                        f"  ⚠️   No stream (fail #{fail_count}): {ch.get('name')}"
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

            # ── Periodic state flush ──────────────────────────────────────────
            global_process_count += 1
            if global_process_count % STATE_SAVE_EVERY == 0:
                save_state(state)
                logger.info(
                    f"  💾 State saved — {global_process_count} processed | "
                    f"{int(time_remaining() // 60)}m left"
                )

        # Rebuild the channel list preserving order
        current_data['channels'] = list(channels_to_keep_map.values())

        # ── PART 2: Discover & add new channels ───────────────────────────────
        if time_remaining() > 300:
            new_candidates = []
            for ch_id in streams_by_id:
                if ch_id in existing_ids:
                    continue
                if ch_id in state:
                    # Already attempted in a previous run (even if failed)
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
                        # Record failure so we don't retry next run immediately
                        state[cid] = {
                            "stream_ok":    False,
                            "last_checked": time.time(),
                            "fail_count":   1,
                            "logo_searched": False,
                        }
                        return None

                    website = details.get('website', '')
                    logo    = (
                        details.get('logo')
                        or find_logo_online(details.get('name', ''), website)
                        or DEFAULT_LOGO
                    )
                    return {
                        "id":         safe_str(details.get('id')),
                        "name":       safe_str(details.get('name'), 'Unknown Channel'),
                        "logoUrl":    safe_str(logo, DEFAULT_LOGO),
                        "streamUrls": [u for u in urls if u and isinstance(u, str)],
                        "category":   rules['category_name'],
                        "languages":  details.get('languages', []),
                        **({"genre": rules['category_name']}
                           if rules['type'] == 'genre' else {}),
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
                            new_channels.append(result)
                            state[result['id']] = {
                                "stream_ok":    True,
                                "last_checked": time.time(),
                                "fail_count":   0,
                                "logo_searched": result['logoUrl'] != DEFAULT_LOGO,
                            }
                            STATS["added"] += 1
                            logger.info(f"  ✅ [NEW] {result['name']}")

                if new_channels:
                    new_channels.sort(key=lambda x: x['name'])
                    current_data['channels'].extend(new_channels)
                    data_modified = True
                    logger.info(f"  📥 Added {len(new_channels)} new channels")

        # ── Save JSON + generate M3U for this category ────────────────────────
        if data_modified:
            create_backup(filepath)
            atomic_save_json(filepath, current_data)

        generate_m3u(current_data, filename)
        all_channels_collection.extend(current_data.get('channels', []))

        # Always save state after finishing a category file
        save_state(state)
        logger.info(
            f"  ✔  Done: {filename} "
            f"({len(current_data['channels'])} channels) | "
            f"{int(time_remaining() // 60)}m left"
        )

    # ── Final outputs ─────────────────────────────────────────────────────────
    if all_channels_collection:
        generate_master_m3u(all_channels_collection)

    save_state(state)
    write_report(state)

    # Progress summary
    total_tracked = len(state)
    total_healthy = sum(1 for v in state.values() if v.get('stream_ok'))
    total_dead    = sum(1 for v in state.values() if not v.get('stream_ok'))

    logger.info(
        f"\n{'═' * 60}\n"
        f"📊  OVERALL PROGRESS\n"
        f"   ✅  Healthy      : {total_healthy}\n"
        f"   ❌  Dead         : {total_dead}\n"
        f"   📦  Total tracked: {total_tracked}\n"
        f"{'═' * 60}"
    )
    logger.info("🎉  All done! Check the 'playlists/' folder for M3U files.")


if __name__ == "__main__":
    update_channels()
