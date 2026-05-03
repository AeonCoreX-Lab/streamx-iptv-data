import json
import requests
import os
import concurrent.futures
import shutil
import time
import logging
import tempfile
import random
from datetime import datetime

# --- ⚙️ CONFIGURATION ---
BASE_DIR = os.getcwd()
CATEGORY_DIR = os.path.join(BASE_DIR, "categories")
BACKUP_DIR = os.path.join(BASE_DIR, "backups")
REPORT_DIR = os.path.join(BASE_DIR, "reports")
PLAYLIST_DIR = os.path.join(BASE_DIR, "playlists")

MAX_BACKUPS_TO_KEEP = 3
MAX_STREAMS_PER_CHANNEL = 3
MAX_REPORTS_TO_KEEP = 5  # Auto-delete old reports

# API Endpoints
STREAMS_API = "https://iptv-org.github.io/api/streams.json"
CHANNELS_API = "https://iptv-org.github.io/api/channels.json"

# Default Assets
DEFAULT_LOGO = "https://raw.githubusercontent.com/iptv-org/api/master/data/categories/no-logo.png"

# Filter Rules
CATEGORY_RULES = {
    "bangladesh.json": {"type": "country", "filter": "BD", "category_name": "Bangladesh"},
    "india.json":      {"type": "country", "filter": "IN", "category_name": "India"},
    "usa.json":        {"type": "country", "filter": "US", "category_name": "USA"},
    "uk.json":         {"type": "country", "filter": "GB", "category_name": "UK"},
    "uae.json":        {"type": "country", "filter": "AE", "category_name": "UAE"},
    "sports.json":     {"type": "genre",   "filter": ["sports"],                          "category_name": "Sports"},
    "kids.json":       {"type": "genre",   "filter": ["kids", "animation"],               "category_name": "Kids"},
    "music.json":      {"type": "genre",   "filter": ["music"],                           "category_name": "Music"},
    "informative.json":{"type": "genre",   "filter": ["documentary","education","science"],"category_name": "Informative"}
}

# --- 🛡️ USER AGENTS ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
]

# --- 📊 STATS ---
STATS = {
    "checked": 0,
    "manual_skipped": 0,
    "repaired": 0,
    "logo_fixed": 0,
    "added": 0,
    "files_updated": 0,
    "m3u_generated": 0
}

# --- 📝 LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger()

# ---------------------------------------------------------------------------
# 🖼️  LOGO SEARCH — No API key required, fully free & unlimited
#
#  Priority order:
#   1. iptv-org API (already embedded in channel_info_map)
#   2. Clearbit Logo API  (company domain → logo, free, no key)
#   3. Brandfetch Community CDN  (free tier, no key needed)
#   4. Wikipedia / Wikimedia Open-Search (public API, no key)
#   5. Google Custom Search fallback via SerpAPI-free proxy (optional)
#   6. DEFAULT_LOGO placeholder
# ---------------------------------------------------------------------------

def _get_headers():
    return {"User-Agent": random.choice(USER_AGENTS)}


def _try_clearbit(channel_name: str) -> str:
    """
    Clearbit Logo API: https://logo.clearbit.com/<domain>
    We guess the domain from the channel name (works great for big networks).
    """
    slug = channel_name.lower()
    slug = slug.replace(" ", "").replace("tv", "").replace("channel", "").replace("hd", "").replace("+", "plus")
    if not slug:
        return ""
    guesses = [
        f"{slug}.com",
        f"{slug}tv.com",
        f"{slug}channel.com",
    ]
    for domain in guesses:
        url = f"https://logo.clearbit.com/{domain}"
        try:
            r = requests.get(url, headers=_get_headers(), timeout=5, allow_redirects=True)
            if r.status_code == 200 and "image" in r.headers.get("Content-Type", ""):
                logger.debug(f"        [Clearbit] ✅ {channel_name} → {url}")
                return url
        except Exception:
            pass
    return ""


def _try_wikimedia(channel_name: str) -> str:
    """
    Wikipedia OpenSearch → first article → page-image via MediaWiki API.
    Completely free, no key.
    """
    try:
        search_url = "https://en.wikipedia.org/w/api.php"
        params = {
            "action": "query",
            "list": "search",
            "srsearch": f"{channel_name} TV channel",
            "srlimit": 1,
            "format": "json"
        }
        r = requests.get(search_url, params=params, headers=_get_headers(), timeout=6)
        results = r.json().get("query", {}).get("search", [])
        if not results:
            return ""

        page_title = results[0]["title"]
        image_params = {
            "action": "query",
            "titles": page_title,
            "prop": "pageimages",
            "pithumbsize": 200,
            "format": "json"
        }
        r2 = requests.get(search_url, params=image_params, headers=_get_headers(), timeout=6)
        pages = r2.json().get("query", {}).get("pages", {})
        for page in pages.values():
            thumb = page.get("thumbnail", {}).get("source", "")
            if thumb:
                logger.debug(f"        [Wiki] ✅ {channel_name} → {thumb}")
                return thumb
    except Exception:
        pass
    return ""


def _try_tvdb_fanart(channel_name: str) -> str:
    """
    TheTVDB / Fanart.tv — open community endpoint, no key for basic logo search.
    Uses the public search proxy at fanart.tv webservice (free tier).
    """
    try:
        url = f"https://webservice.fanart.tv/v3/tv/search?query={requests.utils.quote(channel_name)}"
        r = requests.get(url, headers=_get_headers(), timeout=6)
        if r.status_code == 200:
            data = r.json()
            for item in data:
                logos = item.get("tvbanner") or item.get("hdtvlogo") or item.get("clearlogo")
                if logos and isinstance(logos, list):
                    img = logos[0].get("url", "")
                    if img:
                        return img
    except Exception:
        pass
    return ""


def find_real_logo_online(channel_name: str) -> str:
    """
    Try multiple free logo sources in order. Returns first hit or empty string.
    No API key required for any source.
    """
    if not channel_name:
        return ""

    # 1. Clearbit (works well for commercial/brand channels)
    logo = _try_clearbit(channel_name)
    if logo:
        return logo

    # 2. Wikimedia (works well for public broadcasters)
    logo = _try_wikimedia(channel_name)
    if logo:
        return logo

    # 3. Fanart.tv free tier
    logo = _try_tvdb_fanart(channel_name)
    if logo:
        return logo

    return ""


# ---------------------------------------------------------------------------
# 🛡️  SAFETY & FILE HELPERS
# ---------------------------------------------------------------------------

def cleanup_old_backups():
    if not os.path.exists(BACKUP_DIR):
        return
    all_backups = [f for f in os.listdir(BACKUP_DIR) if f.endswith(".bak")]
    for filename in CATEGORY_RULES.keys():
        file_backups = sorted(f for f in all_backups if f.startswith(f"{filename}_"))
        for old_file in file_backups[:-MAX_BACKUPS_TO_KEEP]:
            try:
                os.remove(os.path.join(BACKUP_DIR, old_file))
            except Exception:
                pass


def cleanup_old_reports():
    """Keep only the N most recent report files."""
    if not os.path.exists(REPORT_DIR):
        return
    reports = sorted(
        [f for f in os.listdir(REPORT_DIR) if f.startswith("report_") and f.endswith(".txt")]
    )
    for old in reports[:-MAX_REPORTS_TO_KEEP]:
        try:
            os.remove(os.path.join(REPORT_DIR, old))
            logger.info(f"🗑️  Deleted old report: {old}")
        except Exception as e:
            logger.warning(f"⚠️ Could not delete old report {old}: {e}")


def create_backup(filepath):
    if not os.path.exists(filepath):
        return
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        shutil.copy2(filepath, os.path.join(BACKUP_DIR, f"{os.path.basename(filepath)}_{timestamp}.bak"))
    except Exception as e:
        logger.warning(f"⚠️ Backup failed: {e}")


def atomic_save_json(filepath, data):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with tempfile.NamedTemporaryFile('w', dir=os.path.dirname(filepath),
                                     delete=False, encoding='utf-8') as tmp:
        json.dump(data, tmp, indent=2, ensure_ascii=False)
        temp_name = tmp.name
    try:
        shutil.move(temp_name, filepath)
        logger.info(f"💾 Saved JSON: {os.path.basename(filepath)}")
        STATS["files_updated"] += 1
    except Exception as e:
        logger.error(f"❌ Save failed: {e}")
        if os.path.exists(temp_name):
            os.remove(temp_name)


def load_json(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {"channels": []}


# ---------------------------------------------------------------------------
# ✅  SAFE STRING HELPER  — root cause of the M3U NoneType crash
# ---------------------------------------------------------------------------

def safe_str(value, default=""):
    """Always return a plain str; never None."""
    if value is None:
        return default
    return str(value).strip()


# ---------------------------------------------------------------------------
# 🎵  M3U GENERATOR  (fixed: guards every field + stream_url)
# ---------------------------------------------------------------------------

def _build_m3u_lines(channels):
    """
    Build a list of M3U text lines from a list of channel dicts.
    Every field is sanitised through safe_str so '\n'.join() never sees None.
    """
    lines = ["#EXTM3U"]
    for ch in channels:
        urls = ch.get('streamUrls') or []
        # Filter out any None / empty entries in the URL list
        urls = [u for u in urls if u and isinstance(u, str) and u.strip()]
        if not urls:
            continue

        stream_url = urls[0].strip()          # guaranteed non-empty str
        name  = safe_str(ch.get('name'),     'Unknown Channel')
        logo  = safe_str(ch.get('logoUrl'),  '')
        cid   = safe_str(ch.get('id'),       '')
        group = safe_str(ch.get('category'), 'Uncategorized')

        extinf = f'#EXTINF:-1 tvg-id="{cid}" tvg-logo="{logo}" group-title="{group}",{name}'
        lines.append(extinf)
        lines.append(stream_url)
    return lines


def generate_m3u_from_json(json_data, filename):
    os.makedirs(PLAYLIST_DIR, exist_ok=True)
    m3u_filename = filename.replace(".json", ".m3u")
    m3u_path = os.path.join(PLAYLIST_DIR, m3u_filename)

    try:
        lines = _build_m3u_lines(json_data.get('channels', []))
        with open(m3u_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        logger.info(f"🎵 Generated M3U: {m3u_filename}")
        STATS["m3u_generated"] += 1
    except Exception as e:
        logger.error(f"❌ M3U Generation failed for {filename}: {e}")


def generate_master_playlist(all_channels):
    os.makedirs(PLAYLIST_DIR, exist_ok=True)
    master_path = os.path.join(PLAYLIST_DIR, "all_channels.m3u")
    try:
        lines = _build_m3u_lines(all_channels)
        with open(master_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        logger.info(f"🌟 Master Playlist generated: all_channels.m3u ({len(all_channels)} channels)")
    except Exception as e:
        logger.error(f"❌ Master M3U failed: {e}")


# ---------------------------------------------------------------------------
# 📄  REPORT
# ---------------------------------------------------------------------------

def write_summary_report():
    os.makedirs(REPORT_DIR, exist_ok=True)
    cleanup_old_reports()   # ← delete old reports BEFORE writing the new one
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    filename = os.path.join(REPORT_DIR, f"report_{timestamp}.txt")
    content = f"""
========================================
   IPTV UPDATE & M3U GENERATOR REPORT
   Date: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
========================================

📊 Statistics:
----------------------------------------
✅ Total Channels Checked : {STATS['checked']}
🛡️ Manual Channels Skipped: {STATS['manual_skipped']}
🩹 Broken Links Repaired  : {STATS['repaired']}
🖼️ Logos Fixed            : {STATS['logo_fixed']}
🆕 New Channels Added     : {STATS['added']}

📂 File Operations:
----------------------------------------
💾 JSON Files Updated     : {STATS['files_updated']}
🎵 M3U Playlists Created  : {STATS['m3u_generated']} + 1 Master

========================================
"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        logger.info(f"📄 Report generated: {filename}")
    except Exception as e:
        logger.error(f"❌ Failed to write report: {e}")


# ---------------------------------------------------------------------------
# 🌐  NETWORK HELPERS
# ---------------------------------------------------------------------------

def check_link_status(url):
    if not url or not isinstance(url, str) or not url.strip():
        return False
    try:
        with requests.get(url.strip(), headers=_get_headers(),
                          stream=True, timeout=(4, 7)) as r:
            return r.status_code == 200
    except Exception:
        return False


def get_multiple_working_streams(channel_id, streams_by_id):
    candidates = streams_by_id.get(channel_id, [])
    if not candidates:
        return []

    check_limit = candidates[:5]
    working_urls = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {
            executor.submit(check_link_status, s.get('url')): s.get('url')
            for s in check_limit
        }
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                if future.result() and url:
                    working_urls.append(url)
                    if len(working_urls) >= MAX_STREAMS_PER_CHANNEL:
                        break
            except Exception:
                pass

    return working_urls


# ---------------------------------------------------------------------------
# 🚀  MAIN
# ---------------------------------------------------------------------------

def update_channels_ultimate():
    logger.info("🚀 Starting Ultimate Channel Updater (JSON + M3U)...")
    cleanup_old_backups()

    # ── Fetch remote data ──────────────────────────────────────────────────
    try:
        logger.info("📡 Fetching IPTV Database...")
        api_streams  = requests.get(STREAMS_API,  timeout=15).json()
        api_channels = requests.get(CHANNELS_API, timeout=15).json()

        channel_info_map = {c['id']: c for c in api_channels}

        streams_by_id: dict = {}
        for s in api_streams:
            if s.get('status') in ('error', 'offline'):
                continue
            cid = s.get('channel')
            url = s.get('url')
            # Guard: skip entries with no URL
            if not cid or not url or not isinstance(url, str) or not url.strip():
                continue
            streams_by_id.setdefault(cid, []).append(s)

    except Exception as e:
        logger.critical(f"❌ API Error: {e}")
        return

    os.makedirs(CATEGORY_DIR, exist_ok=True)
    all_channels_collection = []

    for filename, rules in CATEGORY_RULES.items():
        filepath = os.path.join(CATEGORY_DIR, filename)
        logger.info(f"\n🔍 Processing: {filename}")

        current_data    = load_json(filepath)
        existing_channels = current_data.get('channels', [])
        existing_ids    = {ch['id'] for ch in existing_channels}
        data_modified   = False

        # ── PART 1: Maintenance ────────────────────────────────────────────
        for ch in existing_channels:
            STATS["checked"] += 1
            ch_id = ch.get('id')

            if ch_id not in channel_info_map:
                STATS["manual_skipped"] += 1
                continue

            # Fix broken / missing stream URLs
            current_urls = [u for u in (ch.get('streamUrls') or [])
                            if u and isinstance(u, str) and u.strip()]
            ch['streamUrls'] = current_urls   # clean up any pre-existing None entries

            main_url_dead = not current_urls or not check_link_status(current_urls[0])

            if main_url_dead or len(current_urls) < 2:
                new_urls = get_multiple_working_streams(ch_id, streams_by_id)
                if new_urls and new_urls != current_urls:
                    ch['streamUrls'] = new_urls
                    data_modified = True
                    STATS["repaired"] += 1
                    logger.info(f"     🩹 Streams updated: {ch.get('name')}")

            # Fix missing logos
            current_logo = ch.get('logoUrl', '')
            if not current_logo or current_logo == DEFAULT_LOGO:
                api_logo = channel_info_map.get(ch_id, {}).get('logo', '')
                if api_logo:
                    ch['logoUrl'] = api_logo
                    data_modified = True
                    STATS["logo_fixed"] += 1
                    logger.info(f"     ✅ Logo from API: {ch.get('name')}")
                else:
                    real_logo = find_real_logo_online(ch.get('name', ''))
                    if real_logo:
                        ch['logoUrl'] = real_logo
                        data_modified = True
                        STATS["logo_fixed"] += 1
                        logger.info(f"     ✅ Logo from web: {ch.get('name')}")

        # ── PART 2: Add new channels ───────────────────────────────────────
        streams_to_check = []
        for ch_id, streams in streams_by_id.items():
            if ch_id in existing_ids:
                continue
            ch_details = channel_info_map.get(ch_id)
            if not ch_details:
                continue

            is_match = False
            if rules['type'] == 'country':
                is_match = ch_details.get('country') == rules['filter']
            elif rules['type'] == 'genre':
                api_cats = [c.lower() for c in ch_details.get('categories', [])]
                is_match = any(t.lower() in api_cats for t in rules['filter'])

            if is_match:
                streams_to_check.append(ch_id)

        if streams_to_check:
            logger.info(f"   ⚡ Found {len(streams_to_check)} potential NEW channels...")
            new_channels_list = []

            def process_new_channel(target_ch_id):
                details = channel_info_map.get(target_ch_id)
                working_urls = get_multiple_working_streams(target_ch_id, streams_by_id)
                if working_urls:
                    return (details, working_urls)
                return None

            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(process_new_channel, cid)
                           for cid in streams_to_check]
                for future in concurrent.futures.as_completed(futures):
                    res = future.result()
                    if not res:
                        continue
                    details, urls = res

                    # Prefer API logo → web search → default
                    final_logo = details.get('logo') or ''
                    if not final_logo:
                        final_logo = find_real_logo_online(details.get('name', ''))
                    if not final_logo:
                        final_logo = DEFAULT_LOGO

                    langs = details.get('languages', [])

                    new_channel = {
                        "id":         safe_str(details.get('id')),
                        "name":       safe_str(details.get('name'), 'Unknown Channel'),
                        "logoUrl":    safe_str(final_logo, DEFAULT_LOGO),
                        "streamUrls": [u for u in urls if u and isinstance(u, str)],
                        "category":   rules['category_name'],
                        "languages":  langs
                    }
                    if rules['type'] == 'genre':
                        new_channel["genre"] = rules['category_name']

                    new_channels_list.append(new_channel)
                    STATS["added"] += 1
                    logger.info(f"     ✅ [NEW] {details.get('name')}")

            if new_channels_list:
                new_channels_list.sort(key=lambda x: x['name'])
                current_data['channels'].extend(new_channels_list)
                data_modified = True
                logger.info(f"   📥 Added {len(new_channels_list)} new channels.")

        # ── Save + generate M3U ────────────────────────────────────────────
        if data_modified:
            create_backup(filepath)
            atomic_save_json(filepath, current_data)

        generate_m3u_from_json(current_data, filename)
        all_channels_collection.extend(current_data.get('channels', []))

    # ── Master playlist ────────────────────────────────────────────────────
    if all_channels_collection:
        generate_master_playlist(all_channels_collection)

    write_summary_report()
    logger.info("\n🎉 All updates completed! Check 'playlists' folder for M3U files.")


if __name__ == "__main__":
    update_channels_ultimate()
