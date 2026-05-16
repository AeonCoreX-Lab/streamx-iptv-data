import os
import time
import json
import requests
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Configuration ────────────────────────────────────────────────────────────
API_KEY      = os.environ.get("PRIMARY_API_KEY")
LOGOS_DIR    = "logos"
STATE_FILE   = "scraper_state.json"   # tracks every attempted channel
MAX_RUNTIME  = 5 * 3600 + 15 * 60    # 5h 15m  → safely under GitHub's 6h kill
WORKERS      = 5                      # parallel download threads
RATE_DELAY   = 0.3                    # seconds between API calls per thread

os.makedirs(LOGOS_DIR, exist_ok=True)

TARGET_COUNTRIES   = {'bd', 'us', 'uk', 'gb', 'ae', 'in'}
TARGET_CATEGORIES  = {'sports', 'music', 'kids', 'documentary',
                      'education', 'news', 'informative'}

session = requests.Session()
session.headers.update({"User-Agent": "AeonCoreX-StreamX-LogoScraper/2.0"})

# ─── State helpers ────────────────────────────────────────────────────────────
def load_state() -> dict:
    """Returns {channel_id: 'done'|'failed'|'skipped'}"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

# ─── Channel fetching ─────────────────────────────────────────────────────────
def get_iptv_channels() -> list:
    print("Fetching channels from iptv-org...")
    try:
        r = session.get("https://iptv-org.github.io/api/channels.json", timeout=30)
        return r.json() if r.status_code == 200 else []
    except Exception as e:
        print(f"Channel fetch error: {e}")
        return []

def is_target_channel(ch: dict) -> bool:
    country    = ch.get('country', '').lower()
    categories = {c.lower() for c in ch.get('categories', [])}
    return country in TARGET_COUNTRIES or bool(categories & TARGET_CATEGORIES)

# ─── API auth ─────────────────────────────────────────────────────────────────
def get_tvdb_token() -> str | None:
    if not API_KEY:
        return None
    try:
        r = session.post("https://api4.thetvdb.com/v4/login",
                         json={"apikey": API_KEY}, timeout=15)
        if r.status_code == 200:
            return r.json().get('data', {}).get('token')
    except Exception as e:
        print(f"TVDB auth error: {e}")
    return None

# ─── Logo sources ─────────────────────────────────────────────────────────────
def search_primary_api(name: str, token: str) -> str | None:
    if not token:
        return None
    try:
        url     = f"https://api4.thetvdb.com/v4/search?query={name}&type=company"
        headers = {"Authorization": f"Bearer {token}"}
        r       = session.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json().get('data', [])
            if data:
                return data[0].get('image_url')
    except Exception as e:
        print(f"  Primary API error ({name}): {e}")
    return None

def search_clearbit(website: str) -> str | None:
    if not website:
        return None
    try:
        domain = urlparse(website).netloc
        if domain:
            url = f"https://logo.clearbit.com/{domain}"
            r   = session.head(url, timeout=8)
            if r.status_code == 200:
                return url
    except Exception as e:
        print(f"  Clearbit error: {e}")
    return None

def search_duckduckgo(name: str) -> str | None:
    try:
        url  = (f"https://api.duckduckgo.com/?q={name}+tv+channel+logo"
                f"&format=json&pretty=1")
        r    = session.get(url, timeout=10).json()
        path = r.get("Image")
        if path:
            return f"https://duckduckgo.com{path}" if path.startswith("/") else path
    except Exception as e:
        print(f"  DuckDuckGo error ({name}): {e}")
    return None

# ─── Download ─────────────────────────────────────────────────────────────────
def download_and_save(url: str, save_path: str) -> bool:
    try:
        r = session.get(url, stream=True, timeout=15)
        if r.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)
            return True
    except Exception:
        pass
    return False

# ─── Per-channel worker ───────────────────────────────────────────────────────
def process_channel(channel: dict, token: str, state: dict) -> tuple[str, str]:
    """Returns (channel_id, result_status)"""
    cid     = channel.get('id', '')
    name    = channel.get('name', '')
    website = channel.get('website', '')

    # Already done in a previous run?
    if state.get(cid) == 'done':
        return cid, 'already_done'

    save_path = os.path.join(LOGOS_DIR, f"{cid}.png")
    if os.path.exists(save_path):
        return cid, 'already_done'

    print(f"Processing: {name} ({cid})")

    img_url, source = None, ""

    img_url = search_primary_api(name, token)
    if img_url:
        source = "Primary API"

    if not img_url:
        img_url = search_clearbit(website)
        if img_url:
            source = "Clearbit"

    if not img_url:
        img_url = search_duckduckgo(name)
        if img_url:
            source = "DuckDuckGo"

    time.sleep(RATE_DELAY)  # lightweight rate limit

    if img_url and download_and_save(img_url, save_path):
        print(f"  [OK] {cid}.png  ({source})")
        return cid, 'done'

    print(f"  [MISS] {cid}")
    return cid, 'failed'

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    start_time = time.time()

    state    = load_state()
    channels = get_iptv_channels()
    targets  = [c for c in channels if is_target_channel(c)
                and c.get('id') and c.get('name')
                and state.get(c['id']) != 'done'
                and not os.path.exists(os.path.join(LOGOS_DIR, f"{c['id']}.png"))]

    print(f"Channels remaining: {len(targets)}  |  Already done: {len(state)}")

    token = get_tvdb_token()
    print("TVDB token:", "OK" if token else "NOT available — using Clearbit/DDG only")

    processed = 0
    state_dirty = False

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(process_channel, ch, token, state): ch
                   for ch in targets}

        for future in as_completed(futures):
            # Hard time-gate — commit what we have before GitHub kills the job
            elapsed = time.time() - start_time
            if elapsed >= MAX_RUNTIME:
                print(f"\nTime limit reached ({elapsed/3600:.2f}h). Saving state…")
                executor.shutdown(wait=False, cancel_futures=True)
                break

            cid, status = future.result()
            if status in ('done', 'failed'):
                state[cid]   = status
                state_dirty  = True
                processed   += 1

                # Flush state every 25 channels to survive unexpected kills
                if processed % 25 == 0:
                    save_state(state)
                    print(f"  [STATE SAVED] {processed} processed this run")

    if state_dirty:
        save_state(state)

    remaining = sum(1 for c in channels
                    if is_target_channel(c) and state.get(c.get('id')) != 'done')
    print(f"\nDone. This run: {processed}  |  Still remaining: {remaining}")

if __name__ == "__main__":
    main()