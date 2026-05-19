import os
import time
import json
import csv
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# ─── Configuration ────────────────────────────────────────────────────────────
API_KEY      = os.environ.get("PRIMARY_API_KEY")
LOGOS_DIR    = "logos"
REPORTS_DIR  = "reports"
STATE_FILE   = os.path.join(REPORTS_DIR, "scraper_state.json")
MAX_RUNTIME  = 5 * 3600 + 15 * 60    # 5h 15m
WORKERS      = 5
TVDB_DELAY   = 0.6                    # TVDB primary — slightly more delay
GITHUB_DELAY = 0.3                    # GitHub fallback — fast
MAX_RETRIES  = 3

os.makedirs(LOGOS_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# Israel ('il') REMOVED from target countries
TARGET_COUNTRIES   = {'bd', 'us', 'uk', 'gb', 'ae', 'in', 'pk', 'ca', 'au'}
TARGET_CATEGORIES  = {'sports', 'music', 'kids', 'documentary',
                      'education', 'news', 'informative', 'entertainment', 'movies'}

session = requests.Session()
session.headers.update({
    "User-Agent": "AeonCoreX-StreamX-LogoScraper/4.1",
    "Accept": "application/json, image/png, image/jpeg, image/webp"
})

# ─── Country mapping for tv-logo folders (Israel removed) ──────────────────────
COUNTRY_MAP = {
    'us': 'united-states', 'uk': 'united-kingdom', 'gb': 'united-kingdom',
    'in': 'india', 'bd': 'bangladesh', 'ae': 'united-arab-emirates',
    'pk': 'pakistan', 'ca': 'canada', 'au': 'australia', 'de': 'germany',
    'fr': 'france', 'es': 'spain', 'it': 'italy', 'nl': 'netherlands',
    'tr': 'turkey', 'ru': 'russia', 'ua': 'ukraine', 'br': 'brazil',
    'mx': 'mexico', 'jp': 'japan', 'kr': 'south-korea', 'cn': 'china',
    'za': 'south-africa', 'ng': 'nigeria', 'id': 'indonesia', 'sa': 'saudi-arabia',
    'eg': 'egypt', 'pl': 'poland', 'se': 'sweden', 'no': 'norway',
    'fi': 'finland', 'dk': 'denmark', 'be': 'belgium', 'ch': 'switzerland',
    'at': 'austria', 'pt': 'portugal', 'gr': 'greece', 'cz': 'czech-republic',
    'hu': 'hungary', 'ro': 'romania', 'bg': 'bulgaria', 'hr': 'croatia',
    'si': 'slovenia', 'sk': 'slovakia', 'lt': 'lithuania', 'lv': 'latvia',
    'ee': 'estonia', 'ie': 'ireland', 'nz': 'new-zealand', 'sg': 'singapore',
    'my': 'malaysia', 'th': 'thailand', 'ph': 'philippines', 'vn': 'vietnam',
    'cl': 'chile', 'ar': 'argentina', 'co': 'colombia', 'pe': 'peru',
    'qa': 'qatar', 'kw': 'kuwait', 'bh': 'bahrain', 'om': 'oman',
    'jo': 'jordan', 'lb': 'lebanon', 'sy': 'syria', 'iq': 'iraq',
    'ir': 'iran', 'af': 'afghanistan', 'lk': 'sri-lanka', 'np': 'nepal',
    'mm': 'myanmar', 'kh': 'cambodia', 'la': 'laos', 'bn': 'brunei',
    'mo': 'macau', 'hk': 'hong-kong', 'tw': 'taiwan', 'kz': 'kazakhstan',
    'uz': 'uzbekistan', 'tj': 'tajikistan', 'kg': 'kyrgyzstan',
    'tm': 'turkmenistan', 'mn': 'mongolia', 'ge': 'georgia', 'az': 'azerbaijan',
    'am': 'armenia', 'by': 'belarus', 'md': 'moldova', 'al': 'albania',
    'ba': 'bosnia-and-herzegovina', 'mk': 'north-macedonia', 'me': 'montenegro',
    'rs': 'serbia', 'xk': 'kosovo'
}

# ─── State helpers ────────────────────────────────────────────────────────────
def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)

# ─── Report helpers ───────────────────────────────────────────────────────────
def generate_reports(channels_data: list, state: dict, start_time: float):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    json_path = os.path.join(REPORTS_DIR, f"report_{timestamp}.json")
    report = {
        "generated_at": datetime.now().isoformat(),
        "runtime_seconds": round(time.time() - start_time, 2),
        "total_channels": len(channels_data),
        "summary": {
            "success": sum(1 for c in channels_data if c.get('status') == 'done'),
            "failed": sum(1 for c in channels_data if c.get('status') == 'failed'),
            "skipped": sum(1 for c in channels_data if c.get('status') == 'skipped'),
            "already_done": sum(1 for c in channels_data if c.get('status') == 'already_done'),
        },
        "by_source": {},
        "by_country": {},
        "channels": channels_data
    }
    
    for c in channels_data:
        src = c.get('source', 'unknown')
        report["by_source"][src] = report["by_source"].get(src, 0) + 1
        country = c.get('country', 'unknown')
        report["by_country"][country] = report["by_country"].get(country, 0) + 1
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    csv_path = os.path.join(REPORTS_DIR, f"report_{timestamp}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["channel_id", "name", "country", "status", "source", 
                        "file_size_bytes", "error", "processed_at"])
        for c in channels_data:
            writer.writerow([
                c.get('id'), c.get('name'), c.get('country'), 
                c.get('status'), c.get('source'), c.get('file_size', 0),
                c.get('error', ''), c.get('processed_at', '')
            ])
    
    html_path = os.path.join(REPORTS_DIR, f"report_{timestamp}.html")
    total = len(channels_data)
    success = report["summary"]["success"]
    failed = report["summary"]["failed"]
    skipped = report["summary"]["skipped"]
    already = report["summary"]["already_done"]
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Logo Scraper Report {timestamp}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background: #1a1a2e; color: #eee; }}
        h1 {{ color: #00d4aa; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
        .card {{ background: #16213e; padding: 20px; border-radius: 10px; text-align: center; }}
        .card h2 {{ margin: 0; font-size: 2em; }}
        .card p {{ margin: 5px 0 0; color: #aaa; }}
        .success {{ border-left: 4px solid #00d4aa; }}
        .failed {{ border-left: 4px solid #e74c3c; }}
        .skipped {{ border-left: 4px solid #f39c12; }}
        .already {{ border-left: 4px solid #3498db; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 30px; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #333; }}
        th {{ background: #0f3460; color: #fff; }}
        tr:hover {{ background: #1a1a3e; }}
        .badge {{ padding: 4px 12px; border-radius: 12px; font-size: 0.85em; }}
        .bg-success {{ background: #00d4aa33; color: #00d4aa; }}
        .bg-failed {{ background: #e74c3c33; color: #e74c3c; }}
        .bg-skipped {{ background: #f39c1233; color: #f39c12; }}
        .bg-already {{ background: #3498db33; color: #3498db; }}
    </style>
</head>
<body>
    <h1>📺 Logo Scraper Report</h1>
    <p>Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Runtime: {report['runtime_seconds']}s</p>
    <div class="stats">
        <div class="card success"><h2>{success}</h2><p>✅ New Success</p></div>
        <div class="card already"><h2>{already}</h2><p>🔵 Already Done</p></div>
        <div class="card skipped"><h2>{skipped}</h2><p>⏭️ Skipped</p></div>
        <div class="card failed"><h2>{failed}</h2><p>❌ Failed</p></div>
    </table>
    <h2>By Source</h2>
    <table>
        <tr><th>Source</th><th>Count</th></tr>
        {''.join(f'<tr><td>{k}</td><td>{v}</td></tr>' for k,v in report["by_source"].items())}
    </table>
    <h2>By Country (Top 20)</h2>
    <table>
        <tr><th>Country</th><th>Count</th></tr>
        {''.join(f'<tr><td>{k}</td><td>{v}</td></tr>' for k,v in sorted(report["by_country"].items(), key=lambda x: -x[1])[:20])}
    </table>
    <h2>Channel Details (Top 500)</h2>
    <table>
        <tr><th>ID</th><th>Name</th><th>Country</th><th>Status</th><th>Source</th><th>Size</th><th>Error</th></tr>
        {''.join(f'''
        <tr>
            <td>{c.get('id')}</td>
            <td>{c.get('name')}</td>
            <td>{c.get('country')}</td>
            <td><span class="badge bg-{c.get('status')}">{c.get('status')}</span></td>
            <td>{c.get('source', '-')}</td>
            <td>{c.get('file_size', 0)}</td>
            <td>{c.get('error', '-')}</td>
        </tr>
        ''' for c in channels_data[:500])}
    </table>
    <p style="margin-top:20px;color:#666;">Showing top 500. See CSV/JSON for full data.</p>
</body>
</html>"""
    
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    
    print(f"\n📊 Reports saved:")
    print(f"   JSON: {json_path}")
    print(f"   CSV:  {csv_path}")
    print(f"   HTML: {html_path}")

# ─── Channel fetching ─────────────────────────────────────────────────────────
def get_iptv_channels() -> list:
    print("📡 Fetching channels from iptv-org...")
    try:
        r = session.get("https://iptv-org.github.io/api/channels.json", timeout=30)
        if r.status_code == 200:
            data = r.json()
            print(f"✅ Total channels fetched: {len(data)}")
            return data
        print(f"❌ Failed: HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"❌ Channel fetch error: {e}")
        return []

def is_target_channel(ch: dict) -> bool:
    country    = ch.get('country', '').lower()
    categories = {c.lower() for c in ch.get('categories', [])}
    return country in TARGET_COUNTRIES or bool(categories & TARGET_CATEGORIES)

# ─── Smart Skip Logic ─────────────────────────────────────────────────────────
def should_skip(channel: dict, state: dict) -> tuple[bool, str, int]:
    cid = channel.get('id', '')
    if not cid:
        return True, "no_id", 0
    
    if state.get(cid) == 'done':
        return True, "state_done", 0
    
    logo_path = os.path.join(LOGOS_DIR, f"{cid}.png")
    if os.path.exists(logo_path):
        try:
            size = os.path.getsize(logo_path)
            if size > 200:
                if state.get(cid) != 'done':
                    state[cid] = 'done'
                return True, "file_exists", size
            else:
                os.remove(logo_path)
                return False, "corrupt_removed", 0
        except:
            return False, "file_error", 0
    
    return False, "needs_processing", 0

# ─── TVDB API — PRIMARY SOURCE ───────────────────────────────────────────────
def get_tvdb_token() -> str | None:
    if not API_KEY:
        print("⚠️  TVDB API Key not set — will use GitHub fallback only")
        return None
    try:
        r = session.post("https://api4.thetvdb.com/v4/login",
                         json={"apikey": API_KEY}, timeout=15)
        if r.status_code == 200:
            token = r.json().get('data', {}).get('token')
            if token:
                print("🔑 TVDB: Authentication successful")
                return token
        print(f"❌ TVDB Auth failed: HTTP {r.status_code}")
    except Exception as e:
        print(f"❌ TVDB auth error: {e}")
    return None

def clean_name_for_tvdb(name: str) -> list[str]:
    """Generate multiple search variations for TVDB"""
    if not name:
        return []
    
    variations = [name]
    
    # Remove country codes like (US), [UK], .ua, .de
    cleaned = re.sub(r'[\(\[]\s*[A-Z]{2}\s*[\)\]]', '', name)
    cleaned = re.sub(r'\.\w{2}$', '', cleaned)
    cleaned = re.sub(r'\.\w{2}\.', '.', cleaned)
    
    base = cleaned.strip()
    if base != name:
        variations.append(base)
    
    # Remove common suffixes
    suffixes = ['TV', 'Channel', 'Television', 'Network', 'HD', 'News', 
                'Live', 'Online', 'Digital', 'Satellite', '24', 'Plus']
    for suffix in suffixes:
        pattern = r'\s+' + re.escape(suffix) + r'$'
        no_suffix = re.sub(pattern, '', base, flags=re.IGNORECASE).strip()
        if no_suffix and no_suffix != base and no_suffix not in variations:
            variations.append(no_suffix)
    
    # Remove special chars
    simple = re.sub(r'[^\w\s]', '', base).strip()
    if simple and simple != base and simple not in variations:
        variations.append(simple)
    
    # Remove numbers at start
    no_num = re.sub(r'^\d+\s*', '', base).strip()
    if no_num and no_num != base and no_num not in variations:
        variations.append(no_num)
    
    return list(dict.fromkeys(variations))

def search_tvdb_logo(name: str, token: str) -> str | None:
    if not token or not name:
        return None
    
    search_names = clean_name_for_tvdb(name)
    
    for attempt, query in enumerate(search_names):
        try:
            url = "https://api4.thetvdb.com/v4/search"
            params = {"query": query, "type": "company", "limit": 5}
            headers = {"Authorization": f"Bearer {token}"}
            
            r = session.get(url, params=params, headers=headers, timeout=15)
            
            if r.status_code == 200:
                data = r.json().get('data', [])
                if data:
                    for item in data:
                        img_url = item.get('image_url') or item.get('image')
                        if img_url and isinstance(img_url, str) and img_url.startswith('http'):
                            return img_url
                else:
                    print(f"   ⚠️  TVDB: No results for '{query}'")
            elif r.status_code == 401:
                print(f"   ❌ TVDB: Token expired")
                return None
            elif r.status_code == 429:
                print(f"   ⏳ TVDB: Rate limited. Waiting 2s...")
                time.sleep(2)
                if attempt == 0:
                    continue
            else:
                print(f"   ⚠️  TVDB: HTTP {r.status_code} for '{query}'")
        except Exception as e:
            print(f"   ⚠️  TVDB search error ({query}): {e}")
        
        if attempt < len(search_names) - 1:
            time.sleep(0.3)
    
    return None

# ─── tv-logo/tv-logos (GitHub) — FALLBACK SOURCE ─────────────────────────────
def get_tvlogos_url(channel_name: str, country_code: str) -> str | None:
    if not channel_name:
        return None
    
    folder = COUNTRY_MAP.get(country_code.lower(), country_code.lower())
    base_url = f"https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/{folder}"
    
    clean = channel_name.lower()
    clean = clean.replace('&', 'and')
    clean = clean.replace('+', 'plus')
    clean = re.sub(r'[^\w\s-]', '', clean)
    clean = clean.replace(' ', '-')
    clean = re.sub(r'-+', '-', clean).strip('-')
    
    cc = country_code.lower()
    
    variations = [
        f"{clean}-{cc}.png",
        f"{clean}.png",
    ]
    
    suffixes = ['tv', 'channel', 'hd', 'news', 'sports', 'music', 'kids']
    base_name = clean
    for suffix in suffixes:
        if base_name.endswith(f'-{suffix}'):
            base_name = base_name[:-len(suffix)-1]
            variations.append(f"{base_name}-{cc}.png")
            variations.append(f"{base_name}.png")
    
    no_num = re.sub(r'^\d+-?', '', clean).strip('-')
    if no_num and no_num != clean:
        variations.append(f"{no_num}-{cc}.png")
        variations.append(f"{no_num}.png")
    
    seen = set()
    unique_vars = []
    for v in variations:
        if v not in seen:
            seen.add(v)
            unique_vars.append(v)
    
    for var in unique_vars:
        url = f"{base_url}/{var}"
        try:
            r = session.head(url, timeout=8, allow_redirects=True)
            if r.status_code == 200:
                return url
        except Exception:
            pass
    
    return None

# ─── Download with validation ───────────────────────────────────────────────
def download_and_save(url: str, save_path: str) -> tuple[bool, int]:
    try:
        r = session.get(url, stream=True, timeout=20)
        if r.status_code != 200:
            return False, 0
        
        ct = r.headers.get('content-type', '')
        if 'image' not in ct and 'octet-stream' not in ct:
            return False, 0
        
        temp = save_path + ".tmp"
        with open(temp, 'wb') as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        
        size = os.path.getsize(temp)
        if size < 200:
            os.remove(temp)
            return False, 0
        
        os.replace(temp, save_path)
        return True, size
    except Exception:
        temp = save_path + ".tmp"
        if os.path.exists(temp):
            os.remove(temp)
        return False, 0

# ─── Per-channel worker ──────────────────────────────────────────────────────
def process_channel(channel: dict, tvdb_token: str, state: dict) -> dict:
    cid     = channel.get('id', '')
    name    = channel.get('name', '')
    country = channel.get('country', '').lower()
    
    result = {
        'id': cid,
        'name': name,
        'country': country,
        'status': 'failed',
        'source': 'none',
        'file_size': 0,
        'error': '',
        'processed_at': datetime.now().isoformat()
    }
    
    skip, reason, size = should_skip(channel, state)
    if skip:
        result['status'] = 'already_done' if reason == 'state_done' else 'skipped'
        result['source'] = reason
        result['file_size'] = size
        return result
    
    print(f"🔍 [{cid}] {name} ({country})")
    
    # ─── STAGE 1: TVDB (Primary) ────────────────────────────────────────
    if tvdb_token:
        img_url = search_tvdb_logo(name, tvdb_token)
        if img_url:
            time.sleep(TVDB_DELAY)
            save_path = os.path.join(LOGOS_DIR, f"{cid}.png")
            ok, size = download_and_save(img_url, save_path)
            if ok:
                result['status'] = 'done'
                result['source'] = 'tvdb-api'
                result['file_size'] = size
                print(f"   ✅ TVDB: {cid}.png ({size} bytes)")
                return result
    
    # ─── STAGE 2: tv-logo/tv-logos GitHub (Fallback) ────────────────────
    img_url = get_tvlogos_url(name, country)
    if img_url:
        time.sleep(GITHUB_DELAY)
        save_path = os.path.join(LOGOS_DIR, f"{cid}.png")
        ok, size = download_and_save(img_url, save_path)
        if ok:
            result['status'] = 'done'
            result['source'] = 'tv-logos-github'
            result['file_size'] = size
            print(f"   ✅ GitHub: {cid}.png ({size} bytes)")
            return result
    
    # ─── FAIL ──────────────────────────────────────────────────────────
    result['error'] = 'not_found_any_source'
    print(f"   ❌ MISS: {cid}")
    return result

# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    start_time = time.time()
    print("=" * 60)
    print("🚀 AeonCoreX Smart Logo Scraper v4.1")
    print("   Primary: TVDB API")
    print("   Fallback: tv-logo/tv-logos (GitHub)")
    print("   Israel: REMOVED")
    print("=" * 60)
    
    state    = load_state()
    channels = get_iptv_channels()
    
    if not channels:
        print("❌ No channels loaded.")
        return
    
    targets = []
    skipped_sync = 0
    
    for c in channels:
        if not is_target_channel(c):
            continue
        skip, reason, _ = should_skip(c, state)
        if skip:
            skipped_sync += 1
            continue
        targets.append(c)
    
    print(f"📊 Target channels: {len(targets)} | Already done: {skipped_sync}")
    
    tvdb_token = get_tvdb_token()
    if not tvdb_token:
        print("⚠️  Running in FALLBACK MODE (GitHub only)")
    
    channels_data = []
    processed = 0
    state_dirty = False
    
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(process_channel, ch, tvdb_token, state): ch 
            for ch in targets
        }
        
        for future in as_completed(futures):
            elapsed = time.time() - start_time
            if elapsed >= MAX_RUNTIME:
                print(f"\n⏰ Time limit! Saving...")
                executor.shutdown(wait=False, cancel_futures=True)
                break
            
            try:
                result = future.result()
            except Exception as e:
                print(f"   💥 Worker crash: {e}")
                continue
            
            channels_data.append(result)
            cid = result['id']
            status = result['status']
            
            if status == 'done':
                state[cid] = 'done'
                state_dirty = True
            elif status == 'failed':
                if state.get(cid) != 'done':
                    state[cid] = 'failed'
                    state_dirty = True
            
            processed += 1
            if processed % 25 == 0:
                save_state(state)
                state_dirty = False
                done_cnt = sum(1 for c in channels_data if c['status'] == 'done')
                print(f"💾 Checkpoint @ {processed} (Success: {done_cnt})")
    
    if state_dirty:
        save_state(state)
    
    generate_reports(channels_data, state, start_time)
    
    total_png = len([f for f in os.listdir(LOGOS_DIR) if f.endswith('.png')])
    print(f"\n{'='*60}")
    print(f"🏁 FINISHED")
    print(f"   This run:    {processed} channels")
    print(f"   New logos:     {sum(1 for c in channels_data if c['status'] == 'done')}")
    print(f"   Failed:        {sum(1 for c in channels_data if c['status'] == 'failed')}")
    print(f"   Skipped:       {sum(1 for c in channels_data if c['status'] in ('skipped', 'already_done'))}")
    print(f"   Total PNGs:    {total_png}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()