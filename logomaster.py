import os
import time
import json
import csv
import re
import struct
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse

# ─── Configuration ────────────────────────────────────────────────────────────
LOGO_DEV_KEY = os.environ.get("LOGO_DEV_KEY")      # Logo.dev API key (500K/month free)
API_KEY      = os.environ.get("PRIMARY_API_KEY")     # TVDB API key (optional fallback)
LOGOS_DIR    = "logos"
REPORTS_DIR  = "reports"
STATE_FILE   = "scraper_state.json"
MAPPING_FILE = "channel_logo_mapping.json"
MAX_RUNTIME  = 5 * 3600 + 10 * 60
WORKERS      = 6
LOGO_DEV_DELAY = 0.3                  # Fast - official API (500K/month free tier)
TVDB_DELAY   = 0.5
FALLBACK_DELAY = 0.25
MAX_RETRIES  = 3

os.makedirs(LOGOS_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

session = requests.Session()
session.headers.update({
    "User-Agent": "AeonCoreX-WorldLogo-Scraper/8.0",
    "Accept": "application/json, image/png, image/jpeg, image/webp, image/*"
})

# ─── Country mapping (Israel removed) ─────────────────────────────────────────
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
    'rs': 'serbia', 'xk': 'kosovo', 'cd': 'democratic-republic-of-the-congo',
    'ly': 'libya', 'bo': 'bolivia', 'ec': 'ecuador', 'gt': 'guatemala',
    'hn': 'honduras', 'sv': 'el-salvador', 'ni': 'nicaragua', 'cr': 'costa-rica',
    'pa': 'panama', 'uy': 'uruguay', 'py': 'paraguay', 've': 'venezuela',
    'gy': 'guyana', 'sr': 'suriname', 'gf': 'french-guiana', 'is': 'iceland',
    'mt': 'malta', 'cy': 'cyprus', 'lu': 'luxembourg', 'li': 'liechtenstein',
    'mc': 'monaco', 'ad': 'andorra', 'sm': 'san-marino', 'va': 'vatican-city',
    'fo': 'faroe-islands', 'gl': 'greenland', 'ax': 'aland-islands',
    'sj': 'svalbard', 'gi': 'gibraltar', 'im': 'isle-of-man', 'je': 'jersey',
    'gg': 'guernsey', 'bm': 'bermuda', 'ky': 'cayman-islands', 'bs': 'bahamas',
    'cu': 'cuba', 'jm': 'jamaica', 'ht': 'haiti', 'do': 'dominican-republic',
    'pr': 'puerto-rico', 'tt': 'trinidad-and-tobago', 'bb': 'barbados',
    'gd': 'grenada', 'lc': 'saint-lucia', 'vc': 'saint-vincent',
    'ag': 'antigua-and-barbuda', 'kn': 'saint-kitts', 'dm': 'dominica',
    'ms': 'montserrat', 'ai': 'anguilla', 'vg': 'british-virgin-islands',
    'tc': 'turks-and-caicos', 'fk': 'falkland-islands', 'sh': 'saint-helena',
    'ac': 'ascension', 'ta': 'tristan-da-cunha', 'pn': 'pitcairn', 'nu': 'niue',
    'tk': 'tokelau', 'wf': 'wallis-and-futuna', 'ws': 'samoa', 'to': 'tonga',
    'ki': 'kiribati', 'nr': 'nauru', 'tv': 'tuvalu', 'fm': 'micronesia',
    'mh': 'marshall-islands', 'pw': 'palau', 'ck': 'cook-islands',
    'pf': 'french-polynesia', 'nc': 'new-caledonia', 'pm': 'saint-pierre',
    'gp': 'guadeloupe', 'mq': 'martinique', 're': 'reunion', 'yt': 'mayotte',
    'tf': 'french-southern', 'io': 'british-indian-ocean', 'cc': 'cocos-islands',
    'cx': 'christmas-island', 'nf': 'norfolk-island', 'hm': 'heard-island',
    'aq': 'antarctica', 'bv': 'bouvet-island', 'gs': 'south-georgia',
    'um': 'us-minor-outlying', 'as': 'american-samoa', 'gu': 'guam',
    'mp': 'northern-mariana', 'vi': 'us-virgin-islands'
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
    s = report["summary"]

    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<title>World Logo Report {timestamp}</title>
<style>
body{{font-family:Arial,sans-serif;margin:40px;background:#0f0f23;color:#eee}}
h1{{color:#00ff88}} .stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:15px;margin:20px 0}}
.card{{background:#1a1a3e;padding:20px;border-radius:12px;text-align:center;border-left:4px solid #444}}
.card h2{{margin:0;font-size:2.2em}} .card p{{margin:8px 0 0;color:#aaa;font-size:0.9em}}
.success{{border-color:#00ff88}} .failed{{border-color:#ff4757}} .skipped{{border-color:#ffa502}} .already{{border-color:#3742fa}}
table{{width:100%;border-collapse:collapse;margin-top:25px;font-size:0.9em}}
th,td{{padding:10px;text-align:left;border-bottom:1px solid #333}}
th{{background:#2f3542;color:#fff;position:sticky;top:0}}
tr:hover{{background:#1e1e3f}} .badge{{padding:3px 10px;border-radius:10px;font-size:0.8em}}
.bg-success{{background:#00ff8833;color:#00ff88}} .bg-failed{{background:#ff475733;color:#ff4757}}
.bg-skipped{{background:#ffa50233;color:#ffa502}} .bg-already{{background:#3742fa33;color:#3742fa}}
</style></head><body>
<h1>🌍 World Channel Logo Report</h1>
<p>Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Runtime: {report['runtime_seconds']}s</p>
<div class="stats">
<div class="card success"><h2>{s['success']}</h2><p>✅ New Success</p></div>
<div class="card already"><h2>{s['already_done']}</h2><p>🔵 Already Done</p></div>
<div class="card skipped"><h2>{s['skipped']}</h2><p>⏭️ Skipped</p></div>
<div class="card failed"><h2>{s['failed']}</h2><p>❌ Failed</p></div>
</div>
<h2>By Source</h2><table><tr><th>Source</th><th>Count</th></tr>
{''.join(f'<tr><td>{k}</td><td>{v}</td></tr>' for k,v in report["by_source"].items())}
</table>
<h2>By Country (Top 25)</h2><table><tr><th>Country</th><th>Count</th></tr>
{''.join(f'<tr><td>{k}</td><td>{v}</td></tr>' for k,v in sorted(report["by_country"].items(), key=lambda x: -x[1])[:25])}
</table>
<h2>Recent 300 Channels</h2><table><tr><th>ID</th><th>Name</th><th>Country</th><th>Status</th><th>Source</th><th>Size</th></tr>
{''.join(f'<tr><td>{c.get("id")}</td><td>{c.get("name")}</td><td>{c.get("country")}</td><td><span class="badge bg-{c.get("status")}">{c.get("status")}</span></td><td>{c.get("source","-")}</td><td>{c.get("file_size",0)}</td></tr>' for c in channels_data[-300:])}
</table></body></html>"""

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n📊 Reports: JSON={json_path}, CSV={csv_path}, HTML={html_path}")

def generate_mapping(state: dict):
    flat_mapping = {}
    country_mapping = {}
    total_count = 0

    for country_code in os.listdir(LOGOS_DIR):
        country_path = os.path.join(LOGOS_DIR, country_code)
        if not os.path.isdir(country_path):
            continue
        country_mapping[country_code] = {}
        for fname in os.listdir(country_path):
            if fname.endswith('.png'):
                cid = fname.replace('.png', '')
                rel_path = f"logos/{country_code}/{fname}"
                flat_mapping[cid] = rel_path
                country_mapping[country_code][cid] = rel_path
                total_count += 1

    smart_mapping = {
        "version": "2.0",
        "generated_at": datetime.now().isoformat(),
        "total_logos": total_count,
        "countries": len(country_mapping),
        "flat": flat_mapping,
        "by_country": country_mapping
    }

    with open(MAPPING_FILE, "w", encoding="utf-8") as f:
        json.dump(smart_mapping, f, indent=2, ensure_ascii=False)

    print(f"🗺️  Mapping: {total_count} logos across {len(country_mapping)} countries → {MAPPING_FILE}")

    legacy_file = MAPPING_FILE.replace('.json', '_legacy.json')
    with open(legacy_file, "w", encoding="utf-8") as f:
        json.dump(flat_mapping, f, indent=2, ensure_ascii=False)
    print(f"🗺️  Legacy mapping: {len(flat_mapping)} entries → {legacy_file}")

# ─── Load iptv-org logos.json ─────────────────────────────────────────────────
def load_iptvorg_logos() -> dict:
    print("📡 Loading iptv-org logos database...")
    try:
        r = session.get("https://iptv-org.github.io/api/logos.json", timeout=30)
        if r.status_code == 200:
            data = r.json()
            mapping = {}
            for item in data:
                cid = item.get('channel', '')
                url = item.get('url', '')
                if cid and url:
                    mapping[cid] = url
            print(f"✅ iptv-org logos: {len(mapping)} entries")
            return mapping
        print(f"⚠️  iptv-org logos failed: HTTP {r.status_code}")
    except Exception as e:
        print(f"⚠️  iptv-org logos error: {e}")
    return {}

# ─── Channel fetching ─────────────────────────────────────────────────────────
def get_iptv_channels() -> list:
    print("📡 Fetching ALL world channels from iptv-org...")
    try:
        r = session.get("https://iptv-org.github.io/api/channels.json", timeout=60)
        if r.status_code == 200:
            data = r.json()
            print(f"✅ Total channels: {len(data)}")
            return data
        print(f"❌ Failed: HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"❌ Channel fetch error: {e}")
        return []

# ─── Smart Skip Logic ─────────────────────────────────────────────────────────
def should_skip(channel: dict, state: dict) -> tuple[bool, str, int]:
    cid = channel.get('id', '')
    country = channel.get('country', '').lower()
    if not cid:
        return True, "no_id", 0

    if state.get(cid) == 'done':
        return True, "state_done", 0

    logo_path = os.path.join(LOGOS_DIR, country, f"{cid}.png")
    if os.path.exists(logo_path):
        try:
            size = os.path.getsize(logo_path)
            if size < 200:
                os.remove(logo_path)
                print(f"   🗑️  Removed corrupt file: {cid}.png ({size} bytes)")
                return False, "corrupt_removed", 0

            is_valid, actual_size = validate_image_file(logo_path)
            if is_valid:
                if state.get(cid) != 'done':
                    state[cid] = 'done'
                return True, "file_exists", actual_size
            else:
                os.remove(logo_path)
                print(f"   🗑️  Removed invalid image: {cid}.png")
                return False, "invalid_removed", 0

        except Exception as e:
            print(f"   ⚠️  File check error {cid}: {e}")
            return False, "file_error", 0

    return False, "needs_processing", 0

# ─── BLANK FILE PROTECTION: Deep image validation ─────────────────────────────
def validate_image_file(filepath: str) -> tuple[bool, int]:
    try:
        size = os.path.getsize(filepath)
        if size < 200:
            return False, size

        with open(filepath, 'rb') as f:
            header = f.read(32)

        is_png = header[:8] == b'\x89PNG\r\n\x1a\n'
        is_jpg = header[:3] == b'\xff\xd8\xff'
        is_webp = header[:4] == b'RIFF' and header[8:12] == b'WEBP'
        is_gif = header[:6] in (b'GIF87a', b'GIF89a')
        is_bmp = header[:2] == b'BM'
        is_svg = b'<svg' in header[:200] or header[:5] == b'<?xml'

        if not any([is_png, is_jpg, is_webp, is_gif, is_bmp, is_svg]):
            return False, size

        if is_png:
            try:
                f = open(filepath, 'rb')
                f.read(8)
                chunk_len = struct.unpack('>I', f.read(4))[0]
                chunk_type = f.read(4)
                if chunk_type == b'IHDR':
                    width = struct.unpack('>I', f.read(4))[0]
                    height = struct.unpack('>I', f.read(4))[0]
                    if width < 2 or height < 2:
                        f.close()
                        return False, size
                f.close()
            except Exception:
                pass

        if is_jpg:
            try:
                f = open(filepath, 'rb')
                f.seek(2)
                while True:
                    marker = f.read(2)
                    if len(marker) < 2:
                        break
                    while marker[0] == 0xFF:
                        if marker[1] != 0xFF:
                            break
                        marker = bytes([marker[1]]) + f.read(1)

                    if marker[0] != 0xFF:
                        break

                    if 0xC0 <= marker[1] <= 0xCF and marker[1] not in (0xC4, 0xC8, 0xCC):
                        f.read(3)
                        height = struct.unpack('>H', f.read(2))[0]
                        width = struct.unpack('>H', f.read(2))[0]
                        f.close()
                        if width < 2 or height < 2:
                            return False, size
                        break
                    else:
                        length_bytes = f.read(2)
                        if len(length_bytes) < 2:
                            break
                        length = struct.unpack('>H', length_bytes)[0]
                        f.seek(length - 2, 1)
                f.close()
            except Exception:
                pass

        return True, size

    except Exception:
        return False, 0

# ─── Download with BLANK FILE PROTECTION ──────────────────────────────────────
def download_and_save(url: str, save_path: str) -> tuple[bool, int]:
    temp_path = save_path + ".tmp"
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    try:
        if os.path.exists(temp_path):
            os.remove(temp_path)
    except:
        pass

    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, stream=True, timeout=30)
            if r.status_code != 200:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
                return False, 0

            ct = r.headers.get('content-type', '').lower()
            if not any(x in ct for x in ['image', 'octet-stream', 'png', 'jpeg', 'jpg', 'webp', 'gif', 'svg']):
                return False, 0

            content = BytesIO()
            total_size = 0
            for chunk in r.iter_content(8192):
                content.write(chunk)
                total_size += len(chunk)
                if total_size > 10 * 1024 * 1024:
                    return False, 0

            data = content.getvalue()
            size = len(data)

            if size < 200:
                return False, 0

            header = data[:32]
            is_png = header[:8] == b'\x89PNG\r\n\x1a\n'
            is_jpg = header[:3] == b'\xff\xd8\xff'
            is_webp = header[:4] == b'RIFF' and header[8:12] == b'WEBP'
            is_gif = header[:6] in (b'GIF87a', b'GIF89a')
            is_svg = b'<svg' in data[:200] or data[:5] == b'<?xml'

            if not any([is_png, is_jpg, is_webp, is_gif, is_svg]):
                return False, 0

            if is_png:
                try:
                    ihdr_start = data.find(b'IHDR')
                    if ihdr_start != -1:
                        width = struct.unpack('>I', data[ihdr_start+4:ihdr_start+8])[0]
                        height = struct.unpack('>I', data[ihdr_start+8:ihdr_start+12])[0]
                        if width < 2 or height < 2:
                            return False, 0
                except Exception:
                    pass

            with open(temp_path, 'wb') as f:
                f.write(data)

            is_valid, final_size = validate_image_file(temp_path)
            if not is_valid:
                os.remove(temp_path)
                return False, 0

            os.replace(temp_path, save_path)
            return True, final_size

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)

    try:
        if os.path.exists(temp_path):
            os.remove(temp_path)
    except:
        pass

    return False, 0

# ─── STAGE 1: Logo.dev API — PRIMARY SOURCE ───────────────────────────────────
def get_logo_dev_url(channel_name: str, website: str = None, logo_dev_key: str = None) -> str | None:
    """
    Logo.dev API - Official brand logo API
    Free tier: 500,000 API requests/month
    https://logo.dev
    """
    if not logo_dev_key:
        return None

    domains_to_try = []

    if website:
        try:
            parsed = urlparse(website)
            if parsed.netloc:
                domains_to_try.append(parsed.netloc.replace('www.', ''))
        except:
            pass

    clean_name = channel_name.lower()
    clean_name = re.sub(r'[^\w]', '', clean_name)

    domain_variations = [
        f"{clean_name}.com",
        f"{clean_name}tv.com",
        f"{clean_name}channel.com",
        f"{clean_name}network.com",
    ]

    country_domains = {
        'us': ['.com'], 'uk': ['.co.uk'], 'gb': ['.co.uk'],
        'in': ['.in', '.co.in'], 'bd': ['.bd', '.com.bd'],
        'ae': ['.ae'], 'pk': ['.pk'], 'ca': ['.ca'],
        'au': ['.au'], 'de': ['.de'], 'fr': ['.fr'],
    }

    country_match = re.search(r'\.([a-z]{2})$', channel_name.lower())
    if country_match:
        cc = country_match.group(1)
        tlds = country_domains.get(cc, ['.com'])
        for tld in tlds:
            domain_variations.append(f"{clean_name}{tld}")

    all_domains = domains_to_try + domain_variations

    for domain in all_domains:
        if not domain or '.' not in domain:
            continue

        try:
            # Use GET instead of HEAD for better compatibility
            for retina in [True, False]:
                retina_param = "&retina=true" if retina else ""
                api_url = f"https://img.logo.dev/{domain}?token={logo_dev_key}&format=png{retina_param}"

                r = session.get(api_url, stream=True, timeout=15, allow_redirects=True)
                if r.status_code == 200:
                    ct = r.headers.get('content-type', '').lower()
                    if 'image' in ct:
                        # Verify PNG magic bytes
                        header = r.raw.read(8)
                        r.close()
                        if len(header) >= 8 and header[:8] == b'\x89PNG\r\n\x1a\n':
                            return api_url
                r.close()

        except Exception:
            pass

    return None

# ─── STAGE 2: TVDB API — FALLBACK ────────────────────────────────────────────
def get_tvdb_token() -> str | None:
    if not API_KEY:
        return None
    for attempt in range(MAX_RETRIES):
        try:
            r = session.post("https://api4.thetvdb.com/v4/login",
                             json={"apikey": API_KEY}, timeout=20)
            if r.status_code == 200:
                token = r.json().get('data', {}).get('token')
                if token:
                    return token
        except Exception:
            time.sleep(2 ** attempt)
    return None

def clean_name_for_tvdb(name: str) -> list[str]:
    if not name:
        return []

    variations = [name]
    cleaned = re.sub(r'[\(\[].*?[\)\]]', '', name)
    cleaned = re.sub(r'\.\w{2}$', '', cleaned)
    base = cleaned.strip()
    if base != name:
        variations.append(base)

    suffixes = ['TV', 'Channel', 'Television', 'Network', 'HD', 'News', 
                'Live', 'Online', 'Digital', 'Satellite', '24', 'Plus', 'FM']
    for suffix in suffixes:
        pattern = r'\s+' + re.escape(suffix) + r'$'
        no_suffix = re.sub(pattern, '', base, flags=re.IGNORECASE).strip()
        if no_suffix and no_suffix != base and no_suffix not in variations:
            variations.append(no_suffix)

    simple = re.sub(r'[^\w\s]', '', base).strip()
    if simple and simple != base and simple not in variations:
        variations.append(simple)

    return list(dict.fromkeys(variations))

def search_tvdb_logo(name: str, token: str) -> str | None:
    if not token or not name:
        return None

    search_names = clean_name_for_tvdb(name)

    for attempt, query in enumerate(search_names):
        for retry in range(MAX_RETRIES):
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
                    break
                elif r.status_code == 401:
                    return None
                elif r.status_code == 429:
                    wait = 2 * (retry + 1)
                    time.sleep(wait)
                    continue
                else:
                    break
            except Exception:
                if retry < MAX_RETRIES - 1:
                    time.sleep(2 ** retry)

        if attempt < len(search_names) - 1:
            time.sleep(0.3)

    return None

# ─── STAGE 3: iptv-org logos.json ─────────────────────────────────────────────
def search_iptvorg_logo(cid: str, iptvorg_map: dict) -> str | None:
    if not cid or not iptvorg_map:
        return None
    return iptvorg_map.get(cid)

# ─── STAGE 4: tv-logo/tv-logos ───────────────────────────────────────────────
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

    suffixes = ['tv', 'channel', 'hd', 'news', 'sports', 'music', 'kids', 'fm']
    base_name = clean
    for suffix in suffixes:
        if base_name.endswith(f'-{suffix}'):
            base_name = base_name[:-len(suffix)-1]
            variations.append(f"{base_name}-{cc}.png")
            variations.append(f"{base_name}.png")

    no_num = re.sub(r'^<?d+->?', '', clean).strip('-')
    if no_num and no_num != clean:
        variations.append(f"{no_num}-{cc}.png")
        variations.append(f"{no_num}.png")

    seen = set()
    for v in variations:
        if v in seen:
            continue
        seen.add(v)
        url = f"{base_url}/{v}"
        try:
            r = session.head(url, timeout=10, allow_redirects=True)
            if r.status_code == 200:
                return url
        except Exception:
            pass

    return None

# ─── STAGE 5: MarhyCZ Picons ──────────────────────────────────────────────────
def get_marhycz_url(channel_name: str) -> str | None:
    if not channel_name:
        return None

    clean = channel_name.lower()
    clean = re.sub(r'[\(\[].*?[\)\]]', '', clean)
    clean = re.sub(r'\.\w{2}$', '', clean)
    clean = re.sub(r'[^\w]', '', clean)

    if not clean:
        return None

    variations = [clean]
    for suffix in ['hd', 'sd', 'tv', 'channel', 'news', 'sports', 'plus', 'fm']:
        if clean.endswith(suffix):
            variations.append(clean[:-len(suffix)])

    no_lead_num = re.sub(r'^<?d+', '', clean)
    if no_lead_num and no_lead_num != clean:
        variations.append(no_lead_num)

    seen = set()
    for v in variations:
        if not v or v in seen:
            continue
        seen.add(v)
        url = f"https://marhycz.github.io/picons/640/{v}.png"
        try:
            r = session.head(url, timeout=10, allow_redirects=True)
            if r.status_code == 200:
                return url
        except Exception:
            pass

    return None

# ─── STAGE 6: LyngSat Logo ────────────────────────────────────────────────────
def get_lyngsat_url(channel_name: str, country_code: str) -> str | None:
    if not channel_name:
        return None

    clean = channel_name.lower()
    clean = re.sub(r'[^\w\s]', '', clean)
    clean = clean.replace(' ', '_').replace('-', '_')

    variations = [clean]
    for suffix in ['tv', 'channel', 'hd', 'news', 'sports']:
        if clean.endswith(f'_{suffix}'):
            variations.append(clean[:-len(suffix)-1])

    no_num = re.sub(r'^<?d+_', '', clean)
    if no_num and no_num != clean:
        variations.append(no_num)

    for v in variations:
        if not v:
            continue
        first_letter = v[0] if v else 'a'
        url = f"https://www.lyngsat-logo.com/logo/tv/{first_letter}/{v}.png"
        try:
            r = session.head(url, timeout=10, allow_redirects=True)
            if r.status_code == 200:
                ct = r.headers.get('content-type', '').lower()
                if 'image' in ct:
                    return url
        except Exception:
            pass

    return None

# ─── STAGE 7: Wikipedia API ───────────────────────────────────────────────────
def get_wikipedia_logo_url(channel_name: str) -> str | None:
    if not channel_name:
        return None

    search_term = channel_name.replace(' ', '+')
    try:
        search_url = f"https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch={search_term}+television+channel&format=json&srlimit=1"
        r = session.get(search_url, timeout=15)
        if r.status_code != 200:
            return None

        data = r.json()
        search_results = data.get('query', {}).get('search', [])
        if not search_results:
            return None

        page_title = search_results[0].get('title', '')
        if not page_title:
            return None

        page_url = f"https://en.wikipedia.org/w/api.php?action=query&titles={page_title.replace(' ', '%20')}&prop=pageimages&pithumbsize=500&format=json"
        r = session.get(page_url, timeout=15)
        if r.status_code != 200:
            return None

        data = r.json()
        pages = data.get('query', {}).get('pages', {})
        for page_id, page_data in pages.items():
            thumbnail = page_data.get('thumbnail', {})
            img_url = thumbnail.get('source', '')
            if img_url and img_url.startswith('http'):
                full_url = img_url.replace('/thumb/', '/').rsplit('/', 1)[0]
                return full_url

    except Exception:
        pass

    return None

# ─── Per-channel worker ──────────────────────────────────────────────────────
def process_channel(channel: dict, logo_dev_key: str, tvdb_token: str, iptvorg_map: dict, state: dict) -> dict:
    cid     = channel.get('id', '')
    name    = channel.get('name', '')
    country = channel.get('country', '').lower()
    website = channel.get('website', '')

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

    # ─── STAGE 1: Logo.dev (Primary) ──────────────────────────────────
    if logo_dev_key:
        img_url = get_logo_dev_url(name, website, logo_dev_key)
        if img_url:
            time.sleep(LOGO_DEV_DELAY)
            save_path = os.path.join(LOGOS_DIR, country, f"{cid}.png")
            ok, size = download_and_save(img_url, save_path)
            if ok:
                result['status'] = 'done'
                result['source'] = 'logo-dev-api'
                result['file_size'] = size
                print(f"   ✅ Logo.dev: {cid}.png ({size} bytes)")
                return result

    # ─── STAGE 2: TVDB (Fallback) ───────────────────────────────────────
    if tvdb_token:
        img_url = search_tvdb_logo(name, tvdb_token)
        if img_url:
            time.sleep(TVDB_DELAY)
            save_path = os.path.join(LOGOS_DIR, country, f"{cid}.png")
            ok, size = download_and_save(img_url, save_path)
            if ok:
                result['status'] = 'done'
                result['source'] = 'tvdb-api'
                result['file_size'] = size
                print(f"   ✅ TVDB: {cid}.png ({size} bytes)")
                return result

    # ─── STAGE 3: iptv-org logos.json ───────────────────────────────────
    img_url = search_iptvorg_logo(cid, iptvorg_map)
    if img_url:
        time.sleep(FALLBACK_DELAY)
        save_path = os.path.join(LOGOS_DIR, country, f"{cid}.png")
        ok, size = download_and_save(img_url, save_path)
        if ok:
            result['status'] = 'done'
            result['source'] = 'iptv-org-api'
            result['file_size'] = size
            print(f"   ✅ iptv-org: {cid}.png ({size} bytes)")
            return result

    # ─── STAGE 4: tv-logo/tv-logos ──────────────────────────────────────
    img_url = get_tvlogos_url(name, country)
    if img_url:
        time.sleep(FALLBACK_DELAY)
        save_path = os.path.join(LOGOS_DIR, country, f"{cid}.png")
        ok, size = download_and_save(img_url, save_path)
        if ok:
            result['status'] = 'done'
            result['source'] = 'tv-logos-github'
            result['file_size'] = size
            print(f"   ✅ tv-logos: {cid}.png ({size} bytes)")
            return result

    # ─── STAGE 5: MarhyCZ Picons ────────────────────────────────────────
    img_url = get_marhycz_url(name)
    if img_url:
        time.sleep(FALLBACK_DELAY)
        save_path = os.path.join(LOGOS_DIR, country, f"{cid}.png")
        ok, size = download_and_save(img_url, save_path)
        if ok:
            result['status'] = 'done'
            result['source'] = 'marhycz-picons'
            result['file_size'] = size
            print(f"   ✅ MarhyCZ: {cid}.png ({size} bytes)")
            return result

    # ─── STAGE 6: LyngSat ───────────────────────────────────────────────
    img_url = get_lyngsat_url(name, country)
    if img_url:
        time.sleep(FALLBACK_DELAY)
        save_path = os.path.join(LOGOS_DIR, country, f"{cid}.png")
        ok, size = download_and_save(img_url, save_path)
        if ok:
            result['status'] = 'done'
            result['source'] = 'lyngsat'
            result['file_size'] = size
            print(f"   ✅ LyngSat: {cid}.png ({size} bytes)")
            return result

    # ─── STAGE 7: Wikipedia ─────────────────────────────────────────────
    img_url = get_wikipedia_logo_url(name)
    if img_url:
        time.sleep(FALLBACK_DELAY)
        save_path = os.path.join(LOGOS_DIR, country, f"{cid}.png")
        ok, size = download_and_save(img_url, save_path)
        if ok:
            result['status'] = 'done'
            result['source'] = 'wikipedia'
            result['file_size'] = size
            print(f"   ✅ Wikipedia: {cid}.png ({size} bytes)")
            return result

    # ─── FAIL ──────────────────────────────────────────────────────────
    result['error'] = 'not_found_any_source'
    print(f"   ❌ MISS: {cid}")
    return result

# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    start_time = time.time()
    print("=" * 70)
    print("🚀 AeonCoreX WORLD Logo Scraper v8.0")
    print("   Sources (in order):")
    print("   1️⃣  Logo.dev API (Primary) - Official API, 500K calls/month free")
    print("   2️⃣  TVDB API (Fallback)")
    print("   3️⃣  iptv-org/logos.json (Direct ID match)")
    print("   4️⃣  tv-logo/tv-logos (GitHub raw)")
    print("   5️⃣  MarhyCZ Picons (GitHub Pages CDN)")
    print("   6️⃣  LyngSat Logo Database")
    print("   7️⃣  Wikipedia API (Infobox image)")
    print("   Blank File Protection: ✅ ENABLED")
    print("   Israel: REMOVED")
    print("=" * 70)

    logo_dev_key = LOGO_DEV_KEY
    if not logo_dev_key:
        print("⚠️  LOGO_DEV_KEY not set! Get free key from https://logo.dev")
        print("⚠️  Running without Logo.dev (fallbacks only)")
    else:
        print("🔑 Logo.dev: API key configured (500K requests/month)")

    state       = load_state()
    channels    = get_iptv_channels()
    iptvorg_map = load_iptvorg_logos()

    if not channels:
        print("❌ No channels loaded.")
        return

    targets = []
    skipped_sync = 0
    for c in channels:
        skip, reason, _ = should_skip(c, state)
        if skip:
            skipped_sync += 1
            continue
        targets.append(c)

    print(f"📊 Total channels: {len(channels)}")
    print(f"📊 To process: {len(targets)} | Already done: {skipped_sync}")

    tvdb_token = get_tvdb_token()
    if not tvdb_token:
        print("⚠️  TVDB not available")

    channels_data = []
    processed = 0
    state_dirty = False

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(process_channel, ch, logo_dev_key, tvdb_token, iptvorg_map, state): ch 
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
            if processed % 50 == 0:
                save_state(state)
                state_dirty = False
                done_cnt = sum(1 for c in channels_data if c['status'] == 'done')
                print(f"💾 Checkpoint @ {processed} (Success: {done_cnt})")

    if state_dirty:
        save_state(state)

    generate_reports(channels_data, state, start_time)
    generate_mapping(state)

    total_png = 0
    for root, dirs, files in os.walk(LOGOS_DIR):
        for f in files:
            if f.endswith('.png'):
                total_png += 1
    print(f"\n{'='*70}")
    print(f"🏁 FINISHED")
    print(f"   This run:      {processed} channels")
    print(f"   New logos:       {sum(1 for c in channels_data if c['status'] == 'done')}")
    print(f"   Failed:          {sum(1 for c in channels_data if c['status'] == 'failed')}")
    print(f"   Skipped:         {sum(1 for c in channels_data if c['status'] in ('skipped', 'already_done'))}")
    print(f"   Total PNGs:      {total_png}")
    print(f"{'='*70}")

if __name__ == "__main__":
    main()
