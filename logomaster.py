import os
import time
import requests
from urllib.parse import urlparse

# Configuration
API_KEY = os.environ.get("PRIMARY_API_KEY") # Will be fetched from GitHub Secrets
LOGOS_DIR = "logos"
os.makedirs(LOGOS_DIR, exist_ok=True)

# Target Filters (Adjust as needed)
TARGET_COUNTRIES = ['bd', 'us', 'uk', 'gb', 'ae', 'in']
TARGET_CATEGORIES = ['sports', 'music', 'kids', 'documentary', 'education', 'news', 'informative']

# Initialize Requests Session for better performance
session = requests.Session()
session.headers.update({"User-Agent": "AeonCoreX-StreamX-LogoScraper/1.0"})

def get_iptv_channels():
    print("Fetching channels from iptv-org...")
    response = session.get("https://iptv-org.github.io/api/channels.json")
    return response.json() if response.status_code == 200 else []

def is_target_channel(channel):
    country = channel.get('country', '').lower()
    categories = [c.lower() for c in channel.get('categories', [])]
    
    if country in TARGET_COUNTRIES:
        return True
    for cat in categories:
        if cat in TARGET_CATEGORIES:
            return True
    return False

def get_tvdb_token():
    """Authenticates with TheTVDB v4 and returns a bearer token."""
    if not API_KEY: return None
    try:
        url = "https://api4.thetvdb.com/v4/login"
        res = session.post(url, json={"apikey": API_KEY})
        if res.status_code == 200:
            return res.json().get('data', {}).get('token')
    except Exception as e:
        print(f"TVDB Auth Error: {e}")
    return None

def search_primary_api(channel_name, token):
    """Priority 1: Search using the Official API (TheTVDB example)"""
    if not token: return None
    try:
        url = f"https://api4.thetvdb.com/v4/search?query={channel_name}&type=company"
        headers = {"Authorization": f"Bearer {token}"}
        res = session.get(url, headers=headers)
        if res.status_code == 200:
            data = res.json().get('data', [])
            if data and len(data) > 0:
                img_url = data[0].get('image_url')
                if img_url: return img_url
    except Exception as e:
        print(f"Primary API Error for {channel_name}: {e}")
    return None

def search_clearbit(website_url):
    """Priority 2: Search using Clearbit Logo API"""
    if not website_url: return None
    try:
        domain = urlparse(website_url).netloc
        if domain:
            url = f"https://logo.clearbit.com/{domain}"
            # Check if Clearbit actually has the logo (returns 200) and not a fallback
            res = session.head(url)
            if res.status_code == 200:
                return url
    except Exception as e:
        print(f"Clearbit Error: {e}")
    return None

def search_duckduckgo(channel_name):
    """Priority 3: Search using DuckDuckGo Instant Answer API"""
    try:
        url = f"https://api.duckduckgo.com/?q={channel_name}+tv+channel+logo&format=json&pretty=1"
        res = session.get(url).json()
        image_path = res.get("Image")
        if image_path:
            # DuckDuckGo sometimes returns relative paths
            if image_path.startswith("/"):
                return f"https://duckduckgo.com{image_path}"
            return image_path
    except Exception as e:
        print(f"DuckDuckGo Error for {channel_name}: {e}")
    return None

def download_and_save(url, save_path):
    """Downloads the image and enforces .png format saving"""
    try:
        res = session.get(url, stream=True, timeout=10)
        if res.status_code == 200:
            # We strictly save as .png regardless of source format, 
            # though true format conversion would require Pillow (PIL).
            # For IPTV players, simply saving the raw bytes with .png extension usually works.
            with open(save_path, 'wb') as f:
                for chunk in res.iter_content(1024):
                    f.write(chunk)
            return True
    except:
        pass
    return False

def main():
    channels = get_iptv_channels()
    target_channels = [c for c in channels if is_target_channel(c)]
    print(f"Total target channels to process: {len(target_channels)}")
    
    token = get_tvdb_token()
    if token:
        print("Successfully authenticated with Primary API.")
    else:
        print("Running without Primary API token. Falling back to Clearbit/DDG.")

    run_limit = 50
    processed = 0

    for channel in target_channels:
        if processed >= run_limit:
            print("Run limit reached for this session.")
            break
            
        channel_id = channel.get('id')
        channel_name = channel.get('name')
        website = channel.get('website')
        
        if not channel_id or not channel_name:
            continue
            
        save_path = os.path.join(LOGOS_DIR, f"{channel_id}.png")
        
        # SKIP if logo already exists
        if os.path.exists(save_path):
            continue
            
        print(f"\nProcessing: {channel_name} ({channel_id})")
        img_url = None
        source = ""

        # Tier 1: Primary API
        img_url = search_primary_api(channel_name, token)
        if img_url: source = "Primary API"

        # Tier 2: Clearbit
        if not img_url:
            img_url = search_clearbit(website)
            if img_url: source = "Clearbit"

        # Tier 3: DuckDuckGo
        if not img_url:
            img_url = search_duckduckgo(channel_name)
            if img_url: source = "DuckDuckGo"

        # Download & Save
        if img_url:
            if download_and_save(img_url, save_path):
                print(f"  [SUCCESS] Saved {channel_id}.png (Source: {source})")
                processed += 1
            else:
                print("  [FAILED] Download corrupted or timed out.")
        else:
            print("  [NOT FOUND] Exhausted all 3 sources.")
            
        time.sleep(1) # Prevent rate limiting

if __name__ == "__main__":
    main()
