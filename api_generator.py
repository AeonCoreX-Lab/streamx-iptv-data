#!/usr/bin/env python3
"""
api_generator.py — Auto-generates API endpoints for GitHub Pages
═══════════════════════════════════════════════════════════════════
Run after update_channels.py to create API-friendly JSON files
"""
import json, os, glob
from datetime import datetime

BASE_DIR = os.getcwd()
API_DIR = os.path.join(BASE_DIR, "api", "v1")
CATEGORY_DIR = os.path.join(BASE_DIR, "categories")
PLAYLIST_DIR = os.path.join(BASE_DIR, "playlists")
LOGOS_DIR = os.path.join(BASE_DIR, "logos")
MAPPING_FILE = os.path.join(BASE_DIR, "channel_logo_mapping.json")
EVENTS_FILE = os.path.join(BASE_DIR, "events.json")

def ensure_dirs():
    os.makedirs(os.path.join(API_DIR, "categories"), exist_ok=True)

def load_categories():
    """Load all category JSON files"""
    categories = {}
    for filepath in glob.glob(os.path.join(CATEGORY_DIR, "*.json")):
        name = os.path.basename(filepath).replace('.json', '')
        try:
            with open(filepath, encoding='utf-8') as f:
                data = json.load(f)
            categories[name] = data.get('channels', [])
        except Exception as e:
            print(f"⚠️  Error loading {name}: {e}")
    return categories

def load_logo_mapping():
    """Load logo mapping (v2.0 or legacy)"""
    if not os.path.exists(MAPPING_FILE):
        return {}
    try:
        with open(MAPPING_FILE, encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict) and 'flat' in data:
            return data['flat']
        return data
    except Exception:
        return {}

def generate_all_channels(categories):
    """Generate flat channels list with full URLs"""
    all_channels = []
    for cat_name, channels in categories.items():
        for ch in channels:
            ch_copy = dict(ch)
            ch_copy['_category'] = cat_name
            all_channels.append(ch_copy)
    return all_channels

def generate_logo_mapping(categories):
    """Generate logo URL mapping for API"""
    mapping = {}
    logo_data = load_logo_mapping()

    for cat_name, channels in categories.items():
        for ch in channels:
            cid = ch.get('id', '')
            if cid in logo_data:
                path = logo_data[cid]
                if path.startswith('logos/'):
                    mapping[cid] = f"https://aeoncorex-lab.github.io/streamx-iptv-data/{path}"
                else:
                    mapping[cid] = path
            else:
                logo = ch.get('logoUrl', '')
                if logo:
                    mapping[cid] = logo

    return mapping

def generate_playlists():
    """Generate playlist metadata"""
    playlists = []
    if os.path.exists(PLAYLIST_DIR):
        for filepath in glob.glob(os.path.join(PLAYLIST_DIR, "*.m3u")):
            name = os.path.basename(filepath).replace('.m3u', '')
            size = os.path.getsize(filepath)
            playlists.append({
                "name": name,
                "url": f"https://aeoncorex-lab.github.io/streamx-iptv-data/playlists/{name}.m3u",
                "size_bytes": size,
                "format": "m3u"
            })
    return playlists

def generate_events():
    """Generate events from root events.json"""
    if not os.path.exists(EVENTS_FILE):
        return None
    try:
        with open(EVENTS_FILE, encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'events' in data:
            return data['events']
        elif isinstance(data, dict):
            return [data]
        return None
    except Exception as e:
        print(f"⚠️  Error loading events.json: {e}")
        return None

def generate_stats(categories, mapping, playlists, events=None):
    """Generate API stats"""
    total_channels = sum(len(ch) for ch in categories.values())
    total_logos = len(mapping)
    countries = set()

    for cat_name, channels in categories.items():
        for ch in channels:
            cid = ch.get('id', '')
            if '.' in cid:
                countries.add(cid.split('.')[-1].lower())

    endpoints = {
        "channels": "/api/v1/channels.json",
        "logos": "/api/v1/logos.json",
        "categories": "/api/v1/categories.json",
        "playlists": "/api/v1/playlists.json",
        "stats": "/api/v1/stats.json"
    }

    if events is not None:
        endpoints["events"] = "/api/v1/events.json"

    return {
        "generated_at": datetime.now().isoformat(),
        "total_channels": total_channels,
        "total_logos": total_logos,
        "total_playlists": len(playlists),
        "total_events": len(events) if events else 0,
        "countries": len(countries),
        "country_list": sorted(list(countries)),
        "categories": list(categories.keys()),
        "api_version": "v1",
        "endpoints": endpoints
    }

def save_json(filepath, data):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✅ {os.path.basename(filepath)} ({len(str(data))} bytes)")

def main():
    print("🚀 Generating API endpoints...")
    ensure_dirs()

    # Load data
    categories = load_categories()
    print(f"📂 Loaded {len(categories)} categories")

    # Generate API files
    print("\n📡 Generating endpoints:")

    # 1. All channels flat
    all_channels = generate_all_channels(categories)
    save_json(os.path.join(API_DIR, "channels.json"), {
        "channels": all_channels,
        "count": len(all_channels),
        "generated_at": datetime.now().isoformat()
    })

    # 2. Logo mapping
    logo_mapping = generate_logo_mapping(categories)
    save_json(os.path.join(API_DIR, "logos.json"), {
        "logos": logo_mapping,
        "count": len(logo_mapping),
        "generated_at": datetime.now().isoformat()
    })

    # 3. Categories list
    save_json(os.path.join(API_DIR, "categories.json"), {
        "categories": [
            {
                "name": name,
                "url": f"https://aeoncorex-lab.github.io/streamx-iptv-data/api/v1/categories/{name}.json",
                "channel_count": len(channels)
            }
            for name, channels in categories.items()
        ],
        "generated_at": datetime.now().isoformat()
    })

    # 4. Individual category endpoints
    print("\n📁 Category endpoints:")
    for name, channels in categories.items():
        save_json(os.path.join(API_DIR, "categories", f"{name}.json"), {
            "category": name,
            "channels": channels,
            "count": len(channels),
            "generated_at": datetime.now().isoformat()
        })

    # 5. Playlists
    playlists = generate_playlists()
    save_json(os.path.join(API_DIR, "playlists.json"), {
        "playlists": playlists,
        "count": len(playlists),
        "generated_at": datetime.now().isoformat()
    })

    # 6. Events (NEW)
    events = generate_events()
    if events:
        save_json(os.path.join(API_DIR, "events.json"), {
            "events": events,
            "count": len(events),
            "generated_at": datetime.now().isoformat()
        })
        print(f"\n📅 Events: {len(events)} events loaded")

    # 7. Stats
    stats = generate_stats(categories, logo_mapping, playlists, events)
    save_json(os.path.join(API_DIR, "stats.json"), stats)

    print("\n🎉 API generation complete!")
    print(f"   Base URL: https://aeoncorex-lab.github.io/streamx-iptv-data/api/v1/")
    print(f"   Total channels: {stats['total_channels']}")
    print(f"   Total logos: {stats['total_logos']}")
    print(f"   Countries: {stats['countries']}")
    if events:
        print(f"   Events: {stats['total_events']}")

if __name__ == "__main__":
    main()
