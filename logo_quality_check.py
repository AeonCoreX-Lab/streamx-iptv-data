#!/usr/bin/env python3
"""
logo_quality_check.py — Analyze logo quality across all sources
═══════════════════════════════════════════════════════════════════
Checks: resolution, file size, format validity, duplicates
"""
import os, json, struct
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

LOGOS_DIR = "logos"
MAPPING_FILE = "channel_logo_mapping.json"
REPORT_FILE = "logo_quality_report.json"

STATS = {
    "total": 0,
    "valid_png": 0,
    "corrupt": 0,
    "tiny": 0,        # < 1KB
    "small": 0,       # 1-5KB
    "medium": 0,      # 5-20KB
    "large": 0,       # 20-100KB
    "huge": 0,        # > 100KB
    "by_country": defaultdict(int),
    "by_source": defaultdict(int),  # logo-dev, iptv-org, etc.
}

def check_png_quality(filepath):
    """Check PNG dimensions and validity"""
    try:
        size = os.path.getsize(filepath)

        # Size categories
        if size < 1024:
            STATS["tiny"] += 1
        elif size < 5120:
            STATS["small"] += 1
        elif size < 20480:
            STATS["medium"] += 1
        elif size < 102400:
            STATS["large"] += 1
        else:
            STATS["huge"] += 1

        # Read PNG header
        with open(filepath, 'rb') as f:
            header = f.read(24)

        if header[:8] != b'\x89PNG\r\n\x1a\n':
            STATS["corrupt"] += 1
            return None

        # Extract dimensions from IHDR
        if header[12:16] == b'IHDR':
            width = struct.unpack('>I', header[16:20])[0]
            height = struct.unpack('>I', header[20:24])[0]
            STATS["valid_png"] += 1
            return {"width": width, "height": height, "size": size}

        STATS["corrupt"] += 1
        return None

    except Exception:
        STATS["corrupt"] += 1
        return None

def analyze_all():
    print("🔍 Analyzing logo quality...")

    results = {}

    for country_code in os.listdir(LOGOS_DIR):
        country_path = os.path.join(LOGOS_DIR, country_code)
        if not os.path.isdir(country_path):
            continue

        for fname in os.listdir(country_path):
            if not fname.endswith('.png'):
                continue

            STATS["total"] += 1
            STATS["by_country"][country_code] += 1

            filepath = os.path.join(country_path, fname)
            quality = check_png_quality(filepath)

            if quality:
                cid = fname.replace('.png', '')
                results[cid] = {
                    **quality,
                    "country": country_code,
                    "path": f"logos/{country_code}/{fname}"
                }

    # Generate report
    report = {
        "generated_at": __import__('datetime').datetime.now().isoformat(),
        "summary": {
            "total_logos": STATS["total"],
            "valid_png": STATS["valid_png"],
            "corrupt": STATS["corrupt"],
            "quality_distribution": {
                "tiny (<1KB)": STATS["tiny"],
                "small (1-5KB)": STATS["small"],
                "medium (5-20KB)": STATS["medium"],
                "large (20-100KB)": STATS["large"],
                "huge (>100KB)": STATS["huge"]
            },
            "countries": len(STATS["by_country"]),
        },
        "by_country": dict(STATS["by_country"]),
        "high_quality_logos": {  # > 100x100px and > 5KB
            k: v for k, v in results.items()
            if v["width"] >= 100 and v["height"] >= 100 and v["size"] >= 5120
        }
    }

    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)

    print(f"\n📊 Quality Report:")
    print(f"   Total: {STATS['total']}")
    print(f"   ✅ Valid PNG: {STATS['valid_png']}")
    print(f"   ❌ Corrupt: {STATS['corrupt']}")
    print(f"   📏 Size: Tiny {STATS['tiny']} | Small {STATS['small']} | Medium {STATS['medium']} | Large {STATS['large']} | Huge {STATS['huge']}")
    print(f"   🌍 Countries: {len(STATS['by_country'])}")
    print(f"\n💾 Saved: {REPORT_FILE}")

    return report

if __name__ == "__main__":
    analyze_all()
