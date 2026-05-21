#!/usr/bin/env python3
"""
Logo Migration Script v1.0
Moves existing flat logos to country-wise subfolders
Updates scraper_state.json paths if needed
"""
import os
import json
import shutil
from concurrent.futures import ThreadPoolExecutor

LOGOS_DIR = "logos"
STATE_FILE = "scraper_state.json"
MAPPING_FILE = "channel_logo_mapping.json"

def migrate_logos():
    print("🚀 Starting logo migration...")

    if not os.path.exists(LOGOS_DIR):
        print("❌ logos/ directory not found!")
        return

    moved = 0
    skipped = 0
    errors = 0

    # Get all PNG files in root logos folder
    files = [f for f in os.listdir(LOGOS_DIR) if f.endswith('.png')]
    print(f"📁 Found {len(files)} logos to migrate")

    for fname in files:
        try:
            # Extract country code from filename (e.g., "CNN.us.png" -> "us")
            parts = fname.replace('.png', '').split('.')
            if len(parts) >= 2:
                country = parts[-1].lower()
            else:
                country = "unknown"

            src = os.path.join(LOGOS_DIR, fname)
            dst_dir = os.path.join(LOGOS_DIR, country)
            dst = os.path.join(dst_dir, fname)

            # Create country folder if not exists
            os.makedirs(dst_dir, exist_ok=True)

            # Move file
            if os.path.exists(dst):
                os.remove(src)  # Duplicate, remove source
                skipped += 1
            else:
                shutil.move(src, dst)
                moved += 1

        except Exception as e:
            print(f"   ❌ Error moving {fname}: {e}")
            errors += 1

    print(f"\n✅ Migration complete!")
    print(f"   Moved: {moved}")
    print(f"   Skipped (duplicates): {skipped}")
    print(f"   Errors: {errors}")

    # Clean up empty files in root
    remaining = [f for f in os.listdir(LOGOS_DIR) if f.endswith('.png')]
    if remaining:
        print(f"⚠️  {len(remaining)} files remain in root (no country code detected)")

if __name__ == "__main__":
    migrate_logos()
