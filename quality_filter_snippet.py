import os
import json
import struct
import shutil
from collections import defaultdict
from datetime import datetime

LOGOS_DIR = 'logos'
REPORT_FILE = 'logo_quality_report.json'
ARCHIVE_DIR = 'logos_low_quality_archive'

# এনভায়রনমেন্ট ভ্যারিয়েবল থেকে ইনপুট রিড করা (ডিফল্ট ব্যাকআপ সহ)
ACTION = os.getenv('ACTION_INPUT', 'check')
MIN_SCORE = int(os.getenv('MIN_SCORE_INPUT', '40'))

def run_quality_check():
    stats = {
        'total': 0, 'valid_png': 0, 'corrupt': 0,
        'tiny': 0, 'small': 0, 'medium': 0, 'large': 0, 'huge': 0,
        'by_country': defaultdict(int),
        'low_quality': []
    }

    if not os.path.exists(LOGOS_DIR):
        print(f"❌ Error: {LOGOS_DIR} directory not found.")
        return

    print(f'🔍 Analyzing logos in {LOGOS_DIR}/...')

    for country in os.listdir(LOGOS_DIR):
        country_path = os.path.join(LOGOS_DIR, country)
        if not os.path.isdir(country_path):
            continue

        for fname in os.listdir(country_path):
            if not fname.endswith('.png'):
                continue

            stats['total'] += 1
            stats['by_country'][country] += 1

            filepath = os.path.join(country_path, fname)
            size = os.path.getsize(filepath)

            if size < 1024:
                stats['tiny'] += 1
            elif size < 5120:
                stats['small'] += 1
            elif size < 20480:
                stats['medium'] += 1
            elif size < 102400:
                stats['large'] += 1
            else:
                stats['huge'] += 1

            try:
                with open(filepath, 'rb') as f:
                    header = f.read(24)

                # হেক্স ফরম্যাটে স্ট্যান্ডার্ড PNG সিগনেচার ভ্যালিডেশন
                if header[:8] != b'\x89PNG\r\n\x1a\n':
                    stats['corrupt'] += 1
                    continue

                if header[12:16] == b'IHDR':
                    width = struct.unpack('>I', header[16:20])[0]
                    height = struct.unpack('>I', header[20:24])[0]
                    stats['valid_png'] += 1

                    score = 100
                    if size < 2048:
                        score -= 40
                    elif size < 5120:
                        score -= 20
                    elif size > 102400:
                        score -= 30

                    min_dim = min(width, height)
                    if min_dim < 64:
                        score -= 50
                    elif min_dim < 128:
                        score -= 20
                    elif min_dim < 256:
                        score -= 10

                    ratio = max(width, height) / max(min(width, height), 1)
                    if ratio > 5:
                        score -= 20

                    if score < 40:
                        stats['low_quality'].append({
                            'file': f'{country}/{fname}',
                            'score': score,
                            'size': size,
                            'width': width,
                            'height': height
                        })
            except Exception:
                stats['corrupt'] += 1

    report = {
        'generated_at': datetime.now().isoformat(),
        'summary': {
            'total_logos': stats['total'],
            'valid_png': stats['valid_png'],
            'corrupt': stats['corrupt'],
            'quality_distribution': {
                'tiny (<1KB)': stats['tiny'],
                'small (1-5KB)': stats['small'],
                'medium (5-20KB)': stats['medium'],
                'large (20-100KB)': stats['large'],
                'huge (>100KB)': stats['huge']
            },
            'countries': len(stats['by_country']),
            'low_quality_count': len(stats['low_quality'])
        },
        'by_country': dict(stats['by_country']),
        'low_quality_logos': stats['low_quality'][:100]
    }

    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f'\n📊 Quality Report:')
    print(f'   Total: {stats["total"]}')
    print(f'   ✅ Valid: {stats["valid_png"]}')
    print(f'   ❌ Corrupt: {stats["corrupt"]}')
    print(f'   ⚠️  Low Quality: {len(stats["low_quality"])}')
    print(f'   📁 Saved: {REPORT_FILE}')

def run_filter():
    print(f'🗑️ Filtering logos with score < {MIN_SCORE}...')
    removed = 0
    archived = 0
    errors = 0

    if not os.path.exists(LOGOS_DIR):
        print(f"❌ Error: {LOGOS_DIR} directory not found.")
        return

    for country in os.listdir(LOGOS_DIR):
        country_path = os.path.join(LOGOS_DIR, country)
        if not os.path.isdir(country_path):
            continue

        for fname in os.listdir(country_path):
            if not fname.endswith('.png'):
                continue

            filepath = os.path.join(country_path, fname)

            try:
                size = os.path.getsize(filepath)

                with open(filepath, 'rb') as f:
                    header = f.read(24)

                if header[:8] != b'\x89PNG\r\n\x1a\n':
                    os.remove(filepath)
                    removed += 1
                    continue

                if header[12:16] == b'IHDR':
                    width = struct.unpack('>I', header[16:20])[0]
                    height = struct.unpack('>I', header[20:24])[0]

                    score = 100
                    if size < 2048:
                        score -= 40
                    elif size < 5120:
                        score -= 20
                    elif size > 102400:
                        score -= 30

                    min_dim = min(width, height)
                    if min_dim < 64:
                        score -= 50
                    elif min_dim < 128:
                        score -= 20
                    elif min_dim < 256:
                        score -= 10

                    ratio = max(width, height) / max(min(width, height), 1)
                    if ratio > 5:
                        score -= 20

                    if score < MIN_SCORE:
                        archive_country = os.path.join(ARCHIVE_DIR, country)
                        os.makedirs(archive_country, exist_ok=True)
                        shutil.move(filepath, os.path.join(archive_country, fname))
                        archived += 1
                        print(f'   Archived: {country}/{fname} (score: {score})')
            except Exception as e:
                print(f'   Error: {fname} - {e}')
                errors += 1

    print(f'\n✅ Filter complete:')
    print(f'   Removed (corrupt): {removed}')
    print(f'   Archived (low quality): {archived}')
    print(f'   Errors: {errors}')

if __name__ == '__main__':
    if ACTION in ['check', 'check-and-filter']:
        run_quality_check()
    if ACTION in ['filter', 'check-and-filter']:
        run_filter()
