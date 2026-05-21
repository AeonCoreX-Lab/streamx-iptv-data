
# Add this to logomaster.py after validate_image_file function

def check_logo_quality(filepath: str) -> dict:
    """
    Check logo quality metrics.
    Returns: {"score": 0-100, "width": int, "height": int, "size": int}
    """
    try:
        size = os.path.getsize(filepath)

        with open(filepath, 'rb') as f:
            header = f.read(24)

        if header[:8] != b'\x89PNG\r\n\x1a\n':
            return {"score": 0, "error": "not_png"}

        if header[12:16] == b'IHDR':
            width = struct.unpack('>I', header[16:20])[0]
            height = struct.unpack('>I', header[20:24])[0]

            # Quality scoring
            score = 100

            # Size penalty (too small = bad)
            if size < 2048:      # < 2KB
                score -= 40
            elif size < 5120:    # < 5KB
                score -= 20
            elif size > 102400:  # > 100KB (too big, probably not logo)
                score -= 30

            # Resolution penalty
            min_dim = min(width, height)
            if min_dim < 64:
                score -= 50
            elif min_dim < 128:
                score -= 20
            elif min_dim < 256:
                score -= 10

            # Aspect ratio penalty (logos should be roughly square or wide)
            ratio = max(width, height) / max(min(width, height), 1)
            if ratio > 5:  # Too tall/thin
                score -= 20

            return {
                "score": max(0, score),
                "width": width,
                "height": height,
                "size": size,
                "ratio": round(ratio, 2)
            }

        return {"score": 0, "error": "no_ihdr"}

    except Exception as e:
        return {"score": 0, "error": str(e)}


MIN_QUALITY_SCORE = 40  # Reject logos below this score

def save_logo_with_quality_check(data: bytes, save_path: str) -> tuple[bool, int]:
    """
    Save logo only if quality is acceptable.
    Returns: (success, size_or_score)
    """
    # First do normal save
    temp_path = save_path + ".quality"

    with open(temp_path, 'wb') as f:
        f.write(data)

    # Check quality
    quality = check_logo_quality(temp_path)

    if quality["score"] < MIN_QUALITY_SCORE:
        os.remove(temp_path)
        print(f"   ⚠️  Low quality rejected (score {quality['score']}): {os.path.basename(save_path)}")
        return False, quality["score"]

    # Quality OK, move to final path
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    os.replace(temp_path, save_path)

    return True, os.path.getsize(save_path)
