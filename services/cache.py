import json
import time
from pathlib import Path

CACHE_DIR = Path(__file__).parent.parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)


def get_cached(key: str, ttl_seconds: int = 1800):
    path = CACHE_DIR / f"{key}.json"
    if path.exists():
        data = json.loads(path.read_text())
        if time.time() - data.get("_cached_at", 0) < ttl_seconds:
            return data.get("payload")
    return None


def set_cached(key: str, payload):
    path = CACHE_DIR / f"{key}.json"
    path.write_text(json.dumps({"_cached_at": time.time(), "payload": payload}))
