import os
import sys
import gzip
import json
import time
import argparse
import datetime as dt
from typing import Dict, Optional

import requests

# Public CDN JSON endpoints (no API key required)
FEEDS = {
    "vehicle_positions": "https://cdn.mbta.com/realtime/VehiclePositions.json",
    "trip_updates":      "https://cdn.mbta.com/realtime/TripUpdates.json",
    "alerts":            "https://cdn.mbta.com/realtime/Alerts.json",
}

# If you later switch to v3 endpoints, this can pass the API key automatically:
def make_headers() -> Dict[str, str]:
    api_key = os.getenv("MBTA_API_KEY", "").strip()
    # CDN endpoints don’t require a key; v3 endpoints would
    return {"x-api-key": api_key} if api_key else {}

def fetch(url: str, timeout: int = 30) -> bytes:
    r = requests.get(url, headers=make_headers(), timeout=timeout)
    r.raise_for_status()
    return r.content

def save_gzip(path: str, blob: bytes) -> str:
    gz_path = path + ".gz"
    with gzip.open(gz_path, "wb") as f:
        f.write(blob)
    return gz_path

def summarize(blob: bytes) -> str:
    # Best-effort tiny summary for logs (doesn't fail the run if JSON-parsing fails)
    try:
        data = json.loads(blob)
        keys = list(data.keys())[:3]
        # entity count if present
        n = len(data.get("entity", [])) if isinstance(data.get("entity"), list) else None
        if n is not None:
            return f"size={len(blob)} bytes, keys={keys}, entities={n}"
        return f"size={len(blob)} bytes, keys={keys}"
    except Exception:
        return f"size={len(blob)} bytes"

def main():
    parser = argparse.ArgumentParser(description="MBTA GTFS-RT collector (all three feeds)")
    parser.add_argument("--output", default="data", help="Base output directory (default: data)")
    args = parser.parse_args()

    # Dated folder: data/YYYY-MM-DD
    day = dt.datetime.utcnow().strftime("%Y-%m-%d")
    outdir = os.path.join(args.output, day)
    os.makedirs(outdir, exist_ok=True)

    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    results = []

    for feed_name, url in FEEDS.items():
        try:
            blob = fetch(url)
            out_base = os.path.join(outdir, f"{feed_name}_{ts}.json")
            gz_path = save_gzip(out_base, blob)
            summary = summarize(blob)
            print(f"Saved {gz_path}  [{feed_name}]  {summary}")
            results.append((feed_name, gz_path, len(blob)))
            # be a good citizen (tiny pause)
            time.sleep(0.5)
        except Exception as e:
            print(f"ERROR fetching {feed_name} from {url}: {e}", file=sys.stderr)

    total = sum(sz for _, _, sz in results)
    print(f"Finished: saved {len(results)} feeds, ~{total/1024:.1f} KiB (uncompressed) into {outdir}")

if __name__ == "__main__":
    # Fail the job if absolutely nothing saved
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr)
        sys.exit(1)
