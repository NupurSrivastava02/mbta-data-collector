import os
import sys
import argparse
import datetime as dt
import json
import requests

VEHICLE_POSITIONS_URL = "https://cdn.mbta.com/realtime/VehiclePositions.json"

def fetch_vehicle_positions(timeout=30):
    r = requests.get(VEHICLE_POSITIONS_URL, timeout=timeout)
    r.raise_for_status()
    return r.content

def main():
    parser = argparse.ArgumentParser(description="MBTA realtime collector")
    parser.add_argument("--output", default="data", help="output directory (default: data)")
    args = parser.parse_args()

    # Dated folder: data/YYYY-MM-DD
    day = dt.datetime.utcnow().strftime("%Y-%m-%d")
    outdir = os.path.join(args.output, day)
    os.makedirs(outdir, exist_ok=True)

    # Timestamped file: vehicle_positions_YYYYmmddTHHMMSSZ.json
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(outdir, f"vehicle_positions_{ts}.json")

    # Fetch & write
    blob = fetch_vehicle_positions()
    with open(out_path, "wb") as f:
        f.write(blob)

    # Optional: print a single summary line for logs
    try:
        parsed = json.loads(blob)
        # lightweight summary (varies by feed)
        print(f"Saved {out_path} | size={len(blob)} bytes | keys={list(parsed.keys())[:3]}")
    except Exception:
        print(f"Saved {out_path} | size={len(blob)} bytes")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

