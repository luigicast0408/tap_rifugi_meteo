import os
import json
import requests
import time

OUTPUT_FILE = "/data/huts.json"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Regions with Bounding Boxes (South, West, North, East)
REGIONS = {
    "Trentino-Alto Adige": "46.2,10.5,47.2,12.5",
    "Lombardia": "45.0,8.0,46.5,10.5",
    "Piemonte": "44.8,6.5,46.0,8.5",
    "Veneto": "45.0,11.5,46.5,12.5",
    "Sicilia": "36.0,12.0,38.5,15.5"
}

def fetch_huts_for_bbox(bbox):
    """Executes Overpass query with increased timeout and retry logic."""
    query = f"""
    [out:json][timeout:180];
    node["tourism"="alpine_hut"]({bbox});
    out;
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(OVERPASS_URL, data={'data': query}, timeout=200)
            response.raise_for_status()
            data = response.json()
            return data.get("elements", [])
        except Exception as e:
            print(f"Warning: Attempt {attempt + 1} failed for bbox {bbox}: {e}")
            if attempt < max_retries - 1:
                print("Waiting 30 seconds before retrying...")
                time.sleep(30)
            else:
                print(f"Error: Critical failure for region with bbox {bbox}")
                return []

def main():
    print(f"Starting data acquisition. Working directory: {os.getcwd()}")
    output_dir = os.path.dirname(OUTPUT_FILE)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Use 'a' (append) mode to allow Fluent Bit streaming (tailing)
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        for region, bbox in REGIONS.items():
            print(f"Fetching huts for region: {region}...")
            huts = fetch_huts_for_bbox(bbox)
            print(f"Success: Found {len(huts)} huts")

            for h in huts:
                # Structure record for Spark/Elasticsearch compatibility
                record = {
                    "hut_id": h.get("id"),
                    "name": h.get("tags", {}).get("name", "Unnamed Hut"),
                    "lat": h.get("lat"),
                    "lon": h.get("lon"),
                    "region": region,
                    "ingestion_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                }

                # Write JSONL (one JSON per line)
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

            # Force immediate write to disk for Fluent Bit synchronization
            f.flush()
            os.fsync(f.fileno())

            # Safety delay to respect Overpass API rate limits
            print("Waiting 15 seconds to respect rate limits...")
            time.sleep(15)
    print(f"Process completed. Data stored in {OUTPUT_FILE}")

if __name__ == "__main__":
    main()