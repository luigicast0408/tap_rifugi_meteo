import os
import json
import requests
import time

OUTPUT_FILE = "/data/huts.json"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Regions with Bounding Boxes (South, West, North, East)
REGIONS = {
    "Abruzzo": "41.7,13.0,42.9,15.1",
    "Basilicata": "39.9,15.3,41.2,16.9",
    "Calabria": "37.9,15.5,40.2,17.3",
    "Campania": "39.9,13.7,41.5,15.9",
    "Emilia-Romagna": "43.7,9.1,45.2,12.8",
    "Friuli-Venezia Giulia": "45.5,12.3,46.7,13.9",
    "Lazio": "41.2,11.4,42.9,14.1",
    "Liguria": "43.7,7.5,44.6,10.1",
    "Lombardia": "44.7,8.5,46.6,11.0",
    "Marche": "42.7,12.3,44.0,14.0",
    "Molise": "41.3,13.9,42.1,15.2",
    "Piemonte": "44.3,6.6,46.1,9.2",
    "Puglia": "39.7,14.9,42.0,18.6",
    "Sardegna": "38.8,8.1,41.3,9.8",
    "Sicilia": "36.6,12.4,38.3,15.6",
    "Toscana": "42.2,9.6,44.5,12.4",
    "Trentino-Alto Adige": "45.7,10.4,47.1,12.5",
    "Umbria": "42.3,11.9,43.6,13.2",
    "Valle d'Aosta": "45.5,6.8,45.9,7.9",
    "Veneto": "44.8,10.6,46.7,13.1"
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

                f.write(json.dumps(record, ensure_ascii=False) + "\n")

            f.flush()
            os.fsync(f.fileno())
            print("Waiting 15 seconds to respect rate limits...")
            time.sleep(15)

if __name__ == "__main__":
    main()