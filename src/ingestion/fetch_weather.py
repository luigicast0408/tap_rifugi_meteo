import os
import json
import time
import requests
from datetime import datetime, timezone

INPUT_FILE = "/data/huts.json"
OUTPUT_FILE = "/data/weather.json"

CHUNK_SIZE = 25  #  25 rifugi per volt
PAUSE_BETWEEN_CHUNKS = 5  # Aspetto 5 secondi
SLEEP_BETWEEN_CYCLES = 1800  # 30 Minuti di pausa totale


def is_valid_hut(hut):
    name = hut.get("name")
    if not name: return False
    name_clean = name.lower().strip()
    blacklist = ["rifugio", "bivacco", "malga", "unknown", "unnamed"]
    return not any(x in name_clean for x in blacklist)


def fetch_batch_weather(huts_subset):
    if not huts_subset: return []

    lats = [str(h['lat']) for h in huts_subset]
    lons = [str(h['lon']) for h in huts_subset]

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": ",".join(lats),
        "longitude": ",".join(lons),
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,weather_code,cloud_cover",
        "timezone": "UTC"
    }
    headers = {'User-Agent': 'UniversityProjectDemo/1.0'}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=20)

        if response.status_code == 429:
            print("[429] Rate Limit!")
            return None

        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list): data = [data]
        return data
    except Exception as e:
        print(f"Errore Batch: {e}")
        return []


def main():
    huts = []
    while not huts:
        if os.path.exists(INPUT_FILE) and os.path.getsize(INPUT_FILE) > 0:
            try:
                with open(INPUT_FILE, "r") as f:
                    huts = [json.loads(l) for l in f if l.strip()]
                    huts = [h for h in huts if is_valid_hut(h)]
            except Exception as e:
                print(f"Wait file: {e}")
                time.sleep(5)
        else:
            print("Wait huts.json...")
            time.sleep(5)

    print(f" Load {len(huts)} huts")

    while True:
        open(OUTPUT_FILE, "w").close()
        print(f"[CYCLE START] {datetime.now().strftime('%H:%M:%S')}")

        total_processed = 0
        for i in range(0, len(huts), CHUNK_SIZE):
            chunk = huts[i:i + CHUNK_SIZE]
            weather_results = fetch_batch_weather(chunk)

            if weather_results is None:
                print("Block sleep fo 60 seconds")
                time.sleep(60)
                continue

            if not weather_results: continue

            for hut_info, meteo_data in zip(chunk, weather_results):
                current = meteo_data.get("current", {})
                if not current: continue

                event = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "hut_name": hut_info.get("name"),
                    "region": hut_info.get("region", "Unknown"),
                    "temperature": current.get("temperature_2m"),
                    "humidity": current.get("relative_humidity_2m"),
                    "wind_speed": current.get("wind_speed_10m"),
                    "precipitation": current.get("precipitation"),
                    "weather_code": current.get("weather_code"),
                    "cloud_cover": current.get("cloud_cover"),
                    "location": {"lat": hut_info.get("lat"), "lon": hut_info.get("lon")}
                }

                with open(OUTPUT_FILE, "a") as f:
                    f.write(json.dumps(event) + "\n")

                total_processed += 1

            print(f" block ok {total_processed}/{len(huts)}")
            time.sleep(PAUSE_BETWEEN_CHUNKS)

        print(f"[CYCLE END]  ({SLEEP_BETWEEN_CYCLES}s)...")
        time.sleep(SLEEP_BETWEEN_CYCLES)


if __name__ == "__main__":
    main()