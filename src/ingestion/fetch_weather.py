import os
import json
import time
import requests
from datetime import datetime, timezone

INPUT_FILE = "/data/huts.json"
OUTPUT_FILE = "/data/weather.json"

BATCH_SIZE = 800
MIN_SECONDS_BETWEEN_CALLS = 0.25
SLEEP_BETWEEN_CYCLES = 30
BACKOFF_SECONDS = 60

api_calls = []


def is_valid_hut(hut):
    name = hut.get("name")
    if not name: return False
    name_clean = name.lower().strip()
    blacklist = ["rifugio", "bivacco", "malga", "unknown", "unnamed"]
    return not any(x in name_clean for x in blacklist)


def allow_api_call():
    now = time.time()
    api_calls[:] = [t for t in api_calls if now - t < 60]
    return len(api_calls) < 300


def fetch_weather_api(lat, lon):
    if lat is None or lon is None: return None
    while not allow_api_call():
        time.sleep(0.5)

    url = (f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
           f"&current=temperature_2m,relative_humidity_2m,wind_speed_10m,"
           f"precipitation,weather_code,cloud_cover")

    headers = {'User-Agent': 'UniversityProject/1.0'}

    try:
        response = requests.get(url, timeout=10, headers=headers)
        if response.status_code == 429:
            print(f"[RATE LIMIT] Wait {BACKOFF_SECONDS}s...")
            time.sleep(BACKOFF_SECONDS)
            return None

        response.raise_for_status()
        api_calls.append(time.time())
        return response.json().get("current", {})
    except Exception as e:
        print(f"Errore API: {e}")
        return None


def main():
    print("Weather Ingestion Service Start")
    huts = []
    while not huts:
        if os.path.exists(INPUT_FILE) and os.path.getsize(INPUT_FILE) > 0:
            try:
                with open(INPUT_FILE, "r") as f:
                    huts = [json.loads(l) for l in f if l.strip()] # read line of line
                    huts = [h for h in huts if is_valid_hut(h)]    # check if huts is valid
            except Exception as e:
                print(f"Errore lettura huts.json: {e}")
                time.sleep(5)
        else:
            print(" Wait of huts.json...")
            time.sleep(5)

    print(f"Load {len(huts)}  valid hunts.")

    while True:
        open(OUTPUT_FILE, "w").close()

        print(f"[CYCLE START] {datetime.now().strftime('%H:%M:%S')}")
        written = 0

        for hut in huts[:BATCH_SIZE]:
            data = fetch_weather_api(hut.get("lat"), hut.get("lon"))
            if not data: continue
            event = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "hut_name": hut.get("name"),
                "region": hut.get("region", "Unknown"),
                "temperature": data.get("temperature_2m"),
                "humidity": data.get("relative_humidity_2m"),
                "wind_speed": data.get("wind_speed_10m"),
                "precipitation": data.get("precipitation"),
                "weather_code": data.get("weather_code"),
                "cloud_cover": data.get("cloud_cover"),
                "location": {"lat": hut.get("lat"), "lon": hut.get("lon")}
            }

            with open(OUTPUT_FILE, "a") as f:
                f.write(json.dumps(event) + "\n")

            written += 1
            if written % 50 == 0:
                print(f" Send: {written}/{len(huts)}")
            time.sleep(MIN_SECONDS_BETWEEN_CALLS)

        print(f"[CYCLE END] Send {written} record. Wait {SLEEP_BETWEEN_CYCLES}s...")
        time.sleep(SLEEP_BETWEEN_CYCLES)


if __name__ == "__main__":
    main()