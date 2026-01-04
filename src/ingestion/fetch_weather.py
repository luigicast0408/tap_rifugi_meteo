import os
import json
import time
import random
import requests
from datetime import datetime, timezone

# Paths configured for Docker container volumes
INPUT_FILE = "../../data/huts.json"
OUTPUT_FILE = "../../data/weather.json"


def fetch_weather(lat, lon):
    """Fetches current weather from Open-Meteo with retry logic."""
    url = (f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
           f"&current=temperature_2m,relative_humidity_2m,wind_speed_10m")

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            current = data.get("current", {})

            return {
                "temperature": current.get("temperature_2m"),
                "wind_speed": current.get("wind_speed_10m"),
                "humidity": current.get("relative_humidity_2m")
            }
        except Exception as e:
            print(f"Warning: Attempt {attempt + 1} failed for coordinates ({lat}, {lon}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print(f"Error: Failed to fetch weather data for ({lat}, {lon})")
                return None
    return None


def process_hut_event(hut):
    """Processes weather data for a selected hut and returns a structured event."""
    lat = hut.get("lat")
    lon = hut.get("lon")
    name = hut.get("name", "Unknown Hut")

    if lat is None or lon is None:
        return None

    weather = fetch_weather(lat, lon)

    if weather is not None:
        return {
            "event_type": "weather_update",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "station_name": name,
            "location": {
                "lat": lat,
                "lon": lon
            },
            "temperature": weather["temperature"],
            "humidity": weather["humidity"],
            "wind_speed": weather["wind_speed"]
        }
    return None


def main():
    print(f"Starting Weather Ingestion Service. Working directory: {os.getcwd()}")

    # Step 1: Wait for the source file and load huts
    huts = []
    while not huts:
        if not os.path.exists(INPUT_FILE):
            print(f"Waiting for source file: {INPUT_FILE}...")
            time.sleep(5)
            continue

        try:
            with open(INPUT_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            huts.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            print(f"Error reading source file: {e}")

        if not huts:
            print("Source file is empty or invalid. Waiting for data...")
            time.sleep(5)

    print(f"Success: Loaded {len(huts)} huts. Entering streaming loop.")

    # Step 2: Continuous streaming loop
    while True:
        try:
            selected_hut = random.choice(huts)
            weather_event = process_hut_event(selected_hut)

            if weather_event:
                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(weather_event, ensure_ascii=False) + "\n")
                    # Force write to disk to ensure synchronization
                    f.flush()
                    os.fsync(f.fileno())

                print(f"Update sent: {weather_event['station_name']} | " f"Temp: {weather_event['temperature']}°C")
            time.sleep(5)

        except Exception as e:
            print(f"Critical error in streaming loop: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()