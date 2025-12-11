from __future__ import annotations
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import requests

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Cities with latitude & longitude
CITIES = {
    "Delhi": {"lat": 28.7041, "lon": 77.1025},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639},
}
API_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"
MAX_RETRIES = 3
TIMEOUT_SECONDS = 10
SLEEP_BETWEEN_CALLS = 0.5  # polite pause between requests

def _now_ts() -> str:
    """UTC timestamp for filenames."""
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _save_raw(payload: object, city: str) -> str:
    """Save JSON payload to RAW_DIR, fallback to .txt if needed."""
    ts = _now_ts()
    filename = f"{city.replace(' ', '_').lower()}_raw_{ts}.json"
    path = RAW_DIR / filename
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    except Exception:
        path = RAW_DIR / f"{city.replace(' ', '_').lower()}_raw_{ts}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(repr(payload))
    return str(path.resolve())

def _fetch_city(city: str, lat: float, lon: float) -> Dict[str, Optional[str]]:
    """Fetch AQ data for a single city using Open-Meteo API with retries."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide",
    }
    attempt = 0
    last_error: Optional[str] = None
    while attempt < MAX_RETRIES:
        attempt += 1
        try:
            resp = requests.get(API_BASE, params=params, timeout=TIMEOUT_SECONDS)
            resp.raise_for_status()
            payload = resp.json()
            saved = _save_raw(payload, city)
            print(f"✅ [{city}] fetched and saved -> {saved}")
            return {"city": city, "success": "true", "raw_path": saved}
        except requests.RequestException as e:
            last_error = str(e)
            print(f"⚠️ [{city}] attempt {attempt}/{MAX_RETRIES} failed: {e}")
        time.sleep(2 ** (attempt - 1))  # exponential backoff

    print(f"❌ [{city}] failed after {MAX_RETRIES} attempts. Last error: {last_error}")
    return {"city": city, "success": "false", "error": last_error}

def fetch_all_cities(cities: Optional[Dict[str, Dict[str, float]]] = None) -> List[Dict[str, Optional[str]]]:
    """Fetch data for all cities and return list of results."""
    if cities is None:
        cities = CITIES
    results: List[Dict[str, Optional[str]]] = []
    for city, coords in cities.items():
        res = _fetch_city(city, coords["lat"], coords["lon"])
        results.append(res)
        time.sleep(SLEEP_BETWEEN_CALLS)
    return results

if __name__ == "__main__":
    print("Starting extraction for Open-Meteo Air Quality API")
    print(f"Cities: {list(CITIES.keys())}")
    out = fetch_all_cities(CITIES)
    print("Extraction complete. Summary:")
    for r in out:
        if r.get("success") == "true":
            print(f" - {r['city']}: saved -> {r['raw_path']}")
        else:
            print(f" - {r['city']}: ERROR -> {r.get('error')}")
