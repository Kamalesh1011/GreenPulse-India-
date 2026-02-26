"""
GreenPulse India — Satellite Data Fetcher
==========================================
Pulls REAL environmental data from public satellite / sensor APIs:

  1. NASA FIRMS (VIIRS/MODIS) — Active fire hotspots over India
     API: https://firms.modaps.eosdis.nasa.gov/api/
     Key: Free MAP_KEY (set FIRMS_MAP_KEY in .env)
     Fallback: Returns empty list if key not configured.

  2. Open-Meteo — Real-time weather (temperature, apparent temp, wind)
     API: https://api.open-meteo.com/v1/forecast
     Key: None — completely free, no auth required.

  3. WAQI (World Air Quality Index) — Real AQI from CPCB ground stations
     API: https://api.waqi.info/feed/
     Key: Free token at https://aqicn.org/data-platform/token/
     Fallback: Returns None if token not set.

  4. Sentinel-5P / TROPOMI NO2 via Copernicus WMS metadata string
     (tile URL construction only — no auth needed for public WMS tiles)

All fetches are cached for a configurable TTL to avoid hammering APIs.
Every function degrades gracefully — if the API is unavailable, it
returns None / [] and logs a warning without crashing the pipeline.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import Any
import urllib.request
import urllib.parse
import urllib.error

from dotenv import load_dotenv
load_dotenv()

log = logging.getLogger("greenpulse.satellite")
logging.basicConfig(level=logging.INFO, format="[%(name)s] %(message)s")

# ---------------------------------------------------------------------------
# API Keys (from environment / .env)
# ---------------------------------------------------------------------------
FIRMS_MAP_KEY  = os.getenv("FIRMS_MAP_KEY", "")        # NASA FIRMS
WAQI_TOKEN     = os.getenv("WAQI_TOKEN", "demo")       # WAQI — "demo" works for testing

# ---------------------------------------------------------------------------
# City coordinates — used for Open-Meteo and WAQI lookups
# ---------------------------------------------------------------------------
CITY_COORDS: dict[str, tuple[float, float]] = {
    "Delhi":       (28.6139, 77.2090),
    "Kanpur":      (26.4499, 80.3319),
    "Lucknow":     (26.8467, 80.9462),
    "Patna":       (25.5941, 85.1376),
    "Agra":        (27.1767, 78.0081),
    "Muzaffarpur": (26.1209, 85.3647),
    "Gurgaon":     (28.4595, 77.0266),
    "Jaipur":      (26.9124, 75.7873),
    "Ahmedabad":   (23.0225, 72.5714),
    "Srinagar":    (34.0837, 74.7973),
}

# India bounding box for FIRMS (country=IND)
INDIA_BBOX = "60.0,5.0,100.0,40.0"   # lon_min, lat_min, lon_max, lat_max

# ---------------------------------------------------------------------------
# Simple in-memory cache with TTL
# ---------------------------------------------------------------------------
_cache: dict[str, tuple[Any, float]] = {}
_cache_lock = threading.Lock()

def _cached(key: str, ttl_sec: int, fetcher):
    """
    Return cached value if fresh, otherwise call fetcher(), cache result,
    and return it.  Thread-safe.
    """
    with _cache_lock:
        if key in _cache:
            value, expires_at = _cache[key]
            if time.time() < expires_at:
                return value

    try:
        value = fetcher()
    except Exception as exc:
        log.warning(f"Fetcher for '{key}' failed: {exc}")
        value = None

    with _cache_lock:
        _cache[key] = (value, time.time() + ttl_sec)

    return value


# ---------------------------------------------------------------------------
# HTTP helper (no external deps — uses stdlib urllib)
# ---------------------------------------------------------------------------
def _get_json(url: str, headers: dict | None = None, timeout: int = 10) -> Any:
    """Perform an HTTP GET and return parsed JSON.  Raises on error."""
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get_text(url: str, headers: dict | None = None, timeout: int = 10) -> str:
    """Perform an HTTP GET and return raw text.  Raises on error."""
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8")


# ============================================================================
# 1. NASA FIRMS — Satellite fire / hotspot detection
# ============================================================================

def fetch_firms_hotspots(days: int = 1) -> list[dict]:
    """
    Fetch VIIRS SNPP NRT active fire hotspots over India via the NASA
    FIRMS country API.  Returns a list of dicts, each representing one
    satellite-detected hotspot.

    Requires FIRMS_MAP_KEY in .env.  Returns [] if key is missing.

    Each returned dict:
        lat, lon, brightness, frp, acq_date, acq_time, satellite,
        confidence, daynight

    Endpoint:
        GET https://firms.modaps.eosdis.nasa.gov/api/country/csv/{key}/VIIRS_SNPP_NRT/IND/{days}
    """
    if not FIRMS_MAP_KEY:
        log.warning("FIRMS_MAP_KEY not set — fire hotspot data will be simulated.")
        return []

    def _fetch():
        url = (
            f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/"
            f"{FIRMS_MAP_KEY}/VIIRS_SNPP_NRT/IND/{days}"
        )
        raw   = _get_text(url, timeout=15)
        rows  = []
        reader = csv.DictReader(io.StringIO(raw))
        for row in reader:
            try:
                rows.append({
                    "lat":        float(row.get("latitude",   0)),
                    "lon":        float(row.get("longitude",  0)),
                    "brightness": float(row.get("bright_ti4", 0)),
                    "frp":        float(row.get("frp",        0)),   # fire radiative power (MW)
                    "acq_date":   row.get("acq_date", ""),
                    "acq_time":   row.get("acq_time", ""),
                    "satellite":  row.get("satellite", "SNPP"),
                    "confidence": row.get("confidence", "n"),
                    "daynight":   row.get("daynight", "D"),
                    "source":     "NASA_FIRMS_VIIRS",
                })
            except (ValueError, KeyError):
                continue
        return rows

    result = _cached("firms_hotspots", ttl_sec=600, fetcher=_fetch)   # cache 10 min
    return result or []


def nearest_fire_km(city: str, hotspots: list[dict]) -> float | None:
    """
    Given a list of FIRMS hotspot dicts, compute the distance (km) from
    the city centroid to the nearest detected fire.
    Returns None if hotspots list is empty.
    """
    if not hotspots:
        return None

    import math
    lat1, lon1 = CITY_COORDS.get(city, (20.0, 78.0))
    lat1_r = math.radians(lat1)
    lon1_r = math.radians(lon1)

    min_km = float("inf")
    for hs in hotspots:
        lat2_r = math.radians(hs["lat"])
        lon2_r = math.radians(hs["lon"])
        dlat = lat2_r - lat1_r
        dlon = lon2_r - lon1_r
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
        dist_km = 6371 * 2 * math.asin(math.sqrt(a))
        if dist_km < min_km:
            min_km = dist_km

    return round(min_km, 1)


# ============================================================================
# 2. Open-Meteo — Free real-time weather (no API key required)
# ============================================================================

def fetch_weather(city: str) -> dict | None:
    """
    Fetch current weather for a city from Open-Meteo.
    Returns a dict with temperature_2m, apparent_temperature (heat index
    proxy), wind_speed_10m, relative_humidity_2m, precipitation.

    API: https://api.open-meteo.com/v1/forecast
    No authentication required.
    Cached per city for 15 minutes.
    """
    if city not in CITY_COORDS:
        return None

    def _fetch():
        lat, lon = CITY_COORDS[city]
        params = urllib.parse.urlencode({
            "latitude":          lat,
            "longitude":         lon,
            "current":           ",".join([
                "temperature_2m",
                "apparent_temperature",
                "relative_humidity_2m",
                "wind_speed_10m",
                "precipitation",
                "weather_code",
            ]),
            "wind_speed_unit":   "kmh",
            "timezone":          "Asia/Kolkata",
        })
        url  = f"https://api.open-meteo.com/v1/forecast?{params}"
        data = _get_json(url, timeout=10)
        cur  = data.get("current", {})
        return {
            "city":               city,
            "temperature_c":      cur.get("temperature_2m"),
            "apparent_temp_c":    cur.get("apparent_temperature"),    # feels-like / heat index
            "humidity_pct":       cur.get("relative_humidity_2m"),
            "wind_kmh":           cur.get("wind_speed_10m"),
            "precipitation_mm":   cur.get("precipitation"),
            "weather_code":       cur.get("weather_code"),
            "source":             "Open-Meteo",
            "fetched_at":         datetime.now(timezone.utc).isoformat(),
        }

    return _cached(f"weather_{city}", ttl_sec=900, fetcher=_fetch)   # 15 min cache


def fetch_all_weather() -> dict[str, dict]:
    """
    Fetch weather for all 10 cities in parallel using threads.
    Returns dict[city_name -> weather_dict].
    """
    results: dict[str, dict] = {}
    threads = []

    def _worker(city: str):
        w = fetch_weather(city)
        if w:
            results[city] = w

    for city in CITY_COORDS:
        t = threading.Thread(target=_worker, args=(city,), daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=12)

    return results


# ============================================================================
# 3. WAQI — Real AQI from ground monitoring stations (CPCB India)
# ============================================================================

# WAQI city slugs that match our 10 cities
WAQI_CITY_SLUGS: dict[str, str] = {
    "Delhi":       "delhi",
    "Kanpur":      "kanpur",
    "Lucknow":     "lucknow",
    "Patna":       "patna",
    "Agra":        "agra",
    "Muzaffarpur": "muzaffarpur",
    "Gurgaon":     "gurgaon",
    "Jaipur":      "jaipur",
    "Ahmedabad":   "ahmedabad",
    "Srinagar":    "srinagar",
}


def fetch_waqi_aqi(city: str) -> dict | None:
    """
    Fetch real AQI and pollutant breakdown for a city from the WAQI API.
    Uses the "demo" token for basic access (rate-limited) or your own
    WAQI_TOKEN for higher quota.

    Returns a dict with:
        aqi, pm25, pm10, no2, co, dominant_pollutant, station_name,
        station_time, source
    Returns None on failure.
    """
    slug = WAQI_CITY_SLUGS.get(city)
    if not slug:
        return None

    def _fetch():
        url  = f"https://api.waqi.info/feed/{slug}/?token={WAQI_TOKEN}"
        data = _get_json(url, timeout=10)

        if data.get("status") != "ok":
            return None

        d    = data["data"]
        iaqi = d.get("iaqi", {})

        def _v(key: str) -> float | None:
            val = iaqi.get(key, {}).get("v")
            return round(float(val), 2) if val is not None else None

        return {
            "city":               city,
            "aqi":                int(d.get("aqi", 0)),
            "pm25":               _v("pm25"),
            "pm10":               _v("pm10"),
            "no2":                _v("no2"),
            "co":                 _v("co"),
            "so2":                _v("so2"),
            "o3":                 _v("o3"),
            "dominant_pollutant": d.get("dominentpol", "pm25"),
            "station_name":       d.get("city", {}).get("name", city),
            "station_time":       d.get("time", {}).get("s", ""),
            "lat":                d.get("city", {}).get("geo", [None, None])[0],
            "lon":                d.get("city", {}).get("geo", [None, None])[1],
            "source":             "WAQI/CPCB",
            "fetched_at":         datetime.now(timezone.utc).isoformat(),
        }

    return _cached(f"waqi_{city}", ttl_sec=1800, fetcher=_fetch)   # 30 min cache


def fetch_all_waqi() -> dict[str, dict]:
    """
    Fetch real WAQI AQI for all 10 cities in parallel.
    Returns dict[city_name -> waqi_dict].
    """
    results: dict[str, dict] = {}
    threads = []

    def _worker(city: str):
        w = fetch_waqi_aqi(city)
        if w:
            results[city] = w

    for city in CITY_COORDS:
        t = threading.Thread(target=_worker, args=(city,), daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=12)

    return results


# ============================================================================
# 4. Sentinel-5P TROPOMI via Copernicus WMS (tile URL generator)
# ============================================================================

def tropomi_tile_url(layer: str = "NO2__NRTI_NO2___", date: str | None = None) -> str:
    """
    Construct a Copernicus S5P TROPOMI WMS tile URL for visualization.
    No auth required for public Copernicus WMS preview tiles.

    Layers available:
        NO2__NRTI_NO2___       — Near-real-time NO₂ column
        SO2__NRTI_SO2___       — SO₂ column
        CO___NRTI_CO____       — CO column
        CH4__OFFL_CH4___       — Methane (offline)
        AER_AI_NRTI_AER_AI___  — Aerosol Index

    Returns a WMS GetMap URL for India's bounding box.
    """
    if date is None:
        # Yesterday — S5P NRT latency is ~3h, so yesterday is always available
        date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    base = "https://creodias.sentinel-hub.com/ogc/wms/PUBLIC"
    params = urllib.parse.urlencode({
        "SERVICE":     "WMS",
        "VERSION":     "1.3.0",
        "REQUEST":     "GetMap",
        "LAYERS":      layer,
        "CRS":         "EPSG:4326",
        "BBOX":        "5.0,60.0,40.0,100.0",     # lat_min,lon_min,lat_max,lon_max
        "WIDTH":       800,
        "HEIGHT":      800,
        "FORMAT":      "image/png",
        "TIME":        f"{date}/{date}",
        "TRANSPARENT": "true",
    })
    return f"{base}?{params}"


def nasa_gibs_tile_url(layer: str = "MODIS_Terra_Aerosol", date: str | None = None) -> str:
    """
    Construct a NASA GIBS WMTS tile URL for satellite imagery.
    No authentication required.  Returns a TileMatrixSet URL for India.

    Useful layers:
        MODIS_Terra_Aerosol            — Aerosol optical depth (fire smoke)
        MODIS_Terra_CorrectedReflectance_TrueColor — True colour
        VIIRS_NOAA20_CorrectedReflectance_TrueColor
        MODIS_Terra_Fires_All          — Active fire detections
    """
    if date is None:
        date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    return (
        f"https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi?"
        f"SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap"
        f"&LAYERS={layer}"
        f"&CRS=EPSG:4326&BBOX=5,60,40,100"
        f"&WIDTH=800&HEIGHT=800"
        f"&FORMAT=image/png"
        f"&TIME={date}"
    )


# ============================================================================
# 5. Composite enrichment — blend real satellite data into a sensor reading
# ============================================================================

def enrich_reading_with_satellite(
    reading: dict,
    weather: dict | None,
    waqi: dict | None,
    firms_hotspots: list[dict],
) -> dict:
    """
    Take a simulated sensor reading and overwrite fields with real
    satellite / API data wherever available.

    Priority:
        heat_index     ← Open-Meteo apparent_temperature (real)
        fire_proximity ← NASA FIRMS nearest hotspot distance (real)
        aqi / pm25...  ← WAQI real station data (real)

    If any source is unavailable, keeps the simulated value.
    Returns enriched copy of the reading.
    """
    r = dict(reading)
    city = r["city"]

    # 1. Real heat index (apparent temperature) from Open-Meteo
    if weather and weather.get("apparent_temp_c") is not None:
        r["heat_index"]        = round(weather["apparent_temp_c"], 1)
        r["temperature_c"]     = weather.get("temperature_c")
        r["humidity_pct"]      = weather.get("humidity_pct")
        r["wind_kmh"]          = weather.get("wind_kmh")
        r["weather_source"]    = "OpenMeteo_Real"

    # 2. Real fire proximity from NASA FIRMS satellite
    real_fire_km = nearest_fire_km(city, firms_hotspots)
    if real_fire_km is not None:
        r["fire_proximity_km"] = real_fire_km
        r["fire_source"]       = "NASA_FIRMS_VIIRS"
    else:
        r["fire_source"]       = "simulated"

    # 3. Real AQI from WAQI (CPCB ground stations)
    if waqi and waqi.get("aqi") and waqi["aqi"] > 0:
        r["aqi"]             = waqi["aqi"]
        r["pm25"]            = waqi.get("pm25") or r["pm25"]
        r["pm10"]            = waqi.get("pm10") or r["pm10"]
        r["no2"]             = waqi.get("no2")  or r["no2"]
        r["co"]              = waqi.get("co")   or r["co"]
        r["dominant_pol"]    = waqi.get("dominant_pollutant", "pm25")
        r["station_name"]    = waqi.get("station_name", city)
        r["station_time"]    = waqi.get("station_time", "")
        r["aqi_source"]      = "WAQI_CPCB_Real"
    else:
        r["aqi_source"]      = "simulated"

    # Recompute breathe_score and alert_level with updated values
    from kafka_producer import compute_breathe_score, alert_level as compute_alert
    bs = compute_breathe_score(
        r["aqi"],
        r["river_tox_index"],
        r["fire_proximity_km"],
        r["heat_index"],
    )
    r["breathe_score"] = bs
    r["alert_level"]   = compute_alert(bs)

    return r


# ============================================================================
# 6. Satellite summary — a structured packet for the /api/satellite endpoint
# ============================================================================

def build_satellite_summary(firms_hotspots: list[dict], weather_all: dict, waqi_all: dict) -> dict:
    """
    Build a comprehensive satellite data summary packet to be served
    at GET /api/satellite.  This is what the dashboard's satellite
    panel consumes.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Per-city satellite snapshot
    city_snapshots = []
    for city, (lat, lon) in CITY_COORDS.items():
        fire_km   = nearest_fire_km(city, firms_hotspots)
        weather   = weather_all.get(city, {})
        waqi      = waqi_all.get(city, {})
        city_snapshots.append({
            "city":             city,
            "lat":              lat,
            "lon":              lon,
            # Real weather from Open-Meteo
            "apparent_temp_c":  weather.get("apparent_temp_c"),
            "humidity_pct":     weather.get("humidity_pct"),
            "wind_kmh":         weather.get("wind_kmh"),
            "precipitation_mm": weather.get("precipitation_mm"),
            "weather_code":     weather.get("weather_code"),
            # Real AQI from WAQI/CPCB
            "real_aqi":         waqi.get("aqi"),
            "real_pm25":        waqi.get("pm25"),
            "dominant_pol":     waqi.get("dominant_pollutant"),
            "station_name":     waqi.get("station_name"),
            "station_time":     waqi.get("station_time"),
            # Real fire from NASA FIRMS
            "nearest_fire_km":  fire_km,
            "fire_source":      "NASA_FIRMS_VIIRS" if fire_km is not None and firms_hotspots else "simulated",
        })

    # Fire hotspot summary (top 20 by FRP for the map)
    sorted_hotspots = sorted(firms_hotspots, key=lambda h: h.get("frp", 0), reverse=True)[:20]

    return {
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "firms_hotspots":   sorted_hotspots,
        "total_hotspots":   len(firms_hotspots),
        "city_snapshots":   city_snapshots,
        "tile_urls": {
            "aerosol_optical_depth": nasa_gibs_tile_url("MODIS_Terra_Aerosol", today),
            "true_color":            nasa_gibs_tile_url("MODIS_Terra_CorrectedReflectance_TrueColor", today),
            "active_fires":          nasa_gibs_tile_url("MODIS_Terra_Fires_All_Overlay", today),
            "no2_tropomi":           tropomi_tile_url("NO2__NRTI_NO2___"),
            "date":                  today,
        },
        "data_sources": {
            "fire":    "NASA FIRMS VIIRS SNPP NRT" if FIRMS_MAP_KEY else "simulated (set FIRMS_MAP_KEY)",
            "weather": "Open-Meteo (free, real-time)",
            "aqi":     "WAQI / CPCB India ground stations",
        },
    }


# ============================================================================
# Convenience: fetch everything in one call
# ============================================================================

def fetch_all_satellite_data() -> dict:
    """
    Fetch all satellite/real-time data in parallel and return the
    complete summary dict.  Designed to be called periodically from
    a background thread and cached in the pipeline's in-memory state.
    """
    results: dict = {}

    def _firms():
        results["firms"] = fetch_firms_hotspots(days=1)
    def _weather():
        results["weather"] = fetch_all_weather()
    def _waqi():
        results["waqi"] = fetch_all_waqi()

    threads = [
        threading.Thread(target=_firms,   daemon=True),
        threading.Thread(target=_weather, daemon=True),
        threading.Thread(target=_waqi,    daemon=True),
    ]
    for t in threads: t.start()
    for t in threads: t.join(timeout=20)

    firms    = results.get("firms",   [])
    weather  = results.get("weather", {})
    waqi     = results.get("waqi",    {})

    return build_satellite_summary(firms, weather, waqi)


if __name__ == "__main__":
    # Quick self-test
    print("Fetching all satellite data…")
    data = fetch_all_satellite_data()
    print(json.dumps(data, indent=2))
