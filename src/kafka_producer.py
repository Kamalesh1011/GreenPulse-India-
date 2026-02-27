"""
GreenPulse India — Kafka Producer with Real Satellite Data Enrichment
======================================================================

Streams environmental sensor data every 4 seconds to Kafka topic
``env-sensor-stream``. Each reading is enriched in real-time with:

  • Real heat index      ← Open-Meteo API (no key required)
  • Real fire proximity  ← NASA FIRMS VIIRS satellite (FIRMS_MAP_KEY in .env)
  • Real AQI             ← WAQI / CPCB ground stations (WAQI_TOKEN in .env)

Gracefully falls back to simulated values when APIs are unavailable.
Falls back to writing ``sensor_stream.jsonl`` when Kafka is unavailable.

Usage::

    python src/kafka_producer.py

Environment variables (set in .env):
    FIRMS_MAP_KEY   — NASA FIRMS satellite fire API key (optional)
    WAQI_TOKEN      — WAQI/CPCB AQI API token (optional)
"""

from __future__ import annotations

import json
import os
import random
import time
import threading
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: AQI typical range (min, max) per city — used when real WAQI data is absent
CITY_AQI_CONFIG: dict[str, tuple[int, int]] = {
    "Delhi":       (200, 450),
    "Kanpur":      (200, 430),
    "Patna":       (190, 420),
    "Agra":        (150, 380),
    "Muzaffarpur": (160, 390),
    "Lucknow":     (130, 350),
    "Gurgaon":     (120, 340),
    "Jaipur":      (80,  280),
    "Ahmedabad":   (70,  260),
    "Srinagar":    (50,  180),
}

#: Approximate city coordinates for fire proximity calculations
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

CITIES: list[str] = list(CITY_AQI_CONFIG.keys())
KAFKA_TOPIC: str = "env-sensor-stream"
FALLBACK_FILE: str = "sensor_stream.jsonl"
PRODUCE_INTERVAL_SEC: int = 4
SATELLITE_REFRESH_SEC: int = 600   # refresh every 10 minutes


# ---------------------------------------------------------------------------
# Satellite data cache — shared across producer iterations
# ---------------------------------------------------------------------------

_firms_hotspots: list[dict] = []
_weather_all: dict[str, dict] = {}
_waqi_all: dict[str, dict] = {}
_satellite_lock = threading.Lock()


def _refresh_satellite_data() -> None:
    """Background daemon thread: refresh satellite / external API data periodically.

    Fetches NASA FIRMS fire hotspots, Open-Meteo weather, and WAQI AQI data
    every ``SATELLITE_REFRESH_SEC`` seconds. Thread-safe via ``_satellite_lock``.
    Runs silently without terminating on API errors.
    """
    global _firms_hotspots, _weather_all, _waqi_all

    while True:
        try:
            from satellite_fetcher import (
                fetch_firms_hotspots,
                fetch_all_weather,
                fetch_all_waqi,
            )
            print("[Satellite] ↻ Refreshing: NASA FIRMS · Open-Meteo · WAQI…")
            firms   = fetch_firms_hotspots(days=1)
            weather = fetch_all_weather()
            waqi    = fetch_all_waqi()

            with _satellite_lock:
                _firms_hotspots[:] = firms
                _weather_all.update(weather)
                _waqi_all.update(waqi)

            print(
                f"[Satellite] ✓ Updated: {len(firms)} fire hotspots | "
                f"{len(weather)} weather cities | {len(waqi)} AQI stations"
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[Satellite] ✗ Refresh failed: {exc}")

        time.sleep(SATELLITE_REFRESH_SEC)


# ---------------------------------------------------------------------------
# Breathe Score™ computation
# ---------------------------------------------------------------------------

def compute_breathe_score(
    aqi: int,
    river_tox_index: float,
    fire_proximity_km: float,
    heat_index: float,
) -> float:
    """Compute the GreenPulse Breathe Score™ (0–100).

    The Breathe Score is a composite liveability metric that penalises each
    environmental hazard independently:

    .. code-block:: text

        breathe_score = max(0,
            100 - (aqi / 5)
                - (river_tox_index * 2)
                - (max(0, 500 - fire_proximity_km) / 50)
                - (max(0, heat_index - 35) * 1.5)
        )

    Args:
        aqi: Air Quality Index (0–500+).
        river_tox_index: River toxicity composite (0–10).
        fire_proximity_km: Distance to nearest fire hotspot in kilometres.
        heat_index: Apparent temperature in °C.

    Returns:
        Breathe Score rounded to 1 decimal place, clamped to [0, 100].
    """
    penalty_aqi   = aqi / 5.0
    penalty_river = river_tox_index * 2.0
    penalty_fire  = max(0.0, 500.0 - fire_proximity_km) / 50.0
    penalty_heat  = max(0.0, heat_index - 35.0) * 1.5
    return round(max(0.0, 100.0 - penalty_aqi - penalty_river - penalty_fire - penalty_heat), 1)


def alert_level(breathe_score: float) -> str:
    """Map a Breathe Score to a human-readable alert category.

    Args:
        breathe_score: A value in [0, 100].

    Returns:
        One of ``"SAFE"``, ``"MODERATE"``, ``"POOR"``, or ``"HAZARDOUS"``.
    """
    if breathe_score > 60:  return "SAFE"
    if breathe_score >= 40: return "MODERATE"
    if breathe_score >= 20: return "POOR"
    return "HAZARDOUS"


# ---------------------------------------------------------------------------
# Reading generation
# ---------------------------------------------------------------------------

def _count_nearby_hotspots(city: str, hotspots: list[dict], radius_deg: float = 2.0) -> int:
    """Count fire hotspots within ``radius_deg`` degrees of the city centre.

    Args:
        city: City name — must exist in ``CITY_COORDS``.
        hotspots: List of hotspot dicts with ``lat`` and ``lon`` keys.
        radius_deg: Search radius in decimal degrees (~220 km at 2°).

    Returns:
        Number of matching hotspots.
    """
    city_lat, city_lon = CITY_COORDS.get(city, (20.0, 78.0))
    return sum(
        1 for h in hotspots
        if abs(h.get("lat", 0) - city_lat) < radius_deg
        and abs(h.get("lon", 0) - city_lon) < radius_deg
    )


def generate_reading(city: str) -> dict:
    """Build one enriched sensor reading for a city.

    Base values are simulated from city-specific AQI calibration ranges.
    They are then overwritten with real satellite / API data wherever
    available (NASA FIRMS, Open-Meteo, WAQI/CPCB).

    Args:
        city: City name — must be a key in ``CITY_AQI_CONFIG``.

    Returns:
        Dictionary containing all sensor fields plus metadata fields
        ``data_sources`` and ``satellite_fires_nearby``.
    """
    lo, hi = CITY_AQI_CONFIG[city]
    aqi    = random.randint(lo, hi)

    # Derived pollutants — simulated, correlated with AQI
    pm25 = round(aqi * random.uniform(0.40, 0.55), 2)
    pm10 = round(aqi * random.uniform(0.70, 0.90), 2)
    no2  = round(random.uniform(20, 120) * (aqi / 300), 2)
    co   = round(random.uniform(0.5, 12.0) * (aqi / 300), 2)

    river_tox_index   = round(random.uniform(0.0, 10.0), 2)
    fire_proximity_km = round(random.uniform(0.0, 500.0), 2)
    heat_index        = round(random.uniform(28.0, 52.0), 2)

    data_sources: dict[str, str] = {
        "aqi": "simulated",
        "weather": "simulated",
        "fire": "simulated",
    }

    # --- Enrich with real satellite data (if available) ---
    with _satellite_lock:
        hotspots = list(_firms_hotspots)
        weather  = _weather_all.get(city, {})
        waqi     = _waqi_all.get(city, {})

    # 1. Real heat index from Open-Meteo
    if weather.get("apparent_temp_c") is not None:
        heat_index = round(float(weather["apparent_temp_c"]), 1)
        data_sources["weather"] = "Open-Meteo"

    # 2. Real fire proximity from NASA FIRMS
    if hotspots:
        try:
            from satellite_fetcher import nearest_fire_km  # type: ignore[import]
            real_fire = nearest_fire_km(city, hotspots)
            if real_fire is not None:
                fire_proximity_km = real_fire
                data_sources["fire"] = "NASA_FIRMS"
        except Exception:  # noqa: BLE001
            pass  # Keep simulated value

    # 3. Real AQI from WAQI / CPCB
    if waqi.get("aqi") and int(waqi["aqi"]) > 0:
        aqi  = int(waqi["aqi"])
        pm25 = float(waqi.get("pm25") or pm25)
        pm10 = float(waqi.get("pm10") or pm10)
        no2  = float(waqi.get("no2")  or no2)
        co   = float(waqi.get("co")   or co)
        data_sources["aqi"] = "WAQI_CPCB"

    bs = compute_breathe_score(aqi, river_tox_index, fire_proximity_km, heat_index)

    return {
        "city":                   city,
        "timestamp":              datetime.now(timezone.utc).isoformat(),
        "aqi":                    aqi,
        "pm25":                   round(pm25, 2),
        "pm10":                   round(pm10, 2),
        "no2":                    round(no2, 2),
        "co":                     round(co, 3),
        "river_tox_index":        river_tox_index,
        "fire_proximity_km":      fire_proximity_km,
        "heat_index":             heat_index,
        "breathe_score":          bs,
        "alert_level":            alert_level(bs),
        # Metadata — passed through the pipeline for transparency
        "data_sources":           json.dumps(data_sources),
        "satellite_fires_nearby": _count_nearby_hotspots(city, hotspots),
    }


# ---------------------------------------------------------------------------
# Kafka producer helpers
# ---------------------------------------------------------------------------

#: Fields transmitted to Kafka (strip metadata fields not in SensorSchema)
_KAFKA_FIELDS = frozenset({
    "city", "timestamp", "aqi", "pm25", "pm10", "no2", "co",
    "river_tox_index", "fire_proximity_km", "heat_index",
    "breathe_score", "alert_level",
})


def try_create_kafka_producer() -> Optional[object]:
    """Attempt to create and connect a Kafka producer.

    Returns:
        A connected ``KafkaProducer`` instance, or ``None`` if the broker
        is unreachable.
    """
    try:
        from kafka import KafkaProducer  # type: ignore[import]
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5_000,
            max_block_ms=5_000,
        )
        producer.bootstrap_connected()
        print("[Kafka] ✓ Connected to broker at localhost:9092")
        return producer
    except Exception as exc:  # noqa: BLE001
        print(f"[Kafka] ✗ Could not connect ({exc}). Falling back to file mode.")
        return None


def produce_to_kafka(producer: object, reading: dict) -> None:
    """Send one sensor reading to the Kafka topic.

    Strips metadata-only fields not included in ``SensorSchema``.

    Args:
        producer: A live ``KafkaProducer`` instance.
        reading: Full reading dict from ``generate_reading()``.
    """
    payload = {k: v for k, v in reading.items() if k in _KAFKA_FIELDS}
    producer.send(KAFKA_TOPIC, value=payload)  # type: ignore[attr-defined]
    producer.flush()  # type: ignore[attr-defined]


def produce_to_file(reading: dict) -> None:
    """Append one sensor reading as a JSON line to the fallback JSONL file.

    Args:
        reading: Full reading dict from ``generate_reading()``.
    """
    payload = {k: v for k, v in reading.items() if k in _KAFKA_FIELDS}
    with open(FALLBACK_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    """Start the satellite refresh daemon thread, then run the main producer loop.

    Cycles through all 10 cities in order, emitting one enriched sensor
    reading every ``PRODUCE_INTERVAL_SEC`` seconds.  Automatically falls
    back to file mode if the Kafka broker is unavailable.
    """
    print("=" * 60)
    print("  GreenPulse India — Kafka Producer + Satellite Enrichment")
    print("=" * 60)

    # -- Start satellite data background refresh --
    sat_thread = threading.Thread(
        target=_refresh_satellite_data,
        name="SatelliteRefresher",
        daemon=True,
    )
    sat_thread.start()
    print("[Producer] Satellite data refresh thread started.")
    print("[Producer] Waiting 5s for initial satellite fetch…")
    time.sleep(5)

    producer    = try_create_kafka_producer()
    mode        = "kafka" if producer else "file"
    print(f"[GreenPulse Producer] Streaming in {mode.upper()} mode")

    city_index  = 0
    events_sent = 0

    while True:
        city    = CITIES[city_index % len(CITIES)]
        reading = generate_reading(city)

        sources = json.loads(reading.get("data_sources", "{}"))
        src_tag = "/".join(f"{k}={v}" for k, v in sources.items())

        try:
            if producer:
                produce_to_kafka(producer, reading)
            else:
                produce_to_file(reading)

            events_sent += 1
            print(
                f"[{mode.upper()}] #{events_sent:05d} | {city:<14s} | "
                f"AQI={reading['aqi']:>3d} | BS={reading['breathe_score']:>5.1f} | "
                f"{reading['alert_level']:<10s} | {src_tag}"
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[ERROR] Failed to produce: {exc}")
            if producer:
                print("[Fallback] Switching to file mode…")
                producer = None
                mode     = "file"
                produce_to_file(reading)

        city_index += 1
        time.sleep(PRODUCE_INTERVAL_SEC)


if __name__ == "__main__":
    main()
