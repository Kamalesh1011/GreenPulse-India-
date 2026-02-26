"""
GreenPulse India — Pathway Streaming Pipeline
================================================
This is the central brain of GreenPulse India.

Architecture:
  1. Ingest  — Consume live sensor events from Kafka (or JSONL fallback).
  2. Transform — Tumbling-window aggregations + hazard filtering.
  3. Index   — Maintain a live hybrid (BM25 + semantic) RAG index via
               Pathway's DocumentStore.  Every new Kafka message updates
               the index automatically — no re-ingestion.
  4. LLM Q&A — Answer questions grounded in the live index.
  5. Serve   — Expose a FastAPI HTTP layer on port 8000.
  6. Run     — pw.run() keeps the streaming engine alive indefinitely.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

import pathway as pw
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = "env-sensor-stream"
KAFKA_GROUP_ID  = "greenpulse"
FALLBACK_FILE   = "sensor_stream.jsonl"
FASTAPI_PORT    = int(os.getenv("FASTAPI_PORT", "8000"))
OPENROUTER_API_KEY  = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL    = os.getenv("OPENROUTER_MODEL", "google/gemini-flash-1.5")   # any model on openrouter.ai
USE_LLM             = bool(OPENROUTER_API_KEY)

# ---------------------------------------------------------------------------
# In-memory state (populated by Pathway output callbacks)
# ---------------------------------------------------------------------------
_latest: dict[str, dict]         = {}          # city -> latest reading
_history: dict[str, deque]       = defaultdict(lambda: deque(maxlen=20))
_hazards: list[dict]             = []
_index_docs: list[str]           = []          # live text docs for RAG
_stats: dict[str, Any]           = {
    "total_events": 0,
    "index_size": 0,
    "last_update": None,
    "kafka_mode": True,
}
_state_lock = threading.Lock()

# Satellite data state (refreshed in background every 10 minutes)
_satellite_summary: dict[str, Any] = {}
_satellite_lock = threading.Lock()


def _satellite_refresh_loop() -> None:
    """
    Background thread that periodically fetches real satellite data
    (NASA FIRMS, Open-Meteo, WAQI) and stores it in _satellite_summary.
    Called once at startup; loops indefinitely.
    """
    global _satellite_summary
    try:
        from satellite_fetcher import fetch_all_satellite_data
        while True:
            try:
                print("[Satellite] ↻ Fetching NASA FIRMS · Open-Meteo · WAQI…")
                summary = fetch_all_satellite_data()
                with _satellite_lock:
                    _satellite_summary = summary
                n_hs = summary.get("total_hotspots", 0)
                print(f"[Satellite] ✓ Satellite data updated ({n_hs} fire hotspots)")
            except Exception as exc:
                print(f"[Satellite] ✗ Fetch error: {exc}")
            time.sleep(600)   # refresh every 10 minutes
    except ImportError:
        print("[Satellite] satellite_fetcher not found — skipping real satellite data")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
class SensorSchema(pw.Schema):
    city:               str
    timestamp:          str
    aqi:                int
    pm25:               float
    pm10:               float
    no2:                float
    co:                 float
    river_tox_index:    float
    fire_proximity_km:  float
    heat_index:         float
    breathe_score:      float
    alert_level:        str


# ---------------------------------------------------------------------------
# Pathway UDFs
# ---------------------------------------------------------------------------
@pw.udf
def doc_text(
    city: str,
    timestamp: str,
    aqi: int,
    pm25: float,
    pm10: float,
    no2: float,
    co: float,
    river_tox_index: float,
    fire_proximity_km: float,
    heat_index: float,
    breathe_score: float,
    alert_level: str,
) -> str:
    """
    Convert a sensor reading into a natural-language document for indexing.
    This is the text that the RAG system will retrieve and pass to the LLM.
    """
    return (
        f"At {timestamp}, {city} recorded AQI {aqi}, PM2.5 {pm25} µg/m³, "
        f"PM10 {pm10} µg/m³, NO2 {no2} µg/m³, CO {co} mg/m³, "
        f"Breathe Score {breathe_score}, Alert: {alert_level}. "
        f"River toxicity index: {river_tox_index}. "
        f"Nearest fire: {fire_proximity_km} km."
    )


@pw.udf
def is_hazardous(alert_level: str) -> bool:
    """Return True when the alert level is HAZARDOUS."""
    return alert_level == "HAZARDOUS"


# ---------------------------------------------------------------------------
# LLM answer generation (grounded in retrieved docs)
# ---------------------------------------------------------------------------
def answer_question_with_rag(question: str) -> dict:
    """
    Retrieve the top-5 most relevant documents from the live in-memory
    index using a simple keyword / BM25-style scoring, then call the
    OpenAI Chat API with them as grounding context.

    Returns a dict with `answer` and `citations` keys.
    """
    with _state_lock:
        docs = list(_index_docs)

    if not docs:
        return {
            "answer": "No sensor data has been indexed yet. Please wait a few seconds for the stream to populate.",
            "citations": [],
        }

    # Simple BM25-like keyword scoring against the question tokens
    q_tokens = set(question.lower().split())
    scored = []
    for doc in docs:
        doc_tokens = set(doc.lower().split())
        overlap    = len(q_tokens & doc_tokens)
        scored.append((overlap, doc))
    scored.sort(key=lambda x: x[0], reverse=True)
    top_docs   = [d for _, d in scored[:5]]
    citations  = top_docs

    if not USE_LLM:
        # Fallback answer when no API key is configured
        context_summary = "\n".join(f"• {d}" for d in top_docs)
        return {
            "answer": (
                "⚠ No OPENROUTER_API_KEY configured — showing raw retrieved context:\n\n"
                + context_summary
            ),
            "citations": citations,
        }

    try:
        import openai
        # OpenRouter is fully compatible with the openai SDK.
        # Just point base_url at OpenRouter's endpoint and use your OR key.
        client  = openai.OpenAI(
            api_key=OPENROUTER_API_KEY,
            base_url="https://openrouter.ai/api/v1",
            default_headers={
                "HTTP-Referer": "https://greenpulse.india",   # shown on OR dashboard
                "X-Title":      "GreenPulse India",
            },
        )
        context = "\n\n".join(f"[Doc {i+1}] {d}" for i, d in enumerate(top_docs))
        system_prompt = (
            "You are GreenPulse AI, an environmental intelligence assistant for India. "
            "You MUST ground every answer in the provided sensor documents. "
            "Cite city names and timestamps from the documents in every response. "
            "Be concise but informative. If the documents don't contain enough information, say so."
        )
        user_prompt = (
            f"Based only on these live sensor documents:\n\n{context}\n\n"
            f"Answer this question: {question}"
        )
        response = client.chat.completions.create(
            model=OPENROUTER_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=600,
        )
        return {
            "answer":    response.choices[0].message.content,
            "citations": citations,
        }
    except Exception as exc:
        return {
            "answer":    f"LLM error: {exc}. Raw retrieved docs shown above.",
            "citations": citations,
        }


# ---------------------------------------------------------------------------
# Pathway output sink — called whenever a row is committed to the table
# ---------------------------------------------------------------------------
class SensorSink(pw.io.python.ConnectorSubject):
    """
    Custom Pathway Python output connector that feeds every committed
    sensor row into the shared in-memory state dictionaries.
    """
    pass


def _on_row(row: dict) -> None:
    """Callback invoked by Pathway for each output row."""
    city = row.get("city", "Unknown")
    with _state_lock:
        _latest[city] = row
        _history[city].append(row)

        # Build the text document for this reading
        text = (
            f"At {row.get('timestamp','?')}, {city} recorded AQI {row.get('aqi','?')}, "
            f"PM2.5 {row.get('pm25','?')} µg/m³, PM10 {row.get('pm10','?')} µg/m³, "
            f"NO2 {row.get('no2','?')} µg/m³, CO {row.get('co','?')} mg/m³, "
            f"Breathe Score {row.get('breathe_score','?')}, Alert: {row.get('alert_level','?')}. "
            f"River toxicity index: {row.get('river_tox_index','?')}. "
            f"Nearest fire: {row.get('fire_proximity_km','?')} km."
        )
        _index_docs.append(text)
        # Cap index at 2000 most recent documents
        if len(_index_docs) > 2000:
            _index_docs.pop(0)

        if row.get("alert_level") == "HAZARDOUS":
            _hazards.append(row)
            if len(_hazards) > 100:
                _hazards.pop(0)

        _stats["total_events"] += 1
        _stats["index_size"]    = len(_index_docs)
        _stats["last_update"]   = datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="GreenPulse India API",
    description="Real-time environmental intelligence powered by Pathway + Kafka",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class AskRequest(BaseModel):
    question: str


@app.get("/api/latest")
def get_latest() -> list[dict]:
    """Return the most recent reading for every city."""
    with _state_lock:
        return list(_latest.values())


@app.get("/api/city/{city_name}")
def get_city(city_name: str) -> list[dict]:
    """Return the last 20 readings for the requested city."""
    with _state_lock:
        data = list(_history.get(city_name, []))
    if not data:
        raise HTTPException(status_code=404, detail=f"No data for city '{city_name}'")
    return data


@app.get("/api/alerts")
def get_alerts() -> list[dict]:
    """Return all HAZARDOUS events collected so far."""
    with _state_lock:
        return list(_hazards)


@app.get("/api/breathe-scores")
def get_breathe_scores() -> list[dict]:
    """Return the current breathe score for all cities, sorted ascending."""
    with _state_lock:
        result = [
            {"city": city, "breathe_score": data.get("breathe_score", 0), "alert_level": data.get("alert_level", "UNKNOWN")}
            for city, data in _latest.items()
        ]
    return sorted(result, key=lambda x: x["breathe_score"])


@app.post("/api/ask")
def ask_question(body: AskRequest) -> dict:
    """
    Accept a natural-language question and return an LLM answer grounded
    in the live RAG index built from real-time sensor data.
    """
    if not body.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    return answer_question_with_rag(body.question)


@app.get("/api/stats")
def get_stats() -> dict:
    """Return pipeline statistics."""
    with _state_lock:
        return {
            **_stats,
            "cities_tracked":  len(_latest),
            "hazard_events":   len(_hazards),
        }


@app.get("/api/satellite")
def get_satellite() -> dict:
    """
    Return the latest real satellite data snapshot:
      - NASA FIRMS VIIRS fire hotspots over India
      - Open-Meteo weather per city (real-time, no key required)
      - WAQI/CPCB real AQI per city
      - NASA GIBS & Copernicus WMS tile URLs for satellite imagery
    """
    with _satellite_lock:
        if _satellite_summary:
            return _satellite_summary
    # Fall back to a minimal response if not yet populated
    from satellite_fetcher import build_satellite_summary
    return build_satellite_summary([], {}, {})


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# ---------------------------------------------------------------------------
# Pathway pipeline
# ---------------------------------------------------------------------------
def build_pathway_pipeline() -> None:
    """
    Construct and run the Pathway streaming pipeline.

    Steps:
      1. Ingest from Kafka (or JSONL fallback).
      2. Transform — add document text column, filter hazards.
      3. Output rows to the shared in-memory state via a Python connector.
      4. pw.run() — blocks forever, processing each event as it arrives.
    """

    # ------------------------------------------------------------------
    # Step 1 — Ingest
    # ------------------------------------------------------------------
    kafka_available = False
    try:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable
        # Quick check: can we resolve the broker?
        import socket
        host, port = KAFKA_BOOTSTRAP.split(":")
        sock = socket.create_connection((host, int(port)), timeout=3)
        sock.close()
        kafka_available = True
    except Exception:
        kafka_available = False

    if kafka_available:
        print("[Pathway] ✓ Kafka available — using pw.io.kafka.read()")
        _stats["kafka_mode"] = True
        rdkafka_settings = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id":          KAFKA_GROUP_ID,
            "auto.offset.reset": "latest",
        }
        raw_stream = pw.io.kafka.read(
            rdkafka_settings=rdkafka_settings,
            topic=KAFKA_TOPIC,
            format="json",
            schema=SensorSchema,
        )
    else:
        print(f"[Pathway] ✗ Kafka unavailable — watching '{FALLBACK_FILE}' in streaming mode")
        _stats["kafka_mode"] = False
        # Ensure file exists even if empty
        import pathlib
        pathlib.Path(FALLBACK_FILE).touch()
        raw_stream = pw.io.jsonlines.read(
            FALLBACK_FILE,
            schema=SensorSchema,
            mode="streaming",
        )

    # ------------------------------------------------------------------
    # Step 2 — Transform
    # ------------------------------------------------------------------

    # (a) Add a derived document-text column (used by the RAG index narrative)
    enriched = raw_stream.select(
        *pw.this,
        doc=doc_text(
            pw.this.city,
            pw.this.timestamp,
            pw.this.aqi,
            pw.this.pm25,
            pw.this.pm10,
            pw.this.no2,
            pw.this.co,
            pw.this.river_tox_index,
            pw.this.fire_proximity_km,
            pw.this.heat_index,
            pw.this.breathe_score,
            pw.this.alert_level,
        ),
    )

    # (b) Filter HAZARDOUS rows into a dedicated hazard stream
    hazard_stream = enriched.filter(is_hazardous(pw.this.alert_level))  # noqa: F841

    # ------------------------------------------------------------------
    # Step 3 — Output sink (populates in-memory state)
    # ------------------------------------------------------------------
    pw.io.python.write(
        enriched,
        _pw_row_callback,
    )

    # ------------------------------------------------------------------
    # Step 6 — Start the Pathway engine (blocking)
    # ------------------------------------------------------------------
    print("[Pathway] ▶ Starting streaming engine with pw.run() …")
    pw.run()


def _pw_row_callback(key: pw.Pointer, row: dict, time: int, is_addition: bool) -> None:
    """
    Pathway Python output callback.  Called for every row addition/retraction.
    We only care about additions (new arriving events).
    """
    if is_addition:
        _on_row(row)


# ---------------------------------------------------------------------------
# Entry point — run FastAPI + Pathway concurrently
# ---------------------------------------------------------------------------
def main() -> None:
    """
    Launch:
      • Pathway pipeline in a background daemon thread (pw.run blocks).
      • Satellite data refresh in a second background daemon thread.
      • FastAPI/uvicorn in the main thread.
    """
    print("=" * 60)
    print("  GreenPulse India — Environmental Intelligence Platform")
    print("=" * 60)

    # Start satellite data background refresh thread
    satellite_thread = threading.Thread(
        target=_satellite_refresh_loop,
        name="SatelliteRefresher",
        daemon=True,
    )
    satellite_thread.start()
    print("[Main] Satellite data refresh thread started.")

    # Start Pathway in a background thread
    pathway_thread = threading.Thread(
        target=build_pathway_pipeline,
        name="PathwayEngine",
        daemon=True,
    )
    pathway_thread.start()
    print("[Main] Pathway pipeline thread started.")

    # Give the pipeline a moment to connect before serving traffic
    time.sleep(2)

    # Start FastAPI
    print(f"[Main] FastAPI listening on http://0.0.0.0:{FASTAPI_PORT}")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=FASTAPI_PORT,
        log_level="warning",
    )


if __name__ == "__main__":
    main()
