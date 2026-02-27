"""
GreenPulse India — Pathway Streaming Pipeline + FastAPI Server
==============================================================

This is the central brain of GreenPulse India.

Pipeline stages:

1. **Ingest** — Consume live sensor events from Kafka (or JSONL fallback).
2. **Transform** — Add document-text column via UDF; filter hazardous rows.
3. **Sink** — Python output connector populates shared in-memory state.
4. **RAG** — Live hybrid BM25 keyword index over natural-language sensor docs.
5. **LLM Q&A** — Answer questions grounded in the live index via OpenRouter.
6. **Serve** — FastAPI HTTP endpoints on port 8000.
7. **Run** — ``pw.run()`` keeps the streaming engine alive indefinitely.

Usage::

    python src/pathway_pipeline.py

Environment variables (set in .env):
    KAFKA_BOOTSTRAP       — Kafka broker (default: localhost:9092)
    FASTAPI_PORT          — HTTP port (default: 8000)
    OPENROUTER_API_KEY    — OpenRouter key for AI chat (optional)
    OPENROUTER_MODEL      — LLM model slug (default: google/gemini-flash-1.5)
"""

from __future__ import annotations

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

KAFKA_BOOTSTRAP:    str  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC:        str  = "env-sensor-stream"
KAFKA_GROUP_ID:     str  = "greenpulse"
FALLBACK_FILE:      str  = "sensor_stream.jsonl"
FASTAPI_PORT:       int  = int(os.getenv("FASTAPI_PORT", "8000"))
OPENROUTER_API_KEY: str  = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL:   str  = os.getenv("OPENROUTER_MODEL", "google/gemini-flash-1.5")
USE_LLM:            bool = bool(OPENROUTER_API_KEY)

#: Maximum number of sensor documents held in the live RAG index
RAG_INDEX_MAX_DOCS: int = 2_000

#: Maximum retained hazard events
HAZARD_HISTORY_MAX: int = 100

#: Satellite data refresh interval in seconds
SATELLITE_REFRESH_SEC: int = 600


# ---------------------------------------------------------------------------
# Shared in-memory state — populated by Pathway output callbacks
# ---------------------------------------------------------------------------

_latest:     dict[str, dict]         = {}
_history:    dict[str, deque]        = defaultdict(lambda: deque(maxlen=20))
_hazards:    list[dict]              = []
_index_docs: list[str]               = []
_stats:      dict[str, Any]          = {
    "total_events": 0,
    "index_size":   0,
    "last_update":  None,
    "kafka_mode":   True,
}
_state_lock = threading.Lock()

# Satellite summary — refreshed every 10 minutes in a background thread
_satellite_summary: dict[str, Any] = {}
_satellite_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Satellite data background refresh
# ---------------------------------------------------------------------------

def _satellite_refresh_loop() -> None:
    """Periodically fetch real satellite data and store in ``_satellite_summary``.

    Fetches from NASA FIRMS, Open-Meteo, and WAQI APIs via
    ``satellite_fetcher.fetch_all_satellite_data()``.  Silently skips if the
    module is unavailable (e.g. during unit tests).
    """
    global _satellite_summary
    try:
        from satellite_fetcher import fetch_all_satellite_data  # type: ignore[import]
        while True:
            try:
                print("[Satellite] ↻ Fetching NASA FIRMS · Open-Meteo · WAQI…")
                summary = fetch_all_satellite_data()
                with _satellite_lock:
                    _satellite_summary = summary
                n_hs = summary.get("total_hotspots", 0)
                print(f"[Satellite] ✓ Data updated ({n_hs} fire hotspots)")
            except Exception as exc:  # noqa: BLE001
                print(f"[Satellite] ✗ Fetch error: {exc}")
            time.sleep(SATELLITE_REFRESH_SEC)
    except ImportError:
        print("[Satellite] satellite_fetcher not found — skipping real satellite data")


# ---------------------------------------------------------------------------
# Pathway schema
# ---------------------------------------------------------------------------

class SensorSchema(pw.Schema):
    """Schema for sensor events arriving via Kafka or JSONL."""

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
    """Convert a sensor reading row into a natural-language document string.

    This text is stored in the live RAG index and passed to the LLM as
    grounding context when answering user questions.

    Returns:
        A concise English sentence summarising all sensor fields.
    """
    return (
        f"At {timestamp}, {city} recorded AQI {aqi}, PM2.5 {pm25} µg/m³, "
        f"PM10 {pm10} µg/m³, NO2 {no2} µg/m³, CO {co} mg/m³, "
        f"Breathe Score {breathe_score}, Alert: {alert_level}. "
        f"River toxicity index: {river_tox_index}. "
        f"Nearest fire: {fire_proximity_km} km."
    )


@pw.udf
def is_hazardous(alert_lvl: str) -> bool:
    """Return ``True`` when the alert level is ``"HAZARDOUS"``.

    Args:
        alert_lvl: The ``alert_level`` field from a sensor reading.

    Returns:
        Boolean flag used to filter the hazard stream.
    """
    return alert_lvl == "HAZARDOUS"


# ---------------------------------------------------------------------------
# LLM answer generation (grounded in the live RAG index)
# ---------------------------------------------------------------------------

def _bm25_retrieve(question: str, docs: list[str], top_k: int = 5) -> list[str]:
    """Retrieve the top-``top_k`` most relevant docs using BM25-style keyword scoring.

    Args:
        question: The user's natural-language question.
        docs: Candidate document strings from the live index.
        top_k: Number of documents to return.

    Returns:
        A list of up to ``top_k`` document strings, sorted by descending overlap.
    """
    q_tokens = set(question.lower().split())
    scored   = sorted(
        ((len(q_tokens & set(doc.lower().split())), doc) for doc in docs),
        key=lambda x: x[0],
        reverse=True,
    )
    return [doc for _, doc in scored[:top_k]]


def answer_question_with_rag(question: str) -> dict[str, Any]:
    """Answer a natural-language question grounded in the live sensor index.

    1. Retrieve top-5 documents via BM25 keyword scoring.
    2. (If configured) send them to the OpenRouter LLM for synthesis.
    3. Return the answer text and the raw document citations.

    Args:
        question: The user's question string.

    Returns:
        Dict with keys ``"answer"`` (str) and ``"citations"`` (list[str]).
    """
    with _state_lock:
        docs = list(_index_docs)

    if not docs:
        return {
            "answer": (
                "No sensor data has been indexed yet. "
                "Please wait a few seconds for the stream to populate."
            ),
            "citations": [],
        }

    top_docs = _bm25_retrieve(question, docs, top_k=5)

    if not USE_LLM:
        context_summary = "\n".join(f"• {d}" for d in top_docs)
        return {
            "answer": (
                "⚠ No OPENROUTER_API_KEY configured — showing raw retrieved context:\n\n"
                + context_summary
            ),
            "citations": top_docs,
        }

    try:
        import openai  # type: ignore[import]
        client = openai.OpenAI(
            api_key=OPENROUTER_API_KEY,
            base_url="https://openrouter.ai/api/v1",
            default_headers={
                "HTTP-Referer": "https://greenpulse.india",
                "X-Title":      "GreenPulse India",
            },
        )
        context = "\n\n".join(f"[Doc {i+1}] {d}" for i, d in enumerate(top_docs))
        system_prompt = (
            "You are GreenPulse AI, an environmental intelligence assistant for India. "
            "You MUST ground every answer in the provided sensor documents. "
            "Cite city names and timestamps from the documents in every response. "
            "Be concise but informative. "
            "If the documents don't contain enough information, say so explicitly."
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
            "citations": top_docs,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "answer":    f"LLM error: {exc}. Raw retrieved docs shown above.",
            "citations": top_docs,
        }


# ---------------------------------------------------------------------------
# Pathway output sink callback
# ---------------------------------------------------------------------------

def _on_row(row: dict) -> None:
    """Update all shared in-memory state dictionaries from a committed Pathway row.

    Called by ``_pw_row_callback`` for every *addition* event.  Maintains:
    - ``_latest``  — most recent reading per city
    - ``_history`` — rolling 20-event deque per city
    - ``_hazards`` — HAZARDOUS events (capped at ``HAZARD_HISTORY_MAX``)
    - ``_index_docs`` — natural-language docs for RAG (capped at ``RAG_INDEX_MAX_DOCS``)
    - ``_stats``   — pipeline-level counters

    Args:
        row: The committed Pathway output row as a plain Python dict.
    """
    city = row.get("city", "Unknown")

    text = (
        f"At {row.get('timestamp','?')}, {city} recorded AQI {row.get('aqi','?')}, "
        f"PM2.5 {row.get('pm25','?')} µg/m³, PM10 {row.get('pm10','?')} µg/m³, "
        f"NO2 {row.get('no2','?')} µg/m³, CO {row.get('co','?')} mg/m³, "
        f"Breathe Score {row.get('breathe_score','?')}, Alert: {row.get('alert_level','?')}. "
        f"River toxicity index: {row.get('river_tox_index','?')}. "
        f"Nearest fire: {row.get('fire_proximity_km','?')} km."
    )

    with _state_lock:
        _latest[city] = row
        _history[city].append(row)

        _index_docs.append(text)
        if len(_index_docs) > RAG_INDEX_MAX_DOCS:
            _index_docs.pop(0)

        if row.get("alert_level") == "HAZARDOUS":
            _hazards.append(row)
            if len(_hazards) > HAZARD_HISTORY_MAX:
                _hazards.pop(0)

        _stats["total_events"] += 1
        _stats["index_size"]    = len(_index_docs)
        _stats["last_update"]   = datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="GreenPulse India API",
    description=(
        "Real-time environmental intelligence for 10 Indian cities, "
        "powered by Pathway streaming + Apache Kafka + NASA satellite data."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class AskRequest(BaseModel):
    """Request body for the AI chat endpoint."""

    question: str


@app.get("/api/latest", summary="Latest reading per city")
def get_latest() -> list[dict]:
    """Return the most recent sensor reading for every tracked city."""
    with _state_lock:
        return list(_latest.values())


@app.get("/api/city/{city_name}", summary="History for a single city")
def get_city(city_name: str) -> list[dict]:
    """Return the last 20 sensor readings for the specified city.

    Args:
        city_name: Exact city name (case-sensitive), e.g. ``Delhi``.

    Raises:
        HTTPException(404): If no data has been received for the city yet.
    """
    with _state_lock:
        data = list(_history.get(city_name, []))
    if not data:
        raise HTTPException(status_code=404, detail=f"No data for city '{city_name}'")
    return data


@app.get("/api/alerts", summary="All HAZARDOUS events")
def get_alerts() -> list[dict]:
    """Return all HAZARDOUS events collected since the pipeline started."""
    with _state_lock:
        return list(_hazards)


@app.get("/api/breathe-scores", summary="Breathe Score™ ranking")
def get_breathe_scores() -> list[dict]:
    """Return all cities sorted by Breathe Score ascending (worst first)."""
    with _state_lock:
        result = [
            {
                "city":         city,
                "breathe_score": data.get("breathe_score", 0),
                "alert_level":   data.get("alert_level", "UNKNOWN"),
            }
            for city, data in _latest.items()
        ]
    return sorted(result, key=lambda x: x["breathe_score"])


@app.post("/api/ask", summary="AI chat (live RAG)")
def ask_question(body: AskRequest) -> dict:
    """Answer a natural-language question grounded in the live sensor data stream.

    Retrieves the 5 most relevant documents from the live RAG index using
    BM25-style keyword scoring, then calls an OpenRouter LLM for synthesis.

    Args:
        body: JSON body with a ``question`` string field.

    Raises:
        HTTPException(400): If the question is empty.
    """
    if not body.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    return answer_question_with_rag(body.question)


@app.get("/api/stats", summary="Pipeline statistics")
def get_stats() -> dict:
    """Return real-time pipeline health and throughput statistics."""
    with _state_lock:
        return {
            **_stats,
            "cities_tracked": len(_latest),
            "hazard_events":  len(_hazards),
        }


@app.get("/api/satellite", summary="Real satellite data snapshot")
def get_satellite() -> dict:
    """Return the latest real satellite data snapshot.

    Includes:
    - NASA FIRMS VIIRS fire hotspots over India
    - Open-Meteo apparent temperature per city
    - WAQI/CPCB real AQI per city
    - NASA GIBS & Copernicus WMS tile URLs
    """
    with _satellite_lock:
        if _satellite_summary:
            return _satellite_summary

    # Fallback: on-demand fetch if background thread hasn't run yet
    try:
        from satellite_fetcher import build_satellite_summary  # type: ignore[import]
        return build_satellite_summary([], {}, {})
    except ImportError:
        return {"error": "satellite_fetcher module not available"}


@app.get("/health", summary="Health check")
def health() -> dict:
    """Simple liveness probe for load balancers and monitoring systems."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# ---------------------------------------------------------------------------
# Pathway pipeline construction
# ---------------------------------------------------------------------------

def build_pathway_pipeline() -> None:
    """Construct and start the Pathway streaming pipeline.

    Steps:
    1. Detect Kafka availability; use JSONL fallback if unreachable.
    2. Read incoming events with the ``SensorSchema``.
    3. Derive a ``doc`` column via the ``doc_text()`` UDF.
    4. Filter a dedicated hazard stream via ``is_hazardous()`` UDF.
    5. Write all enriched rows to shared memory via ``_pw_row_callback()``.
    6. Call ``pw.run()`` — this blocks forever, processing events as they arrive.
    """
    # ------ Step 1 — Detect Kafka ------
    kafka_available = _check_kafka_available()
    _stats["kafka_mode"] = kafka_available

    if kafka_available:
        print("[Pathway] ✓ Kafka available — using pw.io.kafka.read()")
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
        import pathlib
        pathlib.Path(FALLBACK_FILE).touch()
        raw_stream = pw.io.jsonlines.read(
            FALLBACK_FILE,
            schema=SensorSchema,
            mode="streaming",
        )

    # ------ Step 2 — Transform ------
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

    # ------ Step 3 — Hazard filter (unused sink, kept as extension point) ------
    _hazard_stream = enriched.filter(is_hazardous(pw.this.alert_level))  # noqa: F841

    # ------ Step 4 — Output sink ------
    pw.io.python.write(enriched, _pw_row_callback)

    # ------ Step 5 — Run ------
    print("[Pathway] ▶ Starting streaming engine with pw.run() …")
    pw.run()


def _check_kafka_available() -> bool:
    """Probe the Kafka broker with a TCP connection.

    Returns:
        ``True`` if the broker is reachable, ``False`` otherwise.
    """
    try:
        import socket
        host, port = KAFKA_BOOTSTRAP.split(":")
        sock = socket.create_connection((host, int(port)), timeout=3)
        sock.close()
        return True
    except Exception:  # noqa: BLE001
        return False


def _pw_row_callback(key: pw.Pointer, row: dict, time: int, is_addition: bool) -> None:  # noqa: ARG001
    """Pathway Python output callback — called for every row event.

    Only processes *addition* events (new data); ignores retractions.

    Args:
        key: Pathway internal row key (unused).
        row: The output row as a plain Python dict.
        time: Pathway logical timestamp (unused).
        is_addition: ``True`` for new rows, ``False`` for retractions.
    """
    if is_addition:
        _on_row(row)


# ---------------------------------------------------------------------------
# Entry point — run FastAPI + Pathway + Satellite concurrently
# ---------------------------------------------------------------------------

def main() -> None:
    """Launch the full GreenPulse India stack.

    Starts three concurrent execution units:
    1. **SatelliteRefresher** — daemon thread; fetches NASA/WAQI/Open-Meteo every 10 min.
    2. **PathwayEngine** — daemon thread; runs ``pw.run()`` indefinitely.
    3. **uvicorn** — main thread; serves FastAPI on ``FASTAPI_PORT``.
    """
    print("=" * 60)
    print("  GreenPulse India — Environmental Intelligence Platform")
    print("=" * 60)

    # Start satellite background refresh
    satellite_thread = threading.Thread(
        target=_satellite_refresh_loop,
        name="SatelliteRefresher",
        daemon=True,
    )
    satellite_thread.start()
    print("[Main] Satellite data refresh thread started.")

    # Start Pathway engine
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
