# Architecture Deep Dive

## Overview

GreenPulse India is built on a **reactive streaming architecture** — every sensor event triggers an immediate cascade through the entire pipeline, from Kafka ingestion to live dashboard update, with sub-second end-to-end latency.

## Core Design Principles

### 1. Event-Driven, Not Poll-Driven

Traditional architectures poll a database on a schedule. GreenPulse uses **Pathway's reactive dataflow**: every Kafka message is processed the instant it arrives. No scheduled jobs, no batch windows, no stale data.

### 2. Single Source of Truth via Kafka

Kafka topic `env-sensor-stream` is the immutable log of all sensor events. Both the Pathway pipeline and any future consumers can replay events from any offset.

### 3. Thread-Safe In-Memory State

Pathway runs in a background daemon thread. The FastAPI server runs in the main thread. State is shared via Python dicts and deques protected by a single `threading.Lock()`. This avoids the overhead of a database while maintaining correctness.

### 4. Dual-Mode Streaming

The platform detects Kafka broker availability at startup and falls back to JSONL file streaming. This makes local development possible without Docker.

### 5. Graceful Satellite Data Degradation

All three external APIs (NASA FIRMS, Open-Meteo, WAQI) are optional. When unavailable, the producer falls back to simulated values that preserve the correct statistical distribution per city.

## Component Interactions

```
kafka_producer.py ──Kafka──▶ pathway_pipeline.py ──HTTP──▶ frontend/index.html
      │                              │
      └──SatelliteThread             └──SatelliteThread
         (NASA/OpenMeteo/WAQI)          (same data, ½ update latency)
```

Both the producer and the pipeline run independent satellite refresh threads. This redundancy means:
- The producer enriches messages before they hit Kafka
- The pipeline can independently serve `/api/satellite` even during producer downtime

## Pathway Reactive Dataflow

```python
raw_stream = pw.io.kafka.read(...)           # Step 1: Ingest
enriched   = raw_stream.select(              # Step 2: Transform
    *pw.this,
    doc=doc_text(*pw.this.*),               #   → doc column for RAG
)
hazards    = enriched.filter(               # Step 3: Filter
    is_hazardous(pw.this.alert_level)
)
pw.io.python.write(enriched, callback)      # Step 4: Sink → in-memory state
pw.run()                                    # Step 5: Start engine (blocking)
```

Every time a new Kafka message arrives, Pathway re-evaluates all downstream dependencies **incrementally** — only the affected rows are recomputed.

## RAG Index Architecture

The live RAG index is a simple list of natural-language strings, updated on every sensor event. Retrieval uses **BM25-style keyword overlap scoring**:

```python
q_tokens = set(question.lower().split())
overlap   = len(q_tokens & set(doc.lower().split()))
```

Top-5 documents are sent to the OpenRouter LLM as grounding context. This gives reliable, citation-backed answers without a vector database.

## Satellite Data Pipeline

```
Background Thread (SATELLITE_REFRESH_SEC = 600)
    │
    ├── fetch_firms_hotspots(days=1)   → NASA FIRMS CSV over India bounding box
    ├── fetch_all_weather()            → Open-Meteo /v1/forecast per city lat/lon
    └── fetch_all_waqi()               → WAQI api.waqi.info per city token
         │
         └── Stored in thread-safe global cache
              │
              └── generate_reading(city) reads cache on each producer cycle
```
