# Changelog

All notable changes to GreenPulse India are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [2.0.0] — 2026-02-27

### 🚀 Added
- **Real satellite data integration** — NASA FIRMS VIIRS fire hotspots, Open-Meteo weather, WAQI/CPCB AQI
- **`satellite_fetcher.py`** — dedicated module for all external API clients with graceful fallback
- **`/api/satellite` endpoint** — real-time satellite data snapshot exposed via FastAPI
- **Satellite panel** in the frontend dashboard (fire hotspot map, per-city satellite AQI/weather)
- **Background satellite refresh thread** in both producer and pipeline (10-minute interval)
- **OpenRouter LLM support** — replaces raw OpenAI key with OpenRouter (access GPT-4, Gemini, Claude)
- Data source transparency in producer output (`aqi=WAQI_CPCB/weather=Open-Meteo/fire=NASA_FIRMS`)

### 🔧 Changed
- `pathway_pipeline.py` now uses `OPENROUTER_API_KEY` + `OPENROUTER_MODEL` instead of `OPENAI_API_KEY`
- Breathe Score formula refined to better reflect real fire proximity data
- Satellite refresh uses both producer-level and pipeline-level daemon threads

### 🐛 Fixed
- Kafka producer now waits 5 seconds after satellite fetch before first event (avoids `simulated` data burst)
- Pipeline satellite thread no longer crashes when `satellite_fetcher.py` is absent

---

## [1.0.0] — 2026-02-26

### 🚀 Added
- Initial release of GreenPulse India
- **Pathway streaming engine** consuming Kafka `env-sensor-stream` topic
- **Kafka producer** simulating 10 Indian city sensors every 4 seconds
- **FastAPI** server with 7 REST endpoints
- **Dark biopunk dashboard** (`frontend/index.html`) polling every 3 seconds
- **Breathe Score™** composite metric (AQI + river toxicity + fire + heat)
- **Live RAG index** with BM25-style retrieval + OpenAI GPT-4o-mini answers
- **Docker Compose** for Kafka + Zookeeper + Kafka UI
- JSONL fallback streaming mode when Kafka is unavailable
- City coverage: Delhi, Kanpur, Patna, Agra, Muzaffarpur, Lucknow, Gurgaon, Jaipur, Ahmedabad, Srinagar

---

[2.0.0]: https://github.com/your-org/GreenPulse-India/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/your-org/GreenPulse-India/releases/tag/v1.0.0
