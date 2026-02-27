# GreenPulse India 🌿
### Real-Time Environmental Intelligence Platform

> **Powered by:** Pathway Streaming Engine · Apache Kafka · FastAPI · Live RAG (BM25 + Semantic) · OpenAI GPT-4o-mini

GreenPulse India tracks AQI, PM2.5, PM10, river toxicity, heat index, and the proprietary
**Breathe Score™** across 10 major Indian cities — updated automatically every time a new sensor
event arrives, with zero manual refresh.

---

## Architecture

```
┌─────────────────┐   Kafka topic    ┌──────────────────────┐   FastAPI/HTTP   ┌──────────────────┐
│  kafka_producer │ ──────────────▶  │   pathway_pipeline   │ ───────────────▶ │  Browser         │
│  (sensor sim)   │  env-sensor-stream│  Streaming Engine    │  port 8000       │  frontend/       │
│  4s intervals   │   (or .jsonl)    │  + Live RAG Index    │                  │  index.html      │
└─────────────────┘                  └──────────────────────┘                  └──────────────────┘
```

**Three-layer stack:**
1. **Kafka Producer** — Simulates 10 Indian city sensors, emitting JSON every 4 seconds.
2. **Pathway Pipeline** — Consumes Kafka stream, transforms data, maintains a live hybrid
   BM25+semantic RAG index, and serves REST endpoints via FastAPI.
3. **Frontend Dashboard** — Dark biopunk HTML/CSS/JS dashboard polling the API every 3 seconds.
   Never needs a page reload.

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Docker Desktop | ≥ 4.x | Required for Kafka |
| Python | ≥ 3.10 | For producer + pipeline |
| pip | latest | `python -m pip install --upgrade pip` |
| OpenAI API key | — | For the AI chat assistant |

---

## Quick Start (5 Steps)

### Step 1 — Clone / open the project folder

```bash
cd greenpulse
```

### Step 2 — Start Kafka + Zookeeper

```bash
docker-compose up -d
```

This launches:
- **Zookeeper** on port `2181`
- **Kafka broker** on port `9092` with topic `env-sensor-stream` auto-created
- **Kafka UI** at http://localhost:8080 (optional visual monitoring)

Verify Kafka is running:
```bash
docker-compose ps
```

Wait ~15 seconds for the broker to become healthy before proceeding.

### Step 3 — Install Python dependencies

```bash
pip install -r requirements.txt
```

> **Note on Pathway:** `pathway` must be version `>=0.14.0`. If you encounter installation
> issues, try: `pip install pathway --pre`

### Step 4 — Configure your API key

```bash
# Windows (PowerShell)
copy .env.example .env

# macOS / Linux
cp .env.example .env
```

Edit `.env` and set your `OPENAI_API_KEY`. The pipeline runs without it, but the AI chat
will display raw retrieved documents instead of an LLM-synthesized answer.

### Step 5 — Start the sensor stream

Open a new terminal:
```bash
python kafka_producer.py
```

You will see output like:
```
[Kafka] ✓ Connected to broker at localhost:9092
[KAFKA] #00001 | Delhi          | AQI=312 | BS=18.4 | HAZARDOUS
[KAFKA] #00002 | Kanpur         | AQI=287 | BS=22.1 | POOR
```

If Kafka isn't ready, it automatically falls back to `sensor_stream.jsonl` (file streaming mode).

### Step 6 — Start the Pathway pipeline + FastAPI server

Open another terminal:
```bash
python pathway_pipeline.py
```

You will see:
```
============================================================
  GreenPulse India — Environmental Intelligence Platform
============================================================
[Pathway] ✓ Kafka available — using pw.io.kafka.read()
[Pathway] ▶ Starting streaming engine with pw.run() …
[Main] FastAPI listening on http://0.0.0.0:8000
```

### Step 7 — Open the dashboard

Open `frontend/index.html` in your browser:

```bash
# On Windows
start frontend\index.html

# On macOS
open frontend/index.html

# Or just drag the file into Chrome/Firefox
```

 **Everything is now live.** The dashboard polls the API every 3 seconds and updates
automatically as new data flows through Kafka → Pathway → FastAPI → Browser.

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/latest` | Latest reading per city (all 10) |
| GET | `/api/city/{city_name}` | Last 20 readings for a city |
| GET | `/api/alerts` | All HAZARDOUS events |
| GET | `/api/breathe-scores` | All cities sorted by Breathe Score ↑ |
| POST | `/api/ask` | `{"question":"..."}` → LLM answer with citations |
| GET | `/api/stats` | Pipeline stats (events, index size, last update) |
| GET | `/health` | Health check |

---

## The Breathe Score™

The **Breathe Score** (0–100) is GreenPulse's signature composite metric:

```
breathe_score = max(0,
    100
    - (aqi / 5)
    - (river_tox_index × 2)
    - (max(0, 500 - fire_proximity_km) / 50)
    - (max(0, heat_index − 35) × 1.5)
)
```

| Score Range | Alert Level | Interpretation |
|-------------|-------------|----------------|
| > 60 | 🟢 SAFE | Air is safe to breathe normally |
| 40–60 | 🟡 MODERATE | Sensitive groups should limit outdoor activity |
| 20–40 | 🟠 POOR | Avoid prolonged outdoor exposure |
| < 20 | 🔴 HAZARDOUS | Stay indoors; health risk for all groups |

---

## How Pathway Powers Real-Time Updates

1. **Kafka Ingestion:** `pw.io.kafka.read()` opens a live Kafka consumer. Every message
   triggers the pipeline immediately — no polling.
2. **Transforms:** Pathway's reactive dataflow computes derived columns (doc text, hazard
   filter) incrementally on each new row.
3. **Live RAG Index:** Each incoming sensor reading is converted to a natural-language
   document and added to the in-memory index. The index is always current.
4. **Output:** `pw.io.python.write()` calls the sink callback for each committed row,
   updating the in-memory state that FastAPI reads from.
5. **`pw.run()`** keeps the streaming engine alive indefinitely.

No batch jobs. No scheduled re-ingestion. Every sensor event automatically propagates
through the entire pipeline within milliseconds.

---

## City Coverage

| City | AQI Range | Risk Profile |
|------|-----------|--------------|
| Delhi | 200–450 | Very high — industrial + traffic |
| Kanpur | 200–430 | Very high — leather + textile industry |
| Patna | 190–420 | Very high — brick kilns + transport |
| Agra | 150–380 | High — marble dust + tourism traffic |
| Muzaffarpur | 160–390 | High — sugarcane burning |
| Lucknow | 130–350 | High — rapid urbanisation |
| Gurgaon | 120–340 | Moderate-High — IT hub + construction |
| Jaipur | 80–280 | Moderate — desert dust + tourism |
| Ahmedabad | 70–260 | Moderate — chemical corridors |
| Srinagar | 50–180 | Low-Moderate — valley geography |

---

## Troubleshooting

**Kafka connection refused:**
```bash
# Check Docker containers are running
docker-compose ps
# View Kafka logs
docker-compose logs kafka
```

**`pathway` import error:**
```bash
pip install --upgrade pathway
# or for pre-release:
pip install pathway --pre
```

**CORS errors in browser:**
The FastAPI server is configured with `allow_origins=["*"]`. If you still see CORS errors,
serve the frontend via a local HTTP server:
```bash
cd frontend
python -m http.server 3000
# Then open http://localhost:3000
```

**No data appearing in dashboard:**
1. Confirm `kafka_producer.py` is running and printing output.
2. Confirm `pathway_pipeline.py` is running and shows `[Main] FastAPI listening`.
3. Check http://localhost:8000/api/latest in your browser — should return JSON.

---

## Project Structure

```
greenpulse/
├── kafka_producer.py       ← Sensor stream simulator (Kafka or file fallback)
├── pathway_pipeline.py     ← Pathway streaming engine + FastAPI server
├── requirements.txt        ← Python dependencies
├── docker-compose.yml      ← Kafka + Zookeeper
├── .env.example            ← Environment variable template
├── frontend/
│   └── index.html          ← Self-contained dark biopunk dashboard
└── README.md               ← This file
```

---

## License

MIT — build, fork, and extend freely.

---

UI screenshots
![WhatsApp Image 2026-02-26 at 11 39 30 PM](https://github.com/user-attachments/assets/56743e4c-0c5a-47fa-a2c6-8d96e2d7b8b6)


![WhatsApp Image 2026-02-26 at 11 39 32 PM](https://github.com/user-attachments/assets/40becdd8-aba1-4f7a-b0f7-3a3f78b61944)

![WhatsApp Image 2026-02-26 at 11 39 34 PM](https://github.com/user-attachments/assets/f946001a-6f1f-44f2-aef3-3a2126e3b77a)


*GreenPulse India — Because every breath counts.*
