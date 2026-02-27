# Breathe Score™ — Formula & Calibration

## Overview

The **Breathe Score™** (0–100) is GreenPulse India's proprietary composite environmental liveability metric. Unlike raw AQI alone, Breathe Score integrates four independent pollution vectors into a single actionable number.

## Formula

```
breathe_score = max(0,
    100
    − (aqi / 5)
    − (river_tox_index × 2)
    − (max(0, 500 − fire_proximity_km) / 50)
    − (max(0, heat_index − 35) × 1.5)
)
```

## Penalty Breakdown

| Penalty | Formula | Max Penalty | Rationale |
|---|---|---|---|
| AQI | `aqi / 5` | 90+ (AQI=450) | Linear; AQI 500 → -100 pts |
| River Toxicity | `river_tox_index × 2` | 20 (index=10) | Indirect ingestion + ecosystem risk |
| Fire Proximity | `max(0, 500-km) / 50` | 10 (km=0) | Smoke particulates at sub-500 km range |
| Heat Index | `max(0, heat-35) × 1.5` | 25+ (heat=52°C) | Heat stress compounding respiratory impact |

## Calibration Notes

- **AQI penalty**: Calibrated so AQI 300 (CPCB Very Poor) maps to ~-60 pts, leaving room for heat/fire penalties.
- **Fire proximity**: Uses 500 km as the smoke impact horizon based on FIRMS studies of Indian wildfire plume extent.
- **Heat index threshold**: 35°C is the approximate "danger zone" for combined heat-humidity stress per WHO guidelines.
- **River toxicity**: Normalised 0–10 composite of biochemical oxygen demand (BOD) and heavy metal concentration proxies.

## Alert Level Mapping

| Score | Level | Color | Health Advisory |
|---|---|---|---|
| > 60 | SAFE | 🟢 Green | Normal outdoor activity for all |
| 40–60 | MODERATE | 🟡 Yellow | Sensitive groups (asthma, elderly) reduce exertion |
| 20–40 | POOR | 🟠 Orange | Avoid prolonged outdoor exposure; use N95 masks |
| < 20 | HAZARDOUS | 🔴 Red | Stay indoors; health risk for all population groups |
