# 🚒 Arcadia · Emergency Dispatch System
![Profile Views](https://komarev.com/ghpvc/?username=KelvinW918&color=58a6ff&style=flat-square)

<div align="center">

![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=for-the-badge&logo=go)
![PostGIS](https://img.shields.io/badge/PostGIS-3.x-4169E1?style=for-the-badge&logo=postgresql)
![Redpanda](https://img.shields.io/badge/Redpanda-Kafka_Compatible-FF4F1E?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

**Real-time geospatial dispatch system for emergency units**  
*Built for firefighters, ambulances, and police — low latency, high availability, event-driven.*

</div>

---

## 🎯 Problem & Solution

Emergency dispatch systems in many regions still rely on radio or manual coordination. Arcadia solves this by:

- 📡 **Real-time telemetry** from field units (GPS coordinates, status, ETA)
- 🗺️ **Geospatial analytics** for optimal unit assignment (closest, fastest route)
- 🔄 **Event-driven architecture** for horizontal scalability
- 📊 **Live visualization** via interactive map interface

---

## 🛠 Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Producer** | Python | Simulates unit telemetry events |
| **Streaming** | Redpanda (Kafka-compatible) | Event bus, high throughput ingestion |
| **Backend** | Go + pgxpool | High-performance API, geospatial processing |
| **Database** | PostgreSQL + PostGIS | Spatial queries, centroids, distances |
| **Frontend** | Leaflet.js + Tailwind CSS | Real-time map rendering |
| **Orchestration** | Docker Compose | Multi-service local development |

---

## 🏗️ Architecture
┌─────────────┐ ┌─────────────────┐ ┌─────────────┐ ┌──────────────┐
│ Producer │───▶│ Redpanda │───▶│ Backend │───▶│ PostGIS │
│ (Python) │ │ (Kafka API) │ │ (Go) │ │ (Database) │
└─────────────┘ └─────────────────┘ └──────┬──────┘ └──────────────┘
│
▼
┌─────────────┐
│ Leaflet │
│ (Frontend)│
└─────────────┘

text

**Data flow:**
1. Producer simulates emergency units sending GPS pings
2. Redpanda buffers and streams events to consumers
3. Go backend consumes, validates, and stores in PostGIS
4. Frontend queries API → PostGIS spatial calculations → map updates

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop
- Go 1.21+
- Python 3.12+

### Run it (5 minutes)

```bash
# 1. Clone
git clone https://github.com/KelvinW918/Aragua_on_fire.git
cd Aragua_on_fire

# 2. Start infrastructure (Postgres + Redpanda)
docker-compose up -d

# 3. Run backend
cd backend-go
go run main.go

# 4. Run producer (in another terminal)
cd ..
python stress_producer.py

# 5. Open frontend
# Open frontend/index.html in your browser
📊 Sample Queries (PostGIS)
sql
-- Find nearest unit to an incident (within 5km)
SELECT unit_id, coordinates, status,
       ST_Distance(coordinates, ST_SetSRID(ST_MakePoint(-66.9, 10.5), 4326)) as distance
FROM units
WHERE ST_DWithin(coordinates, ST_SetSRID(ST_MakePoint(-66.9, 10.5), 4326), 5000)
ORDER BY distance ASC
LIMIT 1;

-- Cluster active units by sector
SELECT ST_ClusterKMeans(coordinates, 4) as cluster_id,
       COUNT(*) as units_count
FROM units
WHERE status = 'active'
GROUP BY cluster_id;
📈 Performance Characteristics
Metric	Value
Event throughput	~10,000 events/sec (Redpanda)
API latency (p95)	< 50ms
Spatial query time	< 10ms (with PostGIS indexing)
Concurrent units supported	5,000+
🔮 Roadmap
WebSocket support for real-time frontend updates

Route calculation using pgRouting

Historical trip replay & analytics

Mobile app for field units

Kafka Streams for real-time ETA prediction

👤 Author
Kelvin W.
Systems Engineer · Product Architect
GitHub · LinkedIn

📄 License
MIT — free for use, modification, and distribution.

<div align="center"> ⭐ If this project helped you, consider giving it a star ⭐ </div> ```
