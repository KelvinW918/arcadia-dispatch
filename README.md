# 🚒 Arcadia · Emergency Dispatch System

Real-time geospatial dispatch system for emergency units
Built for firefighters, ambulances, and police — low latency, high availability, event-driven.

---

## 🌐 Live Demo

| Service | URL |
|---------|-----|
| **Frontend Dashboard** | https://arcadia-dispatch-f1cb589rz-kelvinw918s-projects.vercel.app |
| **Backend API** | https://arcadia-backend-bj5m.onrender.com |
| **API Health** | https://arcadia-backend-bj5m.onrender.com/api/health |
| **API Analytics** | https://arcadia-backend-bj5m.onrender.com/api/analytics |
| **API Units** | https://arcadia-backend-bj5m.onrender.com/api/units |

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
| **Producer** | Python + aiokafka | Simulates unit telemetry events |
| **Streaming** | Redpanda (Kafka-compatible) | Event bus, high throughput ingestion |
| **Orchestrator** | Python + H3 + PostGIS | AI-driven unit assignment |
| **Backend API** | Go + pgxpool | High-performance API, geospatial processing |
| **Database** | PostgreSQL + PostGIS | Spatial queries, centroids, distances |
| **Frontend** | Leaflet.js + Tailwind CSS | Real-time map rendering |
| **Orchestration** | Docker Compose | Multi-service local development |
| **Deployment** | Render + Vercel | Cloud-native hosting |

---

## 🏗️ Architecture

┌─────────────────┐ ┌──────────────┐ ┌─────────────────┐ ┌──────────────┐
│ Stress Producer │────▶│   Redpanda   │────▶│  Orchestrator  │────▶│   Redpanda   │
│    (Python)     │ │   (Topic:    │ │    (Python)     │ │   (Topic:    │
│    Generates    │ │     raw-     │ │   - Triage      │ │  dispatched- │
│    incidents    │ │  incidents)  │ │   - H3 Asign.   │ │   orders)    │
└─────────────────┘ └──────────────┘ └─────────────────┘ └──────┬───────┘
                                                                 │
                                                                 ▼
┌─────────────────┐ ┌──────────────┐ ┌─────────────────┐ ┌──────────────┐
│    Frontend     │────▶│   Backend    │◀────│   Persistence   │────▶│  PostgreSQL  │
│   (Leaflet)     │ │      Go      │ │     Worker      │ │  + PostGIS   │
│   Map & Stats   │ │   API REST   │ │    (Python)     │ │   Database   │
└─────────────────┘ └──────────────┘ └─────────────────┘ └──────────────┘

**Data Flow:**
1. **Stress Producer** generates simulated emergency incidents
2. **Redpanda** streams events to the orchestrator
3. **Orchestrator** processes with H3 geospatial indexing and assigns units
4. **Persistence Worker** saves dispatched orders to PostgreSQL
5. **Go Backend** exposes data via REST API
6. **Frontend** visualizes units and analytics in real-time

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop
- Go 1.21+
- Python 3.11+
- PostgreSQL 16+ with PostGIS

### Run it (5 minutes)

```bash
# 1. Clone
git clone [https://github.com/KelvinW918/arcadia-dispatch.git](https://github.com/KelvinW918/arcadia-dispatch.git)
cd arcadia-dispatch

# 2. Start infrastructure (Postgres + Redpanda)
docker-compose up -d

# 3. Run migrations
python init_timescale.py

# 4. Run backend
cd backend-go
go run main.go

# 5. Run orchestrator (in another terminal)
cd ..
python agent_orchestrator.py

# 6. Run persistence worker (in another terminal)
python persistence_worker.py

# 7. Run stress producer (in another terminal)
python stress_producer.py

# 8. Open frontend
# Open frontend/index.html in your browser or use Live Server

---

## 📊 Sample Queries (PostGIS)

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

-- Get unit analytics
SELECT 
    assigned_resource_id,
    COUNT(*) as total_despachos,
    ROUND(AVG(eta_minutes)::numeric, 2) as eta_promedio,
    ST_AsText(ST_Centroid(ST_Collect(route_geom))) as centroide
FROM dispatched_orders_history
GROUP BY assigned_resource_id;
WHERE status = 'active'
GROUP BY cluster_id;

---

## 📈 Performance Characteristics
Metric              	                 Value
Event throughput	       ~10,000 events/sec (Redpanda)
API latency (p95)	       < 50ms
Spatial query time	       < 10ms (with PostGIS indexing)
Concurrent units supported	5,000+
ETA calculation	       < 5ms per unit

---

## 🔮 Roadmap
- WebSocket support for real-time frontend updates
-Route calculation using pgRouting
-Historical trip replay & analytics
-Mobile app for field units
-Kafka Streams for real-time ETA prediction
-Ollama/LLM integration for semantic triage
-Multi-region deployment support

---

## 👤 Author
Kelvin W.
Systems Engineer · Product Architect

https://img.shields.io/badge/GitHub-KelvinW918-181717?style=for-the-badge&logo=github
https://img.shields.io/badge/LinkedIn-KelvinW918-0077B5?style=for-the-badge&logo=linkedin

---

## 📄 License
MIT — free for use, modification, and distribution.

<div align="center">
⭐ If this project helped you, consider giving it a star ⭐

</div> ```
