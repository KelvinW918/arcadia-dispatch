package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Configuración de infraestructura - USAR RENDER
const (
	KafkaBrokers = "127.0.0.1:19092"
	Topic        = "dispatched-orders"
	GroupID      = "persistence-go-group"
	ConnString   = "postgres://arcadia_db_user:f0N5Lq7w5NA3mo4rwyfOV0ahFBHlu8CY@dpg-d8pcskv7f7vs73cri750-a.oregon-postgres.render.com:5432/arcadia_db"
)

// Estructuras para decodificar el evento de Redpanda
type Geometry struct {
	Coordinates [][]float64 `json:"coordinates"`
}
type RouteGeoJSON struct {
	Geometry Geometry `json:"geometry"`
}
type DispatchedOrder struct {
	OrderID            string       `json:"order_id"`
	IncidentID         string       `json:"incident_id"`
	AssignedResourceID string       `json:"assigned_resource_id"`
	Priority           string       `json:"priority"`
	H3IndexRes8        string       `json:"h3_index_res8"`
	ETAMinutes         float64      `json:"eta_minutes"`
	RouteGeoJSON       RouteGeoJSON `json:"route_geojson"`
}

// Estructura para la respuesta de la API Analítica
type UnitMetric struct {
	Unidad               string  `json:"unidad"`
	TotalDespachos       int     `json:"total_despachos"`
	ETAPromedio          float64 `json:"eta_promedio_min"`
	CentroideOperaciones string  `json:"centroide_operaciones"`
}

// Estructura para la respuesta de unidades (frontend)
type UnitData struct {
	ID              string  `json:"id"`
	IncidentID      string  `json:"incident_id"`
	Resource        string  `json:"resource"`
	Priority        string  `json:"priority"`
	ETA             float64 `json:"eta"`
	Lat             float64 `json:"lat"`
	Lng             float64 `json:"lng"`
	Route           string  `json:"route"`
	CreatedAt       string  `json:"created_at"`
}

func main() {
	ctx := context.Background()

	log.Println("🚀 Inicializando Backend de Alto Rendimiento en Go...")

	// 1. Lanzar el Consumidor de Redpanda en una Goroutine independiente
	go startKafkaConsumer(ctx)

	// 2. Levantar el Servidor HTTP de Analítica en el hilo principal
	// Middleware CORS
	corsMiddleware := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			next(w, r)
		}
	}

	http.HandleFunc("/api/analytics", corsMiddleware(handleAnalytics))
	http.HandleFunc("/api/units", corsMiddleware(handleUnits))
	http.HandleFunc("/api/health", corsMiddleware(handleHealth))

	log.Println("📊 Servidor API escuchando en http://127.0.0.1:8081")
	log.Println("   🔹 /api/analytics - Estadísticas de unidades")
	log.Println("   🔹 /api/units - Unidades despachadas")
	log.Println("   🔹 /api/health - Health check")
	
	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatalf("Error en el servidor HTTP: %v", err)
	}
}

// --- HEALTH CHECK ---
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status": "ok", "message": "Backend Arcadia funcionando"}`))
}

// --- TRABAJADOR DE CONSUMO Y PERSISTENCIA ESPACIAL ---
func startKafkaConsumer(ctx context.Context) {
	// Inicializar cliente de Redpanda
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(KafkaBrokers),
		kgo.ConsumerGroup(GroupID),
		kgo.ConsumeTopics(Topic),
	)
	if err != nil {
		log.Fatalf("Error conectando a Redpanda: %v", err)
	}
	defer cl.Close()

	log.Println("💾 Guardián de almacenamiento (Go) escuchando 'dispatched-orders'...")

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Printf("Errores de lectura en Kafka: %v", errs)
			continue
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			
			// Procesar cada mensaje de forma concurrente con una Goroutine
			go func(val []byte) {
				var order DispatchedOrder
				if err := json.Unmarshal(val, &order); err != nil {
					log.Printf("Error decodificando JSON: %v", err)
					return
				}

				// Convertir coordenadas a WKT LineString para PostGIS
				var points []string
				for _, coord := range order.RouteGeoJSON.Geometry.Coordinates {
					points = append(points, fmt.Sprintf("%f %f", coord[0], coord[1]))
				}
				wktLine := fmt.Sprintf("LINESTRING(%s)", strings.Join(points, ","))

				// Conectar e Insertar en la Base de Datos
				db, err := pgx.Connect(ctx, ConnString)
				if err != nil {
					log.Printf("Error de conexión a DB: %v", err)
					return
				}
				defer db.Close(ctx)

				query := `
					INSERT INTO dispatched_orders_history 
					(order_id, incident_id, assigned_resource_id, priority, h3_index_res8, eta_minutes, route_geom)
					VALUES ($1, $2, $3, $4, $5, $6, ST_GeomFromText($7, 4326));`

				_, err = db.Exec(ctx, query, 
					order.OrderID, order.IncidentID, order.AssignedResourceID, 
					order.Priority, order.H3IndexRes8, order.ETAMinutes, wktLine)
				
				if err != nil {
					log.Printf("Error insertando en PostGIS: %v", err)
					return
				}

				log.Printf("💾 [Go Worker] ÓRDEN GUARDADA -> ID: %s | Unidad: %s", order.OrderID, order.AssignedResourceID)
			}(record.Value)
		}
		// Confirmar offsets procesados de forma síncrona
		cl.CommitRecords(ctx, fetches.Records()...)
	}
}

// --- ENDPOINT: ANALÍTICA ---
func handleAnalytics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := pgx.Connect(ctx, ConnString)
	if err != nil {
		http.Error(w, `{"error": "No se pudo conectar a la base de datos"}`, http.StatusInternalServerError)
		return
	}
	defer db.Close(ctx)

	query := `
		SELECT 
			assigned_resource_id,
			COUNT(*),
			ROUND(AVG(eta_minutes)::numeric, 2),
			ST_AsText(ST_Centroid(ST_Collect(route_geom)))
		FROM dispatched_orders_history
		GROUP BY assigned_resource_id;`

	rows, err := db.Query(ctx, query)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "%v"}`, err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var metrics []UnitMetric
	for rows.Next() {
		var m UnitMetric
		if err := rows.Scan(&m.Unidad, &m.TotalDespachos, &m.ETAPromedio, &m.CentroideOperaciones); err != nil {
			log.Printf("Error leyendo fila de DB: %v", err)
			continue
		}
		metrics = append(metrics, m)
	}

	// Si no hay datos, devolver array vacío
	if metrics == nil {
		metrics = []UnitMetric{}
	}
	
	json.NewEncoder(w).Encode(metrics)
}

// --- ENDPOINT: UNIDADES PARA EL FRONTEND ---
func handleUnits(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := pgx.Connect(ctx, ConnString)
	if err != nil {
		http.Error(w, `{"error": "No se pudo conectar a la base de datos"}`, http.StatusInternalServerError)
		return
	}
	defer db.Close(ctx)

	// Consultar las últimas órdenes con información de ruta
	query := `
		SELECT 
			order_id,
			incident_id,
			assigned_resource_id,
			priority,
			eta_minutes,
			ST_AsText(route_geom) as route_wkt,
			created_at
		FROM dispatched_orders_history 
		ORDER BY created_at DESC 
		LIMIT 50;`

	rows, err := db.Query(ctx, query)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "%v"}`, err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var units []UnitData
	for rows.Next() {
		var u UnitData
		var routeWKT string
		var createdAt time.Time
		
		err := rows.Scan(&u.ID, &u.IncidentID, &u.Resource, &u.Priority, &u.ETA, &routeWKT, &createdAt)
		if err != nil {
			log.Printf("Error leyendo fila: %v", err)
			continue
		}
		
		u.CreatedAt = createdAt.Format(time.RFC3339)
		
		// Extraer lat/lng del centro de la ruta (punto medio)
		// Parsear WKT para obtener el centro aproximado
		// Simple: usar el punto medio del primer y último punto de la ruta
		u.Lat = 10.24 // Valor por defecto (Maracay)
		u.Lng = -67.60
		
		// Si hay ruta, extraer coordenadas aproximadas
		if len(routeWKT) > 10 {
			// Extraer números de la WKT
			// Formato: LINESTRING(lon1 lat1, lon2 lat2, ...)
			coordsStr := strings.TrimPrefix(routeWKT, "LINESTRING(")
			coordsStr = strings.TrimSuffix(coordsStr, ")")
			points := strings.Split(coordsStr, ",")
			if len(points) > 0 {
				// Tomar el primer punto como referencia
				firstPoint := strings.TrimSpace(points[0])
				parts := strings.Fields(firstPoint)
				if len(parts) >= 2 {
					var lon, lat float64
					fmt.Sscanf(parts[0], "%f", &lon)
					fmt.Sscanf(parts[1], "%f", &lat)
					u.Lng = lon
					u.Lat = lat
				}
			}
		}
		
		units = append(units, u)
	}

	// Si no hay datos, devolver array vacío
	if units == nil {
		units = []UnitData{}
	}
	
	json.NewEncoder(w).Encode(units)
}