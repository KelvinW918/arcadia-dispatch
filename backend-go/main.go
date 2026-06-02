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

// Configuración de infraestructura
const (
	KafkaBrokers = "127.0.0.1:19092"
	Topic        = "dispatched-orders"
	GroupID      = "persistence-go-group"
	ConnString   = "postgres://postgres:supersecretpassword@127.0.0.1:5432/postgres"
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

func main() {
	ctx := context.Background()

	log.Println("🚀 Inicializando Backend de Alto Rendimiento en Go...")

	// 1. Lanzar el Consumidor de Redpanda en una Goroutine independiente
	go startKafkaConsumer(ctx)

	// 2. Levantar el Servidor HTTP de Analítica en el hilo principal
	http.HandleFunc("/api/analytics", handleAnalytics)
	log.Println("📊 Servidor API escuchando en http://127.0.0.1:8081/api/analytics")
	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatalf("Error en el servidor HTTP: %v", err)
	}
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

// --- ENDPOINT DE LA API HTTP (ANALÍTICA EN VIVO) ---
func handleAnalytics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*") // Habilitar CORS para tu futuro Frontend

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

	json.NewEncoder(w).Encode(metrics)
}