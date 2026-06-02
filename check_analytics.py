import pg8000
import tabulate  # Si no lo tienes, puedes hacer pip install tabulate o simplemente imprimir normal

DB_USER = "postgres" 
DB_PASSWORD = "supersecretpassword"
DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "postgres"

def get_spatial_metrics():
    conn = pg8000.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME)
    cursor = conn.cursor()
    
    query = """
        SELECT 
            assigned_resource_id as unidad,
            COUNT(*) as total_despachos,
            ROUND(AVG(eta_minutes)::numeric, 2) as eta_promedio_min,
            ST_AsText(ST_Centroid(ST_Collect(route_geom))) as centroide_operaciones
        FROM dispatched_orders_history
        GROUP BY assigned_resource_id
        ORDER BY total_despachos DESC;
    """
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        print("\n📊 REPORTE DE RENDIMIENTO GEO-TEMPORAL EN TIEMPO REAL:")
        print("-" * 85)
        print(f"{'UNIDAD':<20} | {'TOTAL DESPACHOS':<16} | {'ETA PROMEDIO':<14} | {'CENTROIDE OPERACIONES'}")
        print("-" * 85)
        for row in rows:
            print(f"{row[0]:<20} | {row[1]:<16} | {row[2]:<14} | {row[3]}")
        print("-" * 85)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    get_spatial_metrics()