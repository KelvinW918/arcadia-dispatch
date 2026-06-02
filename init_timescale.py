import logging
import pg8000

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_USER = "postgres" 
DB_PASSWORD = "supersecretpassword"
DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "postgres"

def run_migrations():
    logger.info("Iniciando migración geo-temporal (PostgreSQL + PostGIS Nativo)...")
    
    try:
        conn = pg8000.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # 1. Asegurar extensión espacial
        logger.info("Asegurando extensión PostGIS...")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        
        # 2. Creación de la Tabla Base de Historial
        logger.info("Creando estructura de tabla 'dispatched_orders_history'...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dispatched_orders_history (
                order_id UUID NOT NULL,
                incident_id UUID NOT NULL,
                assigned_resource_id VARCHAR(50) NOT NULL,
                priority VARCHAR(20) NOT NULL,
                h3_index_res8 VARCHAR(15) NOT NULL,
                eta_minutes DOUBLE PRECISION NOT NULL,
                route_geom GEOMETRY(LineString, 4326),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        
        # 3. Índices robustos de alto rendimiento para simular la velocidad de series temporales
        logger.info("Construyendo índices (B-Tree temporal para analítica y GiST espacial)...")
        # Índice temporal descendente para que los últimos incidentes se lean instantáneamente
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_history_time ON dispatched_orders_history (created_at DESC);")
        # Índice compuesto para buscar rápido qué unidades atienden qué zonas en el tiempo
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_history_resource ON dispatched_orders_history (assigned_resource_id, created_at DESC);")
        # Índice espacial GiST para consultas geográficas sobre las rutas calculadas
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_history_geo ON dispatched_orders_history USING gist(route_geom);")
        
        logger.info(" 💾 Esquema geo-temporal inicializado con éxito usando PostGIS nativo.")
        
        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Falla crítica ejecutando la migración SQL: {e}")

if __name__ == "__main__":
    run_migrations()