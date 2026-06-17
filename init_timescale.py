import logging
import os
import pg8000
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Variables de entorno para producción
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "supersecretpassword")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "postgres")

def run_migrations():
    logger.info("🚀 Iniciando migración geo-temporal (PostgreSQL + PostGIS Nativo)...")
    logger.info(f"📡 Conectando a {DB_HOST}:{DB_PORT}/{DB_NAME}")
    
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
        logger.info("🗺️ Asegurando extensión PostGIS...")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        
        # 2. Creación de la Tabla Base de Historial
        logger.info("📊 Creando estructura de tabla 'dispatched_orders_history'...")
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
        
        # 3. Índices robustos
        logger.info("🔍 Construyendo índices (B-Tree temporal y GiST espacial)...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_history_time ON dispatched_orders_history (created_at DESC);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_history_resource ON dispatched_orders_history (assigned_resource_id, created_at DESC);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_history_geo ON dispatched_orders_history USING gist(route_geom);")
        
        # 4. Tabla de unidades de emergencia (necesaria para el orchestrator)
        logger.info("🚑 Creando tabla 'emergency_units'...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS emergency_units (
                id VARCHAR(50) PRIMARY KEY,
                type VARCHAR(30) NOT NULL,
                status VARCHAR(20) NOT NULL,
                current_h3_res8 VARCHAR(15) NOT NULL,
                geom GEOMETRY(Point, 4326)
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_units_geom ON emergency_units USING gist(geom);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_units_h3 ON emergency_units(current_h3_res8);")
        
        # 5. Insertar unidades de ejemplo
        logger.info("🚑 Insertando unidades de emergencia...")
        cursor.execute("""
            INSERT INTO emergency_units (id, type, status, current_h3_res8, geom)
            VALUES 
            ('UNIT-AMBULANCE-01', 'Ambulance', 'AVAILABLE', '88756ad297fffff', ST_SetSRID(ST_MakePoint(-67.5928, 10.2458), 4326)),
            ('UNIT-FIRE-03', 'Firetruck', 'AVAILABLE', '88756ad295fffff', ST_SetSRID(ST_MakePoint(-67.4764, 10.2239), 4326)),
            ('UNIT-POLICE-09', 'Police', 'AVAILABLE', '88756ad291fffff', ST_SetSRID(ST_MakePoint(-67.4572, 10.1883), 4326))
            ON CONFLICT (id) DO UPDATE SET status = 'AVAILABLE';
        """)
        
        logger.info("💾 Esquema geo-temporal inicializado con éxito usando PostGIS nativo.")
        
        cursor.close()
        conn.close()
        logger.info("🎉 Migración completada exitosamente!")

    except Exception as e:
        logger.error(f"❌ Falla crítica ejecutando la migración SQL: {e}")

if __name__ == "__main__":
    run_migrations()