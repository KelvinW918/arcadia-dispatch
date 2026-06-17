import asyncio
import json
import logging
import os
import ssl
import pg8000
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

# Cargar variables de entorno desde .env (sobrescribiendo las existentes)
load_dotenv(override=True)

# Debug - Verificar variables cargadas
print("🔍 DEBUG - Variables cargadas:")
print(f"DB_HOST: {os.getenv('DB_HOST')}")
print(f"DB_USER: {os.getenv('DB_USER')}")
print(f"DB_NAME: {os.getenv('DB_NAME')}")
print(f"KAFKA_BROKERS: {os.getenv('KAFKA_BROKERS')}")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Variables de entorno para producción
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "127.0.0.1:19092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")
TOPIC_DISPATCHED = os.getenv("KAFKA_TOPIC_DISPATCHED", "dispatched-orders")
GROUP_ID = os.getenv("PERSISTENCE_GROUP_ID", "persistence-storage-group")

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "supersecretpassword")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "postgres")

def create_ssl_context():
    """Crea contexto SSL para SASL_SSL"""
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    return ssl_context

class PersistenceWorker:
    def __init__(self):
        self.consumer = None

    async def initialize(self):
        """Inicializa el consumidor dedicado a persistir las órdenes despacho."""
        logger.info("🚀 Iniciando Worker de Persistencia Geo-temporal...")
        logger.info(f"📡 Conectando a Redpanda: {KAFKA_BROKERS}")
        logger.info(f"🗄️ Conectando a PostgreSQL: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        
        # Configuración SSL para Redpanda Cloud
        ssl_context = create_ssl_context()
        
        # Consumidor para leer órdenes despachadas
        self.consumer = AIOKafkaConsumer(
            TOPIC_DISPATCHED,
            bootstrap_servers=KAFKA_BROKERS,
            security_protocol="SASL_SSL" if KAFKA_USERNAME else "PLAINTEXT",
            ssl_context=ssl_context if KAFKA_USERNAME else None,
            sasl_mechanism="SCRAM-SHA-256" if KAFKA_USERNAME else None,
            sasl_plain_username=KAFKA_USERNAME if KAFKA_USERNAME else None,
            sasl_plain_password=KAFKA_PASSWORD if KAFKA_PASSWORD else None,
            group_id=GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            max_poll_records=10,
            session_timeout_ms=30000
        )
        
        await self.consumer.start()
        logger.info(f"💾 Guardián de almacenamiento escuchando '{TOPIC_DISPATCHED}'...")

    def _convert_route_to_wkt(self, coordinates: list) -> str:
        """Transforma coordenadas GeoJSON [[lon, lat], ...] a formato WKT LINESTRING para PostGIS."""
        if not coordinates or len(coordinates) < 2:
            return "LINESTRING(0 0, 0 0)"
        point_strings = [f"{coord[0]} {coord[1]}" for coord in coordinates if len(coord) >= 2]
        if not point_strings:
            return "LINESTRING(0 0, 0 0)"
        return f"LINESTRING({', '.join(point_strings)})"

    def _save_to_db(self, order_data: dict):
        """Abre una conexión dedicada por hilo para insertar los datos de forma aislada."""
        conn = None
        cursor = None
        try:
            conn = pg8000.connect(
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME
            )
            cursor = conn.cursor()
            
            # Extraer la ruta geográfica del GeoJSON
            route_geojson = order_data.get("route_geojson", {})
            geometry = route_geojson.get("geometry", {})
            coords = geometry.get("coordinates", [])
            wkt_line = self._convert_route_to_wkt(coords)
            
            # Extraer datos de la orden
            order_id = order_data.get("order_id")
            incident_id = order_data.get("incident_id")
            assigned_resource_id = order_data.get("assigned_resource_id")
            priority = order_data.get("priority")
            h3_index_res8 = order_data.get("h3_index_res8")
            eta_minutes = order_data.get("eta_minutes")
            
            # Verificar que todos los datos necesarios existen
            if not all([order_id, incident_id, assigned_resource_id, priority, h3_index_res8, eta_minutes is not None]):
                logger.warning(f"⚠️ Datos incompletos en orden: {order_data}")
                return
            
            cursor.execute(
                """
                INSERT INTO dispatched_orders_history 
                (order_id, incident_id, assigned_resource_id, priority, h3_index_res8, eta_minutes, route_geom)
                VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326));
                """,
                [
                    order_id,
                    incident_id,
                    assigned_resource_id,
                    priority,
                    h3_index_res8,
                    eta_minutes,
                    wkt_line
                ]
            )
            conn.commit()
            logger.info(f"💾 ORDEN GUARDADA -> ID: {order_id} | Recurso: {assigned_resource_id}")
            
        except Exception as e:
            logger.error(f"❌ Error guardando en DB: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    async def process_orders(self):
        """Loop asíncrono para capturar eventos e indexarlos sin bloquear el loop principal."""
        loop = asyncio.get_running_loop()
        processed_count = 0
        try:
            async for msg in self.consumer:
                try:
                    order_data = json.loads(msg.value.decode('utf-8'))
                    
                    # Ejecutamos la inserción síncrona en el pool de hilos del executor
                    await loop.run_in_executor(None, self._save_to_db, order_data)
                    
                    processed_count += 1
                    if processed_count % 10 == 0:
                        logger.info(f"📊 Procesadas {processed_count} órdenes hasta ahora")
                    
                    await self.consumer.commit()
                    
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Error decodificando mensaje: {e}")
                    await self.consumer.commit()  # Saltar mensaje corrupto
                except Exception as e:
                    logger.error(f"❌ Error procesando registro de orden: {e}")
                    await self.consumer.commit()  # Saltar mensaje problemático
        finally:
            if self.consumer:
                await self.consumer.stop()
                logger.info(f"🏁 Worker detenido. Total procesado: {processed_count}")

if __name__ == "__main__":
    worker = PersistenceWorker()
    try:
        async def main():
            await worker.initialize()
            await worker.process_orders()
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Worker de persistencia apagado por el operador.")