import asyncio
import json
import logging
import os
import ssl
import h3
import aiohttp
import pg8000
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from schemas import RawIncidentEvent, DispatchedOrderEvent, PriorityEnum, GeoJsonRoute, GeoJsonGeometry
from dotenv import load_dotenv

# Cargar variables de entorno desde .env (sobrescribiendo las existentes)
load_dotenv(override=True)

# Debug - Verificar variables cargadas
print("🔍 DEBUG - Variables cargadas:")
print(f"DB_HOST: {os.getenv('DB_HOST')}")
print(f"DB_USER: {os.getenv('DB_USER')}")
print(f"DB_PASSWORD: {os.getenv('DB_PASSWORD')}")
print(f"DB_NAME: {os.getenv('DB_NAME')}")
print(f"KAFKA_BROKERS: {os.getenv('KAFKA_BROKERS')}")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Variables de entorno para producción
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "127.0.0.1:19092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "raw-incidents")
TOPIC_DISPATCHED = os.getenv("KAFKA_TOPIC_DISPATCHED", "dispatched-orders")
OLLAMA_ENDPOINT = os.getenv("OLLAMA_ENDPOINT", "http://127.0.0.1:11434/api/generate")

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

class AgentOrchestratorService:
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.http_session = None

    async def initialize(self):
        """Inicializa las conexiones a la infraestructura y monta el esquema de PostGIS."""
        logger.info("🚀 Conectando con Redpanda, PostGIS y preparando cliente HTTP...")
        
        # Configuración SSL para Redpanda Cloud
        ssl_context = create_ssl_context()
        
        # Consumidor optimizado para evitar rebalances causados por la latencia de Ollama
        self.consumer = AIOKafkaConsumer(
            TOPIC_RAW,
            bootstrap_servers=KAFKA_BROKERS,
            security_protocol="SASL_SSL" if KAFKA_USERNAME else "PLAINTEXT",
            ssl_context=ssl_context if KAFKA_USERNAME else None,
            sasl_mechanism="SCRAM-SHA-256" if KAFKA_USERNAME else None,
            sasl_plain_username=KAFKA_USERNAME if KAFKA_USERNAME else None,
            sasl_plain_password=KAFKA_PASSWORD if KAFKA_PASSWORD else None,
            group_id="emergency-agents-group",
            enable_auto_commit=False,
            auto_offset_reset="latest",
            max_poll_interval_ms=300000,
            max_poll_records=5,
            session_timeout_ms=30000
        )
        
        # Productor para enviar órdenes despachadas
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            security_protocol="SASL_SSL" if KAFKA_USERNAME else "PLAINTEXT",
            ssl_context=ssl_context if KAFKA_USERNAME else None,
            sasl_mechanism="SCRAM-SHA-256" if KAFKA_USERNAME else None,
            sasl_plain_username=KAFKA_USERNAME if KAFKA_USERNAME else None,
            sasl_plain_password=KAFKA_PASSWORD if KAFKA_PASSWORD else None,
            acks="all",
            max_batch_size=16384,
            linger_ms=10
        )
        
        self.http_session = aiohttp.ClientSession()
        
        # Conexión temporal de inicialización
        try:
            init_conn = pg8000.connect(
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME
            )
            init_conn.autocommit = True
            logger.info("🗄️ Conexión inicial establecida. Asegurando tablas...")
            
            cursor = init_conn.cursor()
            try:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
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
                cursor.execute("""
                    INSERT INTO emergency_units (id, type, status, current_h3_res8, geom)
                    VALUES 
                    ('UNIT-AMBULANCE-01', 'Ambulance', 'AVAILABLE', '88756ad297fffff', ST_SetSRID(ST_MakePoint(-67.5928, 10.2458), 4326)),
                    ('UNIT-FIRE-03', 'Firetruck', 'AVAILABLE', '88756ad295fffff', ST_SetSRID(ST_MakePoint(-67.4764, 10.2239), 4326)),
                    ('UNIT-POLICE-09', 'Police', 'AVAILABLE', '88756ad291fffff', ST_SetSRID(ST_MakePoint(-67.4572, 10.1883), 4326))
                    ON CONFLICT (id) DO UPDATE SET status = 'AVAILABLE';
                """)
                logger.info("🗺️ Infraestructura espacial PostGIS / H3 lista para operar.")
            finally:
                cursor.close()
                init_conn.close()
                
        except Exception as e:
            logger.error(f"❌ Error crítico inicializando componentes: {e}")
            raise e
        
        await self.consumer.start()
        await self.producer.start()
        logger.info(f"🚀 Enjambre de Agentes listo! Consumiendo de: {TOPIC_RAW}")

    # --- AGENTE 1: Triage Semántico (Híbrido) ---
    async def _agent_triage(self, description: str) -> PriorityEnum:
        """Clasifica la prioridad usando Ollama o reglas"""
        
        # Si hay Ollama configurado, intentar usarlo
        if OLLAMA_ENDPOINT and "http" in OLLAMA_ENDPOINT:
            payload = {
                "model": "llama3",
                "prompt": f"Classify emergency priority as Critical, High, Medium, or Low based on: {description}. Return only the word.",
                "stream": False,
                "options": {"temperature": 0.0}
            }
            try:
                async with self.http_session.post(OLLAMA_ENDPOINT, json=payload, timeout=1.5) as resp:
                    if resp.status == 200:
                        res_json = await resp.json()
                        text = res_json.get("response", "").strip()
                        for p in PriorityEnum:
                            if p.value.lower() in text.lower():
                                return p
            except Exception as e:
                logger.warning(f"Ollama no disponible, usando reglas: {e}")

        # Fallback: Reglas simples (siempre disponibles)
        desc_lower = description.lower()
        if "incendio" in desc_lower or "paro" in desc_lower or "industrial" in desc_lower:
            return PriorityEnum.CRITICAL
        elif "arma" in desc_lower or "robo" in desc_lower:
            return PriorityEnum.HIGH
        elif "colisión" in desc_lower or "choque" in desc_lower:
            return PriorityEnum.MEDIUM
        return PriorityEnum.LOW

    # --- AGENTE 2: Geolocalizador e Indexador Espacial Thread-Safe ---
    def _agent_spatial_allocation(self, lat: float, lon: float) -> tuple[str, str]:
        """Calcula el índice H3 creando una conexión dedicada por hilo para evitar colisiones."""
        h3_index = h3.latlng_to_cell(lat, lon, 8)
        
        # Conexión efímera exclusiva para este hilo
        conn = pg8000.connect(
            user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME
        )
        cursor = conn.cursor()
        try:
            # 1. Intento por celda H3 exacta
            cursor.execute(
                "SELECT id FROM emergency_units WHERE current_h3_res8 = %s AND status = 'AVAILABLE' LIMIT 1;", 
                [h3_index]
            )
            row = cursor.fetchone()
            if row:
                return h3_index, row[0]
            
            # 2. Respaldo espacial KNN usando PostGIS indexado
            cursor.execute(
                """
                SELECT id FROM emergency_units 
                WHERE status = 'AVAILABLE'
                ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326) 
                LIMIT 1;
                """,
                [lon, lat]
            )
            fallback_row = cursor.fetchone()
            assigned_id = fallback_row[0] if fallback_row else "UNIT-VOLUNTEER-RESERVE"
            return h3_index, assigned_id
        finally:
            cursor.close()
            conn.close()

    # --- AGENTE 3: Optimizador de Despacho y Enrutamiento ---
    async def _agent_dispatch_optimization(self, priority: PriorityEnum, lat: float, lon: float) -> tuple[float, list]:
        """Calcula ETA y ruta optimizada"""
        eta = 3.5 if priority == PriorityEnum.CRITICAL else 8.0
        route_coordinates = [
            [-67.60, 10.24], 
            [(lon + -67.60)/2, (lat + 10.24)/2], 
            [lon, lat]
        ]
        return eta, route_coordinates

    # --- PIPELINE / ORQUESTADOR DE EVENTOS ---
    async def process_message(self, msg):
        try:
            raw_data = json.loads(msg.value.decode('utf-8'))
            incident = RawIncidentEvent(**raw_data)
            
            # AGENTE 1: Triage semántico
            priority = await self._agent_triage(incident.description)
            
            # AGENTE 2: Asignación espacial (ejecutado en hilo separado)
            loop = asyncio.get_running_loop()
            h3_hex, assigned_unit = await loop.run_in_executor(
                None, self._agent_spatial_allocation, 
                incident.coordinates.latitude, incident.coordinates.longitude
            )
            
            # AGENTE 3: Optimización de despacho
            eta, route = await self._agent_dispatch_optimization(
                priority, incident.coordinates.latitude, incident.coordinates.longitude
            )
            
            # Crear orden de despacho
            order = DispatchedOrderEvent(
                incident_id=incident.incident_id,
                assigned_resource_id=assigned_unit,
                priority=priority,
                h3_index_res8=h3_hex,
                eta_minutes=eta,
                route_geojson=GeoJsonRoute(
                    geometry=GeoJsonGeometry(coordinates=route),
                    properties={"optimized_by": "H3HexagonalAllocation"}
                )
            )
            
            # Enviar al topic de órdenes despachadas
            await self.producer.send_and_wait(
                TOPIC_DISPATCHED,
                value=order.model_dump_json().encode('utf-8'),
                key=str(order.order_id).encode('utf-8')
            )
            
            logger.info(f"🎯 ORDEN DISPARADA: Incidente {incident.incident_id} -> Asignado a {assigned_unit} [{priority.value}] | ETA: {eta} min")
            await self.consumer.commit()
            
        except Exception as e:
            logger.error(f"❌ Error procesando evento: {e}")

    async def start_loop(self):
        try:
            async for msg in self.consumer:
                asyncio.create_task(self.process_message(msg))
        finally:
            await self.shutdown()

    async def shutdown(self):
        logger.info("Cerrando recursos ordenadamente...")
        if self.consumer: 
            await self.consumer.stop()
        if self.producer: 
            await self.producer.stop()
        if self.http_session: 
            await self.http_session.close()

if __name__ == "__main__":
    orchestrator = AgentOrchestratorService()
    try:
        async def run_pipeline():
            await orchestrator.initialize()
            await orchestrator.start_loop()
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        logger.info("Sistema de despacho apagado por el operador.")