import asyncio
import json
import random
import logging
from datetime import datetime
from uuid import uuid4
from aiokafka import AIOKafkaProducer
from schemas import RawIncidentEvent, Coordinates

# Configuración básica de logs en consola
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092" # Puerto mapeado en tu docker-compose
TOPIC_RAW_INCIDENTS = "raw-incidents"

# Plantillas de emergencias simuladas
INCIDENT_TEMPLATES = [
    "Incendio estructural de gran magnitud en planta industrial con heridos atrapados.",
    "Colisión vehicular menor entre dos autos particulares, sin heridos reportados.",
    "Reporte de robo en establecimiento comercial en progreso, sospechoso armado.",
    "Paro cardíaco en progreso de paciente de la tercera edad, requiere soporte vital básico inmediato."
]

async def simulate_single_incident(producer: AIOKafkaProducer, id_num: int):
    """Genera y envía un único evento de incidente simulado."""
    # Coordenadas simuladas en el eje urbano central
    lat = random.uniform(10.20, 10.30)
    lon = random.uniform(-67.65, -67.50)
    
    event = RawIncidentEvent(
        incident_id=uuid4(),
        timestamp=datetime.utcnow(),
        description=random.choice(INCIDENT_TEMPLATES),
        coordinates=Coordinates(latitude=lat, longitude=lon),
        source_device=f"IoT-SENSOR-{random.randint(1000, 9999)}"
    )
    
    # Serializar a JSON string y codificar a bytes para la red
    payload = event.model_dump_json().encode('utf-8')
    key = str(event.incident_id).encode('utf-8')
    
    try:
        # Envío asíncrono no bloqueante
        await producer.send_and_wait(TOPIC_RAW_INCIDENTS, value=payload, key=key)
        logger.info(f" Simulación [{id_num}]: Evento enviado exitosamente -> ID: {event.incident_id}")
    except Exception as e:
        logger.error(f" Error enviando simulación [{id_num}]: {e}")

async def main():
    logger.info("Iniciando conexión con el clúster de Redpanda...")
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all" # Garantía de consistencia fuerte
    )
    
    await producer.start()
    logger.info("Productor conectado. Lanzando ráfaga concurrente de incidentes...")
    
    try:
        # Lanzamos 5 simulaciones al mismo tiempo usando asyncio.gather
        tasks = [simulate_single_incident(producer, i) for i in range(1, 6)]
        await asyncio.gather(*tasks)
    finally:
        await producer.stop()
        logger.info("Productor cerrado de manera limpia.")

if __name__ == "__main__":
    asyncio.run(main())