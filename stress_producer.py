import asyncio
import json
import random
import uuid
import logging
from datetime import datetime
from aiokafka import AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:19092"
TOPIC_NAME = "raw-incidents"

# Límites geográficos aproximados del eje urbano de Aragua (Maracay y alrededores)
LAT_MIN, LAT_MAX = 10.1500, 10.3000
LON_MIN, LON_MAX = -67.6500, -67.4500

EMERGENCY_TYPES = ["Medical", "Fire", "Traffic Accident", "Structural Risk"]
SEVERITIES = ["Low", "Medium", "High", "Critical"]

async def generate_incident():
    """Genera un payload de incidente que cumple estrictamente con el esquema del orquestador."""
    incident_type = random.choice(EMERGENCY_TYPES)
    severity = random.choice(SEVERITIES)
    
    # Coordenadas simuladas en el eje de Aragua
    lat = random.uniform(LAT_MIN, LAT_MAX)
    lon = random.uniform(LON_MIN, LON_MAX)
    
    return {
        "incident_id": str(uuid.uuid4()),
        "description": f"Reporte de emergencia: {incident_type} detectado con severidad {severity}.",
        "coordinates": {
            "latitude": lat,
            "longitude": lon
        },
        "source_device": random.choice(["MOBILE_APP", "TRAFFIC_CAMERA", "911_CALL"]),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

async def run_stress_test(total_messages: int, rate_per_second: int):
    """Inyecta ráfagas de mensajes controladas en Redpanda."""
    logger.info(f"🔥 Iniciando prueba de estrés: {total_messages} incidentes en ráfagas de {rate_per_second}/seg...")
    
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    
    sent_count = 0

    try:
        while sent_count < total_messages:
            # Crear un lote de tareas concurrentes
            batch_size = min(rate_per_second, total_messages - sent_count)
            tasks = [generate_incident() for _ in range(batch_size)]
            incidents = await asyncio.gather(*tasks)
            
            for incident in incidents:
                payload = json.dumps(incident).encode('utf-8')
                await producer.send(TOPIC_NAME, payload)
            
            sent_count += batch_size
            logger.info(f"⚡ Ráfaga enviada: {sent_count}/{total_messages} incidentes en el bus.")
            
            # Esperar un segundo exacto antes de la siguiente ráfaga
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"Error en el productor de estrés: {e}")
    finally:
        await producer.stop()
        logger.info("🏁 Prueba de estrés completada por el inyector.")

if __name__ == "__main__":
    # Configuración de la prueba: 200 mensajes en total, disparando 20 por segundo
    TOTAL_INCIDENTES = 200
    RITMO_POR_SEGUNDO = 20
    
    try:
        asyncio.run(run_stress_test(TOTAL_INCIDENTES, RITMO_POR_SEGUNDO))
    except KeyboardInterrupt:
        logger.info("Inyección abortada por el usuario.")