import asyncio
import json
import random
import uuid
import logging
import os
import ssl
from datetime import datetime, timezone  # ← AQUÍ EL CAMBIO
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv(override=True)

# Después de load_dotenv()
print("🔍 DEBUG - Variables cargadas:")
print(f"KAFKA_BROKERS: {os.getenv('KAFKA_BROKERS')}")
print(f"KAFKA_USERNAME: {os.getenv('KAFKA_USERNAME')}")
print(f"KAFKA_PASSWORD: {os.getenv('KAFKA_PASSWORD')}")
print(f"KAFKA_TOPIC: {os.getenv('KAFKA_TOPIC')}")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Variables de entorno para producción
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "127.0.0.1:19092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "raw-incidents")

# Modo de operación: 'stress' para pruebas de estrés, 'continuous' para producción
MODE = os.getenv("PRODUCER_MODE", "continuous")  # stress o continuous

# Límites geográficos aproximados del eje urbano de Aragua (Maracay y alrededores)
LAT_MIN, LAT_MAX = 10.1500, 10.3000
LON_MIN, LON_MAX = -67.6500, -67.4500

EMERGENCY_TYPES = ["Medical", "Fire", "Traffic Accident", "Structural Risk"]
SEVERITIES = ["Low", "Medium", "High", "Critical"]

def create_producer():
    """Crea un productor con autenticación SASL_SSL si está configurada"""
    
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        # Configuración SASL_SSL para Redpanda Cloud
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        logger.info(f"🔐 Conectando con autenticación SASL_SSL a {KAFKA_BROKERS}")
        
        return AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            security_protocol="SASL_SSL",
            ssl_context=ssl_context,
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            acks="all",
            max_batch_size=16384,
            linger_ms=10
        )
    else:
        # Configuración local (sin SSL)
        logger.info(f"🔌 Conectando sin autenticación a {KAFKA_BROKERS}")
        return AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            acks="all"
        )

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
        "timestamp": datetime.now(timezone.utc).isoformat(),  # ← AQUÍ EL CAMBIO
        "type": incident_type,
        "severity": severity
    }

async def run_stress_test(total_messages: int, rate_per_second: int):
    """Inyecta ráfagas de mensajes controladas en Redpanda."""
    logger.info(f"🔥 Iniciando prueba de estrés: {total_messages} incidentes en ráfagas de {rate_per_second}/seg...")
    
    producer = create_producer()
    await producer.start()
    logger.info(f"✅ Conectado a Redpanda! Enviando al topic: {TOPIC_NAME}")
    
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
        logger.error(f"❌ Error en el productor de estrés: {e}")
    finally:
        await producer.stop()
        logger.info("🏁 Prueba de estrés completada por el inyector.")

async def run_continuous_producer(interval_seconds: int = 10, batch_size: int = 3):
    """Ejecuta el productor continuamente en producción."""
    logger.info(f"🚀 Iniciando productor en modo continuo (cada {interval_seconds}s, {batch_size} incidentes por lote)...")
    
    producer = create_producer()
    await producer.start()
    logger.info(f"✅ Conectado a Redpanda! Enviando al topic: {TOPIC_NAME}")
    
    total_sent = 0
    
    try:
        while True:
            # Generar y enviar un lote de incidentes
            tasks = [generate_incident() for _ in range(batch_size)]
            incidents = await asyncio.gather(*tasks)
            
            for incident in incidents:
                payload = json.dumps(incident).encode('utf-8')
                await producer.send(TOPIC_NAME, payload)
                total_sent += 1
            
            logger.info(f"📤 Enviados {batch_size} incidentes (Total: {total_sent})")
            
            # Esperar antes del siguiente lote
            await asyncio.sleep(interval_seconds)
            
    except KeyboardInterrupt:
        logger.info("⏹️ Productor detenido por el usuario")
    except Exception as e:
        logger.error(f"❌ Error en el productor continuo: {e}")
    finally:
        await producer.stop()
        logger.info(f"🏁 Productor detenido. Total enviado: {total_sent} incidentes")

if __name__ == "__main__":
    # Configuración según el modo
    MODE = os.getenv("PRODUCER_MODE", "continuous")
    
    if MODE == "stress":
        # Modo prueba de estrés
        TOTAL_INCIDENTES = int(os.getenv("STRESS_TOTAL", "200"))
        RITMO_POR_SEGUNDO = int(os.getenv("STRESS_RATE", "20"))
        
        try:
            asyncio.run(run_stress_test(TOTAL_INCIDENTES, RITMO_POR_SEGUNDO))
        except KeyboardInterrupt:
            logger.info("Inyección abortada por el usuario.")
    else:
        # Modo continuo (producción)
        INTERVALO = int(os.getenv("PRODUCER_INTERVAL", "10"))
        BATCH = int(os.getenv("PRODUCER_BATCH", "3"))
        
        try:
            asyncio.run(run_continuous_producer(INTERVALO, BATCH))
        except KeyboardInterrupt:
            logger.info("Productor continuo detenido por el usuario.")