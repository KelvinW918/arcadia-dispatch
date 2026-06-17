import asyncio
import ssl
from aiokafka import AIOKafkaProducer

async def test_redpanda():
    """Prueba de conexión a Redpanda Cloud"""
    
    # Configuración
    BROKER = "d8pdqqurhnmrt12oecjg.any.us-east-1.mpx.prd.cloud.redpanda.com:9092"
    USERNAME = "arcadia-prod"
    PASSWORD = "TYzKrB0qVDVsEPIakwgY77JirgvHyA"
    TOPIC = "raw-incidents"
    
    print(f"🔌 Conectando a Redpanda: {BROKER}")
    
    # Crear contexto SSL para SASL_SSL
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    producer = AIOKafkaProducer(
        bootstrap_servers=BROKER,
        security_protocol="SASL_SSL",
        ssl_context=ssl_context,
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD
    )
    
    try:
        # Iniciar productor
        await producer.start()
        print("✅ Conectado exitosamente a Redpanda!")
        
        # Enviar mensaje de prueba
        test_message = b'{"test": "hello from Arcadia", "timestamp": "2026-06-17T17:00:00Z"}'
        await producer.send(TOPIC, test_message)
        print(f"✅ Mensaje enviado al topic '{TOPIC}'")
        
        print("✅ Prueba completada con éxito!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await producer.stop()
        print("🔌 Conexión cerrada")

if __name__ == "__main__":
    asyncio.run(test_redpanda())