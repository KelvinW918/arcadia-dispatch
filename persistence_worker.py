import asyncio
import json
import logging
import pg8000
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configuración de infraestructura
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:19092"
DB_USER = "postgres" 
DB_PASSWORD = "supersecretpassword"
DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "postgres"

class PersistenceWorker:
    def __init__(self):
        self.consumer = None

    async def initialize(self):
        """Inicializa el consumidor dedicado a persistir las órdenes despacho."""
        logger.info("Iniciando Worker de Persistencia Geo-temporal...")
        self.consumer = AIOKafkaConsumer(
            "dispatched-orders",
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="persistence-storage-group",
            enable_auto_commit=False,
            auto_offset_reset="earliest"
        )
        await self.consumer.start()
        logger.info("💾 Guardián de almacenamiento escuchando 'dispatched-orders'...")

    def _convert_route_to_wkt(self, coordinates: list) -> str:
        """Transforma coordenadas GeoJSON [[lon, lat], ...] a formato WKT LINESTRING para PostGIS."""
        point_strings = [f"{coord[0]} {coord[1]}" for coord in coordinates]
        return f"LINESTRING({', '.join(point_strings)})"

    def _save_to_db(self, order_data: dict):
        """Abre una conexión dedicada por hilo para insertar los datos de forma aislada."""
        conn = pg8000.connect(
            user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME
        )
        cursor = conn.cursor()
        try:
            # Extraer la ruta geográfica del GeoJSON
            route_geojson = order_data.get("route_geojson", {})
            coords = route_geojson.get("geometry", {}).get("coordinates", [])
            wkt_line = self._convert_route_to_wkt(coords)

            cursor.execute(
                """
                INSERT INTO dispatched_orders_history 
                (order_id, incident_id, assigned_resource_id, priority, h3_index_res8, eta_minutes, route_geom)
                VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326));
                """,
                [
                    order_data["order_id"],
                    order_data["incident_id"],
                    order_data["assigned_resource_id"],
                    order_data["priority"],
                    order_data["h3_index_res8"],
                    order_data["eta_minutes"],
                    wkt_line
                ]
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    async def process_orders(self):
        """Loop asíncrono para capturar eventos e indexarlos sin bloquear el loop principal."""
        loop = asyncio.get_running_loop()
        try:
            async for msg in self.consumer:
                try:
                    order_data = json.loads(msg.value.decode('utf-8'))
                    
                    # Ejecutamos la inserción síncrona en el pool de hilos del executor
                    await loop.run_in_executor(None, self._save_to_db, order_data)
                    
                    logger.info(f"💾 ÓRDEN GUARDADA -> ID: {order_data['order_id']} | Recurso: {order_data['assigned_resource_id']}")
                    await self.consumer.commit()
                except Exception as e:
                    logger.error(f"Error procesando registro de orden individual: {e}")
        finally:
            if self.consumer:
                await self.consumer.stop()

if __name__ == "__main__":
    worker = PersistenceWorker()
    try:
        async def main():
            await worker.initialize()
            await worker.process_orders()
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker de persistencia apagado por el operador.")