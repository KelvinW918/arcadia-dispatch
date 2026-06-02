Aragua Emergency Dispatch System
Sistema de despacho de emergencias geoespacial en tiempo real diseñado para el monitoreo y gestión de unidades de campo.

Descripción del Proyecto
Este sistema procesa telemetría geoespacial en tiempo real proveniente de unidades de emergencia (bomberos, ambulancias, policía). Utiliza una arquitectura basada en eventos para garantizar alta disponibilidad y escalabilidad, persistiendo datos complejos en PostGIS y visualizándolos mediante un mapa interactivo.

Stack Tecnológico
Backend: Go (Golang) con pgxpool para gestión de conexiones de alto rendimiento.

Ingesta de Datos: Redpanda (API compatible con Kafka) para el streaming de eventos.

Base de Datos: PostgreSQL con extensión PostGIS para análisis y cálculos espaciales (centroides, geometrías).

Frontend: Leaflet.js para renderizado de mapas y Tailwind CSS para la interfaz.

Infraestructura: Docker Compose para la orquestación de servicios (Postgres, Redpanda, Redis).

Arquitectura del Sistema
El flujo de datos sigue este esquema:

El Productor (Python) inyecta eventos de unidades de emergencia a Redpanda.

El Backend en Go consume los eventos, procesa las coordenadas y las inserta en PostGIS.

El Frontend consulta la API en Go, la cual realiza cálculos espaciales en tiempo real mediante PostGIS y actualiza el mapa.

Instrucciones de Ejecución
1. Requisitos Previos
Docker Desktop instalado.

Go v1.21 o superior.

Python 3.12 o superior.

2. Configuración
Clonar el repositorio:
git clone https://github.com/KelvinW918/Aragua_on_fire.git
cd Aragua_on_fire

Levantar la infraestructura (Base de datos y Redpanda):
docker-compose up -d

Ejecutar el backend (Servidor API):
cd backend-go
go run main.go

Iniciar la simulación (Productor de eventos):
python stress_producer.py

Visualización:
Abrir el archivo frontend/index.html en tu navegador.

Autor
Kelvin W. (KelvinW918)
