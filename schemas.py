from datetime import datetime
from enum import Enum
from typing import List, Dict, Any
from uuid import UUID, uuid4
from pydantic import BaseModel, Field

class PriorityEnum(str, Enum):
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"

class Coordinates(BaseModel):
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)

# Contrato para el tópico: raw-incidents
class RawIncidentEvent(BaseModel):
    incident_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    description: str = Field(..., min_length=10)
    coordinates: Coordinates
    source_device: str

# Estructuras GeoJSON estándar para el despacho final
class GeoJsonGeometry(BaseModel):
    type: str = "LineString"
    coordinates: List[List[float]]

class GeoJsonRoute(BaseModel):
    type: str = "Feature"
    geometry: GeoJsonGeometry
    properties: Dict[str, Any]

# Contrato para el tópico: dispatched-orders
class DispatchedOrderEvent(BaseModel):
    order_id: UUID = Field(default_factory=uuid4)
    incident_id: UUID
    assigned_resource_id: str
    priority: PriorityEnum
    h3_index_res8: str
    eta_minutes: float = Field(..., ge=0)
    route_geojson: GeoJsonRoute
    dispatched_at: datetime = Field(default_factory=datetime.utcnow)