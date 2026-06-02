-- Habilitar extensión espacial nativa de PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- Tabla simulada para el inventario de unidades de emergencia
CREATE TABLE IF NOT EXISTS emergency_units (
    id VARCHAR(50) PRIMARY KEY,
    unit_type VARCHAR(30) NOT NULL,
    current_h3_res8 VARCHAR(15) NOT NULL,
    status VARCHAR(20) DEFAULT 'AVAILABLE',
    geom GEOMETRY(Point, 4326)
);

-- Crear índice espacial para búsquedas ultrarrápidas de respaldo
CREATE INDEX IF NOT EXISTS idx_units_geom ON emergency_units USING GIST(geom);
-- Crear índice hash para matching directo por celda H3
CREATE INDEX IF NOT EXISTS idx_units_h3 ON emergency_units(current_h3_res8);

-- Insertar algunas unidades de prueba en zonas aleatorias (Cerca de Maracay/Eje Central como ejemplo)
INSERT INTO emergency_units (id, unit_type, current_h3_res8, status, geom) VALUES
('UNIT-AMBULANCE-01', 'Ambulance', '88741e2511fffff', 'AVAILABLE', ST_SetSRID(ST_MakePoint(-67.59, 10.25), 4326)),
('UNIT-PATROL-02', 'Police', '88741e2513fffff', 'AVAILABLE', ST_SetSRID(ST_MakePoint(-67.58, 10.26), 4326)),
('UNIT-FIRE-03', 'Firetruck', '88741e2515fffff', 'AVAILABLE', ST_SetSRID(ST_MakePoint(-67.60, 10.24), 4326))
ON CONFLICT (id) DO NOTHING;