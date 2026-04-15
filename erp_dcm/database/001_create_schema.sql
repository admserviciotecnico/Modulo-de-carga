-- ============================================================
--  ERP DCM — Módulo de Carga de Ingeniería
--  Script: 001_create_schema.sql
--  Motor:  PostgreSQL 14+
--  Orden de ejecución: único, idempotente (IF NOT EXISTS)
-- ============================================================

-- Extensión para UUID
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
--  TIPOS ENUMERADOS
-- ============================================================

DO $$ BEGIN
  CREATE TYPE estado_conjunto AS ENUM ('EN_EDICION', 'APROBADO');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE rol_usuario AS ENUM ('ADMIN', 'INGENIERO', 'PRODUCCION', 'READONLY');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ============================================================
--  TABLA: usuarios
--  Usuarios del sistema ERP. Usada por todos los módulos.
-- ============================================================
CREATE TABLE IF NOT EXISTS usuarios (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre      VARCHAR(120) NOT NULL,
    email       VARCHAR(200) NOT NULL UNIQUE,
    rol         rol_usuario  NOT NULL DEFAULT 'INGENIERO',
    activo      BOOLEAN      NOT NULL DEFAULT TRUE,
    creado_en   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  usuarios           IS 'Usuarios del sistema ERP';
COMMENT ON COLUMN usuarios.rol       IS 'ADMIN · INGENIERO · PRODUCCION · READONLY';

-- ============================================================
--  TABLA: tipos_proceso
--  Catálogo de tipos de pieza (CHA, MEC, SOL, etc.)
--  Administrable desde UI — no hardcodeado.
-- ============================================================
CREATE TABLE IF NOT EXISTS tipos_proceso (
    codigo      VARCHAR(10)  PRIMARY KEY,        -- ej: 'CHA', 'MEC'
    nombre      VARCHAR(80)  NOT NULL,           -- ej: 'Chapa', 'Mecanizado'
    activo      BOOLEAN      NOT NULL DEFAULT TRUE,
    orden_ui    SMALLINT     NOT NULL DEFAULT 0  -- orden de aparición en selects
);

COMMENT ON TABLE tipos_proceso IS 'Catálogo de tipos de pieza/proceso (CHA, MEC, SOL, ARM…)';

-- Seed inicial — los tipos del maestro de la demo v5
INSERT INTO tipos_proceso (codigo, nombre, orden_ui) VALUES
    ('COM', 'Comprado',           1),
    ('CHA', 'Chapa',              2),
    ('MEC', 'Mecanizado',         3),
    ('INY', 'Inyección plástico', 4),
    ('TER', 'Termoformado',       5),
    ('PLA', 'Placa plástica',     6),
    ('SOL', 'Soldadura',          7),
    ('ARM', 'Armado',             8)
ON CONFLICT (codigo) DO NOTHING;

-- ============================================================
--  TABLA: procesos_maestro
--  Listado de procesos disponibles por tipo de pieza,
--  con proveedor default sugerido.
-- ============================================================
CREATE TABLE IF NOT EXISTS procesos_maestro (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tipo_proceso     VARCHAR(10) NOT NULL REFERENCES tipos_proceso(codigo) ON UPDATE CASCADE,
    nombre_proceso   VARCHAR(150) NOT NULL,
    proveedor_default VARCHAR(150),
    orden_sugerido   SMALLINT    NOT NULL DEFAULT 0,
    activo           BOOLEAN     NOT NULL DEFAULT TRUE,
    UNIQUE (tipo_proceso, nombre_proceso)
);

COMMENT ON TABLE  procesos_maestro                  IS 'Procesos disponibles por tipo de pieza';
COMMENT ON COLUMN procesos_maestro.proveedor_default IS 'Proveedor sugerido al asignar el proceso';

-- Seed — maestro completo de la demo v5
INSERT INTO procesos_maestro (tipo_proceso, nombre_proceso, proveedor_default, orden_sugerido) VALUES
    -- COM
    ('COM', 'Abrir O/C Importar',              'Compras importacion',    1),
    ('COM', 'Abrir O/C Nacional',              'Compras nacionales',     2),
    -- CHA
    ('CHA', 'Corte x laser',                   'Laser chapa',            1),
    ('CHA', 'Plegado',                         'Plegado',                2),
    ('CHA', 'Perforado y roscado',             'Mecanica',               3),
    ('CHA', 'Rebabado',                        'TODOS',                  4),
    ('CHA', 'Stockear para soldadura',         'TODOS',                  5),
    ('CHA', 'Stockear para armado final',      'TODOS',                  6),
    ('CHA', 'Tratamiento Pintura',             'Tecnoesmalte',           7),
    ('CHA', 'Tratamiento zincado',             'Niquelado San Martin',   8),
    ('CHA', 'Tratamiento pavonado',            'Mecanica',               9),
    -- MEC
    ('MEC', 'Tornear',                         'Mecanica',               1),
    ('MEC', 'Fresar',                          'Mecanica',               2),
    ('MEC', 'Perforar',                        'Mecanica',               3),
    ('MEC', 'Roscar',                          'Mecanica',               4),
    ('MEC', 'Cortar',                          'Mecanica',               5),
    ('MEC', 'Stockear para soldadura',         'TODOS',                  6),
    ('MEC', 'Stockear para armado final',      'TODOS',                  7),
    ('MEC', 'Tratamiento Pintura',             'Tecnoesmalte',           8),
    ('MEC', 'Tratamiento zincado',             'Niquelado San Martin',   9),
    ('MEC', 'Tratamiento pavonado',            'Mecanica',               10),
    ('MEC', 'Curvado o doblado de caños',      'Reyper',                 11),
    -- INY
    ('INY', 'Inyectar plastico',               'Moltec',                 1),
    ('INY', 'Inyectar con insertos',           'Moltec',                 2),
    ('INY', 'Mecanizar',                       'Mecanica',               3),
    ('INY', 'Stockear para armado final',      'TODOS',                  4),
    ('INY', 'Tratamiento Pintura Plasticos',   'Supercolor',             5),
    -- TER
    ('TER', 'Termoformar',                     'Vacumpac',               1),
    ('TER', 'Mecanizar',                       'Mecanica',               2),
    ('TER', 'Perforar y roscar',               'Mecanica',               3),
    ('TER', 'Stockear para armado final',      'TODOS',                  4),
    ('TER', 'Tratamiento Pintura plasticos',   'Supercolor',             5),
    -- PLA
    ('PLA', 'Corte por laser plastico',        'laser PLAST',            1),
    ('PLA', 'Perforar y roscar',               'Mecanica',               2),
    ('PLA', 'Stockear para armado final',      'TODOS',                  3),
    ('PLA', 'Tratamiento Pintura',             'Supercolor',             4),
    -- SOL
    ('SOL', 'Rebabar',                         'Soldadura',              1),
    ('SOL', 'Soldar subconjunto',              'Soldadura',              2),
    ('SOL', 'Soldar tornillos por proyeccion', 'Soldadura',              3),
    ('SOL', 'Pulir',                           'Soldadura',              4),
    -- ARM
    ('ARM', 'Armado mecanico',                 'Armado mecanico',        1)
ON CONFLICT (tipo_proceso, nombre_proceso) DO NOTHING;

-- ============================================================
--  TABLA: conjuntos
--  Cabecera de una BOM importada desde SolidWorks.
--  Una BOM aprobada queda "freezada" (solo lectura).
-- ============================================================
CREATE TABLE IF NOT EXISTS conjuntos (
    id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    codigo           VARCHAR(80)   NOT NULL,          -- ej: 'AB200'
    descripcion      VARCHAR(250),
    version          VARCHAR(20)   NOT NULL DEFAULT '1',
    estado           estado_conjunto NOT NULL DEFAULT 'EN_EDICION',
    creado_por       UUID          REFERENCES usuarios(id) ON DELETE SET NULL,
    aprobado_por     UUID          REFERENCES usuarios(id) ON DELETE SET NULL,
    creado_en        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    aprobado_en      TIMESTAMPTZ,
    actualizado_en   TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  conjuntos         IS 'BOM completa de un producto (cabecera)';
COMMENT ON COLUMN conjuntos.estado  IS 'EN_EDICION: editable. APROBADO: freezado, solo lectura.';
COMMENT ON COLUMN conjuntos.version IS 'Versión del conjunto (manual o autoincrementada)';

-- Trigger: actualizar actualizado_en automáticamente
CREATE OR REPLACE FUNCTION fn_set_actualizado_en()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.actualizado_en = NOW(); RETURN NEW; END;
$$;

DROP TRIGGER IF EXISTS trg_conjuntos_actualizado_en ON conjuntos;
CREATE TRIGGER trg_conjuntos_actualizado_en
    BEFORE UPDATE ON conjuntos
    FOR EACH ROW EXECUTE FUNCTION fn_set_actualizado_en();

-- ============================================================
--  TABLA: items_bom
--  Cada fila de la BOM importada.
--  columnas_extra guarda todo lo que SolidWorks exporta
--  y que no tiene columna propia en el modelo fijo.
-- ============================================================
CREATE TABLE IF NOT EXISTS items_bom (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    conjunto_id     UUID         NOT NULL REFERENCES conjuntos(id) ON DELETE CASCADE,
    numero_item     SMALLINT     NOT NULL,           -- número de fila en la BOM
    codigo_pieza    VARCHAR(80)  NOT NULL,
    descripcion     VARCHAR(250),
    cantidad        NUMERIC(10,3) NOT NULL DEFAULT 1,
    tipo            VARCHAR(10)  REFERENCES tipos_proceso(codigo) ON UPDATE CASCADE,
    material        VARCHAR(150),
    peso            VARCHAR(50),                    -- string para preservar unidades
    columnas_extra  JSONB        NOT NULL DEFAULT '{}',
    creado_en       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (conjunto_id, numero_item)
);

COMMENT ON TABLE  items_bom              IS 'Ítems individuales de una BOM';
COMMENT ON COLUMN items_bom.tipo         IS 'FK a tipos_proceso — determina qué procesos están disponibles';
COMMENT ON COLUMN items_bom.columnas_extra IS 'Columnas adicionales del Excel de SolidWorks (JSONB)';

CREATE INDEX IF NOT EXISTS idx_items_bom_conjunto ON items_bom(conjunto_id);
CREATE INDEX IF NOT EXISTS idx_items_bom_tipo     ON items_bom(tipo);

-- ============================================================
--  TABLA: archivos_dwg
--  Registro de los planos DWG asociados a cada ítem.
--  El archivo físico vive en la red interna; acá se guarda
--  la ruta y metadatos. vigente=true marca el plano activo.
-- ============================================================
CREATE TABLE IF NOT EXISTS archivos_dwg (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id          UUID         NOT NULL REFERENCES items_bom(id) ON DELETE CASCADE,
    nombre_archivo   VARCHAR(250) NOT NULL,
    ruta_servidor    VARCHAR(500) NOT NULL,   -- ruta UNC completa en la red
    subido_por       VARCHAR(120),            -- nombre o email (sin FK obligatoria)
    subido_en        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    vigente          BOOLEAN      NOT NULL DEFAULT TRUE
);

COMMENT ON TABLE  archivos_dwg          IS 'Planos DWG por ítem. vigente=true indica el plano activo.';
COMMENT ON COLUMN archivos_dwg.vigente  IS 'Solo uno por ítem debería estar vigente a la vez';

CREATE INDEX IF NOT EXISTS idx_archivos_dwg_item ON archivos_dwg(item_id);

-- Función: al insertar un DWG vigente, desactivar los anteriores del mismo ítem
CREATE OR REPLACE FUNCTION fn_dwg_un_vigente()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.vigente = TRUE THEN
        UPDATE archivos_dwg
        SET vigente = FALSE
        WHERE item_id = NEW.item_id AND id <> NEW.id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dwg_un_vigente ON archivos_dwg;
CREATE TRIGGER trg_dwg_un_vigente
    AFTER INSERT OR UPDATE ON archivos_dwg
    FOR EACH ROW EXECUTE FUNCTION fn_dwg_un_vigente();

-- ============================================================
--  TABLA: operaciones_item
--  Las operaciones/procesos asignados a cada ítem de la BOM.
--  Máximo 10 por ítem (validado en aplicación y constraint).
-- ============================================================
CREATE TABLE IF NOT EXISTS operaciones_item (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id         UUID         NOT NULL REFERENCES items_bom(id) ON DELETE CASCADE,
    orden           SMALLINT     NOT NULL,           -- 1-10, define secuencia
    proceso         VARCHAR(150) NOT NULL,
    tipo_proceso    VARCHAR(10)  REFERENCES tipos_proceso(codigo) ON UPDATE CASCADE,
    proveedor       VARCHAR(150),
    creado_en       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (item_id, orden),
    CONSTRAINT max_10_operaciones CHECK (orden BETWEEN 1 AND 10)
);

COMMENT ON TABLE  operaciones_item       IS 'Secuencia de operaciones de fabricación por ítem';
COMMENT ON COLUMN operaciones_item.orden IS '1-10. Define el orden de ejecución en producción';

CREATE INDEX IF NOT EXISTS idx_operaciones_item ON operaciones_item(item_id);

-- ============================================================
--  TABLA: historial_estados
--  Auditoría de cada cambio de estado de un conjunto.
--  Inmutable: no hay UPDATE ni DELETE sobre esta tabla.
-- ============================================================
CREATE TABLE IF NOT EXISTS historial_estados (
    id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    conjunto_id      UUID          NOT NULL REFERENCES conjuntos(id) ON DELETE CASCADE,
    estado_anterior  VARCHAR(30),
    estado_nuevo     VARCHAR(30)   NOT NULL,
    usuario_id       UUID          REFERENCES usuarios(id) ON DELETE SET NULL,
    comentario       TEXT,
    fecha            TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE historial_estados IS 'Auditoría inmutable de transiciones de estado de conjuntos';

CREATE INDEX IF NOT EXISTS idx_historial_conjunto ON historial_estados(conjunto_id);

-- ============================================================
--  VISTA: v_conjuntos_aprobados
--  Lo que consumirá el módulo de producción:
--  solo conjuntos freezados, con conteo de ítems.
-- ============================================================
CREATE OR REPLACE VIEW v_conjuntos_aprobados AS
SELECT
    c.id,
    c.codigo,
    c.descripcion,
    c.version,
    c.aprobado_en,
    u.nombre        AS aprobado_por_nombre,
    COUNT(i.id)     AS total_items
FROM conjuntos c
LEFT JOIN usuarios u    ON u.id = c.aprobado_por
LEFT JOIN items_bom i   ON i.conjunto_id = c.id
WHERE c.estado = 'APROBADO'
GROUP BY c.id, c.codigo, c.descripcion, c.version, c.aprobado_en, u.nombre;

COMMENT ON VIEW v_conjuntos_aprobados IS 'Interfaz de lectura para el módulo de producción';

-- ============================================================
--  VISTA: v_bom_completa
--  BOM expandida con plano vigente y cantidad de operaciones.
--  Útil para exportar JSON hacia producción.
-- ============================================================
CREATE OR REPLACE VIEW v_bom_completa AS
SELECT
    i.id                  AS item_id,
    i.conjunto_id,
    c.codigo              AS codigo_conjunto,
    c.estado              AS estado_conjunto,
    i.numero_item,
    i.codigo_pieza,
    i.descripcion,
    i.cantidad,
    i.tipo,
    i.material,
    i.peso,
    i.columnas_extra,
    d.nombre_archivo      AS dwg_nombre,
    d.ruta_servidor       AS dwg_ruta,
    COUNT(o.id)           AS total_operaciones
FROM items_bom i
JOIN conjuntos c           ON c.id = i.conjunto_id
LEFT JOIN archivos_dwg d   ON d.item_id = i.id AND d.vigente = TRUE
LEFT JOIN operaciones_item o ON o.item_id = i.id
GROUP BY i.id, c.id, c.codigo, c.estado, d.nombre_archivo, d.ruta_servidor
ORDER BY i.conjunto_id, i.numero_item;

COMMENT ON VIEW v_bom_completa IS 'BOM expandida con DWG vigente y conteo de operaciones';
