"""
ERP DCM — Módulo de Carga de Ingeniería
API REST con FastAPI + asyncpg

Instalación:
    pip install fastapi uvicorn asyncpg python-multipart pydantic

Ejecución local:
    uvicorn api_modulo_carga:app --reload --port 8000

Documentación automática:
    http://localhost:8000/docs
"""

from __future__ import annotations

import os
import uuid
import shutil
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import asyncpg
from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ─── Configuración ────────────────────────────────────────────────────────────

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/erp_dcm"
)

# Ruta base donde se guardarán los DWG en el servidor de archivos.
# En producción apuntar a la UNC: \\192.168.88.223\Ingeniería\Producción\06_Planos_para_fabricacion
DWG_BASE_PATH = Path(
    os.getenv("DWG_BASE_PATH", "./archivos_dwg_local")
)
DWG_BASE_PATH.mkdir(parents=True, exist_ok=True)


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="ERP DCM — Módulo de Carga de Ingeniería",
    version="1.0.0",
    description="API para gestión de BOMs, planos DWG y procesos de fabricación",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # Ajustar a dominio del frontend en producción
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Pool de conexiones ───────────────────────────────────────────────────────

_pool: asyncpg.Pool | None = None


async def get_db() -> asyncpg.Connection:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with _pool.acquire() as conn:
        yield conn


@app.on_event("startup")
async def startup():
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)


@app.on_event("shutdown")
async def shutdown():
    if _pool:
        await _pool.close()


# ─── Enums y constantes ───────────────────────────────────────────────────────

class EstadoConjunto(str, Enum):
    EN_EDICION = "EN_EDICION"
    APROBADO   = "APROBADO"


MAX_OPERACIONES = 10


# ─── Schemas de entrada/salida (Pydantic) ─────────────────────────────────────

class OperacionIn(BaseModel):
    orden:        int   = Field(..., ge=1, le=10, description="Posición en la secuencia (1-10)")
    proceso:      str   = Field(..., max_length=150)
    tipo_proceso: Optional[str] = Field(None, max_length=10)
    proveedor:    Optional[str] = Field(None, max_length=150)


class OperacionOut(OperacionIn):
    id:         uuid.UUID
    item_id:    uuid.UUID
    creado_en:  datetime


class ItemBomIn(BaseModel):
    numero_item:    int              = Field(..., ge=1)
    codigo_pieza:   str              = Field(..., max_length=80)
    descripcion:    Optional[str]    = Field(None, max_length=250)
    cantidad:       float            = Field(1, gt=0)
    tipo:           Optional[str]    = Field(None, max_length=10)
    material:       Optional[str]    = Field(None, max_length=150)
    peso:           Optional[str]    = Field(None, max_length=50)
    columnas_extra: dict[str, Any]   = Field(default_factory=dict)
    operaciones:    list[OperacionIn] = Field(default_factory=list)

    class Config:
        json_schema_extra = {
            "example": {
                "numero_item": 1,
                "codigo_pieza": "AB200-001",
                "descripcion": "Chasis principal",
                "cantidad": 1,
                "tipo": "CHA",
                "material": "AISI 304",
                "peso": "4.2 kg",
                "columnas_extra": {"norma": "DIN 912"},
                "operaciones": [
                    {"orden": 1, "proceso": "Corte x laser", "tipo_proceso": "CHA", "proveedor": "Laser chapa"},
                    {"orden": 2, "proceso": "Plegado",        "tipo_proceso": "CHA", "proveedor": "Plegado"},
                ]
            }
        }


class ItemBomOut(ItemBomIn):
    id:          uuid.UUID
    conjunto_id: uuid.UUID
    creado_en:   datetime
    dwg_vigente: Optional[str] = None     # nombre del archivo DWG activo


class ConjuntoIn(BaseModel):
    codigo:      str           = Field(..., max_length=80)
    descripcion: Optional[str] = Field(None, max_length=250)
    version:     str           = Field("1", max_length=20)
    items:       list[ItemBomIn] = Field(default_factory=list)


class ConjuntoOut(BaseModel):
    id:            uuid.UUID
    codigo:        str
    descripcion:   Optional[str]
    version:       str
    estado:        EstadoConjunto
    creado_en:     datetime
    aprobado_en:   Optional[datetime]
    total_items:   int = 0


class AprobacionIn(BaseModel):
    usuario_id:  uuid.UUID
    comentario:  Optional[str] = None


class ProcesosOut(BaseModel):
    codigo:            str
    nombre:            str
    proveedor_default: Optional[str]
    orden_sugerido:    int


class TipoProcesoOut(BaseModel):
    codigo:    str
    nombre:    str
    procesos:  list[ProcesosOut]


# ─── Helpers internos ─────────────────────────────────────────────────────────

async def _conjunto_o_404(conn: asyncpg.Connection, conjunto_id: uuid.UUID):
    row = await conn.fetchrow("SELECT * FROM conjuntos WHERE id = $1", conjunto_id)
    if not row:
        raise HTTPException(status_code=404, detail="Conjunto no encontrado")
    return row


async def _assert_editable(conjunto):
    if conjunto["estado"] == EstadoConjunto.APROBADO:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El conjunto está aprobado (freezado) y no puede modificarse"
        )


# ─── ENDPOINTS: Maestro de procesos ───────────────────────────────────────────

@app.get(
    "/maestro/tipos",
    response_model=list[TipoProcesoOut],
    tags=["Maestro de procesos"],
    summary="Devuelve todos los tipos de proceso con sus procesos disponibles"
)
async def get_maestro(conn: asyncpg.Connection = Depends(get_db)):
    tipos = await conn.fetch(
        "SELECT codigo, nombre FROM tipos_proceso WHERE activo = TRUE ORDER BY orden_ui"
    )
    result = []
    for t in tipos:
        procesos = await conn.fetch(
            """SELECT nombre_proceso AS codigo, nombre_proceso AS nombre,
                      proveedor_default, orden_sugerido
               FROM procesos_maestro
               WHERE tipo_proceso = $1 AND activo = TRUE
               ORDER BY orden_sugerido""",
            t["codigo"]
        )
        result.append({
            "codigo":   t["codigo"],
            "nombre":   t["nombre"],
            "procesos": [dict(p) for p in procesos],
        })
    return result


# ─── ENDPOINTS: Conjuntos ─────────────────────────────────────────────────────

@app.get(
    "/conjuntos",
    response_model=list[ConjuntoOut],
    tags=["Conjuntos"],
    summary="Lista todos los conjuntos (borradores y aprobados)"
)
async def listar_conjuntos(
    estado: Optional[EstadoConjunto] = None,
    conn:   asyncpg.Connection = Depends(get_db)
):
    query = """
        SELECT c.*, COUNT(i.id) AS total_items
        FROM conjuntos c
        LEFT JOIN items_bom i ON i.conjunto_id = c.id
        {where}
        GROUP BY c.id
        ORDER BY c.actualizado_en DESC
    """
    if estado:
        rows = await conn.fetch(
            query.format(where="WHERE c.estado = $1"), estado.value
        )
    else:
        rows = await conn.fetch(query.format(where=""))
    return [dict(r) for r in rows]


@app.post(
    "/conjuntos",
    response_model=ConjuntoOut,
    status_code=status.HTTP_201_CREATED,
    tags=["Conjuntos"],
    summary="Crea un nuevo conjunto (borrador) con sus ítems y operaciones"
)
async def crear_conjunto(
    payload: ConjuntoIn,
    conn:    asyncpg.Connection = Depends(get_db)
):
    async with conn.transaction():
        conjunto_id = await conn.fetchval(
            """INSERT INTO conjuntos (codigo, descripcion, version)
               VALUES ($1, $2, $3) RETURNING id""",
            payload.codigo, payload.descripcion, payload.version
        )

        await _insertar_items(conn, conjunto_id, payload.items)

        await conn.execute(
            """INSERT INTO historial_estados (conjunto_id, estado_anterior, estado_nuevo, comentario)
               VALUES ($1, NULL, 'EN_EDICION', 'Conjunto creado')""",
            conjunto_id
        )

    return await _fetch_conjunto_out(conn, conjunto_id)


@app.get(
    "/conjuntos/{conjunto_id}",
    tags=["Conjuntos"],
    summary="Detalle completo de un conjunto con todos sus ítems y operaciones"
)
async def get_conjunto(
    conjunto_id: uuid.UUID,
    conn:        asyncpg.Connection = Depends(get_db)
):
    conjunto = await _conjunto_o_404(conn, conjunto_id)

    items_raw = await conn.fetch(
        "SELECT * FROM items_bom WHERE conjunto_id = $1 ORDER BY numero_item",
        conjunto_id
    )

    items_out = []
    for item in items_raw:
        operaciones = await conn.fetch(
            "SELECT * FROM operaciones_item WHERE item_id = $1 ORDER BY orden",
            item["id"]
        )
        dwg = await conn.fetchrow(
            "SELECT nombre_archivo, ruta_servidor FROM archivos_dwg WHERE item_id = $1 AND vigente = TRUE",
            item["id"]
        )
        items_out.append({
            **dict(item),
            "operaciones": [dict(o) for o in operaciones],
            "dwg_vigente": dwg["nombre_archivo"] if dwg else None,
            "dwg_ruta":    dwg["ruta_servidor"]  if dwg else None,
        })

    return {**dict(conjunto), "items": items_out}


@app.put(
    "/conjuntos/{conjunto_id}",
    tags=["Conjuntos"],
    summary="Guarda cambios en un conjunto EN_EDICION (borrador)"
)
async def actualizar_conjunto(
    conjunto_id: uuid.UUID,
    payload:     ConjuntoIn,
    conn:        asyncpg.Connection = Depends(get_db)
):
    conjunto = await _conjunto_o_404(conn, conjunto_id)
    await _assert_editable(conjunto)

    async with conn.transaction():
        await conn.execute(
            "UPDATE conjuntos SET codigo=$1, descripcion=$2, version=$3 WHERE id=$4",
            payload.codigo, payload.descripcion, payload.version, conjunto_id
        )
        # Reemplazar ítems: borrar y reinsertar (simple y predecible)
        await conn.execute(
            "DELETE FROM items_bom WHERE conjunto_id = $1", conjunto_id
        )
        await _insertar_items(conn, conjunto_id, payload.items)

    return await _fetch_conjunto_out(conn, conjunto_id)


@app.post(
    "/conjuntos/{conjunto_id}/aprobar",
    tags=["Conjuntos"],
    summary="Aprueba (freezea) un conjunto — operación irreversible"
)
async def aprobar_conjunto(
    conjunto_id: uuid.UUID,
    payload:     AprobacionIn,
    conn:        asyncpg.Connection = Depends(get_db)
):
    conjunto = await _conjunto_o_404(conn, conjunto_id)
    await _assert_editable(conjunto)

    # Validación mínima: todos los ítems deben tener al menos una operación
    items_sin_ops = await conn.fetchval(
        """SELECT COUNT(*) FROM items_bom i
           WHERE i.conjunto_id = $1
             AND NOT EXISTS (
                 SELECT 1 FROM operaciones_item o WHERE o.item_id = i.id
             )""",
        conjunto_id
    )
    if items_sin_ops > 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{items_sin_ops} ítem(s) no tienen operaciones asignadas"
        )

    async with conn.transaction():
        await conn.execute(
            """UPDATE conjuntos
               SET estado = 'APROBADO', aprobado_por = $1, aprobado_en = NOW()
               WHERE id = $2""",
            payload.usuario_id, conjunto_id
        )
        await conn.execute(
            """INSERT INTO historial_estados
               (conjunto_id, estado_anterior, estado_nuevo, usuario_id, comentario)
               VALUES ($1, 'EN_EDICION', 'APROBADO', $2, $3)""",
            conjunto_id, payload.usuario_id, payload.comentario
        )

    return {"ok": True, "mensaje": "Conjunto aprobado y freezado correctamente"}


# ─── ENDPOINTS: Archivos DWG ──────────────────────────────────────────────────

@app.post(
    "/items/{item_id}/dwg",
    status_code=status.HTTP_201_CREATED,
    tags=["Planos DWG"],
    summary="Sube un archivo DWG para un ítem y registra la ruta en DB"
)
async def subir_dwg(
    item_id:     uuid.UUID,
    archivo:     UploadFile = File(...),
    subido_por:  str = "sistema",
    conn:        asyncpg.Connection = Depends(get_db)
):
    # Verificar que el ítem existe y el conjunto es editable
    item = await conn.fetchrow("SELECT * FROM items_bom WHERE id = $1", item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Ítem no encontrado")

    conjunto = await conn.fetchrow(
        "SELECT * FROM conjuntos WHERE id = $1", item["conjunto_id"]
    )
    await _assert_editable(conjunto)

    # Validar extensión
    if not archivo.filename.lower().endswith(".dwg"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Solo se aceptan archivos .dwg"
        )

    # Guardar el archivo físicamente
    destino = DWG_BASE_PATH / archivo.filename
    with destino.open("wb") as f:
        shutil.copyfileobj(archivo.file, f)

    ruta_servidor = str(destino.resolve())

    async with conn.transaction():
        # Desactivar DWGs anteriores del mismo ítem (el trigger lo hace,
        # pero hacemos el UPDATE explícito para claridad)
        await conn.execute(
            "UPDATE archivos_dwg SET vigente = FALSE WHERE item_id = $1",
            item_id
        )
        dwg_id = await conn.fetchval(
            """INSERT INTO archivos_dwg (item_id, nombre_archivo, ruta_servidor, subido_por)
               VALUES ($1, $2, $3, $4) RETURNING id""",
            item_id, archivo.filename, ruta_servidor, subido_por
        )

    return {
        "id":             dwg_id,
        "nombre_archivo": archivo.filename,
        "ruta_servidor":  ruta_servidor,
    }


@app.get(
    "/items/{item_id}/dwg",
    tags=["Planos DWG"],
    summary="Lista todos los planos DWG de un ítem (historial)"
)
async def listar_dwg(
    item_id: uuid.UUID,
    conn:    asyncpg.Connection = Depends(get_db)
):
    rows = await conn.fetch(
        """SELECT id, nombre_archivo, ruta_servidor, subido_por, subido_en, vigente
           FROM archivos_dwg WHERE item_id = $1 ORDER BY subido_en DESC""",
        item_id
    )
    return [dict(r) for r in rows]


# ─── ENDPOINTS: Operaciones ───────────────────────────────────────────────────

@app.put(
    "/items/{item_id}/operaciones",
    tags=["Operaciones"],
    summary="Reemplaza todas las operaciones de un ítem"
)
async def set_operaciones(
    item_id:    uuid.UUID,
    operaciones: list[OperacionIn],
    conn:        asyncpg.Connection = Depends(get_db)
):
    if len(operaciones) > MAX_OPERACIONES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Máximo {MAX_OPERACIONES} operaciones por ítem"
        )

    item = await conn.fetchrow("SELECT * FROM items_bom WHERE id = $1", item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Ítem no encontrado")

    conjunto = await conn.fetchrow(
        "SELECT * FROM conjuntos WHERE id = $1", item["conjunto_id"]
    )
    await _assert_editable(conjunto)

    async with conn.transaction():
        await conn.execute(
            "DELETE FROM operaciones_item WHERE item_id = $1", item_id
        )
        for op in operaciones:
            await conn.execute(
                """INSERT INTO operaciones_item
                   (item_id, orden, proceso, tipo_proceso, proveedor)
                   VALUES ($1, $2, $3, $4, $5)""",
                item_id, op.orden, op.proceso, op.tipo_proceso, op.proveedor
            )

    return {"ok": True, "operaciones_guardadas": len(operaciones)}


# ─── ENDPOINT: Exportar BOM aprobada (para módulo de producción) ──────────────

@app.get(
    "/conjuntos/{conjunto_id}/exportar",
    tags=["Integración"],
    summary="Exporta la BOM completa de un conjunto aprobado (contrato con producción)"
)
async def exportar_bom(
    conjunto_id: uuid.UUID,
    conn:        asyncpg.Connection = Depends(get_db)
):
    conjunto = await _conjunto_o_404(conn, conjunto_id)
    if conjunto["estado"] != EstadoConjunto.APROBADO:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Solo se pueden exportar conjuntos aprobados"
        )

    rows = await conn.fetch(
        "SELECT * FROM v_bom_completa WHERE conjunto_id = $1",
        conjunto_id
    )

    items_out = []
    for row in rows:
        ops = await conn.fetch(
            "SELECT orden, proceso, tipo_proceso, proveedor FROM operaciones_item WHERE item_id = $1 ORDER BY orden",
            row["item_id"]
        )
        items_out.append({
            **{k: v for k, v in dict(row).items() if k != "columnas_extra"},
            "columnas_extra": dict(row["columnas_extra"]) if row["columnas_extra"] else {},
            "operaciones": [dict(o) for o in ops],
        })

    return {
        "conjunto_id":   str(conjunto_id),
        "codigo":        conjunto["codigo"],
        "version":       conjunto["version"],
        "aprobado_en":   conjunto["aprobado_en"].isoformat() if conjunto["aprobado_en"] else None,
        "exportado_en":  datetime.utcnow().isoformat(),
        "items":         items_out,
    }


# ─── Helpers privados ─────────────────────────────────────────────────────────

async def _insertar_items(
    conn: asyncpg.Connection,
    conjunto_id: uuid.UUID,
    items: list[ItemBomIn]
):
    import json
    for item in items:
        item_id = await conn.fetchval(
            """INSERT INTO items_bom
               (conjunto_id, numero_item, codigo_pieza, descripcion,
                cantidad, tipo, material, peso, columnas_extra)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id""",
            conjunto_id,
            item.numero_item,
            item.codigo_pieza,
            item.descripcion,
            item.cantidad,
            item.tipo,
            item.material,
            item.peso,
            json.dumps(item.columnas_extra),
        )
        for op in item.operaciones:
            await conn.execute(
                """INSERT INTO operaciones_item
                   (item_id, orden, proceso, tipo_proceso, proveedor)
                   VALUES ($1,$2,$3,$4,$5)""",
                item_id, op.orden, op.proceso, op.tipo_proceso, op.proveedor
            )


async def _fetch_conjunto_out(
    conn: asyncpg.Connection,
    conjunto_id: uuid.UUID
) -> dict:
    row = await conn.fetchrow(
        """SELECT c.*, COUNT(i.id) AS total_items
           FROM conjuntos c
           LEFT JOIN items_bom i ON i.conjunto_id = c.id
           WHERE c.id = $1
           GROUP BY c.id""",
        conjunto_id
    )
    return dict(row)
