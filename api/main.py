"""
main.py
───────
OpenSky Analytics API — FastAPI

Arrancar desde la carpeta api:
    uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload

Documentación interactiva:
    http://localhost:8000/docs

Probar con curl:
    curl -H "X-API-Key: admin-key-123" http://localhost:8000/analytics/top-countries

Documentación interactiva (Swagger):
    http://localhost:8000/docs

Autenticación:
    Usar header:
        X-API-Key: admin-key-123
        X-API-Key: analyst-key-123
        X-API-Key: viewer-key-123
"""

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

# Asegura que tanto api/ (para auth, queries) como la raíz (para config)
# estén en el path, sin importar desde dónde se lance uvicorn.
_API_DIR  = Path(__file__).parent
_ROOT_DIR = _API_DIR.parent
for _p in [str(_API_DIR), str(_ROOT_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from auth import list_users, verify_key, require_roles
import queries as q


# ═════════════════════════════════════════════════════════════════════════════
# ROLES
# ═════════════════════════════════════════════════════════════════════════════

ADMIN_ONLY = ["admin"]
ANALYTICS_ROLES = ["admin", "analyst"]
VIEWER_ROLES = ["admin", "analyst", "viewer"]


# ═════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)

logger = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# LIFESPAN — init / teardown del driver Neo4j
# ═════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Iniciando API — conectando a Neo4j...")
    try:
        q.get_driver()
        logger.info("Neo4j OK.")
    except Exception as e:
        logger.error(f"No se pudo conectar a Neo4j al arrancar: {e}")

    yield

    logger.info("Cerrando API — liberando driver Neo4j...")
    q.close_driver()


# ═════════════════════════════════════════════════════════════════════════════
# APP
# ═════════════════════════════════════════════════════════════════════════════

app = FastAPI(
    title="OpenSky Analytics API",
    description=(
        "API REST sobre el grafo Neo4j del pipeline "
        "OpenSky → Cassandra → Spark → Neo4j.\n\n"
        "**Autenticación:** header `X-API-Key` con una llave registrada en Cassandra.\n\n"
        "**Roles:** `admin`, `analyst`, `viewer`."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)


# CORS — permite consumir la API desde navegador
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["X-API-Key"],
)


# ═════════════════════════════════════════════════════════════════════════════
# HANDLER GLOBAL DE ERRORES
# ═════════════════════════════════════════════════════════════════════════════

@app.exception_handler(Exception)
async def generic_handler(request, exc):
    logger.error(f"Error no manejado: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Error interno. Revisa que Neo4j esté disponible."},
    )


# ═════════════════════════════════════════════════════════════════════════════
# ENDPOINTS PÚBLICOS
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/", tags=["Sistema"], summary="Raíz")
def root():
    """Verificación rápida de que la API está corriendo."""
    return {
        "status": "ok",
        "api": "OpenSky Analytics",
        "version": "1.0.0",
        "auth": "Use header X-API-Key for protected endpoints",
    }


@app.get("/health", tags=["Sistema"], summary="Estado del sistema")
def health():
    """
    Verifica conectividad con Neo4j y retorna conteos del grafo.
    No requiere autenticación — útil para monitoreo.
    """
    try:
        stats = q.graph_stats()
        return {
            "status": "ok",
            "neo4j": "connected",
            "graph": stats,
        }
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "error",
                "neo4j": "unreachable",
                "detail": str(e),
            },
        )


# ═════════════════════════════════════════════════════════════════════════════
# ENDPOINTS DE AUTENTICACIÓN / PRUEBA DE ROLES
# ═════════════════════════════════════════════════════════════════════════════

@app.get(
    "/health-secure",
    tags=["Sistema"],
    summary="Estado seguro — requiere API key válida",
)
def health_secure(user=Depends(verify_key)):
    """
    Endpoint de prueba para validar que la API key existe,
    está activa y se puede consultar en Cassandra.
    """
    return {
        "status": "ok",
        "message": "API key válida",
        "user": user,
    }


@app.get(
    "/viewer/protected",
    tags=["Acceso"],
    summary="Endpoint protegido para viewer, analyst y admin",
)
def viewer_protected(user=Depends(require_roles(VIEWER_ROLES))):
    return {
        "message": "Acceso permitido a endpoint de lectura",
        "user": user,
    }


@app.get(
    "/analytics/protected",
    tags=["Acceso"],
    summary="Endpoint protegido para analyst y admin",
)
def analytics_protected(user=Depends(require_roles(ANALYTICS_ROLES))):
    return {
        "message": "Acceso permitido a análisis",
        "user": user,
    }


# ═════════════════════════════════════════════════════════════════════════════
# ENDPOINTS ANALÍTICOS — requieren rol admin o analyst
# ═════════════════════════════════════════════════════════════════════════════

@app.get(
    "/analytics/top-countries",
    tags=["Flota"],
    summary="Q1 — Top países por flota",
)
def top_countries(
    limit: int = Query(10, ge=1, le=100, description="Número de resultados"),
    user=Depends(require_roles(ANALYTICS_ROLES)),
):
    """
    Top N países ordenados por cantidad de aeronaves registradas.

    Relación consultada: `(Country)-[:OPERATES]->(Aircraft)`.
    """
    try:
        return q.q1_top_countries(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get(
    "/analytics/top-speed",
    tags=["Vuelo"],
    summary="Q2 — Aeronaves más rápidas",
)
def top_speed(
    limit: int = Query(10, ge=1, le=100),
    min_snapshots: int = Query(3, ge=1, description="Mínimo de snapshots para incluir"),
    user=Depends(require_roles(ANALYTICS_ROLES)),
):
    """
    Top N aeronaves por velocidad promedio en vuelo.
    Excluye registros donde `on_ground = true`.
    """
    try:
        return q.q2_top_speed(limit=limit, min_snapshots=min_snapshots)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get(
    "/analytics/proximity-hubs",
    tags=["Proximidad"],
    summary="Q3 — Hubs de proximidad aérea",
)
def proximity_hubs(
    limit: int = Query(10, ge=1, le=100),
    user=Depends(require_roles(ANALYTICS_ROLES)),
):
    """
    Aeronaves con más vecinas cercanas en el mismo instante.
    """
    try:
        return q.q3_proximity_hub(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get(
    "/analytics/trajectory/{icao24}",
    tags=["Vuelo"],
    summary="Q4 — Trayectoria de una aeronave",
)
def aircraft_trajectory(
    icao24: str,
    user=Depends(require_roles(ANALYTICS_ROLES)),
):
    """
    Serie temporal de snapshots para el avión con código ICAO24 dado.
    """
    icao24 = icao24.lower().strip()

    try:
        data = q.q4_aircraft_trajectory(icao24=icao24)

        if not data:
            raise HTTPException(
                status_code=404,
                detail=f"No se encontraron snapshots para icao24='{icao24}'.",
            )

        return data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get(
    "/analytics/most-tracked",
    tags=["Vuelo"],
    summary="Q4-helper — Avión con más snapshots",
)
def most_tracked(user=Depends(require_roles(ANALYTICS_ROLES))):
    """
    Retorna el ICAO24 del avión con mayor cantidad de snapshots registrados.
    """
    try:
        result = q.q4_most_tracked_aircraft()

        if not result:
            raise HTTPException(404, detail="No hay snapshots en el grafo aún.")

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get(
    "/analytics/country-interactions",
    tags=["Proximidad"],
    summary="Q5 — Interacciones aéreas entre países",
)
def country_interactions(
    limit: int = Query(15, ge=1, le=100),
    user=Depends(require_roles(ANALYTICS_ROLES)),
):
    """
    Pares de países cuyas aeronaves han volado cerca entre sí.
    """
    try:
        return q.q5_country_interactions(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get(
    "/analytics/position-sources",
    tags=["Flota"],
    summary="Q6 — Distribución por fuente de posición",
)
def position_sources(user=Depends(require_roles(ANALYTICS_ROLES))):
    """
    Distribución de aeronaves por tecnología de rastreo.

    Valores posibles: ADS-B, ASTERIX, MLAT, FLARM, Unknown.
    """
    try:
        return q.q6_position_sources()
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get(
    "/analytics/departure-hotspots",
    tags=["Aeropuertos"],
    summary="Q7 — Hotspots de salida",
)
def departure_hotspots(
    limit: int = Query(20, ge=1, le=100),
    user=Depends(require_roles(ANALYTICS_ROLES)),
):
    """
    Aeropuertos con más despegues detectados.
    """
    try:
        return q.q7_departure_hotspots(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get(
    "/analytics/arrival-hotspots",
    tags=["Aeropuertos"],
    summary="Q8 — Hotspots de llegada",
)
def arrival_hotspots(
    limit: int = Query(20, ge=1, le=100),
    user=Depends(require_roles(ANALYTICS_ROLES)),
):
    """
    Aeropuertos con más aterrizajes detectados.
    """
    try:
        return q.q8_arrival_hotspots(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get(
    "/analytics/top-routes",
    tags=["Rutas"],
    summary="Q9 — Rutas más frecuentes",
)
def top_routes(
    limit: int = Query(25, ge=1, le=100),
    user=Depends(require_roles(ANALYTICS_ROLES)),
):
    """
    Pares origen → destino más frecuentes.
    """
    try:
        return q.q9_top_routes(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get(
    "/analytics/net-traffic",
    tags=["Aeropuertos"],
    summary="Q10 — Tráfico neto por aeropuerto",
)
def net_traffic(
    limit: int = Query(30, ge=1, le=100),
    user=Depends(require_roles(ANALYTICS_ROLES)),
):
    """
    Salidas menos llegadas por aeropuerto.
    """
    try:
        return q.q10_net_traffic(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.get(
    "/analytics/aircraft-history/{icao24}",
    tags=["Rutas"],
    summary="Q11 — Historial de aeropuertos de un avión",
)
def aircraft_history(
    icao24: str,
    user=Depends(require_roles(ANALYTICS_ROLES)),
):
    """
    Secuencia cronológica de aeropuertos visitados por una aeronave.
    """
    icao24 = icao24.lower().strip()

    try:
        data = q.q11_aircraft_history(icao24=icao24)

        if not data:
            raise HTTPException(
                status_code=404,
                detail=f"No hay historial de aeropuertos para icao24='{icao24}'.",
            )

        return data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ═════════════════════════════════════════════════════════════════════════════
# ADMIN — requiere rol admin
# ═════════════════════════════════════════════════════════════════════════════

@app.get(
    "/admin/users",
    tags=["Admin"],
    summary="Lista de usuarios registrados",
)
def list_registered_users(user=Depends(require_roles(ADMIN_ONLY))):
    """
    Retorna usuarios registrados en Cassandra.
    No expone las API keys.
    Solo puede consultarlo un usuario con rol admin.
    """
    return {
        "users": list_users(),
        "requested_by": user,
    }
