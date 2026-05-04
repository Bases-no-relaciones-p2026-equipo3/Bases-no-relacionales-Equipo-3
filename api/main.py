"""
main.py
───────
OpenSky Analytics API — FastAPI

Arrancar (desarrollo):
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload

Arrancar (producción):
    uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2

Probar con curl (reemplaza <IP> con tu IP WireGuard):
    curl -H "X-API-Key: sk-nombre-..." http://<IP>:8000/analytics/top-countries

Documentación interactiva (Swagger):
    http://<IP>:8000/docs
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

from auth import list_users, verify_key
import queries as q

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)



# ── Lifespan (init / teardown del driver Neo4j) ───────────────────────────────
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


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="OpenSky Analytics API",
    description=(
        "API REST sobre el grafo Neo4j del pipeline OpenSky → Cassandra → Spark → Neo4j.\n\n"
        "**Autenticación:** header `X-API-Key` con tu key personal.\n\n"
        "**Todos los endpoints requieren autenticación.**"
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS — permite consumir la API desde navegador dentro del túnel WireGuard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # ajustar a IPs WireGuard si se quiere más restrictivo
    allow_methods=["GET"],
    allow_headers=["X-API-Key"],
)


# ── Handler global de errores Neo4j ───────────────────────────────────────────
@app.exception_handler(Exception)
async def generic_handler(request, exc):
    logger.error(f"Error no manejado: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Error interno. Revisa que Neo4j esté disponible."},
    )


# ═════════════════════════════════════════════════════════════════════════════
# ENDPOINTS PÚBLICOS (sin auth)
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/", tags=["Sistema"], summary="Raíz")
def root():
    """Verificación rápida de que la API está corriendo."""
    return {"status": "ok", "api": "OpenSky Analytics", "version": "1.0.0"}


@app.get("/health", tags=["Sistema"], summary="Estado del sistema")
def health():
    """
    Verifica conectividad con Neo4j y retorna conteos del grafo.
    No requiere autenticación — útil para monitoreo.
    """
    try:
        stats = q.graph_stats()
        return {"status": "ok", "neo4j": "connected", "graph": stats}
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "error", "neo4j": "unreachable", "detail": str(e)},
        )


# ═════════════════════════════════════════════════════════════════════════════
# ENDPOINTS ANALÍTICOS — requieren X-API-Key
# ═════════════════════════════════════════════════════════════════════════════

# ── Q1 ────────────────────────────────────────────────────────────────────────
@app.get(
    "/analytics/top-countries",
    tags=["Flota"],
    summary="Q1 — Top países por flota",
)
def top_countries(
    limit: int = Query(10, ge=1, le=100, description="Número de resultados"),
    user: str = Depends(verify_key),
):
    """
    Top N países ordenados por cantidad de aeronaves registradas.

    Relación consultada: `(Country)-[:OPERATES]->(Aircraft)`
    """
    try:
        return q.q1_top_countries(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── Q2 ────────────────────────────────────────────────────────────────────────
@app.get(
    "/analytics/top-speed",
    tags=["Vuelo"],
    summary="Q2 — Aeronaves más rápidas",
)
def top_speed(
    limit: int = Query(10, ge=1, le=100),
    min_snapshots: int = Query(3, ge=1, description="Mínimo de snapshots para incluir"),
    user: str = Depends(verify_key),
):
    """
    Top N aeronaves por velocidad promedio en vuelo (excluye on_ground).

    Retorna velocidad en m/s y km/h.
    """
    try:
        return q.q2_top_speed(limit=limit, min_snapshots=min_snapshots)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── Q3 ────────────────────────────────────────────────────────────────────────
@app.get(
    "/analytics/proximity-hubs",
    tags=["Proximidad"],
    summary="Q3 — Hubs de proximidad aérea",
)
def proximity_hubs(
    limit: int = Query(10, ge=1, le=100),
    user: str = Depends(verify_key),
):
    """
    Aeronaves con más vecinas a ≤50 km en el mismo instante.

    Alta densidad indica zonas de espera, aproximación o tráfico intenso.
    """
    try:
        return q.q3_proximity_hub(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── Q4 ────────────────────────────────────────────────────────────────────────
@app.get(
    "/analytics/trajectory/{icao24}",
    tags=["Vuelo"],
    summary="Q4 — Trayectoria de una aeronave",
)
def aircraft_trajectory(
    icao24: str,
    user: str = Depends(verify_key),
):
    """
    Serie temporal de snapshots para el avión con código ICAO24 dado.

    Incluye: altitud barométrica, velocidad, tasa vertical, posición GPS.

    Si no conoces el icao24, usa primero `GET /analytics/most-tracked`.
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
def most_tracked(user: str = Depends(verify_key)):
    """Retorna el icao24 del avión con mayor cantidad de snapshots registrados."""
    try:
        result = q.q4_most_tracked_aircraft()
        if not result:
            raise HTTPException(404, detail="No hay snapshots en el grafo aún.")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── Q5 ────────────────────────────────────────────────────────────────────────
@app.get(
    "/analytics/country-interactions",
    tags=["Proximidad"],
    summary="Q5 — Interacciones aéreas entre países",
)
def country_interactions(
    limit: int = Query(15, ge=1, le=100),
    user: str = Depends(verify_key),
):
    """
    Pares de países cuyas aeronaves han volado a ≤50 km entre sí.

    Útil para identificar zonas de tráfico internacional compartido.
    """
    try:
        return q.q5_country_interactions(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── Q6 ────────────────────────────────────────────────────────────────────────
@app.get(
    "/analytics/position-sources",
    tags=["Flota"],
    summary="Q6 — Distribución por fuente de posición",
)
def position_sources(user: str = Depends(verify_key)):
    """
    Distribución de aeronaves por tecnología de rastreo.

    Valores posibles: ADS-B, ASTERIX, MLAT, FLARM, Unknown.
    """
    try:
        return q.q6_position_sources()
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── Q7 ────────────────────────────────────────────────────────────────────────
@app.get(
    "/analytics/departure-hotspots",
    tags=["Aeropuertos"],
    summary="Q7 — Hotspots de salida",
)
def departure_hotspots(
    limit: int = Query(20, ge=1, le=100),
    user: str = Depends(verify_key),
):
    """
    Aeropuertos con más despegues detectados (confianza HIGH o MEDIUM).

    Solo incluye eventos donde el avión estaba a ≤15 km del aeropuerto.
    """
    try:
        return q.q7_departure_hotspots(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── Q8 ────────────────────────────────────────────────────────────────────────
@app.get(
    "/analytics/arrival-hotspots",
    tags=["Aeropuertos"],
    summary="Q8 — Hotspots de llegada",
)
def arrival_hotspots(
    limit: int = Query(20, ge=1, le=100),
    user: str = Depends(verify_key),
):
    """
    Aeropuertos con más aterrizajes detectados (confianza HIGH o MEDIUM).
    """
    try:
        return q.q8_arrival_hotspots(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── Q9 ────────────────────────────────────────────────────────────────────────
@app.get(
    "/analytics/top-routes",
    tags=["Rutas"],
    summary="Q9 — Rutas más frecuentes",
)
def top_routes(
    limit: int = Query(25, ge=1, le=100),
    user: str = Depends(verify_key),
):
    """
    Pares origen → destino más frecuentes.

    Solo cuenta vuelos donde el despegue ocurrió antes del aterrizaje
    y ambos eventos tienen confianza HIGH o MEDIUM.
    """
    try:
        return q.q9_top_routes(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── Q10 ───────────────────────────────────────────────────────────────────────
@app.get(
    "/analytics/net-traffic",
    tags=["Aeropuertos"],
    summary="Q10 — Tráfico neto por aeropuerto",
)
def net_traffic(
    limit: int = Query(30, ge=1, le=100),
    user: str = Depends(verify_key),
):
    """
    Salidas − llegadas por aeropuerto.

    - **Positivo** → más salidas que llegadas (aeropuerto emisor / origen)
    - **Negativo** → más llegadas que salidas (hub receptor / destino)
    """
    try:
        return q.q10_net_traffic(limit=limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── Q11 ───────────────────────────────────────────────────────────────────────
@app.get(
    "/analytics/aircraft-history/{icao24}",
    tags=["Rutas"],
    summary="Q11 — Historial de aeropuertos de un avión",
)
def aircraft_history(
    icao24: str,
    user: str = Depends(verify_key),
):
    """
    Secuencia cronológica de aeropuertos visitados por el avión dado.

    Incluye tipo de evento (DEPARTED_FROM / ARRIVED_AT), confianza y
    distancia al aeropuerto en el momento del evento.
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


# ── Admin (solo info, sin mutaciones) ─────────────────────────────────────────
@app.get(
    "/admin/users",
    tags=["Admin"],
    summary="Lista de usuarios registrados",
)
def list_registered_users(user: str = Depends(verify_key)):
    """
    Retorna los nombres de usuarios con API key activa.
    No expone las keys.
    """
    return {"users": list_users(), "requested_by": user}
