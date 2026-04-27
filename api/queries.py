"""
queries.py
──────────
Las 11 consultas analíticas del notebook opensky_neo4j_queries,
adaptadas como funciones Python que retornan listas de dicts (JSON-serializable).

Cada función corresponde a un endpoint de la API.
El driver Neo4j se inicializa una sola vez al importar el módulo.
"""

import logging
from contextlib import contextmanager
from typing import Any

from neo4j import GraphDatabase, exceptions as neo4j_exc
import pandas as pd

logger = logging.getLogger(__name__)

# ── Config — cambiar IPs cuando pasen a distribuido ──────────────────────────
NEO4J_URI = "bolt://10.15.20.X:7687"   # reemplaza X con la IP real
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"
NEO4J_DATABASE = "neo4j"

NEAR_RADIUS_KM = 50

# ── Driver (singleton) ────────────────────────────────────────────────────────
_driver = None


def get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        _driver.verify_connectivity()
        logger.info("Driver Neo4j inicializado.")
    return _driver


def close_driver():
    global _driver
    if _driver:
        _driver.close()
        _driver = None
        logger.info("Driver Neo4j cerrado.")


@contextmanager
def _session():
    driver = get_driver()
    with driver.session(database=NEO4J_DATABASE) as session:
        yield session


def _run(cypher: str, **params) -> list[dict[str, Any]]:
    """Ejecuta una query Cypher y retorna lista de dicts."""
    try:
        with _session() as s:
            result = s.run(cypher, **params)
            return [dict(record) for record in result]
    except neo4j_exc.ServiceUnavailable as e:
        logger.error(f"Neo4j no disponible: {e}")
        raise
    except neo4j_exc.CypherSyntaxError as e:
        logger.error(f"Error de sintaxis Cypher: {e}")
        raise


# ── Q1 — Top países con más aeronaves ────────────────────────────────────────
def q1_top_countries(limit: int = 10) -> list[dict]:
    """Top N países por cantidad de aeronaves registradas."""
    return _run("""
        MATCH (c:Country)-[:OPERATES]->(a:Aircraft)
        RETURN c.name AS country, count(a) AS aircraft_count
        ORDER BY aircraft_count DESC
        LIMIT $limit
    """, limit=limit)


# ── Q2 — Aeronaves con mayor velocidad promedio ───────────────────────────────
def q2_top_speed(limit: int = 10, min_snapshots: int = 3) -> list[dict]:
    """Top N aeronaves por velocidad promedio en vuelo (m/s y km/h)."""
    return _run("""
        MATCH (a:Aircraft)-[r:SNAPSHOT]->(s:Snapshot)
        WHERE r.on_ground = false AND r.velocity IS NOT NULL
        WITH a, avg(r.velocity) AS avg_vel, count(r) AS snapshots
        WHERE snapshots >= $min_snapshots
        RETURN a.icao24       AS icao24,
               a.callsign     AS callsign,
               round(avg_vel, 2)       AS avg_velocity_ms,
               round(avg_vel * 3.6, 2) AS avg_velocity_kmh,
               snapshots
        ORDER BY avg_velocity_ms DESC
        LIMIT $limit
    """, limit=limit, min_snapshots=min_snapshots)


# ── Q3 — Hub de proximidad ────────────────────────────────────────────────────
def q3_proximity_hub(limit: int = 10) -> list[dict]:
    """Aeronaves con más vecinas a ≤50 km simultáneamente (hubs de tráfico)."""
    return _run("""
        MATCH (a:Aircraft)-[r:NEAR]-(b:Aircraft)
        WITH a, count(DISTINCT b) AS near_count, avg(r.dist_km) AS avg_dist
        RETURN a.icao24           AS icao24,
               a.callsign         AS callsign,
               near_count,
               round(avg_dist, 2) AS avg_dist_km
        ORDER BY near_count DESC
        LIMIT $limit
    """, limit=limit)


# ── Q4 — Trayectoria temporal de una aeronave ─────────────────────────────────
def q4_aircraft_trajectory(icao24: str) -> list[dict]:
    """
    Serie temporal de snapshots para un avión específico.
    Incluye altitud, velocidad, tasa vertical y posición.
    """
    return _run("""
        MATCH (a:Aircraft {icao24: $icao24})-[r:SNAPSHOT]->()
        WHERE r.baro_altitude IS NOT NULL
        RETURN r.snapshot_time  AS timestamp,
               r.baro_altitude  AS altitude_m,
               r.velocity       AS velocity_ms,
               r.velocity_kmh   AS velocity_kmh,
               r.vertical_rate  AS vertical_rate_ms,
               r.latitude       AS latitude,
               r.longitude      AS longitude,
               r.on_ground      AS on_ground
        ORDER BY r.snapshot_time ASC
    """, icao24=icao24)


# ── Q4-helper — Avión con más snapshots (para usar sin parámetro) ─────────────
def q4_most_tracked_aircraft() -> dict:
    """Retorna el icao24 del avión con más snapshots registrados."""
    results = _run("""
        MATCH (a:Aircraft)-[r:SNAPSHOT]->()
        RETURN a.icao24 AS icao24, count(r) AS snapshots
        ORDER BY snapshots DESC
        LIMIT 1
    """)
    return results[0] if results else {}


# ── Q5 — Interacciones entre países ──────────────────────────────────────────
def q5_country_interactions(limit: int = 15) -> list[dict]:
    """Pares de países cuyas aeronaves han estado a ≤50 km entre sí."""
    return _run("""
        MATCH (c1:Country)-[:OPERATES]->(a1:Aircraft)-[:NEAR]-(a2:Aircraft)<-[:OPERATES]-(c2:Country)
        WHERE c1 <> c2
        WITH c1.name AS country_a, c2.name AS country_b, count(*) AS interactions
        RETURN country_a, country_b, interactions
        ORDER BY interactions DESC
        LIMIT $limit
    """, limit=limit)


# ── Q6 — Distribución por fuente de posición ─────────────────────────────────
def q6_position_sources() -> list[dict]:
    """Distribución de aeronaves por tecnología de rastreo (ADS-B, MLAT, etc.)."""
    return _run("""
        MATCH (a:Aircraft)
        RETURN a.position_source_label AS source_label,
               count(a) AS aircraft_count
        ORDER BY aircraft_count DESC
    """)


# ── Q7 — Hotspots de salida ───────────────────────────────────────────────────
def q7_departure_hotspots(limit: int = 20) -> list[dict]:
    """Aeropuertos con más despegues detectados (confianza HIGH o MEDIUM)."""
    return _run("""
        MATCH (a:Aircraft)-[r:DEPARTED_FROM]->(ap:Airport)
        WHERE r.confidence IN ['HIGH', 'MEDIUM']
        RETURN ap.icao            AS airport_icao,
               ap.iata            AS airport_iata,
               ap.name            AS airport_name,
               ap.city            AS city,
               ap.country         AS country,
               count(r)           AS departures,
               count(DISTINCT a)  AS unique_aircraft
        ORDER BY departures DESC
        LIMIT $limit
    """, limit=limit)


# ── Q8 — Hotspots de llegada ──────────────────────────────────────────────────
def q8_arrival_hotspots(limit: int = 20) -> list[dict]:
    """Aeropuertos con más aterrizajes detectados (confianza HIGH o MEDIUM)."""
    return _run("""
        MATCH (a:Aircraft)-[r:ARRIVED_AT]->(ap:Airport)
        WHERE r.confidence IN ['HIGH', 'MEDIUM']
        RETURN ap.icao            AS airport_icao,
               ap.iata            AS airport_iata,
               ap.name            AS airport_name,
               ap.city            AS city,
               ap.country         AS country,
               count(r)           AS arrivals,
               count(DISTINCT a)  AS unique_aircraft
        ORDER BY arrivals DESC
        LIMIT $limit
    """, limit=limit)


# ── Q9 — Rutas más frecuentes ─────────────────────────────────────────────────
def q9_top_routes(limit: int = 25) -> list[dict]:
    """
    Pares origen→destino más frecuentes.
    Solo cuenta vuelos donde el despegue ocurrió antes del aterrizaje.
    """
    return _run("""
        MATCH (a:Aircraft)-[dep:DEPARTED_FROM]->(origin:Airport),
              (a)-[arr:ARRIVED_AT]->(dest:Airport)
        WHERE dep.event_time < arr.event_time
          AND dep.confidence IN ['HIGH', 'MEDIUM']
          AND arr.confidence IN ['HIGH', 'MEDIUM']
          AND origin <> dest
        WITH origin.icao  AS origin_icao,
             origin.iata  AS origin_iata,
             origin.city  AS origin_city,
             dest.icao    AS dest_icao,
             dest.iata    AS dest_iata,
             dest.city    AS dest_city,
             count(*)     AS flights
        RETURN origin_icao, origin_iata, origin_city,
               dest_icao,   dest_iata,   dest_city,
               flights
        ORDER BY flights DESC
        LIMIT $limit
    """, limit=limit)


# ── Q10 — Tráfico neto por aeropuerto ────────────────────────────────────────
def q10_net_traffic(limit: int = 30) -> list[dict]:
    """
    Salidas − llegadas por aeropuerto.
    Positivo = más salidas (emisor). Negativo = más llegadas (hub receptor).
    """
    return _run("""
        MATCH (ap:Airport)
        OPTIONAL MATCH (ap)<-[dep:DEPARTED_FROM]-(:Aircraft)
            WHERE dep.confidence IN ['HIGH', 'MEDIUM']
        OPTIONAL MATCH (ap)<-[arr:ARRIVED_AT]-(:Aircraft)
            WHERE arr.confidence IN ['HIGH', 'MEDIUM']
        WITH ap,
             count(DISTINCT dep) AS departures,
             count(DISTINCT arr) AS arrivals
        WHERE departures + arrivals > 0
        RETURN ap.icao    AS icao,
               ap.iata    AS iata,
               ap.name    AS name,
               ap.country AS country,
               departures,
               arrivals,
               departures - arrivals AS net_flow
        ORDER BY departures + arrivals DESC
        LIMIT $limit
    """, limit=limit)


# ── Q11 — Historial de un avión ───────────────────────────────────────────────
def q11_aircraft_history(icao24: str) -> list[dict]:
    """
    Secuencia cronológica de aeropuertos visitados por un avión.
    Incluye tipo de evento (DEPARTED_FROM / ARRIVED_AT), confianza y distancia.
    """
    return _run("""
        MATCH (a:Aircraft {icao24: $icao24})-[r:DEPARTED_FROM|ARRIVED_AT]->(ap:Airport)
        RETURN type(r)      AS event_type,
               r.event_time AS event_time,
               ap.icao      AS airport_icao,
               ap.iata      AS airport_iata,
               ap.name      AS airport_name,
               ap.city      AS city,
               ap.country   AS country,
               r.confidence AS confidence,
               r.dist_km    AS dist_km
        ORDER BY event_time ASC
    """, icao24=icao24)


# ── Verificación del grafo ────────────────────────────────────────────────────
def graph_stats() -> dict[str, int]:
    """Conteos de nodos y relaciones en el grafo. Útil para el endpoint /health."""
    queries = {
        "aircraft":      "MATCH (a:Aircraft) RETURN count(a) AS n",
        "countries":     "MATCH (c:Country) RETURN count(c) AS n",
        "airports":      "MATCH (a:Airport) RETURN count(a) AS n",
        "snapshots":     "MATCH (s:Snapshot) RETURN count(s) AS n",
        "operates":      "MATCH ()-[r:OPERATES]->() RETURN count(r) AS n",
        "near":          "MATCH ()-[r:NEAR]-() RETURN count(r)/2 AS n",
        "departed_from": "MATCH ()-[r:DEPARTED_FROM]->() RETURN count(r) AS n",
        "arrived_at":    "MATCH ()-[r:ARRIVED_AT]->() RETURN count(r) AS n",
    }
    stats = {}
    with _session() as s:
        for key, cypher in queries.items():
            stats[key] = s.run(cypher).single()["n"]
    return stats
