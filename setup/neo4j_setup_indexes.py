"""
neo4j_setup_indexes.py
───────────────────────
Crea los constraints e índices en Neo4j necesarios para el pipeline.

Ejecutar UNA sola vez antes de correr el pipeline:
    python setup/neo4j_setup_indexes.py

Qué crea:
  Constraints (unicidad + índice implícito):
    - Aircraft.icao24
    - Country.name
    - Airport.icao

  Índices adicionales (para acelerar lookups frecuentes):
    - Aircraft.callsign
    - Snapshot.snapshot_time
    - Airport.iata
    - Airport.country
"""

import logging
import sys

from neo4j import GraphDatabase, exceptions as neo4j_exc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
#NEO4J_URI = "bolt://10.15.20.X:7687"   # reemplaza X con la IP real
NEO4J_URI = "bolt://localhost:7687"  # debe decir esto
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"
NEO4J_DATABASE = "neo4j"

# ── Constraints: garantizan unicidad y crean índice implícito ─────────────────
CONSTRAINTS = [
    (
        "constraint_aircraft_icao24",
        "CREATE CONSTRAINT constraint_aircraft_icao24 IF NOT EXISTS "
        "FOR (a:Aircraft) REQUIRE a.icao24 IS UNIQUE",
    ),
    (
        "constraint_country_name",
        "CREATE CONSTRAINT constraint_country_name IF NOT EXISTS "
        "FOR (c:Country) REQUIRE c.name IS UNIQUE",
    ),
    (
        "constraint_airport_icao",
        "CREATE CONSTRAINT constraint_airport_icao IF NOT EXISTS "
        "FOR (ap:Airport) REQUIRE ap.icao IS UNIQUE",
    ),
]

# ── Índices adicionales ───────────────────────────────────────────────────────
INDEXES = [
    (
        "idx_aircraft_callsign",
        "CREATE INDEX idx_aircraft_callsign IF NOT EXISTS "
        "FOR (a:Aircraft) ON (a.callsign)",
    ),
    (
        "idx_snapshot_time",
        "CREATE INDEX idx_snapshot_time IF NOT EXISTS "
        "FOR (s:Snapshot) ON (s.snapshot_time)",
    ),
    (
        "idx_airport_iata",
        "CREATE INDEX idx_airport_iata IF NOT EXISTS "
        "FOR (ap:Airport) ON (ap.iata)",
    ),
    (
        "idx_airport_country",
        "CREATE INDEX idx_airport_country IF NOT EXISTS "
        "FOR (ap:Airport) ON (ap.country)",
    ),
]


def main():
    logger.info(f"Conectando a Neo4j en {NEO4J_URI}...")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
    except neo4j_exc.ServiceUnavailable as e:
        logger.error(f"No se pudo conectar a Neo4j: {e}")
        sys.exit(1)

    with driver.session(database=NEO4J_DATABASE) as session:

        logger.info("Creando constraints...")
        for name, cypher in CONSTRAINTS:
            try:
                session.run(cypher)
                logger.info(f"  ✅ {name}")
            except neo4j_exc.ClientError as e:
                logger.warning(f"  ⚠️  {name}: {e.message}")

        logger.info("Creando índices adicionales...")
        for name, cypher in INDEXES:
            try:
                session.run(cypher)
                logger.info(f"  ✅ {name}")
            except neo4j_exc.ClientError as e:
                logger.warning(f"  ⚠️  {name}: {e.message}")

        # Verificar lo que quedó
        logger.info("\nResumen del esquema en Neo4j:")
        result = session.run("SHOW CONSTRAINTS")
        constraints = [r.data() for r in result]
        logger.info(f"  Constraints activos: {len(constraints)}")
        for c in constraints:
            logger.info(f"    - {c.get('name')} ({c.get('type')})")

        result = session.run("SHOW INDEXES WHERE type <> 'LOOKUP'")
        indexes = [r.data() for r in result]
        logger.info(f"  Índices activos (sin lookup): {len(indexes)}")
        for idx in indexes:
            logger.info(f"    - {idx.get('name')} [{idx.get('state')}]")

    driver.close()
    logger.info("\n✅ Setup de Neo4j completado.")
    logger.info("Siguiente paso: python setup/cassandra_schema_migration.py")


if __name__ == "__main__":
    main()
