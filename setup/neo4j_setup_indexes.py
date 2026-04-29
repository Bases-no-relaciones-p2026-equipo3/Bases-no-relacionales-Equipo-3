"""
neo4j_setup_indexes.py
───────────────────────
Crea constraints e índices en Neo4j.
 
Ejecutar UNA sola vez antes de correr el pipeline:
    uv run python setup/neo4j_setup_indexes.py
 
Configuración via .env o variables de entorno:
    NEO4J_HOST — IP de Neo4j (default: localhost)
    NEO4J_PORT — Puerto Bolt (default: 7687)
"""
 
import logging
import sys
from pathlib import Path
 
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE, print_config
 
from neo4j import GraphDatabase, exceptions as neo4j_exc
 
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s — %(message)s")
logger = logging.getLogger(__name__)
 
CONSTRAINTS = [
    ("constraint_aircraft_icao24",
     "CREATE CONSTRAINT constraint_aircraft_icao24 IF NOT EXISTS FOR (a:Aircraft) REQUIRE a.icao24 IS UNIQUE"),
    ("constraint_country_name",
     "CREATE CONSTRAINT constraint_country_name IF NOT EXISTS FOR (c:Country) REQUIRE c.name IS UNIQUE"),
    ("constraint_airport_icao",
     "CREATE CONSTRAINT constraint_airport_icao IF NOT EXISTS FOR (ap:Airport) REQUIRE ap.icao IS UNIQUE"),
]
 
INDEXES = [
    ("idx_aircraft_callsign",
     "CREATE INDEX idx_aircraft_callsign IF NOT EXISTS FOR (a:Aircraft) ON (a.callsign)"),
    ("idx_snapshot_time",
     "CREATE INDEX idx_snapshot_time IF NOT EXISTS FOR (s:Snapshot) ON (s.snapshot_time)"),
    ("idx_airport_iata",
     "CREATE INDEX idx_airport_iata IF NOT EXISTS FOR (ap:Airport) ON (ap.iata)"),
    ("idx_airport_country",
     "CREATE INDEX idx_airport_country IF NOT EXISTS FOR (ap:Airport) ON (ap.country)"),
]
 
 
def main():
    print_config()
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
 
        logger.info("Creando índices...")
        for name, cypher in INDEXES:
            try:
                session.run(cypher)
                logger.info(f"  ✅ {name}")
            except neo4j_exc.ClientError as e:
                logger.warning(f"  ⚠️  {name}: {e.message}")
 
        indexes = session.run("SHOW INDEXES WHERE type <> 'LOOKUP'").data()
        for idx in indexes:
            logger.info(f"  - {idx.get('name')} [{idx.get('state')}]")
 
    driver.close()
    logger.info("✅ Setup de Neo4j completado.")
 
 
if __name__ == "__main__":
    main()