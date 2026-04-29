"""
cassandra_schema_migration.py
──────────────────────────────
Crea las tablas del keyspace 'opensky' en Cassandra.
 
Ejecutar UNA sola vez antes de correr el pipeline:
    uv run python setup/cassandra_schema_migration.py
 
Configuración via .env o variables de entorno:
    CASSANDRA_HOST  — IP de Cassandra (default: localhost)
    CASSANDRA_PORT  — Puerto CQL     (default: 9041)
"""
 
import sys
from pathlib import Path
 
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    CASSANDRA_NODE_IPS, CASSANDRA_PORT,
    CASSANDRA_USER, CASSANDRA_PASSWORD, CASSANDRA_KEYSPACE,
    print_config,
)
 
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, RoundRobinPolicy
 
 
def main():
    print_config()
 
    auth = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
    cluster = Cluster(
        contact_points=CASSANDRA_NODE_IPS,
        port=CASSANDRA_PORT,
        auth_provider=auth,
        load_balancing_policy=RoundRobinPolicy(),
        default_retry_policy=RetryPolicy(),
    )
 
    session = cluster.connect()
    print(f"Conectado a: {cluster.metadata.cluster_name}")
 
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    print(f"✅ Keyspace '{CASSANDRA_KEYSPACE}' listo.")
 
    session.set_keyspace(CASSANDRA_KEYSPACE)
 
    session.execute("""
        CREATE TABLE IF NOT EXISTS flight_events (
            icao24          text,
            event_time      timestamp,
            event_type      text,
            latitude        double,
            longitude       double,
            airport_icao    text,
            airport_name    text,
            confidence      text,
            gap_seconds     int,
            batch_id        text,
            PRIMARY KEY (icao24, event_time, event_type)
        ) WITH CLUSTERING ORDER BY (event_time DESC, event_type ASC)
          AND default_time_to_live = 2592000
    """)
    print("✅ Tabla 'flight_events' lista.")
 
    session.execute("""
        CREATE TABLE IF NOT EXISTS airports (
            icao_code   text PRIMARY KEY,
            iata_code   text,
            name        text,
            city        text,
            country     text,
            latitude    double,
            longitude   double,
            altitude_ft int,
            type        text
        )
    """)
    print("✅ Tabla 'airports' lista.")
 
    session.execute("""
        CREATE INDEX IF NOT EXISTS airports_country_idx ON airports (country)
    """)
    print("✅ Índice airports_country_idx creado.")
 
    cluster.shutdown()
    print("\nMigración completada. Siguiente: uv run python setup/load_airports.py")
 
 
if __name__ == "__main__":
    main()
 