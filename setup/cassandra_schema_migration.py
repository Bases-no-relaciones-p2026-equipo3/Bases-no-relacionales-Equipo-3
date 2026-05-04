"""
cassandra_schema_migration.py
─────────────────────────────
Crea el keyspace y las tablas necesarias en Cassandra para el pipeline OpenSky.

Tablas:
- state_vectors: snapshots crudos de OpenSky.
- flight_events: eventos derivados por Spark.
- airports: catálogo de aeropuertos.
- api_users: usuarios/API keys para control de acceso de FastAPI.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, RoundRobinPolicy

from config import (
    CASSANDRA_NODE_IPS,
    CASSANDRA_PORT,
    CASSANDRA_USER,
    CASSANDRA_PASSWORD,
    CASSANDRA_KEYSPACE,
    print_config,
)


def main():
    print_config()

    auth = PlainTextAuthProvider(
        username=CASSANDRA_USER,
        password=CASSANDRA_PASSWORD,
    )

    cluster = Cluster(
        contact_points=CASSANDRA_NODE_IPS,
        port=CASSANDRA_PORT,
        auth_provider=auth,
        load_balancing_policy=RoundRobinPolicy(),
        default_retry_policy=RetryPolicy(),
        connect_timeout=15,
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
        CREATE TABLE IF NOT EXISTS state_vectors (
            icao24 text,
            snapshot_time timestamp,
            callsign text,
            origin_country text,
            time_position bigint,
            last_contact bigint,
            longitude double,
            latitude double,
            baro_altitude double,
            on_ground boolean,
            velocity double,
            true_track double,
            vertical_rate double,
            geo_altitude double,
            squawk text,
            spi boolean,
            position_source int,
            category int,
            PRIMARY KEY (icao24, snapshot_time)
        ) WITH CLUSTERING ORDER BY (snapshot_time DESC)
        AND default_time_to_live = 604800
    """)
    print("✅ Tabla 'state_vectors' lista.")

    session.execute("""
        CREATE TABLE IF NOT EXISTS flight_events (
            icao24 text,
            event_time timestamp,
            event_type text,
            airport_icao text,
            airport_iata text,
            airport_name text,
            country text,
            latitude double,
            longitude double,
            altitude double,
            velocity double,
            callsign text,
            PRIMARY KEY (icao24, event_time, event_type)
        ) WITH CLUSTERING ORDER BY (event_time DESC)
    """)
    print("✅ Tabla 'flight_events' lista.")

    session.execute("""
        CREATE TABLE IF NOT EXISTS airports (
            ident text PRIMARY KEY,
            type text,
            name text,
            latitude_deg double,
            longitude_deg double,
            elevation_ft int,
            continent text,
            iso_country text,
            iso_region text,
            municipality text,
            scheduled_service text,
            gps_code text,
            iata_code text,
            local_code text
        )
    """)
    print("✅ Tabla 'airports' lista.")

    session.execute("""
        CREATE TABLE IF NOT EXISTS api_users (
            api_key text PRIMARY KEY,
            username text,
            role text,
            active boolean,
            created_at timestamp
        )
    """)
    print("✅ Tabla 'api_users' lista.")

    session.execute("""
        CREATE INDEX IF NOT EXISTS airports_country_idx
        ON airports (iso_country)
    """)
    print("✅ Índice airports_country_idx creado.")

    cluster.shutdown()

    print()
    print("Migración completada. Siguiente: uv run python setup/load_airports.py")


if __name__ == "__main__":
    main()
