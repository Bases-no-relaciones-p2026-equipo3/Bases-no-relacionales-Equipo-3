"""
cassandra_schema_migration.py
──────────────────────────────
Crea las dos tablas nuevas en el keyspace 'opensky':

  flight_events  — despegues y aterrizajes detectados por Spark
  airports       — catálogo de aeropuertos con posición geográfica

Ejecutar UNA sola vez antes de correr el pipeline actualizado:
    python cassandra_schema_migration.py
"""

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, RoundRobinPolicy

# ── Config (mismos valores que el resto del pipeline) ─────────────────────────
CASSANDRA_NODE_IPS = ["10.15.20.18", "10.15.20.18", "10.15.20.18"]
CASSANDRA_PORT     = 9041
CASSANDRA_USER     = "cassandra"
CASSANDRA_PASSWORD = "cassandra"
CASSANDRA_KEYSPACE = "opensky"


def main():
    auth    = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
    cluster = Cluster(
        contact_points=CASSANDRA_NODE_IPS,
        port=CASSANDRA_PORT,
        auth_provider=auth,
        load_balancing_policy=RoundRobinPolicy(),
        default_retry_policy=RetryPolicy(),
    )
    session = cluster.connect(CASSANDRA_KEYSPACE)
    print(f"Conectado a: {cluster.metadata.cluster_name}")

    # ── Tabla 1: flight_events ─────────────────────────────────────────────────
    # Registra cada transición on_ground detectada por Spark.
    #
    # Partition key : icao24           — todos los eventos de un avión juntos
    # Clustering key: event_time DESC  — el más reciente primero
    #                 event_type       — diferencia TAKEOFF vs LANDING en mismo ts
    #
    # airport_icao  — código ICAO del aeropuerto más cercano (puede ser NULL si
    #                 no hay aeropuerto conocido a menos de AIRPORT_RADIUS_KM)
    # confidence    — 'HIGH' si el avión estaba <2 km del aeropuerto,
    #                 'MEDIUM' si estaba entre 2-10 km, 'LOW' si más lejos
    # gap_seconds   — segundos entre el snapshot anterior y este; valores altos
    #                 indican que el evento se infirió con menos certeza
    session.execute("""
        CREATE TABLE IF NOT EXISTS flight_events (
            icao24          text,
            event_time      timestamp,
            event_type      text,        -- 'TAKEOFF' | 'LANDING'
            latitude        double,
            longitude       double,
            airport_icao    text,        -- puede ser NULL
            airport_name    text,        -- puede ser NULL
            confidence      text,        -- 'HIGH' | 'MEDIUM' | 'LOW'
            gap_seconds     int,         -- segundos desde snapshot anterior
            batch_id        text,        -- identificador del batch de Spark
            PRIMARY KEY (icao24, event_time, event_type)
        ) WITH CLUSTERING ORDER BY (event_time DESC, event_type ASC)
          AND default_time_to_live = 2592000
    """)
    print("✅ Tabla 'flight_events' lista.")

    # ── Tabla 2: airports ──────────────────────────────────────────────────────
    # Catálogo estático de aeropuertos cargado desde OurAirports.
    # Se carga una sola vez con load_airports.py y no cambia entre batches.
    #
    # Partition key: icao_code (único, búsquedas por código)
    # No necesita clustering key — un aeropuerto = una fila.
    session.execute("""
        CREATE TABLE IF NOT EXISTS airports (
            icao_code       text PRIMARY KEY,
            iata_code       text,
            name            text,
            city            text,
            country         text,
            latitude        double,
            longitude       double,
            altitude_ft     int,
            type            text         -- 'large_airport' | 'medium_airport' | etc.
        )
    """)
    print("✅ Tabla 'airports' lista.")

    # ── Índices secundarios para consultas por país ────────────────────────────
    # Cassandra no permite filtrar por columnas que no son clave sin ALLOW FILTERING.
    # Un índice secundario en 'country' permite queries como:
    #   SELECT * FROM airports WHERE country = 'Mexico'
    session.execute("""
        CREATE INDEX IF NOT EXISTS airports_country_idx
        ON airports (country)
    """)
    print("✅ Índice airports_country_idx creado.")

    cluster.shutdown()
    print("\nMigración completada. Siguiente paso: ejecutar load_airports.py")


if __name__ == "__main__":
    main()
