"""
cassandra_schema_migration.py
──────────────────────────────
Crea las dos tablas nuevas en el keyspace 'opensky':
 
  flight_events  — despegues y aterrizajes detectados por Spark
  airports       — catálogo de aeropuertos con posición geográfica
 
Ejecutar UNA sola vez antes de correr el pipeline actualizado:
    python setup/cassandra_schema_migration.py
"""
 
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, RoundRobinPolicy
 
# ── Config ────────────────────────────────────────────────────────────────────
# localhost = Cassandra corre en Docker en esta misma máquina (puerto 9041 = node-1)
# En producción distribuida: cambiar a la IP WireGuard de la máquina de Cassandra
CASSANDRA_NODE_IPS = ["localhost"]
CASSANDRA_PORT     = 9041
CASSANDRA_USER     = "cassandra"
CASSANDRA_PASSWORD = "cassandra"
CASSANDRA_KEYSPACE = "opensky"
 
 
def main():
    auth = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
    cluster = Cluster(
        contact_points=CASSANDRA_NODE_IPS,
        port=CASSANDRA_PORT,
        auth_provider=auth,
        load_balancing_policy=RoundRobinPolicy(),
        default_retry_policy=RetryPolicy(),
    )
 
    # Conectar SIN keyspace primero (no puede conectarse a uno que no existe)
    session = cluster.connect()
    print(f"Conectado a: {cluster.metadata.cluster_name}")
 
    # Crear el keyspace si no existe
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS opensky
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    print("✅ Keyspace 'opensky' listo.")
 
    # Ahora sí seleccionar el keyspace
    session.set_keyspace(CASSANDRA_KEYSPACE)
 
    # ── Tabla 1: flight_events ─────────────────────────────────────────────────
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
 
    # ── Tabla 2: airports ──────────────────────────────────────────────────────
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
            type            text
        )
    """)
    print("✅ Tabla 'airports' lista.")
 
    # ── Índice secundario por país ─────────────────────────────────────────────
    session.execute("""
        CREATE INDEX IF NOT EXISTS airports_country_idx
        ON airports (country)
    """)
    print("✅ Índice airports_country_idx creado.")
 
    cluster.shutdown()
    print("\nMigración completada. Siguiente paso: uv run python setup/load_airports.py")
 
 
if __name__ == "__main__":
    main()