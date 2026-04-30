"""
load_airports.py
─────────────────
Descarga el catálogo público de aeropuertos de OurAirports.com
y lo carga en la tabla 'airports' de Cassandra.
 
Ejecutar UNA sola vez (o cuando quieras actualizar el catálogo):
    uv run python setup/load_airports.py
 
Configuración via .env o variables de entorno:
    CASSANDRA_HOST — IP de Cassandra (default: localhost)
    CASSANDRA_PORT — Puerto CQL     (default: 9041)
"""
 
import csv
import io
import sys
import time
from pathlib import Path
 
import requests
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, RoundRobinPolicy
from cassandra.query import BatchStatement
 
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    CASSANDRA_NODE_IPS, CASSANDRA_PORT,
    CASSANDRA_USER, CASSANDRA_PASSWORD, CASSANDRA_KEYSPACE,
    print_config,
)
 
OURAIRPORTS_URL = "https://ourairports.com/data/airports.csv"
AIRPORT_TYPES   = {"large_airport", "medium_airport"}
BATCH_SIZE      = 10   # bajo para no superar el límite de 5KB por batch
 
 
def download_airports() -> list[dict]:
    print(f"Descargando catálogo desde {OURAIRPORTS_URL} ...")
    resp = requests.get(OURAIRPORTS_URL, timeout=60)
    resp.raise_for_status()
 
    reader  = csv.DictReader(io.StringIO(resp.text))
    records = []
    for row in reader:
        if row.get("type") not in AIRPORT_TYPES:
            continue
        icao = row.get("ident", "").strip()
        if not icao or len(icao) != 4:
            continue
        try:
            lat = float(row["latitude_deg"])
            lon = float(row["longitude_deg"])
        except (ValueError, KeyError):
            continue

        # ── INICIO FILTRO REGIONAL (EUROPA) ──────────────────────────────────
        # Para volver a descargar TODOS los aeropuertos a nivel global,
        # simplemente comenta (pon un #) o borra estas dos líneas de abajo:
        if not (34.0 <= lat <= 72.0 and -25.0 <= lon <= 45.0):
            continue
        # ── FIN FILTRO REGIONAL ──────────────────────────────────────────────
        
        records.append({
            "icao_code":   icao,
            "iata_code":   row.get("iata_code", "").strip() or None,
            "name":        row.get("name", "").strip() or None,
            "city":        row.get("municipality", "").strip() or None,
            "country":     row.get("iso_country", "").strip() or None,
            "latitude":    lat,
            "longitude":   lon,
            "altitude_ft": int(float(row["elevation_ft"])) if row.get("elevation_ft") else None,
            "type":        row.get("type", "").strip() or None,
        })
 
    print(f"Aeropuertos filtrados ({'/'.join(AIRPORT_TYPES)}): {len(records):,}")
    return records
 
 
def load_to_cassandra(records: list[dict]):
    auth = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
    cluster = Cluster(
        contact_points=CASSANDRA_NODE_IPS,
        port=CASSANDRA_PORT,
        auth_provider=auth,
        load_balancing_policy=RoundRobinPolicy(),
        default_retry_policy=RetryPolicy(),
    )
    session = cluster.connect(CASSANDRA_KEYSPACE)
 
    prepared = session.prepare("""
        INSERT INTO airports (
            icao_code, iata_code, name, city, country,
            latitude, longitude, altitude_ft, type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM
 
    inserted = 0
    start    = time.time()
 
    for i in range(0, len(records), BATCH_SIZE):
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        for r in records[i : i + BATCH_SIZE]:
            batch.add(prepared, (
                r["icao_code"], r["iata_code"], r["name"], r["city"], r["country"],
                r["latitude"],  r["longitude"], r["altitude_ft"], r["type"],
            ))
        session.execute(batch)
        inserted += len(records[i : i + BATCH_SIZE])
        if inserted % 500 == 0 or inserted == len(records):
            print(f"  {inserted:>5} / {len(records)} insertados...")
 
    elapsed = time.time() - start
    cluster.shutdown()
    print(f"✅ {inserted:,} aeropuertos cargados en {elapsed:.1f}s")
 
 
def main():
    print_config()
    records = download_airports()
    if not records:
        print("Sin registros — abortando.")
        return
    load_to_cassandra(records)
    print("\nListo. Siguiente: uv run python setup/neo4j_setup_indexes.py")
 
 
if __name__ == "__main__":
    main()
 