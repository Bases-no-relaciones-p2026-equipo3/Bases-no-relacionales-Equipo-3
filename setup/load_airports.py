"""
load_airports.py
─────────────────
Descarga el catálogo público de aeropuertos de OurAirports.com
y lo carga en la tabla 'airports' de Cassandra.

Fuente: https://ourairports.com/data/airports.csv  (~60 000 aeropuertos)

Ejecutar UNA sola vez (o cuando quieras actualizar el catálogo):
    python load_airports.py

Filtra por defecto a large_airport y medium_airport para reducir ruido.
Cambia AIRPORT_TYPES para incluir small_airport si tu región lo requiere.
"""

import csv
import io
import time

import requests
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, RoundRobinPolicy
from cassandra.query import BatchStatement

# ── Config ────────────────────────────────────────────────────────────────────
CASSANDRA_NODE_IPS = ["localhost", "localhost", "localhost"]
CASSANDRA_PORT     = 9041
CASSANDRA_USER     = "cassandra"
CASSANDRA_PASSWORD = "cassandra"
CASSANDRA_KEYSPACE = "opensky"

OURAIRPORTS_URL = "https://ourairports.com/data/airports.csv"

# Tipos de aeropuerto a incluir — excluir heliports, seaplane_base, balloonport, etc.
AIRPORT_TYPES = {"large_airport", "medium_airport"}

BATCH_SIZE = 50  # filas por batch de Cassandra


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
            continue  # solo códigos ICAO de 4 letras
        try:
            lat = float(row["latitude_deg"])
            lon = float(row["longitude_deg"])
        except (ValueError, KeyError):
            continue
        records.append({
            "icao_code":    icao,
            "iata_code":    row.get("iata_code", "").strip() or None,
            "name":         row.get("name", "").strip() or None,
            "city":         row.get("municipality", "").strip() or None,
            "country":      row.get("iso_country", "").strip() or None,
            "latitude":     lat,
            "longitude":    lon,
            "altitude_ft":  int(float(row["elevation_ft"])) if row.get("elevation_ft") else None,
            "type":         row.get("type", "").strip() or None,
        })

    print(f"Aeropuertos filtrados ({'/'.join(AIRPORT_TYPES)}): {len(records):,}")
    return records


def load_to_cassandra(records: list[dict]):
    auth    = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
    cluster = Cluster(
        contact_points=CASSANDRA_NODE_IPS,
        port=CASSANDRA_PORT,
        auth_provider=auth,
        load_balancing_policy=RoundRobinPolicy(),
        default_retry_policy=RetryPolicy(),
    )
    session = cluster.connect(CASSANDRA_KEYSPACE)

    cql = """
        INSERT INTO airports (
            icao_code, iata_code, name, city, country,
            latitude, longitude, altitude_ft, type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(cql)
    prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM

    inserted = 0
    start    = time.time()

    for i in range(0, len(records), BATCH_SIZE):
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        for r in records[i : i + BATCH_SIZE]:
            batch.add(prepared, (
                r["icao_code"], r["iata_code"], r["name"], r["city"], r["country"],
                r["latitude"], r["longitude"], r["altitude_ft"], r["type"],
            ))
        session.execute(batch)
        inserted += len(records[i : i + BATCH_SIZE])
        if inserted % 500 == 0 or inserted == len(records):
            print(f"  {inserted:>5} / {len(records)} insertados...")

    elapsed = time.time() - start
    cluster.shutdown()
    print(f"✅ {inserted:,} aeropuertos cargados en {elapsed:.1f}s")


def main():
    records = download_airports()
    if not records:
        print("Sin registros — abortando.")
        return
    load_to_cassandra(records)
    print("\nListo. Ahora puedes correr pipeline_orchestrator.py")


if __name__ == "__main__":
    main()
