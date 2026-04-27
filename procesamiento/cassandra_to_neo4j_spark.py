"""
cassandra_to_neo4j_spark.py  (v2 — con detección de vuelos)
─────────────────────────────────────────────────────────────
Extiende el job original con:

  1. Carga del catálogo de aeropuertos desde Cassandra.
  2. Detección de eventos TAKEOFF / LANDING por avión usando
     ventanas de Spark ordenadas por snapshot_time.
  3. Match geoespacial evento → aeropuerto más cercano.
  4. Escritura de eventos en Cassandra (tabla flight_events).
  5. Nodos Airport y relaciones DEPARTED_FROM / ARRIVED_AT en Neo4j.

Algoritmo de detección
──────────────────────
Para cada icao24, ordena los snapshots por tiempo y calcula
la columna lag de on_ground. Una transición es:

  on_ground_prev = True  → on_ground = False  ⟹  TAKEOFF
  on_ground_prev = False → on_ground = True   ⟹  LANDING

El aeropuerto asignado es el más cercano dentro de AIRPORT_RADIUS_KM.
Si no hay ninguno, el evento se guarda con airport_icao = NULL.

Confianza del evento
────────────────────
  HIGH   → distancia al aeropuerto < 2 km
  MEDIUM → 2 km ≤ distancia < 10 km
  LOW    → 10 km ≤ distancia < AIRPORT_RADIUS_KM
  NONE   → sin aeropuerto conocido a menos de AIRPORT_RADIUS_KM
"""

import math
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
from neo4j import GraphDatabase

# ── Configuración ─────────────────────────────────────────────────────────────
CASSANDRA_HOST     = "10.15.20.18"
CASSANDRA_PORT     = 9041
CASSANDRA_USER     = "cassandra"
CASSANDRA_PASSWORD = "cassandra"
CASSANDRA_KEYSPACE = "opensky"
CASSANDRA_TABLE    = "state_vectors"
CASSANDRA_AIRPORTS = "airports"
CASSANDRA_EVENTS   = "flight_events"

NEO4J_URI = "bolt://10.15.20.X:7687"   # reemplaza X con la IP real
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"

SPARK_APP_NAME   = "opensky-cassandra-to-neo4j-v2"
SPARK_MASTER     = "spark://spark-master.rgorosti.vpn.itam.mx:6077"
SPARK_DRIVER_MEM = "2g"
SPARK_EXEC_MEM   = "2g"

NEAR_RADIUS_KM    = 50    # radio para relaciones NEAR entre aeronaves
AIRPORT_RADIUS_KM = 15    # radio máximo para asignar aeropuerto a un evento
NEO4J_OVERWRITE   = True

CASSANDRA_CONNECTOR = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
NEO4J_CONNECTOR     = "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.2_for_spark_3"

BATCH_ID = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ── SparkSession ──────────────────────────────────────────────────────────────
def build_spark():
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.driver.memory",   SPARK_DRIVER_MEM)
        .config("spark.executor.memory", SPARK_EXEC_MEM)
        .config("spark.jars.packages",   f"{CASSANDRA_CONNECTOR},{NEO4J_CONNECTOR}")
        .config("spark.cassandra.connection.host",           CASSANDRA_HOST)
        .config("spark.cassandra.connection.port",           str(CASSANDRA_PORT))
        .config("spark.cassandra.auth.username",             CASSANDRA_USER)
        .config("spark.cassandra.auth.password",             CASSANDRA_PASSWORD)
        .config("spark.cassandra.input.split.size_in_mb",   "64")
        .config("spark.cassandra.input.fetch.size_in_rows", "5000")
        .getOrCreate()
    )


# ── Lectura desde Cassandra ───────────────────────────────────────────────────
def read_cassandra(spark, table):
    df = (
        spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(keyspace=CASSANDRA_KEYSPACE, table=table)
        .load()
    )
    print(f"[Spark] {table}: {df.count():,} filas leídas")
    return df


# ── Limpieza y enriquecimiento ────────────────────────────────────────────────
def clean_and_enrich(df_raw):
    df = (
        df_raw
        .filter(F.col("icao24").isNotNull() & F.col("snapshot_time").isNotNull())
        .dropDuplicates(["icao24", "snapshot_time"])
        .withColumn("callsign",
            F.when(F.trim(F.col("callsign")) == "", None)
             .otherwise(F.trim(F.col("callsign"))))
        .withColumn("origin_country",
            F.when(F.trim(F.col("origin_country")) == "", F.lit("Unknown"))
             .otherwise(F.trim(F.col("origin_country"))))
        .withColumn("latitude",        F.col("latitude").cast(DoubleType()))
        .withColumn("longitude",       F.col("longitude").cast(DoubleType()))
        .withColumn("baro_altitude",   F.col("baro_altitude").cast(DoubleType()))
        .withColumn("geo_altitude",    F.col("geo_altitude").cast(DoubleType()))
        .withColumn("velocity",        F.col("velocity").cast(DoubleType()))
        .withColumn("true_track",      F.col("true_track").cast(DoubleType()))
        .withColumn("vertical_rate",   F.col("vertical_rate").cast(DoubleType()))
        .withColumn("position_source", F.col("position_source").cast(IntegerType()))
        .withColumn("category",        F.col("category").cast(IntegerType()))
        .withColumn("velocity_kmh",    F.round(F.col("velocity") * 3.6, 2))
        .withColumn("position_source_label",
            F.when(F.col("position_source") == 0, "ADS-B")
             .when(F.col("position_source") == 1, "ASTERIX")
             .when(F.col("position_source") == 2, "MLAT")
             .when(F.col("position_source") == 3, "FLARM")
             .otherwise("Unknown"))
        .withColumn("snapshot_time_str",
            F.date_format(F.col("snapshot_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    )
    df.cache()
    print(f"[Spark] Filas limpias: {df.count():,}")
    return df


# ── Detección de eventos de vuelo ─────────────────────────────────────────────
def detect_flight_events(df_clean):
    """
    Detecta transiciones on_ground para cada avión.

    Ordena snapshots por (icao24, snapshot_time ASC) y calcula el lag
    de on_ground. Transición True→False = TAKEOFF; False→True = LANDING.
    gap_seconds mide el tiempo entre snapshots consecutivos — gaps grandes
    indican menor certeza sobre cuándo ocurrió exactamente la transición.
    """
    w = Window.partitionBy("icao24").orderBy("snapshot_time")

    df_transitions = (
        df_clean
        .filter(
            F.col("on_ground").isNotNull() &
            F.col("latitude").isNotNull() &
            F.col("longitude").isNotNull()
        )
        .withColumn("on_ground_prev",
            F.lag("on_ground", 1).over(w))
        .withColumn("snapshot_time_prev",
            F.lag("snapshot_time", 1).over(w))
        .filter(F.col("on_ground_prev").isNotNull())
        .filter(F.col("on_ground") != F.col("on_ground_prev"))
        .withColumn("event_type",
            F.when(
                (F.col("on_ground_prev") == True) & (F.col("on_ground") == False),
                F.lit("TAKEOFF")
            ).otherwise(F.lit("LANDING"))
        )
        .withColumn("gap_seconds",
            (F.col("snapshot_time").cast("long") -
             F.col("snapshot_time_prev").cast("long")).cast(IntegerType())
        )
        .select(
            "icao24",
            F.col("snapshot_time").alias("event_time"),
            "event_type",
            "latitude",
            "longitude",
            "gap_seconds",
            F.lit(BATCH_ID).alias("batch_id"),
        )
    )

    n        = df_transitions.count()
    takeoffs = df_transitions.filter(F.col("event_type") == "TAKEOFF").count()
    print(f"[Eventos] Total={n:,}  TAKEOFF={takeoffs:,}  LANDING={n - takeoffs:,}")
    return df_transitions


# ── Match evento → aeropuerto más cercano ─────────────────────────────────────
def match_airports(df_events, df_airports):
    """
    Para cada evento encuentra el aeropuerto más cercano dentro de
    AIRPORT_RADIUS_KM. Pre-filtra con bounding box rectangular antes
    de calcular Haversine exacto para evitar un full cross-join.
    """
    deg_margin = AIRPORT_RADIUS_KM / 111.0

    def haversine_fn(lat1, lon1, lat2, lon2):
        if any(v is None for v in [lat1, lon1, lat2, lon2]):
            return None
        R    = 6371.0
        phi1 = math.radians(lat1);  phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlam = math.radians(lon2 - lon1)
        a    = (math.sin(dphi/2)**2
                + math.cos(phi1) * math.cos(phi2) * math.sin(dlam/2)**2)
        return round(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)), 3)

    haversine_udf = F.udf(haversine_fn, DoubleType())

    df_ap = df_airports.select(
        F.col("icao_code").alias("ap_icao"),
        F.col("name").alias("ap_name"),
        F.col("latitude").alias("ap_lat"),
        F.col("longitude").alias("ap_lon"),
    ).cache()

    # Join con bounding box aproximado + distancia exacta
    df_candidates = (
        df_events.alias("ev")
        .join(df_ap.alias("ap"), on=(
            (F.col("ap.ap_lat").between(
                F.col("ev.latitude")  - deg_margin,
                F.col("ev.latitude")  + deg_margin)) &
            (F.col("ap.ap_lon").between(
                F.col("ev.longitude") - deg_margin,
                F.col("ev.longitude") + deg_margin))
        ), how="left")
        .withColumn("dist_km", haversine_udf(
            F.col("ev.latitude"),  F.col("ev.longitude"),
            F.col("ap.ap_lat"),    F.col("ap.ap_lon"),
        ))
        .filter(
            F.col("dist_km").isNull() |
            (F.col("dist_km") <= AIRPORT_RADIUS_KM)
        )
    )

    # Quedarse con el aeropuerto más cercano por evento
    w_near = Window.partitionBy(
        "ev.icao24", "ev.event_time", "ev.event_type"
    ).orderBy(F.col("dist_km").asc_nulls_last())

    df_matched = (
        df_candidates
        .withColumn("rank", F.rank().over(w_near))
        .filter(F.col("rank") == 1)
        .withColumn("confidence",
            F.when(F.col("dist_km").isNull(),  F.lit("NONE"))
             .when(F.col("dist_km") < 2.0,     F.lit("HIGH"))
             .when(F.col("dist_km") < 10.0,    F.lit("MEDIUM"))
             .otherwise(                        F.lit("LOW")))
        .select(
            F.col("ev.icao24"),
            F.col("ev.event_time"),
            F.col("ev.event_type"),
            F.col("ev.latitude"),
            F.col("ev.longitude"),
            F.col("ap_icao").alias("airport_icao"),
            F.col("ap_name").alias("airport_name"),
            F.col("dist_km"),
            "confidence",
            F.col("ev.gap_seconds"),
            F.col("ev.batch_id"),
        )
    )

    df_ap.unpersist()
    for row in df_matched.groupBy("confidence").count().collect():
        print(f"[Match]  {row['confidence']:<8}: {row['count']:>6,}")
    return df_matched


# ── Escritura de eventos en Cassandra ─────────────────────────────────────────
def write_events_cassandra(df_matched):
    df_matched.select(
        "icao24", "event_time", "event_type",
        "latitude", "longitude",
        "airport_icao", "airport_name",
        "confidence", "gap_seconds", "batch_id",
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_EVENTS) \
        .mode("append") \
        .save()
    print(f"[Cassandra] flight_events: {df_matched.count():,} eventos escritos")


# ── Nodos Airport en Neo4j ────────────────────────────────────────────────────
def write_airports_neo4j(df_airports, neo4j_opts, write_mode):
    df_ap = df_airports.select(
        F.col("icao_code").alias("icao"),
        F.col("iata_code").alias("iata"),
        "name", "city", "country",
        "latitude", "longitude", "altitude_ft", "type",
    ).filter(F.col("icao").isNotNull())

    df_ap.write \
        .format("org.neo4j.spark.DataSource").options(**neo4j_opts) \
        .option("labels", ":Airport").option("node.keys", "icao") \
        .mode(write_mode).save()
    print(f"[Neo4j] Airport:       {df_ap.count():,}")


# ── Relaciones DEPARTED_FROM y ARRIVED_AT en Neo4j ────────────────────────────
def write_flight_relations_neo4j(df_matched, neo4j_opts):
    df_with_ap = df_matched.filter(
        F.col("airport_icao").isNotNull() &
        (F.col("confidence") != "NONE")
    )

    for event_type, rel_name in [("TAKEOFF", "DEPARTED_FROM"),
                                  ("LANDING", "ARRIVED_AT")]:
        df_rel = df_with_ap.filter(F.col("event_type") == event_type).select(
            F.col("icao24").alias("source.icao24"),
            F.col("airport_icao").alias("target.icao"),
            F.col("event_time").cast("string").alias("rel.event_time"),
            F.col("confidence").alias("rel.confidence"),
            F.col("gap_seconds").alias("rel.gap_seconds"),
            F.col("dist_km").alias("rel.dist_km"),
        )
        n = df_rel.count()
        if n > 0:
            df_rel.write \
                .format("org.neo4j.spark.DataSource").options(**neo4j_opts) \
                .option("relationship", rel_name) \
                .option("relationship.save.strategy", "keys") \
                .option("relationship.source.labels", ":Aircraft") \
                .option("relationship.source.node.keys", "source.icao24:icao24") \
                .option("relationship.target.labels", ":Airport") \
                .option("relationship.target.node.keys", "target.icao:icao") \
                .mode("append").save()
        print(f"[Neo4j] {rel_name:<16}: {n:>6,}")


# ── Grafo base (sin cambios respecto a v1) ────────────────────────────────────
def write_base_neo4j(df_clean, neo4j_opts, write_mode):
    df_countries = (
        df_clean.select("origin_country").distinct()
                .withColumnRenamed("origin_country", "name")
    )
    df_countries.write \
        .format("org.neo4j.spark.DataSource").options(**neo4j_opts) \
        .option("labels", ":Country").option("node.keys", "name") \
        .mode(write_mode).save()
    print(f"[Neo4j] Country:       {df_countries.count():,}")

    w = Window.partitionBy("icao24").orderBy(F.col("snapshot_time").desc())
    df_aircraft = (
        df_clean.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("icao24", "callsign", "origin_country",
                "position_source", "position_source_label", "category")
    )
    df_aircraft.write \
        .format("org.neo4j.spark.DataSource").options(**neo4j_opts) \
        .option("labels", ":Aircraft").option("node.keys", "icao24") \
        .mode(write_mode).save()
    print(f"[Neo4j] Aircraft:      {df_aircraft.count():,}")

    df_operates = df_aircraft.select(
        F.col("origin_country").alias("source.name"),
        F.col("icao24").alias("target.icao24"),
    )
    df_operates.write \
        .format("org.neo4j.spark.DataSource").options(**neo4j_opts) \
        .option("relationship", "OPERATES") \
        .option("relationship.save.strategy", "keys") \
        .option("relationship.source.labels", ":Country") \
        .option("relationship.source.node.keys", "source.name:name") \
        .option("relationship.target.labels", ":Aircraft") \
        .option("relationship.target.node.keys", "target.icao24:icao24") \
        .mode("overwrite").save()
    print(f"[Neo4j] OPERATES:      {df_operates.count():,}")

    df_snaps = df_clean.select(
        F.col("icao24").alias("source.icao24"),
        F.concat_ws("_", F.col("icao24"), F.col("snapshot_time_str")).alias("target.id"),
        F.col("snapshot_time_str").alias("rel.snapshot_time"),
        F.col("latitude").alias("rel.latitude"),
        F.col("longitude").alias("rel.longitude"),
        F.col("baro_altitude").alias("rel.baro_altitude"),
        F.col("geo_altitude").alias("rel.geo_altitude"),
        F.col("velocity").alias("rel.velocity"),
        F.col("velocity_kmh").alias("rel.velocity_kmh"),
        F.col("true_track").alias("rel.true_track"),
        F.col("vertical_rate").alias("rel.vertical_rate"),
        F.col("on_ground").alias("rel.on_ground"),
        F.col("squawk").alias("rel.squawk"),
        F.col("spi").alias("rel.spi"),
    )
    df_snaps.write \
        .format("org.neo4j.spark.DataSource").options(**neo4j_opts) \
        .option("relationship", "SNAPSHOT") \
        .option("relationship.save.strategy", "keys") \
        .option("relationship.source.labels", ":Aircraft") \
        .option("relationship.source.node.keys", "source.icao24:icao24") \
        .option("relationship.target.labels", ":Snapshot") \
        .option("relationship.target.node.keys", "target.id:id") \
        .option("relationship.target.save.mode", "overwrite") \
        .mode("overwrite").save()
    print(f"[Neo4j] SNAPSHOT:      {df_snaps.count():,}")


def compute_and_write_near(df_clean, neo4j_opts):
    def haversine_km(lat1, lon1, lat2, lon2):
        if any(v is None for v in [lat1, lon1, lat2, lon2]):
            return None
        R    = 6371.0
        phi1 = math.radians(lat1);  phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1);  dlam = math.radians(lon2 - lon1)
        a    = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
        return round(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)), 2)

    haversine_udf = F.udf(haversine_km, DoubleType())
    df_airborne = df_clean.filter(
        (F.col("on_ground") == False) &
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    ).select("icao24", "snapshot_time", "snapshot_time_str",
             "latitude", "longitude").cache()

    df_near = (
        df_airborne.alias("a")
        .join(df_airborne.alias("b"), on=(
            (F.col("a.snapshot_time") == F.col("b.snapshot_time")) &
            (F.col("a.icao24") < F.col("b.icao24"))
        ), how="inner")
        .withColumn("dist_km", haversine_udf(
            F.col("a.latitude"), F.col("a.longitude"),
            F.col("b.latitude"), F.col("b.longitude"),
        ))
        .filter(F.col("dist_km") <= NEAR_RADIUS_KM)
        .select(
            F.col("a.icao24").alias("source.icao24"),
            F.col("b.icao24").alias("target.icao24"),
            F.col("a.snapshot_time_str").alias("rel.snapshot_time"),
            F.col("dist_km").alias("rel.dist_km"),
        )
    )
    near_count = df_near.count()
    df_near.write \
        .format("org.neo4j.spark.DataSource").options(**neo4j_opts) \
        .option("relationship", "NEAR") \
        .option("relationship.save.strategy", "keys") \
        .option("relationship.source.labels", ":Aircraft") \
        .option("relationship.source.node.keys", "source.icao24:icao24") \
        .option("relationship.target.labels", ":Aircraft") \
        .option("relationship.target.node.keys", "target.icao24:icao24") \
        .mode("overwrite").save()
    df_airborne.unpersist()
    print(f"[Neo4j] NEAR:          {near_count:,} (radio={NEAR_RADIUS_KM} km)")


# ── Verificación ──────────────────────────────────────────────────────────────
def verify_neo4j():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    queries = {
        "Aircraft":      "MATCH (a:Aircraft) RETURN count(a) AS n",
        "Country":       "MATCH (c:Country) RETURN count(c) AS n",
        "Airport":       "MATCH (a:Airport) RETURN count(a) AS n",
        "Snapshot":      "MATCH (s:Snapshot) RETURN count(s) AS n",
        "OPERATES":      "MATCH ()-[r:OPERATES]->() RETURN count(r) AS n",
        "SNAPSHOT":      "MATCH ()-[r:SNAPSHOT]->() RETURN count(r) AS n",
        "NEAR":          "MATCH ()-[r:NEAR]-() RETURN count(r)/2 AS n",
        "DEPARTED_FROM": "MATCH ()-[r:DEPARTED_FROM]->() RETURN count(r) AS n",
        "ARRIVED_AT":    "MATCH ()-[r:ARRIVED_AT]->() RETURN count(r) AS n",
    }
    print("\n[Neo4j] Estado del grafo:")
    with driver.session() as session:
        for label, q in queries.items():
            n = session.run(q).single()["n"]
            print(f"  {label:<16}: {n:>8,}")
    driver.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print(f"[Job] cassandra_to_neo4j_spark v2  |  batch_id={BATCH_ID}")
    print("=" * 65)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_raw      = read_cassandra(spark, CASSANDRA_TABLE)
        df_airports = read_cassandra(spark, CASSANDRA_AIRPORTS)
        df_clean    = clean_and_enrich(df_raw)

        neo4j_opts = {
            "url":                           NEO4J_URI,
            "authentication.basic.username": NEO4J_USER,
            "authentication.basic.password": NEO4J_PASSWORD,
            "batch.size":                    "1000",
        }
        write_mode = "overwrite" if NEO4J_OVERWRITE else "append"

        print("\n── Grafo base ──────────────────────────────────────────────")
        write_base_neo4j(df_clean, neo4j_opts, write_mode)

        print("\n── Proximidad en vuelo ─────────────────────────────────────")
        compute_and_write_near(df_clean, neo4j_opts)

        print("\n── Detección de eventos ────────────────────────────────────")
        df_events  = detect_flight_events(df_clean)
        df_matched = match_airports(df_events, df_airports)

        print("\n── Escritura Cassandra ──────────────────────────────────────")
        write_events_cassandra(df_matched)

        print("\n── Aeropuertos y relaciones Neo4j ──────────────────────────")
        write_airports_neo4j(df_airports, neo4j_opts, write_mode)
        write_flight_relations_neo4j(df_matched, neo4j_opts)

        verify_neo4j()
        df_clean.unpersist()
        print("\n[Job] Completado exitosamente.")

    except Exception as e:
        print(f"\n[Job] ERROR: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
