"""
opensky_to_cassandra.py
────────────────────────
Ingesta continua: OpenSky Network → Apache Cassandra.

Corre en loop infinito hasta Ctrl+C.
Hace polling a OpenSky cada POLL_INTERVAL_SECONDS y escribe en Cassandra.

Uso:
    uv run python ingesta/opensky_to_cassandra.py

Configuración via .env o variables de entorno:
    CASSANDRA_HOST — IP de Cassandra (default: localhost)
    CASSANDRA_PORT — Puerto CQL     (default: 9041)

Las credenciales de OpenSky van también en .env:
    OPENSKY_CLIENT_ID
    OPENSKY_CLIENT_SECRET
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

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

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  opensky_ingesta — %(message)s",
)
logger = logging.getLogger("opensky_ingesta")

# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ═════════════════════════════════════════════════════════════════════════════

# ── OpenSky OAuth2 — requiere OPENSKY_CLIENT_ID y OPENSKY_CLIENT_SECRET en .env ─
OPENSKY_CLIENT_ID     = os.getenv("OPENSKY_CLIENT_ID")
OPENSKY_CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")

if not OPENSKY_CLIENT_ID or not OPENSKY_CLIENT_SECRET:
    raise EnvironmentError(
        "Faltan credenciales de OpenSky en el .env:\n"
        "  OPENSKY_CLIENT_ID     = tu-client-id\n"
        "  OPENSKY_CLIENT_SECRET = tu-client-secret\n"
        "Obtén las tuyas en: https://opensky-network.org/my-opensky"
    )


OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
OPENSKY_API_BASE         = "https://opensky-network.org/api"
TOKEN_REFRESH_MARGIN_SEC = 60

# ── Polling ───────────────────────────────────────────────────────────────────
POLL_INTERVAL_SECONDS  = 20
MAX_CONSECUTIVE_ERRORS = 5
BATCH_SIZE             = 10    # bajo para no superar límite de 5KB por batch

# ── Ingesta ───────────────────────────────────────────────────────────────────
CASSANDRA_TABLE = "state_vectors"
# Bounding Box para Europa (LatSur, LonOeste, LatNorte, LonEste)
BOUNDING_BOX    = (34.0, -25.0, 72.0, 45.0)    # None = mundo completo. Ej: (14.5, -118.4, 32.7, -86.7) para México

STATE_VECTOR_FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source", "category",
]


# ═════════════════════════════════════════════════════════════════════════════
# TOKEN MANAGER
# ═════════════════════════════════════════════════════════════════════════════

class TokenManager:
    def __init__(self, client_id: str, client_secret: str):
        self._client_id     = client_id
        self._client_secret = client_secret
        self._token         = None
        self._expires_at    = None

    def get_token(self) -> str:
        if self._token and self._expires_at and datetime.now() < self._expires_at:
            return self._token
        return self._refresh()

    def _refresh(self) -> str:
        resp = requests.post(
            OPENSKY_TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     self._client_id,
                "client_secret": self._client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data             = resp.json()
        self._token      = data["access_token"]
        expires_in       = data.get("expires_in", 1800)
        self._expires_at = datetime.now() + timedelta(seconds=expires_in - TOKEN_REFRESH_MARGIN_SEC)
        logger.info(f"Token OAuth2 renovado (expira en {expires_in}s).")
        return self._token

    def headers(self) -> dict:
        return {"Authorization": f"Bearer {self.get_token()}"}


# ═════════════════════════════════════════════════════════════════════════════
# OPENSKY API
# ═════════════════════════════════════════════════════════════════════════════

def fetch_state_vectors(
    token_mgr: TokenManager,
    bbox: Optional[tuple] = None,
) -> Optional[Dict[str, Any]]:
    params = {}
    if bbox:
        lat_min, lon_min, lat_max, lon_max = bbox
        params.update({"lamin": lat_min, "lomin": lon_min, "lamax": lat_max, "lomax": lon_max})

    try:
        resp = requests.get(
            f"{OPENSKY_API_BASE}/states/all",
            headers=token_mgr.headers(),
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.HTTPError as e:
        logger.warning(f"HTTP {e.response.status_code} desde OpenSky: {e.response.text[:200]}")
        return None
    except Exception as e:
        logger.warning(f"Error al contactar OpenSky: {e}")
        return None

    raw_states = data.get("states") or []
    parsed = []
    for sv in raw_states:
        padded = list(sv) + [None] * (len(STATE_VECTOR_FIELDS) - len(sv))
        parsed.append(dict(zip(STATE_VECTOR_FIELDS, padded)))

    return {"time": data.get("time"), "states": parsed}


# ═════════════════════════════════════════════════════════════════════════════
# CASSANDRA
# ═════════════════════════════════════════════════════════════════════════════

def connect_cassandra():
    auth = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
    cluster = Cluster(
        contact_points=CASSANDRA_NODE_IPS,
        port=CASSANDRA_PORT,
        auth_provider=auth,
        load_balancing_policy=RoundRobinPolicy(),
        default_retry_policy=RetryPolicy(),
        connect_timeout=15,
    )
    session = cluster.connect()
    logger.info(f"Conectado a Cassandra: {cluster.metadata.cluster_name}")
    return cluster, session


def setup_schema(session):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(CASSANDRA_KEYSPACE)

    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (
            icao24          text,
            snapshot_time   timestamp,
            callsign        text,
            origin_country  text,
            time_position   bigint,
            last_contact    bigint,
            longitude       double,
            latitude        double,
            baro_altitude   double,
            on_ground       boolean,
            velocity        double,
            true_track      double,
            vertical_rate   double,
            geo_altitude    double,
            squawk          text,
            spi             boolean,
            position_source int,
            category        int,
            PRIMARY KEY (icao24, snapshot_time)
        ) WITH CLUSTERING ORDER BY (snapshot_time DESC)
          AND default_time_to_live = 604800
    """)
    logger.info(f"Tabla '{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}' lista.")


def prepare_insert(session):
    stmt = session.prepare(f"""
        INSERT INTO {CASSANDRA_TABLE} (
            icao24, snapshot_time, callsign, origin_country,
            time_position, last_contact,
            longitude, latitude, baro_altitude, on_ground,
            velocity, true_track, vertical_rate,
            geo_altitude, squawk, spi, position_source, category
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return stmt


def insert_states(session, prepared_stmt, api_response: Dict[str, Any]) -> tuple:
    if not api_response or not api_response.get("states"):
        return 0, 0.0

    snapshot_ts = datetime.fromtimestamp(api_response["time"], tz=timezone.utc)
    states      = api_response["states"]
    inserted    = 0
    t_start     = time.time()

    for i in range(0, len(states), BATCH_SIZE):
        chunk = states[i : i + BATCH_SIZE]
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        for sv in chunk:
            batch.add(prepared_stmt, (
                sv["icao24"], snapshot_ts, sv["callsign"], sv["origin_country"],
                sv["time_position"], sv["last_contact"],
                sv["longitude"], sv["latitude"], sv["baro_altitude"], sv["on_ground"],
                sv["velocity"], sv["true_track"], sv["vertical_rate"],
                sv["geo_altitude"], sv["squawk"], sv["spi"],
                sv["position_source"], sv["category"],
            ))
        session.execute(batch)
        inserted += len(chunk)

    t_end = time.time()
    latency = t_end - t_start
    return inserted, latency


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    print_config()
    logger.info("=" * 60)
    logger.info("OpenSky → Cassandra ingesta arrancando")
    logger.info(f"  Intervalo : {POLL_INTERVAL_SECONDS}s")
    logger.info(f"  Batch size: {BATCH_SIZE}")
    logger.info(f"  BBox      : {BOUNDING_BOX or 'mundo completo'}")
    logger.info("=" * 60)

    token_mgr          = TokenManager(OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET)
    cluster, session   = connect_cassandra()
    setup_schema(session)
    prepared_stmt      = prepare_insert(session)

    iteration          = 0
    total_inserted     = 0
    consecutive_errors = 0

    try:
        while True:
            iteration += 1
            ts_start = datetime.now(tz=timezone.utc)

            try:
                response = fetch_state_vectors(token_mgr, bbox=BOUNDING_BOX)
                if response is None:
                    raise ValueError("Respuesta vacía de OpenSky")

                n_states           = len(response.get("states", []))
                n_inserted, db_time = insert_states(session, prepared_stmt, response)
                total_inserted    += n_inserted
                consecutive_errors = 0

                elapsed = (datetime.now(tz=timezone.utc) - ts_start).total_seconds()
                throughput = n_inserted / db_time if db_time > 0 else 0
                logger.info(
                    f"Iter {iteration:>4} | aeronaves: {n_states:>5} | "
                    f"insertadas: {n_inserted:>5} | total: {total_inserted:>8,} | "
                    f"db_lat: {db_time:.2f}s | throughput: {throughput:.1f} rec/s | {elapsed:.1f}s total"
                )

            except Exception as e:
                consecutive_errors += 1
                wait = POLL_INTERVAL_SECONDS * min(consecutive_errors, MAX_CONSECUTIVE_ERRORS)
                logger.warning(f"ERROR iter {iteration} ({consecutive_errors} consecutivos): {e} — reintentando en {wait}s")
                time.sleep(wait)
                continue

            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info(f"Detenido. Total insertado: {total_inserted:,} filas.")
    finally:
        cluster.shutdown()
        logger.info("Conexión Cassandra cerrada.")


if __name__ == "__main__":
    main()