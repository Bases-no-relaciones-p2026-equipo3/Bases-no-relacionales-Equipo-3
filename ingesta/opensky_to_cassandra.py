"""
opensky_to_cassandra.py
────────────────────────
Ingesta continua de la API REST de OpenSky Network hacia Cassandra.

Convertido desde opensky_to_cassandra.ipynb para ser ejecutado
directamente desde el pipeline_orchestrator.py.

Uso directo:
    python opensky_to_cassandra.py

Configuración:
    Ajusta las variables de la sección 1 según tu entorno WireGuard.
"""

import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, RoundRobinPolicy
from cassandra.query import BatchStatement

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("opensky_ingesta")


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 1 — CONFIGURACIÓN
# ═════════════════════════════════════════════════════════════════════════════

# ── OpenSky OAuth2 ────────────────────────────────────────────────────────────
# Crea tu API client en: https://opensky-network.org/my-opensky
OPENSKY_CLIENT_ID     = "ric78-api-client"
OPENSKY_CLIENT_SECRET = "2qQQJF4zlMd9cqBqgLMYy9A9o13zjTdU"

# ── Cassandra (red WireGuard 10.15.20.x) ─────────────────────────────────────
CASSANDRA_NODE_IPS = ["localhost", "localhost", "localhost"]
CASSANDRA_NODE_RPC_PORTS = [9041, 9041, 9041]
CASSANDRA_USER           = "cassandra"
CASSANDRA_PASSWORD       = "cassandra"

# ── Keyspace / tabla destino ──────────────────────────────────────────────────
KEYSPACE = "opensky"
TABLE    = "state_vectors"

# ── Polling ───────────────────────────────────────────────────────────────────
POLL_INTERVAL_SECONDS = 30
POLL_ITERATIONS       = None  # None = loop infinito; entero = N iteraciones (tests)

# ── Ingesta ───────────────────────────────────────────────────────────────────
# True  = vaciar la tabla antes de iniciar (solo datos nuevos)
# False = conservar datos históricos acumulados
OPENSKY_START_FROM_SCRATCH = False

# ── Bounding box opcional (None = todo el mundo) ──────────────────────────────
# Formato: (lat_min, lon_min, lat_max, lon_max)
# Ejemplo México: (14.5, -118.4, 32.7, -86.7)
#         Europa: (34.5, -25.0, 71.5, 45.0)
BOUNDING_BOX = None

# ── Tamaño de batch para inserciones ─────────────────────────────────────────
CASSANDRA_BATCH_SIZE = 10

# ── Resiliencia ───────────────────────────────────────────────────────────────
MAX_CONSECUTIVE_ERRORS = 5


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 2 — AUTENTICACIÓN OPENSKY (OAuth2)
# ═════════════════════════════════════════════════════════════════════════════

OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
OPENSKY_API_BASE         = "https://opensky-network.org/api"
TOKEN_REFRESH_MARGIN_SEC = 60  # renovar 60 s antes de que expire


class TokenManager:
    """Gestiona el ciclo de vida del token OAuth2 de OpenSky."""

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
        )
        resp.raise_for_status()
        data             = resp.json()
        self._token      = data["access_token"]
        expires_in       = data.get("expires_in", 1800)
        self._expires_at = datetime.now() + timedelta(
            seconds=expires_in - TOKEN_REFRESH_MARGIN_SEC
        )
        logger.info(f"[TokenManager] Token renovado, expira en {expires_in}s")
        return self._token

    def headers(self) -> dict:
        return {"Authorization": f"Bearer {self.get_token()}"}


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3 — CONSUMO DE LA API OPENSKY
# ═════════════════════════════════════════════════════════════════════════════

# Nombres de los 18 campos que devuelve /states/all
STATE_VECTOR_FIELDS = [
    "icao24",          # 0  – dirección ICAO24 (hex)
    "callsign",        # 1  – indicativo de llamada
    "origin_country",  # 2  – país de origen
    "time_position",   # 3  – Unix ts de la última posición
    "last_contact",    # 4  – Unix ts del último contacto
    "longitude",       # 5  – longitud WGS-84 (grados)
    "latitude",        # 6  – latitud WGS-84 (grados)
    "baro_altitude",   # 7  – altitud barométrica (metros)
    "on_ground",       # 8  – True si está en tierra
    "velocity",        # 9  – velocidad sobre el suelo (m/s)
    "true_track",      # 10 – rumbo (grados, 0=norte, sentido horario)
    "vertical_rate",   # 11 – tasa vertical (m/s)
    "sensors",         # 12 – IDs de sensores (puede ser null)
    "geo_altitude",    # 13 – altitud geométrica (metros)
    "squawk",          # 14 – código transponder
    "spi",             # 15 – indicador de propósito especial
    "position_source", # 16 – fuente de posición (0=ADS-B, 1=ASTERIX, 2=MLAT)
    "category",        # 17 – categoría de aeronave (puede estar ausente)
]


def fetch_state_vectors(
    token_mgr: TokenManager,
    bbox: Optional[tuple] = None,
    icao24: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Llama a GET /states/all y devuelve un dict con:
      - 'time': timestamp Unix de la respuesta
      - 'states': lista de dicts con los 18 campos por aeronave
    Retorna None si la llamada falla.
    """
    params = {}
    if bbox:
        lat_min, lon_min, lat_max, lon_max = bbox
        params.update({
            "lamin": lat_min, "lomin": lon_min,
            "lamax": lat_max, "lomax": lon_max,
        })
    if icao24:
        params["icao24"] = icao24

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
        logger.error(f"[OpenSky] HTTP error: {e.response.status_code} – {e.response.text[:200]}")
        return None
    except Exception as e:
        logger.error(f"[OpenSky] Error inesperado: {e}")
        return None

    raw_states = data.get("states") or []
    parsed = []
    for sv in raw_states:
        padded = list(sv) + [None] * (len(STATE_VECTOR_FIELDS) - len(sv))
        parsed.append(dict(zip(STATE_VECTOR_FIELDS, padded)))

    return {"time": data.get("time"), "states": parsed}


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 4 — CONEXIÓN A CASSANDRA
# ═════════════════════════════════════════════════════════════════════════════

def build_cassandra_session():
    """Crea y retorna una sesión de Cassandra conectada al keyspace."""
    auth_provider = PlainTextAuthProvider(
        username=CASSANDRA_USER,
        password=CASSANDRA_PASSWORD,
    )
    cluster = Cluster(
        contact_points=CASSANDRA_NODE_IPS,
        port=CASSANDRA_NODE_RPC_PORTS[0],
        auth_provider=auth_provider,
        load_balancing_policy=RoundRobinPolicy(),
        default_retry_policy=RetryPolicy(),
        connect_timeout=15,
    )
    session = cluster.connect()
    logger.info(f"Conectado al clúster: {cluster.metadata.cluster_name}")
    logger.info(f"Nodos conocidos: {[h.address for h in cluster.metadata.all_hosts()]}")
    return cluster, session


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 5 — CREAR KEYSPACE Y TABLA
# ═════════════════════════════════════════════════════════════════════════════

def setup_schema(session):
    """Crea el keyspace y la tabla si no existen."""
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': 3
        }}
    """)
    session.set_keyspace(KEYSPACE)
    logger.info(f"Keyspace '{KEYSPACE}' listo.")

    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
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
    logger.info(f"Tabla '{KEYSPACE}.{TABLE}' lista.")

    if OPENSKY_START_FROM_SCRATCH:
        session.execute(f"TRUNCATE {KEYSPACE}.{TABLE}")
        logger.info(f"Tabla '{KEYSPACE}.{TABLE}' vaciada. Lista para nueva ingesta.")
    else:
        logger.info(f"Conservando datos existentes en '{KEYSPACE}.{TABLE}'.")


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 6 — INSERCIÓN POR LOTES
# ═════════════════════════════════════════════════════════════════════════════

INSERT_CQL = f"""
    INSERT INTO {TABLE} (
        icao24, snapshot_time, callsign, origin_country,
        time_position, last_contact,
        longitude, latitude, baro_altitude, on_ground,
        velocity, true_track, vertical_rate,
        geo_altitude, squawk, spi, position_source, category
    ) VALUES (
        ?, ?, ?, ?,
        ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?, ?
    )
"""


def insert_states(session, prepared_insert, api_response: Dict[str, Any]) -> int:
    """
    Inserta los state vectors de una respuesta de la API en Cassandra.
    Devuelve el número de filas insertadas.
    """
    if not api_response or not api_response.get("states"):
        return 0

    snapshot_ts = datetime.fromtimestamp(
        api_response["time"], tz=timezone.utc
    )

    states   = api_response["states"]
    inserted = 0

    for i in range(0, len(states), CASSANDRA_BATCH_SIZE):
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        chunk = states[i : i + CASSANDRA_BATCH_SIZE]

        for sv in chunk:
            batch.add(
                prepared_insert,
                (
                    sv["icao24"],
                    snapshot_ts,
                    sv["callsign"],
                    sv["origin_country"],
                    sv["time_position"],
                    sv["last_contact"],
                    sv["longitude"],
                    sv["latitude"],
                    sv["baro_altitude"],
                    sv["on_ground"],
                    sv["velocity"],
                    sv["true_track"],
                    sv["vertical_rate"],
                    sv["geo_altitude"],
                    sv["squawk"],
                    sv["spi"],
                    sv["position_source"],
                    sv["category"],
                ),
            )
        session.execute(batch)
        inserted += len(chunk)

    return inserted


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 7 — LOOP DE POLLING
# ═════════════════════════════════════════════════════════════════════════════

def run_polling(session, prepared_insert, token_mgr: TokenManager):
    """Ejecuta el loop principal de ingesta."""
    logger.info(
        f"Iniciando polling cada {POLL_INTERVAL_SECONDS}s "
        f"({'∞' if POLL_ITERATIONS is None else POLL_ITERATIONS} iteraciones)"
    )

    iteration          = 0
    total_inserted     = 0
    consecutive_errors = 0

    try:
        while POLL_ITERATIONS is None or iteration < POLL_ITERATIONS:
            iteration += 1
            ts_start = datetime.now(tz=timezone.utc)

            try:
                response = fetch_state_vectors(token_mgr, bbox=BOUNDING_BOX)

                if response is None:
                    raise ValueError("Respuesta vacía de OpenSky")

                n_states   = len(response.get("states", []))
                n_inserted = insert_states(session, prepared_insert, response)
                total_inserted    += n_inserted
                consecutive_errors = 0

                elapsed = (datetime.now(tz=timezone.utc) - ts_start).total_seconds()
                logger.info(
                    f"Iter {iteration:>3} | "
                    f"aeronaves: {n_states:>5} | "
                    f"insertadas: {n_inserted:>5} | "
                    f"total: {total_inserted:>8} | "
                    f"{elapsed:.1f}s"
                )

            except Exception as e:
                consecutive_errors += 1
                wait = POLL_INTERVAL_SECONDS * min(consecutive_errors, MAX_CONSECUTIVE_ERRORS)
                logger.warning(
                    f"ERROR iter {iteration} ({consecutive_errors} consecutivos): {e} "
                    f"— reintentando en {wait}s"
                )
                time.sleep(wait)
                continue

            if POLL_ITERATIONS is None or iteration < POLL_ITERATIONS:
                time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info(f"Detenido por el usuario. Total insertado: {total_inserted} filas.")

    logger.info("Polling finalizado.")


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    logger.info("=" * 60)
    logger.info("OpenSky → Cassandra ingesta arrancando...")
    logger.info(f"  Cassandra : {CASSANDRA_NODE_IPS[0]}:{CASSANDRA_NODE_RPC_PORTS[0]}")
    logger.info(f"  Keyspace  : {KEYSPACE}.{TABLE}")
    logger.info(f"  Intervalo : {POLL_INTERVAL_SECONDS}s")
    logger.info("=" * 60)

    token_mgr = TokenManager(OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET)

    cluster, session = build_cassandra_session()
    try:
        setup_schema(session)

        prepared_insert = session.prepare(INSERT_CQL)
        prepared_insert.consistency_level = ConsistencyLevel.LOCAL_QUORUM

        run_polling(session, prepared_insert, token_mgr)
    finally:
        cluster.shutdown()
        logger.info("Conexión a Cassandra cerrada.")


if __name__ == "__main__":
    main()
