"""
pipeline_orchestrator.py
─────────────────────────
Punto de entrada del sistema distribuido OpenSky.
 
Al iniciar, borra automáticamente:
  - Cassandra: state_vectors y flight_events (evita saturar RAM entre ejecuciones)
  - Neo4j: todos los nodos y relaciones (grafo limpio)
  - Los aeropuertos en Cassandra se conservan (datos de referencia estáticos)
 
Durante la ejecución:
  - Cassandra acumula snapshots continuamente (necesario para que Spark
    compare estados consecutivos y detecte despegues/aterrizajes)
  - Spark lee los snapshots acumulados y reconstruye Neo4j cada 5 min
 
Uso:
    python pipeline_orchestrator.py
 
Detener:
    Ctrl+C — ambos procesos se detienen limpiamente.
 
Logs:
    logs/orchestrator.log
    logs/ingesta.log
    logs/spark.log
"""
 
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Thread
 
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, RoundRobinPolicy
from neo4j import GraphDatabase
 
# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ═════════════════════════════════════════════════════════════════════════════
 
SPARK_INTERVAL_MIN = 5
 
SPARK_PACKAGES = (
    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
    "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.2_for_spark_3"
)
 
# ── Cassandra ─────────────────────────────────────────────────────────────────
CASSANDRA_NODE_IPS = ["localhost"]
CASSANDRA_PORT     = 9041
CASSANDRA_USER     = "cassandra"
CASSANDRA_PASSWORD = "cassandra"
CASSANDRA_KEYSPACE = "opensky"
 
# ── Neo4j ─────────────────────────────────────────────────────────────────────
NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"
 
# ── Rutas ─────────────────────────────────────────────────────────────────────
ROOT_DIR       = Path(__file__).parent
INGESTA_SCRIPT = ROOT_DIR / "ingesta"       / "opensky_to_cassandra.py"
SPARK_SCRIPT   = ROOT_DIR / "procesamiento" / "cassandra_to_neo4j_spark.py"
LOGS_DIR       = ROOT_DIR / "logs"
 
# ═════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═════════════════════════════════════════════════════════════════════════════
 
LOGS_DIR.mkdir(exist_ok=True)
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "orchestrator.log"),
    ],
)
logger = logging.getLogger("orchestrator")
 
 
# ═════════════════════════════════════════════════════════════════════════════
# RESET AL INICIO
# ═════════════════════════════════════════════════════════════════════════════
 
def reset_cassandra():
    """
    Vacía state_vectors y flight_events al iniciar.
    Los aeropuertos NO se tocan — son referencia estática cargada una sola vez.
    """
    logger.info("Limpiando Cassandra (state_vectors, flight_events)...")
    try:
        auth = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
        cluster = Cluster(
            contact_points=CASSANDRA_NODE_IPS,
            port=CASSANDRA_PORT,
            auth_provider=auth,
            load_balancing_policy=RoundRobinPolicy(),
            default_retry_policy=RetryPolicy(),
            connect_timeout=15,
        )
        session = cluster.connect(CASSANDRA_KEYSPACE)
        session.execute("TRUNCATE state_vectors")
        logger.info("  ✅ state_vectors vaciada.")
        session.execute("TRUNCATE flight_events")
        logger.info("  ✅ flight_events vaciada.")
        cluster.shutdown()
    except Exception as e:
        logger.error(f"  ❌ Error limpiando Cassandra: {e}")
        logger.error("  Verifica que Cassandra esté corriendo.")
        sys.exit(1)
 
 
def reset_neo4j():
    """
    Borra todos los nodos y relaciones de Neo4j.
    Los constraints e índices se conservan.
    Borra en batches de 10,000 para no agotar memoria en una sola transacción.
    """
    logger.info("Limpiando Neo4j...")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            total = 0
            while True:
                result = session.run("""
                    MATCH (n)
                    WITH n LIMIT 10000
                    DETACH DELETE n
                    RETURN count(n) AS deleted
                """)
                deleted = result.single()["deleted"]
                total += deleted
                if deleted == 0:
                    break
                logger.info(f"  Borrados {total:,} nodos hasta ahora...")
        driver.close()
        logger.info("  ✅ Neo4j limpio.")
    except Exception as e:
        logger.error(f"  ❌ Error limpiando Neo4j: {e}")
        logger.error("  Verifica que Neo4j esté corriendo.")
        sys.exit(1)
 
 
def reset_databases():
    logger.info("=" * 60)
    logger.info("RESET INICIAL — limpiando datos de ejecuciones anteriores")
    logger.info("  (aeropuertos en Cassandra se conservan)")
    logger.info("=" * 60)
    reset_cassandra()
    reset_neo4j()
    logger.info("Reset completado. Iniciando pipeline...")
    logger.info("=" * 60)
 
 
# ═════════════════════════════════════════════════════════════════════════════
# PROCESO 1 — INGESTA CONTINUA
# ═════════════════════════════════════════════════════════════════════════════
 
def run_ingesta(stop_event: Event):
    if not INGESTA_SCRIPT.exists():
        logger.error(f"Script no encontrado: {INGESTA_SCRIPT}")
        stop_event.set()
        return
 
    log_file = open(LOGS_DIR / "ingesta.log", "a")
    logger.info(f"Ingesta iniciando desde {INGESTA_SCRIPT}")
 
    while not stop_event.is_set():
        try:
            proc = subprocess.Popen(
                [sys.executable, str(INGESTA_SCRIPT)],
                stdout=log_file,
                stderr=log_file,
            )
            logger.info(f"Ingesta PID {proc.pid} corriendo.")
 
            while proc.poll() is None:
                if stop_event.is_set():
                    proc.terminate()
                    proc.wait(timeout=5)
                    log_file.close()
                    return
                time.sleep(2)
 
            rc = proc.returncode
            if stop_event.is_set():
                break
            if rc != 0:
                logger.warning(f"Ingesta terminó con código {rc}. Reintentando en 30s...")
                time.sleep(30)
 
        except Exception as e:
            logger.error(f"Error al lanzar ingesta: {e}. Reintentando en 30s...")
            time.sleep(30)
 
    log_file.close()
    logger.info("Hilo de ingesta terminado.")
 
 
# ═════════════════════════════════════════════════════════════════════════════
# PROCESO 2 — JOB SPARK PERIÓDICO
# ═════════════════════════════════════════════════════════════════════════════
 
def run_spark_loop(stop_event: Event):
    log_file = open(LOGS_DIR / "spark.log", "a")
    logger.info(f"Loop Spark iniciado. Primer job en {SPARK_INTERVAL_MIN} min.")
 
    stop_event.wait(timeout=SPARK_INTERVAL_MIN * 60)
 
    while not stop_event.is_set():
        ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        logger.info(f"[{ts}] Lanzando job Spark...")
 
        sep = "-" * 60 + "\n"
        log_file.write(f"\n{sep}[{ts}] Nueva ejecución Spark\n{sep}")
        log_file.flush()
 
        try:
            cmd = ["spark-submit", "--packages", SPARK_PACKAGES, str(SPARK_SCRIPT)]
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=log_file)
            logger.info(f"Spark PID {proc.pid}.")
 
            while proc.poll() is None:
                if stop_event.is_set():
                    proc.terminate()
                    proc.wait(timeout=30)
                    log_file.close()
                    return
                time.sleep(5)
 
            rc = proc.returncode
            if rc == 0:
                logger.info("Job Spark completado.")
            else:
                logger.warning(f"Job Spark terminó con código {rc}. Ver logs/spark.log.")
 
        except FileNotFoundError:
            logger.error(
                "'spark-submit' no encontrado. "
                "Spark debe instalarse en esta máquina o usar la imagen Docker."
            )
        except Exception as e:
            logger.error(f"Error inesperado: {e}")
 
        if not stop_event.is_set():
            logger.info(f"Próximo job Spark en {SPARK_INTERVAL_MIN} min.")
            stop_event.wait(timeout=SPARK_INTERVAL_MIN * 60)
 
    log_file.close()
    logger.info("Loop Spark terminado.")
 
 
# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════
 
def main():
    logger.info("=" * 60)
    logger.info("OpenSky Pipeline Orchestrator")
    logger.info(f"  Cassandra : {CASSANDRA_NODE_IPS[0]}:{CASSANDRA_PORT}")
    logger.info(f"  Neo4j     : {NEO4J_URI}")
    logger.info(f"  Intervalo : {SPARK_INTERVAL_MIN} min")
    logger.info("=" * 60)
 
    # Limpiar datos de ejecuciones anteriores
    reset_databases()
 
    stop_event = Event()
    ingesta_thread = Thread(target=run_ingesta,    args=(stop_event,), daemon=True, name="ingesta")
    spark_thread   = Thread(target=run_spark_loop, args=(stop_event,), daemon=True, name="spark")
 
    ingesta_thread.start()
    spark_thread.start()
 
    try:
        while ingesta_thread.is_alive() or spark_thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Ctrl+C — deteniendo pipeline...")
        stop_event.set()
        ingesta_thread.join(timeout=15)
        spark_thread.join(timeout=35)
        logger.info("Pipeline detenido limpiamente.")
 
 
if __name__ == "__main__":
    main()