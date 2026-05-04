"""
pipeline_orchestrator.py
─────────────────────────
Punto de entrada del sistema distribuido OpenSky.
 
Al iniciar borra datos de ejecuciones anteriores (state_vectors, flight_events,
nodos Neo4j) y lanza en paralelo ingesta continua + job Spark periódico.
 
Configuración via .env o variables de entorno:
    CASSANDRA_HOST — IP de Cassandra (default: localhost)
    NEO4J_HOST     — IP de Neo4j     (default: localhost)
    SPARK_MASTER   — URL del master  (default: local[*])
"""
 
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Thread
 
sys.path.insert(0, str(Path(__file__).parent))
from config import (
    CASSANDRA_NODE_IPS, CASSANDRA_PORT,
    CASSANDRA_USER, CASSANDRA_PASSWORD, CASSANDRA_KEYSPACE,
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,
    SPARK_MASTER, print_config,
)
 
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
 
ROOT_DIR       = Path(__file__).parent
INGESTA_SCRIPT = ROOT_DIR / "ingesta"       / "opensky_to_cassandra.py"
SPARK_SCRIPT   = ROOT_DIR / "procesamiento" / "cassandra_to_neo4j_spark.py"
LOGS_DIR       = ROOT_DIR / "logs"
 
# ── Resolver spark-submit desde el venv si no está en el PATH del sistema ────
def _resolve_spark_submit() -> str:
    """Busca spark-submit en el venv actual antes de recurrir al PATH global."""
    candidates = [
        Path(sys.executable).parent / "spark-submit.cmd",   # Windows venv
        Path(sys.executable).parent / "spark-submit",        # Linux/Mac venv
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return "spark-submit"  # último recurso: PATH global
 
SPARK_SUBMIT = _resolve_spark_submit()
 
# ── Auto-configurar JAVA_HOME si no está definido ─────────────────────────────
def _ensure_spark_env():
    """Configura JAVA_HOME y SPARK_HOME si no están ya definidos."""
    # JAVA_HOME — busca JDK local compatible
    if not os.environ.get("JAVA_HOME"):
        candidates = [
            Path(r"C:\Program Files\Java\jdk-23"),
            Path(r"C:\Program Files\Java\jdk-17"),
            Path(r"C:\Program Files\Eclipse Adoptium\jdk-17.0.11.9-hotspot"),
        ]
        for c in candidates:
            if c.exists():
                os.environ["JAVA_HOME"] = str(c)
                break
 
    # SPARK_HOME — usa el pyspark instalado en el venv
    if not os.environ.get("SPARK_HOME"):
        try:
            import pyspark
            spark_home = str(Path(pyspark.__file__).parent)
            os.environ["SPARK_HOME"] = spark_home
        except ImportError:
            pass  # pyspark no instalado; fallará más adelante con buen mensaje
 
# ── Validar versión de PySpark ────────────────────────────────────────────────
REQUIRED_PYSPARK = "3.5.1"
 
def _check_pyspark_version():
    """
    Verifica que pyspark==3.5.1 esté instalado.
 
    Los conectores de Cassandra y Neo4j están compilados para Spark 3.x / Scala 2.12.
    Spark 4.x usa Scala 2.13 e introduce incompatibilidades binarias que producen:
        java.lang.NoSuchMethodError: scala.jdk.CollectionConverters$...
    """
    try:
        import pyspark
        version = pyspark.__version__
        if version != REQUIRED_PYSPARK:
            logger.error("=" * 65)
            logger.error(f"  VERSION DE PYSPARK INCORRECTA: {version}")
            logger.error(f"  Se requiere pyspark=={REQUIRED_PYSPARK} (Scala 2.12).")
            logger.error(f"  Spark {version} puede ser Scala 2.13 — incompatible con")
            logger.error("  los conectores de Cassandra y Neo4j del proyecto.")
            logger.error("")
            logger.error("  Para corregir:")
            logger.error("      uv remove pyspark")
            logger.error(f"      uv add pyspark=={REQUIRED_PYSPARK}")
            logger.error("=" * 65)
            sys.exit(1)
        logger.info(f"PySpark {version} (Scala 2.12) ✅")
    except ImportError:
        logger.error("pyspark no está instalado. Ejecuta: uv add pyspark==3.5.1")
        sys.exit(1)
 
# ═════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═════════════════════════════════════════════════════════════════════════════
 
LOGS_DIR.mkdir(exist_ok=True)
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "orchestrator.log", encoding="utf-8"), # <--- Agrega esto
    ],
)
logger = logging.getLogger("orchestrator")
 
 
# ═════════════════════════════════════════════════════════════════════════════
# RESET AL INICIO
# ═════════════════════════════════════════════════════════════════════════════
 
def reset_cassandra():
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
        sys.exit(1)
 
 
def reset_neo4j():
    logger.info("Limpiando Neo4j...")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            total = 0
            while True:
                deleted = session.run("""
                    MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS d
                """).single()["d"]
                total += deleted
                if deleted == 0:
                    break
                logger.info(f"  Borrados {total:,} nodos...")
        driver.close()
        logger.info("  ✅ Neo4j limpio.")
    except Exception as e:
        logger.error(f"  ❌ Error limpiando Neo4j: {e}")
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
 
    log_file = open(LOGS_DIR / "ingesta.log", "a", encoding="utf-8") # <--- Agrega esto
    logger.info(f"Ingesta iniciando desde {INGESTA_SCRIPT}")
 
    while not stop_event.is_set():
        try:
            proc = subprocess.Popen(
                [sys.executable, str(INGESTA_SCRIPT)],
                stdout=log_file, stderr=log_file,
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
 
 
# ═════════════════════════════════════════════════════════════════════════════
# PROCESO 2 — JOB SPARK PERIÓDICO
# ═════════════════════════════════════════════════════════════════════════════
 
def run_spark_loop(stop_event: Event):
    log_file = open(LOGS_DIR / "spark.log", "a", encoding="utf-8") # <--- Agrega esto
    logger.info(f"Loop Spark iniciado. Primer job en {SPARK_INTERVAL_MIN} min.")
 
    stop_event.wait(timeout=SPARK_INTERVAL_MIN * 60)
 
    while not stop_event.is_set():
        ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        logger.info(f"[{ts}] Lanzando job Spark...")
        log_file.write(f"\n{'─'*60}\n[{ts}] Nueva ejecución\n{'─'*60}\n")
        log_file.flush()
 
        try:
            logger.info(f"Usando spark-submit: {SPARK_SUBMIT}")
            # --master se pasa explícitamente para que Spark lo procese
            # antes de ejecutar el script (requerido en modo cluster remoto)
            cmd = [
                SPARK_SUBMIT,
                "--master",   SPARK_MASTER,
                "--packages", SPARK_PACKAGES,
                str(SPARK_SCRIPT),
            ]
            proc = subprocess.Popen(
                cmd,
                stdout=log_file, stderr=log_file,
                env=os.environ.copy(),
            )
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
            logger.error(f"'spark-submit' no encontrado en: {SPARK_SUBMIT}. Revisa que pyspark esté instalado en el venv.")
        except Exception as e:
            logger.error(f"Error inesperado: {e}")
 
        if not stop_event.is_set():
            logger.info(f"Próximo job Spark en {SPARK_INTERVAL_MIN} min.")
            stop_event.wait(timeout=SPARK_INTERVAL_MIN * 60)
 
    log_file.close()
 
 
# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════
 
def main():
    os.environ["PYTHONUTF8"] = "1"
    _ensure_spark_env()              # auto-detecta JAVA_HOME y SPARK_HOME si faltan
    _check_pyspark_version()         # falla rápido si pyspark != 3.5.1
    logger.info(f"spark-submit resuelto en: {SPARK_SUBMIT}")
    print_config()
    reset_databases()
 
    stop_event     = Event()
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