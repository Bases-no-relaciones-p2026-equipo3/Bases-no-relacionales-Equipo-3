"""
pipeline_orchestrator.py
─────────────────────────
Punto de entrada del sistema distribuido OpenSky.

Lanza en paralelo dos procesos:
  1. Ingesta continua  — exporta opensky_to_cassandra.ipynb como script y lo corre
  2. Procesamiento     — ejecuta cassandra_to_neo4j_spark.py cada SPARK_INTERVAL_MIN minutos

Uso:
    python pipeline_orchestrator.py

Detener:
    Ctrl+C  (ambos procesos se detienen limpiamente)

Notas:
  - La ingesta corre de forma continua (loop infinito con POLL_INTERVAL_SECONDS=20).
  - El job de Spark corre cada SPARK_INTERVAL_MIN minutos sobre los datos acumulados.
  - Los logs de cada proceso van a logs/ingesta.log y logs/spark.log.
"""

import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from threading import Event, Thread

# ── Configuración ─────────────────────────────────────────────────────────────
SPARK_INTERVAL_MIN  = 5        # cada cuántos minutos corre el job de Spark
SPARK_SUBMIT_CMD    = "spark-submit"
SPARK_PACKAGES      = (
    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
    "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.2_for_spark_3"
)

# Rutas relativas al repositorio
ROOT_DIR        = Path(__file__).parent
INGESTA_SCRIPT  = ROOT_DIR / "ingesta"  / "opensky_to_cassandra.py"
SPARK_SCRIPT    = ROOT_DIR / "procesamiento" / "cassandra_to_neo4j_spark.py"
LOGS_DIR        = ROOT_DIR / "logs"

# ── Logging ───────────────────────────────────────────────────────────────────
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


# ── Hilo de ingesta ───────────────────────────────────────────────────────────
def run_ingesta(stop_event: Event):
    """
    Corre el script de ingesta como subprocess.
    Si falla, espera 30 s y reintenta (tolerancia a errores de red).
    """
    if not INGESTA_SCRIPT.exists():
        logger.error(
            f"Script de ingesta no encontrado: {INGESTA_SCRIPT}\n"
            "Exporta el notebook con:\n"
            "  jupyter nbconvert --to script ingesta/opensky_to_cassandra.ipynb"
        )
        stop_event.set()
        return

    log_file = open(LOGS_DIR / "ingesta.log", "a")
    logger.info(f"Iniciando ingesta desde {INGESTA_SCRIPT}")

    while not stop_event.is_set():
        try:
            proc = subprocess.Popen(
                [sys.executable, str(INGESTA_SCRIPT)],
                stdout=log_file,
                stderr=log_file,
            )
            logger.info(f"Ingesta PID {proc.pid} corriendo.")

            # Esperar a que termine o a señal de stop
            while proc.poll() is None:
                if stop_event.is_set():
                    proc.terminate()
                    logger.info("Ingesta detenida por señal.")
                    return
                time.sleep(2)

            rc = proc.returncode
            if rc != 0:
                logger.warning(f"Ingesta terminó con código {rc}. Reintentando en 30 s...")
                time.sleep(30)
            else:
                logger.info("Ingesta terminó normalmente.")
                return

        except Exception as e:
            logger.error(f"Error al lanzar ingesta: {e}. Reintentando en 30 s...")
            time.sleep(30)

    log_file.close()


# ── Hilo de procesamiento Spark ───────────────────────────────────────────────
def run_spark_loop(stop_event: Event):
    """
    Ejecuta el job de Spark cada SPARK_INTERVAL_MIN minutos.
    Espera a que termine antes de programar la siguiente ejecución.
    """
    log_file = open(LOGS_DIR / "spark.log", "a")
    logger.info(f"Loop Spark iniciado. Intervalo: {SPARK_INTERVAL_MIN} min.")

    # Esperar un ciclo de ingesta antes del primer run
    logger.info(f"Esperando {SPARK_INTERVAL_MIN} min antes del primer job Spark...")
    stop_event.wait(timeout=SPARK_INTERVAL_MIN * 60)

    while not stop_event.is_set():
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        logger.info(f"[{ts}] Lanzando job Spark...")

        try:
            cmd = [
                SPARK_SUBMIT_CMD,
                "--packages", SPARK_PACKAGES,
                str(SPARK_SCRIPT),
            ]
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=log_file)
            logger.info(f"Spark PID {proc.pid}.")

            while proc.poll() is None:
                if stop_event.is_set():
                    proc.terminate()
                    logger.info("Spark detenido por señal.")
                    return
                time.sleep(5)

            rc = proc.returncode
            if rc == 0:
                logger.info("Job Spark completado exitosamente.")
            else:
                logger.warning(f"Job Spark terminó con código {rc}. Revisa logs/spark.log.")

        except FileNotFoundError:
            logger.error(
                f"'{SPARK_SUBMIT_CMD}' no encontrado. "
                "Verifica que Spark esté instalado y en el PATH."
            )
        except Exception as e:
            logger.error(f"Error al lanzar Spark: {e}")

        # Esperar hasta el siguiente ciclo
        logger.info(f"Próximo job Spark en {SPARK_INTERVAL_MIN} min.")
        stop_event.wait(timeout=SPARK_INTERVAL_MIN * 60)

    log_file.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("OpenSky Pipeline Orchestrator arrancando...")
    logger.info(f"  Ingesta script : {INGESTA_SCRIPT}")
    logger.info(f"  Spark script   : {SPARK_SCRIPT}")
    logger.info(f"  Intervalo Spark: {SPARK_INTERVAL_MIN} min")
    logger.info(f"  Logs           : {LOGS_DIR}/")
    logger.info("=" * 60)

    stop_event = Event()

    ingesta_thread = Thread(target=run_ingesta, args=(stop_event,), daemon=True, name="ingesta")
    spark_thread   = Thread(target=run_spark_loop, args=(stop_event,), daemon=True, name="spark")

    ingesta_thread.start()
    spark_thread.start()

    try:
        while ingesta_thread.is_alive() or spark_thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Ctrl+C recibido — deteniendo procesos...")
        stop_event.set()
        ingesta_thread.join(timeout=10)
        spark_thread.join(timeout=10)
        logger.info("Pipeline detenido limpiamente.")


if __name__ == "__main__":
    main()
