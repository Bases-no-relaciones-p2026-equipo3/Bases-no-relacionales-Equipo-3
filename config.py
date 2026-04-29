"""
config.py
─────────
Carga centralizada de configuración desde variables de entorno o .env.

Todos los scripts del pipeline importan desde aquí:
    from config import CASSANDRA_HOST, CASSANDRA_PORT, NEO4J_URI, SPARK_MASTER

Orden de precedencia:
    1. Variable de entorno del sistema ($env:CASSANDRA_HOST en PowerShell)
    2. Archivo .env en la raíz del repositorio
    3. Valor por defecto (localhost — modo desarrollo)

Cambiar de local a distribuido sin tocar ningún script:
    Editar .env con las IPs WireGuard del equipo y relanzar.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar .env desde la raíz del repositorio
_ROOT = Path(__file__).parent
load_dotenv(_ROOT / ".env", override=False)   # override=False: env del sistema tiene prioridad

# ── Cassandra ─────────────────────────────────────────────────────────────────
CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST",     "localhost")
CASSANDRA_PORT     = int(os.getenv("CASSANDRA_PORT", "9041"))
CASSANDRA_USER     = os.getenv("CASSANDRA_USER",     "cassandra")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", "cassandra")
CASSANDRA_KEYSPACE = "opensky"

CASSANDRA_NODE_IPS = [CASSANDRA_HOST]

# ── Neo4j ─────────────────────────────────────────────────────────────────────
NEO4J_HOST     = os.getenv("NEO4J_HOST",     "localhost")
NEO4J_PORT     = int(os.getenv("NEO4J_PORT", "7687"))
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
NEO4J_URI      = f"bolt://{NEO4J_HOST}:{NEO4J_PORT}"
NEO4J_DATABASE = "neo4j"

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_MASTER     = os.getenv("SPARK_MASTER",     "local[*]")
SPARK_DRIVER_MEM = os.getenv("SPARK_DRIVER_MEM", "2g")
SPARK_EXEC_MEM   = os.getenv("SPARK_EXEC_MEM",   "2g")


def print_config():
    """Imprime la configuración activa — útil para verificar antes de correr."""
    print("─" * 50)
    print("Configuración activa:")
    print(f"  Cassandra : {CASSANDRA_HOST}:{CASSANDRA_PORT} (keyspace: {CASSANDRA_KEYSPACE})")
    print(f"  Neo4j     : {NEO4J_URI}")
    print(f"  Spark     : {SPARK_MASTER}")
    print("─" * 50)
