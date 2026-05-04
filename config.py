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

# Los 3 nodos del cluster Cassandra corren en la MISMA máquina (diferente puerto).
# El driver se conecta a CASSANDRA_HOST:CASSANDRA_PORT y descubre los demás nodos
# automáticamente vía gossip. En modo distribuido solo hay que cambiar CASSANDRA_HOST
# a la IP WireGuard de la máquina que tiene Cassandra (Andre: 10.15.20.18).
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

# Host del driver Spark — necesario cuando SPARK_MASTER es un cluster remoto.
# El driver debe anunciarse con una IP alcanzable desde los workers.
# En modo local[*] no tiene efecto.
SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST", "")


def print_config():
    """Imprime la configuración activa — útil para verificar antes de correr."""
    print("─" * 50)
    print("Configuración activa:")
    print(f"  Cassandra : {','.join(CASSANDRA_NODE_IPS)}:{CASSANDRA_PORT} (keyspace: {CASSANDRA_KEYSPACE})")
    print(f"  Neo4j     : {NEO4J_URI}")
    print(f"  Spark     : {SPARK_MASTER}")
    if SPARK_DRIVER_HOST:
        print(f"  Driver IP : {SPARK_DRIVER_HOST}")
    print("─" * 50)
