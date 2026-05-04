from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys
from pathlib import Path

# Agregar raíz para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    CASSANDRA_NODE_IPS, CASSANDRA_PORT,
    CASSANDRA_USER, CASSANDRA_PASSWORD, CASSANDRA_KEYSPACE
)

def setup_api_users():
    print(f"--- Configurando Usuarios de la API en Cassandra ---")
    
    auth = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
    cluster = Cluster(contact_points=CASSANDRA_NODE_IPS, port=CASSANDRA_PORT, auth_provider=auth)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    
    # 1. Crear tabla de usuarios
    print("   Creando tabla api_users...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS api_users (
            api_key  text PRIMARY KEY,
            username text,
            role     text,
            active   boolean
        )
    """)
    
    # 2. Insertar usuarios de prueba
    users = [
        ("admin-key-2026", "admin_itam", "Admin", True),
        ("analyst-key-2026", "analista_equipo3", "Analyst", True),
        ("expired-key", "ex_alumno", "Analyst", False)
    ]
    
    stmt = session.prepare("INSERT INTO api_users (api_key, username, role, active) VALUES (?, ?, ?, ?)")
    
    for key, name, role, active in users:
        session.execute(stmt, (key, name, role, active))
        print(f"   [+] Usuario '{name}' creado con rol '{role}'")
        
    print("\n[OK] Control de accesos inicializado en Cassandra.")
    cluster.shutdown()

if __name__ == "__main__":
    setup_api_users()
