import time
import uuid
import random
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import sys
from pathlib import Path

# Agregar raíz para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    CASSANDRA_NODE_IPS, CASSANDRA_PORT,
    CASSANDRA_USER, CASSANDRA_PASSWORD, CASSANDRA_KEYSPACE
)

def run_stress_test(total_records=5000, batch_size=50):
    print(f"--- Iniciando Prueba de Carga para Cassandra ---")
    print(f"   Objetivo: {total_records} registros")
    print(f"   Batch Size: {batch_size}")
    
    auth = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
    
    # Intentar con la IP configurada, o caer a localhost:9041 si falla
    print(f"   Conectando a {CASSANDRA_NODE_IPS} puerto {CASSANDRA_PORT}...")
    try:
        cluster = Cluster(contact_points=CASSANDRA_NODE_IPS, port=CASSANDRA_PORT, auth_provider=auth)
        session = cluster.connect()
    except Exception:
        print(f"   [!] No se pudo conectar a la IP principal. Intentando con localhost:9041...")
        cluster = Cluster(contact_points=['127.0.0.1'], port=9041, auth_provider=auth)
        session = cluster.connect()

    try:
        session.set_keyspace(CASSANDRA_KEYSPACE)
    except Exception:
        print(f"   [!] El keyspace '{CASSANDRA_KEYSPACE}' no existe. ¿Ya corriste el setup/cassandra_schema_migration.py?")
        cluster.shutdown()
        return
    
    # Asegurar que la tabla existe para la prueba
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS state_vectors (
            icao24          text,
            snapshot_time   timestamp,
            callsign        text,
            origin_country  text,
            longitude       double,
            latitude        double,
            on_ground       boolean,
            PRIMARY KEY (icao24, snapshot_time)
        )
    """)

    stmt = session.prepare(f"""
        INSERT INTO state_vectors (
            icao24, snapshot_time, callsign, origin_country, 
            longitude, latitude, on_ground
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    
    start_time = time.time()
    inserted = 0
    errors = 0
    
    for i in range(0, total_records, batch_size):
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        current_batch_size = min(batch_size, total_records - i)
        
        for j in range(current_batch_size):
            # Generar datos aleatorios
            icao = f"stress-{uuid.uuid4().hex[:6]}"
            batch.add(stmt, (
                icao, 
                datetime.now(tz=timezone.utc), 
                "STRESS", 
                "TestLand", 
                random.uniform(-180, 180), 
                random.uniform(-90, 90), 
                False
            ))
        
        try:
            session.execute(batch)
            inserted += current_batch_size
            if inserted % 1000 == 0:
                print(f"   ... insertados {inserted}/{total_records}")
        except Exception as e:
            print(f"   [!] Error en batch: {e}")
            errors += 1
            
    end_time = time.time()
    duration = end_time - start_time
    throughput = inserted / duration if duration > 0 else 0
    
    print("\n" + "="*40)
    print("--- RESULTADOS DE LA PRUEBA DE CARGA ---")
    print("="*40)
    print(f"Registros insertados : {inserted}")
    print(f"Errores encontrados  : {errors}")
    print(f"Tiempo total         : {duration:.2f} segundos")
    print(f"Caudal (Throughput)  : {throughput:.1f} registros/segundo")
    print(f"Pérdida de mensajes  : {0 if errors == 0 else (errors/total_records)*100:.2f}%")
    print("="*40)
    
    cluster.shutdown()

if __name__ == "__main__":
    run_stress_test()
