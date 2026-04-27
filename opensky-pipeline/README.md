# OpenSky Pipeline

Sistema distribuido de captura y análisis de tráfico aéreo en tiempo real.

```
OpenSky API → Cassandra → Spark → Neo4j → FastAPI
```

Las máquinas están interconectadas mediante WireGuard.

---

## Arquitectura

| Componente | Tecnología | Rol |
|---|---|---|
| Ingesta | Python + OpenSky REST | Polling cada 20 s → Cassandra |
| Almacenamiento raw | Apache Cassandra | state_vectors, flight_events, airports |
| Procesamiento | Apache Spark | Limpieza, enriquecimiento, detección de vuelos |
| Grafo analítico | Neo4j | Aircraft, Airport, Country, relaciones NEAR/DEPARTED/ARRIVED |
| API | FastAPI | Exposición de las 11 queries analíticas |

---

## Estructura del repositorio

```
opensky-pipeline/
├── pipeline_orchestrator.py        ← Punto de entrada: lanza ingesta + Spark cada 5 min
│
├── ingesta/
│   └── opensky_to_cassandra.ipynb  ← Notebook de ingesta OpenSky → Cassandra
│
├── procesamiento/
│   ├── cassandra_to_neo4j_spark.py ← Job de Spark (spark-submit)
│   └── cassandra_to_neo4j_spark.ipynb
│
├── analisis/
│   └── opensky_neo4j_queries.ipynb ← 11 queries sobre el grafo Neo4j
│
├── setup/                          ← Ejecutar una sola vez al iniciar el proyecto
│   ├── cassandra_schema_migration.py
│   ├── load_airports.py
│   └── neo4j_setup_indexes.py
│
├── api/
│   ├── main.py
│   ├── auth.py
│   ├── queries.py
│   ├── keys.json.example           ← Copiar a keys.json y rellenar (nunca commitear keys.json)
│   ├── requirements.txt
│   └── README.md
│
└── infra/
    └── docker/
        ├── dockerfile.spark-jupyter
        └── dockerfile.spark-job-venv
```

---

## Puesta en marcha

### 1. Setup inicial (una sola vez)

```bash
# Crear tablas en Cassandra
python setup/cassandra_schema_migration.py

# Cargar catálogo de aeropuertos (~3 000 aeropuertos large/medium)
python setup/load_airports.py

# Crear constraints e índices en Neo4j
python setup/neo4j_setup_indexes.py
```

### 2. Arrancar el pipeline

```bash
pip install cassandra-driver neo4j requests
python pipeline_orchestrator.py
```

El orquestador lanza dos hilos en paralelo:
- **Ingesta**: polling a OpenSky cada 20 s → Cassandra (continuo)
- **Procesamiento**: `spark-submit` cada 5 min → Neo4j

### 3. Arrancar la API

```bash
cd api/
cp keys.json.example keys.json   # editar con keys reales
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

Swagger UI disponible en `http://<IP>:8000/docs`.

---

## IPs del cluster (WireGuard)

| Máquina | IP WireGuard | Servicio |
|---|---|---|
| M1 | 10.0.0.1 | Ingesta + API |
| M2 | 10.0.0.2 | Spark Master / Jupyter |
| M3 | 10.0.0.3 | Cassandra |
| M4 | 10.0.0.4 | Neo4j |

Ajusta `CASSANDRA_HOST`, `NEO4J_URI` y `SPARK_MASTER` en cada script según este mapa.

---

## Agregar un compañero a la API

```bash
# Generar key
python -c "import secrets; print('sk-nombre-' + secrets.token_urlsafe(24))"

# Añadir a keys.json
{ "nombre": "sk-nombre-..." }
```

No es necesario reiniciar la API — las keys se leen en cada request.
