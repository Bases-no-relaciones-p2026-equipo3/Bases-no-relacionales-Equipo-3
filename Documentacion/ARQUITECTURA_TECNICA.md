# Arquitectura Técnica del Pipeline

Sistema distribuido de captura, procesamiento y análisis de tráfico aéreo en tiempo real.

```
OpenSky REST API
      │  polling cada 20 s
      ▼
Apache Cassandra  ──── tabla: state_vectors (datos crudos, TTL 7 días)
      │                tabla: flight_events (eventos detectados)
      │  spark-submit cada 5 min
      ▼
Apache Spark (PySpark 3.5.1 / Scala 2.12)
      │  limpieza + enriquecimiento + detección de vuelos
      ▼
Neo4j (grafo)     ──── nodos: Aircraft, Country, Airport, Snapshot
      │                relaciones: OPERATES, SNAPSHOT, NEAR, DEPARTED_FROM, ARRIVED_AT
      │
      ▼
FastAPI           ──── 13 endpoints analíticos con autenticación por API key
```

---

## Mapa de archivos

```
Bases-no-relacionales-Equipo-3/
│
├── pipeline_orchestrator.py        ← Punto de entrada del sistema
├── config.py                       ← Carga centralizada de configuración (.env)
├── pyproject.toml                  ← Dependencias Python (pyspark==3.5.1 fijado)
├── env.example                     ← Plantilla de variables de entorno
│
├── ingesta/
│   └── opensky_to_cassandra.py     ← Ingesta continua OpenSky → Cassandra
│
├── procesamiento/
│   └── cassandra_to_neo4j_spark.py ← Job Spark: Cassandra → Neo4j
│
├── setup/
│   ├── cassandra_schema_migration.py ← Crea tablas y keyspace en Cassandra
│   ├── load_airports.py              ← Carga catálogo de ~5,000 aeropuertos
│   └── neo4j_setup_indexes.py        ← Crea constraints e índices en Neo4j
│
├── api/
│   ├── main.py                     ← Aplicación FastAPI (13 endpoints)
│   ├── queries.py                  ← Consultas Cypher para Neo4j
│   └── auth.py                     ← Autenticación por API key (keys.json)
│
├── analisis/
│   └── opensky_neo4j_queries.ipynb ← Notebook de exploración del grafo
│
├── infra/
│   ├── cassandra/
│   │   └── cassandra-cluster.docker-compose.yml  ← Cluster de 3 nodos Cassandra
│   ├── neo4j/
│   │   └── neo4j.docker-compose.yml              ← Instancia Neo4j con APOC y GDS
│   └── spark/
│       ├── spark-cluster.docker-compose.yml      ← Cluster Spark (master + 3 workers)
│       ├── dockerfile.spark-jupyter              ← Imagen con Jupyter + PySpark
│       └── dockerfile.spark-job-venv             ← Imagen para jobs de producción
│
└── Documentacion/
    ├── COMO_CORRER.md                ← Guía de instalación y ejecución
    ├── ARQUITECTURA_TECNICA.md       ← Este archivo
    ├── INSTRUCTIVO_DESPLIEGUE.md     ← Pasos por rol (Andre, Víctor, Regina, Fernanda)
    └── INSTRUCTIVO_ROLES_INTERCAMBIABLES.md
```

---

## Descripción técnica de cada archivo

### `config.py`

Carga centralizada de toda la configuración desde variables de entorno o el archivo `.env`.

```python
from config import CASSANDRA_HOST, CASSANDRA_PORT, NEO4J_URI, SPARK_MASTER
```

**Orden de precedencia:**
1. Variable de entorno del sistema (`$env:CASSANDRA_HOST` en PowerShell)
2. Archivo `.env` en la raíz del repositorio
3. Valor por defecto (`localhost`)

Esto permite cambiar el entorno (local → distribuido) simplemente editando `.env`, sin modificar ningún script.

---

### `pipeline_orchestrator.py`

Punto de entrada del sistema. Orquesta los dos procesos del pipeline en hilos paralelos.

**Al arrancar hace:**
1. Valida que `pyspark==3.5.1` esté instalado (falla rápido con mensaje claro si no).
2. Detecta y configura `JAVA_HOME` y `SPARK_HOME` automáticamente desde el venv.
3. Limpia los datos de ejecuciones anteriores: trunca `state_vectors` y `flight_events` en Cassandra y borra todos los nodos de Neo4j.
4. Lanza dos hilos daemon:
   - **Hilo `ingesta`** — ejecuta `opensky_to_cassandra.py` en loop. Si falla, espera 30 s y reintenta.
   - **Hilo `spark`** — espera 5 minutos, luego ejecuta `spark-submit` cada 5 minutos. Escribe salida en `logs/spark.log`.

**Funciones clave:**
- `_resolve_spark_submit()` — localiza `spark-submit` dentro del `.venv` antes de recurrir al PATH global.
- `_ensure_spark_env()` — configura `JAVA_HOME` (jdk-23 o jdk-17) y `SPARK_HOME` (directorio de pyspark en el venv) si no están definidos.
- `_check_pyspark_version()` — verifica `pyspark.__version__ == "3.5.1"`, termina el proceso si no.
- `reset_cassandra()` — trunca `state_vectors` y `flight_events`.
- `reset_neo4j()` — borra todos los nodos en lotes de 10,000 para evitar timeouts.

---

### `ingesta/opensky_to_cassandra.py`

Ingesta continua. Hace polling a la API REST de OpenSky Network y escribe en Cassandra.

**Flujo:**
```
OAuth2 token (client_credentials)
    ↓
GET /api/states/all?lamin=34&lomin=-25&lamax=72&lomax=45   (Europa)
    ↓
Parsear lista de state vectors → list[dict]
    ↓
INSERT en Cassandra (tabla state_vectors) en batches de 10
    ↓
Esperar 20 s → repetir
```

**Configuración relevante:**
| Parámetro | Valor | Descripción |
|---|---|---|
| `POLL_INTERVAL_SECONDS` | 20 | Intervalo entre consultas a la API |
| `BATCH_SIZE` | 10 | Filas por batch en Cassandra (bajo para no superar 5 KB/batch) |
| `BOUNDING_BOX` | Europa (34°N–72°N, 25°W–45°E) | Región geográfica de captura |
| `MAX_CONSECUTIVE_ERRORS` | 5 | Errores seguidos antes de escalar el tiempo de espera |

**Tabla Cassandra `state_vectors`:**
- **Partition key**: `icao24` (identificador único de la aeronave)
- **Clustering key**: `snapshot_time DESC` (ordenado del más reciente al más antiguo)
- **TTL**: 7 días (604,800 s) — los datos caducan automáticamente

**`TokenManager`**: Gestiona el token OAuth2 de OpenSky, lo renueva automáticamente 60 s antes de que expire.

---

### `procesamiento/cassandra_to_neo4j_spark.py`

Job de Spark que lee de Cassandra, procesa los datos y escribe el grafo en Neo4j. Se ejecuta cada 5 minutos desde el orquestador con `spark-submit`.

**Fases del job:**

#### 1. Lectura desde Cassandra
Lee las tablas `state_vectors` y `airports` usando el conector `spark-cassandra-connector`.

#### 2. Limpieza y enriquecimiento (`clean_and_enrich`)
- Filtra filas sin `icao24` o `snapshot_time`.
- Normaliza `callsign` (elimina espacios, nulos vacíos).
- Castea tipos numéricos.
- Calcula `velocity_kmh` (conversión m/s → km/h).
- Etiqueta la fuente de posición: `0→ADS-B`, `1→ASTERIX`, `2→MLAT`, `3→FLARM`.

#### 3. Detección de eventos de vuelo (`detect_flight_events`)
Usa **ventanas de Spark** ordenadas por `snapshot_time` para cada aeronave:
- Calcula el valor anterior de `on_ground` con `F.lag()`.
- Una transición `True→False` es un **TAKEOFF**.
- Una transición `False→True` es un **LANDING**.
- Registra `gap_seconds` (tiempo entre snapshots consecutivos) como indicador de confianza.

#### 4. Match con aeropuertos (`match_airports`)
Para cada evento detectado, encuentra el aeropuerto más cercano:
1. Pre-filtra con una bounding box rectangular (margen ≈ 15 km) para evitar un cross-join completo.
2. Calcula la distancia exacta con la fórmula de **Haversine**.
3. Asigna confianza: `HIGH` (<2 km), `MEDIUM` (2–10 km), `LOW` (10–15 km), `NONE` (>15 km o sin aeropuerto).

#### 5. Escritura en Cassandra (`write_events_cassandra`)
Guarda los eventos detectados en `flight_events` para histórico persistente.

#### 6. Escritura del grafo en Neo4j
Escribe los siguientes nodos y relaciones usando `neo4j-connector-apache-spark`:

| Elemento | Descripción |
|---|---|
| `:Country` | Países de origen de las aeronaves |
| `:Aircraft` | Aeronave con atributos del snapshot más reciente |
| `:Snapshot` | Cada observación de posición con sus métricas |
| `:Airport` | Aeropuertos del catálogo (ICAO, IATA, ciudad, coordenadas) |
| `OPERATES` | Relación País → Aeronave |
| `SNAPSHOT` | Relación Aeronave → Observación temporal |
| `NEAR` | Aeronaves en vuelo que estaban a ≤ 50 km entre sí |
| `DEPARTED_FROM` | Aeronave → Aeropuerto de despegue |
| `ARRIVED_AT` | Aeronave → Aeropuerto de aterrizaje |

**Compatibilidad de versiones (crítico):**
Los conectores usan sufijo `_2.12` (Scala 2.12), compatibles con **Spark 3.5.x**. Spark 4.x usa Scala 2.13 y rompe la compatibilidad binaria.

---

### `setup/cassandra_schema_migration.py`

Crea el keyspace `opensky` y las tablas necesarias si no existen. Se corre **una sola vez** al inicio del proyecto.

**Tablas que crea:**
- `state_vectors` — datos crudos de posición de aeronaves (TTL: 7 días)
- `flight_events` — despegues y aterrizajes detectados por Spark
- `airports` — catálogo estático de aeropuertos

---

### `setup/load_airports.py`

Descarga y carga el catálogo de aeropuertos desde una fuente pública (OurAirports / similar) en la tabla `airports` de Cassandra. Incluye ~5,000 aeropuertos grandes y medianos con:
- Códigos ICAO e IATA
- Nombre, ciudad, país
- Coordenadas (lat, lon) y altitud
- Tipo de aeropuerto

Este catálogo es el que usa el job de Spark para el match geoespacial de eventos de vuelo.

---

### `setup/neo4j_setup_indexes.py`

Crea en Neo4j los **constraints de unicidad** y los **índices de búsqueda** necesarios para el rendimiento del pipeline. Se corre una sola vez antes del primer job de Spark.

**Constraints:**
- `Aircraft.icao24` — único
- `Country.name` — único
- `Airport.icao` — único

**Índices:**
- `Aircraft.callsign`
- `Snapshot.snapshot_time`
- `Airport.iata`
- `Airport.country`

---

### `api/main.py`

Aplicación **FastAPI** que expone 13 endpoints analíticos sobre el grafo Neo4j. Cada endpoint ejecuta una consulta Cypher definida en `queries.py`.

**Endpoints principales:**

| Endpoint | Query Cypher |
|---|---|
| `GET /analytics/top-countries` | Top países por número de aeronaves |
| `GET /analytics/top-speed` | Aeronaves con mayor velocidad registrada |
| `GET /analytics/proximity-hubs` | Aeronaves con más relaciones NEAR (hubs de tráfico) |
| `GET /analytics/most-tracked` | Aeronave con más snapshots capturados |
| `GET /analytics/trajectory/{icao24}` | Posiciones ordenadas de un avión específico |
| `GET /analytics/country-interactions` | Pares de países cuyas aeronaves estuvieron cerca |
| `GET /analytics/position-sources` | Distribución por tecnología (ADS-B, MLAT, ASTERIX) |
| `GET /analytics/departure-hotspots` | Aeropuertos con más despegues detectados |
| `GET /analytics/arrival-hotspots` | Aeropuertos con más aterrizajes detectados |
| `GET /analytics/top-routes` | Rutas más frecuentes (origen → destino) |
| `GET /analytics/net-traffic` | Balance de tráfico neto por aeropuerto |
| `GET /analytics/aircraft-history/{icao24}` | Historial completo de vuelos de una aeronave |
| `GET /health` | Estado del grafo (conteo de nodos y relaciones) |

### `api/auth.py`

Autenticación por **API key** en el header `X-API-Key`. Las keys se cargan de `api/keys.json` en cada request (sin necesidad de reiniciar). El archivo `keys.json` nunca se sube al repositorio.

---

## Esquema de datos: Cassandra

### `state_vectors` — datos crudos

| Columna | Tipo | Descripción |
|---|---|---|
| `icao24` | text | **Partition key** — ID único ICAO de la aeronave |
| `snapshot_time` | timestamp | **Clustering key DESC** — momento de la captura |
| `callsign` | text | Identificador del vuelo |
| `origin_country` | text | País de registro |
| `latitude` / `longitude` | double | Posición geográfica |
| `baro_altitude` | double | Altitud barométrica (metros) |
| `geo_altitude` | double | Altitud geométrica (metros) |
| `velocity` | double | Velocidad horizontal (m/s) |
| `true_track` | double | Dirección de vuelo (grados) |
| `vertical_rate` | double | Tasa de ascenso/descenso (m/s) |
| `on_ground` | boolean | `true` si está en tierra |
| `position_source` | int | 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM |

### `flight_events` — eventos detectados

| Columna | Tipo | Descripción |
|---|---|---|
| `icao24` | text | Partition key |
| `event_time` | timestamp | Clustering key |
| `event_type` | text | `TAKEOFF` o `LANDING` |
| `airport_icao` | text | Código del aeropuerto asignado (puede ser NULL) |
| `confidence` | text | `HIGH`, `MEDIUM`, `LOW`, `NONE` |
| `dist_km` | double | Distancia al aeropuerto (km) |
| `gap_seconds` | int | Tiempo entre snapshots (confianza temporal) |
| `batch_id` | text | ID del job de Spark que creó el evento |

---

## Esquema del grafo: Neo4j

```
(:Country {name})
    -[:OPERATES]→
(:Aircraft {icao24, callsign, origin_country, position_source, category})
    -[:SNAPSHOT {lat, lon, altitude, velocity, on_ground, ...}]→
(:Snapshot {id, snapshot_time})

(:Aircraft) -[:NEAR {dist_km, snapshot_time}]- (:Aircraft)

(:Aircraft) -[:DEPARTED_FROM {event_time, confidence, dist_km}]→ (:Airport)
(:Aircraft) -[:ARRIVED_AT   {event_time, confidence, dist_km}]→ (:Airport)

(:Airport {icao, iata, name, city, country, latitude, longitude, altitude_ft})
```

---

## Infraestructura Docker

### Cassandra (`infra/cassandra/cassandra-cluster.docker-compose.yml`)

Cluster de **3 nodos** en modo bridge:
- `cassandra-node-1` → puerto `9041` (native transport)
- `cassandra-node-2` → puerto `9042`
- `cassandra-node-3` → puerto `9043`

Todos los nodos usan `GossipingPropertyFileSnitch` para soporte multi-datacenter. Las IPs seed son las del host `10.15.20.18` (máquina de Andre).

### Neo4j (`infra/neo4j/neo4j.docker-compose.yml`)

Instancia única con plugins **APOC** y **Graph Data Science (GDS)**:
- Puerto `7474` — Neo4j Browser (HTTP)
- Puerto `7687` — Bolt (driver Python)
- Autenticación: `neo4j / password`
- Memoria heap máxima: 2 GB

### Spark (`infra/spark/spark-cluster.docker-compose.yml`)

Cluster de 1 master + 3 workers + 1 nodo Jupyter:
- Master en `spark://spark-master.rgorosti.vpn.itam.mx:6077`
- Puerto de UI del master: `6080`
- Cada worker: 2 cores, 2 GB RAM
- Todos los nodos están conectados a las redes `cassandra-net` y `neo4j-net` (externas) para poder alcanzar las bases de datos.

---

## Dependencias y versiones fijadas

| Paquete | Versión | Motivo del pin |
|---|---|---|
| `pyspark` | `==3.5.1` | Compatibilidad con conectores Scala 2.12 |
| `cassandra-driver` | `>=3.29.3` | Driver Python para Cassandra 5.x |
| `neo4j` | `>=5.24.0` | Driver Python para Neo4j 5.x |
| `fastapi` | `>=0.115.0` | API REST |
| `pandas` | `>=2.2.3` | Manipulación de datos en setup |
| `python-dotenv` | `>=1.2.1` | Carga de `.env` |

Los conectores de Spark que se descargan en tiempo de ejecución:
```
com.datastax.spark:spark-cassandra-connector_2.12:3.5.1
org.neo4j:neo4j-connector-apache-spark_2.12:5.3.2_for_spark_3
```
---

## Cumplimiento de Requerimientos de Ingesta

El pipeline ha sido diseñado para cumplir estrictamente con los tres pilares de la entrega:

### 1. Estado de Verdad Operativa (Operational Stage)
Cassandra actúa como nuestra "Single Source of Truth" para el estado actual de la flota. 
- **Latencia Mínima**: Al usar `icao24` como partition key, la consulta del estado más reciente de una aeronave es una lectura de punto (point read) que no requiere escaneos de tabla ni joins.
- **Frescura**: La ingesta escribe con `LOCAL_QUORUM` (2 de 3 nodos deben confirmar), garantizando que las lecturas posteriores vean el dato más reciente de manera consistente.
- **Acceso Directo**: La API de FastAPI permite consultar el estado crudo directamente desde Cassandra antes de que el proceso OLAP (Spark) lo mueva a Neo4j.

### 2. Garantía de Caudal (Throughput)
El sistema garantiza que no se pierden mensajes incluso bajo picos de tráfico:
- **Batches Atómicos**: El script de ingesta agrupa los registros en batches optimizados para Cassandra (evitando el overhead de miles de peticiones individuales).
- **Escalabilidad Horizontal**: Si el caudal de OpenSky aumentara, el cluster de 3 nodos reparte la carga de escritura automáticamente mediante el particionador `Murmur3`.
- **Validación Empírica**: Se incluye el script `setup/test_load_cassandra.py` que simula cargas masivas (miles de registros por segundo) para demostrar que el sistema no presenta pérdida de mensajes bajo estrés.

### 3. Resiliencia y Alta Disponibilidad (HA)
El sistema posee capacidad de recuperación automática ante fallos críticos:
- **Redundancia Física**: Gracias al `Replication Factor = 3`, cada dato vive en los 3 nodos. Si uno o hasta dos nodos fallan simultáneamente, los datos permanecen disponibles.
- **Políticas de Reintento**: El driver de Python implementa `RetryPolicy` y `RoundRobinPolicy`. Si un nodo no responde, el driver redirige la consulta a un nodo saludable de forma transparente para la aplicación.
- **Orquestación Robusta**: El `pipeline_orchestrator.py` monitorea los hilos de ingesta. Si el proceso de captura muere por un error de red o timeout, el orquestador lo reinicia automáticamente tras un periodo de enfriamiento.
