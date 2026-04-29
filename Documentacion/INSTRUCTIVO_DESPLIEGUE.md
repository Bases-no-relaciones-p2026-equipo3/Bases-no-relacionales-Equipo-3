# Instructivo de Despliegue Distribuido — Equipo 3

Pipeline de tráfico aéreo en tiempo real sobre red WireGuard.

```
OpenSky API → Cassandra → Spark → Neo4j → FastAPI
  (Andre)      (Andre)  (Regina) (Víctor) (Fernanda)
```

---

## Mapa del equipo

| Integrante | IP WireGuard | Rol |
|---|---|---|
| **Andre** | `10.15.20.18` | Cassandra + Ingesta OpenSky |
| **Víctor** | `10.15.20.12` | Neo4j |
| **Regina** | `10.15.20.13` | Spark + Orquestador |
| **Fernanda** | `10.15.20.20` | API FastAPI + Análisis |

---

## Prerequisitos para todos

- [ ] WireGuard activo y conectado al tunnel del equipo
- [ ] Repositorio clonado:

- [ ] Dependencias instaladas:

```powershell
uv sync
```

- [ ] Verificar conectividad básica antes de empezar:

```powershell
ping 10.15.20.18   # Andre — Cassandra
ping 10.15.20.12   # Víctor — Neo4j
```

---

## Orden de arranque

Respetar este orden — cada paso depende del anterior:

```
1. Andre    → levantar Cassandra
2. Víctor   → levantar Neo4j
3. Andre    → correr setup (migration + airports + neo4j indexes)
4. Andre    → correr ingesta (opensky_to_cassandra.py)
5. Regina   → correr orquestador (pipeline_orchestrator.py)
6. Fernanda → levantar API (uvicorn)
```

---

## Andre — Cassandra + Ingesta `10.15.20.18`

### Cada sesión — Paso 1: Levantar Cassandra

```powershell
docker start cassandra-node-1 cassandra-node-2 cassandra-node-3
```

Esperar 30 segundos y verificar:

```powershell
docker ps | findstr cassandra
# Los 3 nodos deben aparecer como (healthy)
```

Confirmar al equipo que Cassandra está lista.

### Cada sesión — Paso 2: Correr setup

> Solo necesario si es la primera vez o si se borraron los datos.

```powershell
uv run python setup/cassandra_schema_migration.py
uv run python setup/load_airports.py
uv run python setup/neo4j_setup_indexes.py
```

Salida esperada al final:
```
✅ Keyspace 'opensky' listo.
✅ Tabla 'flight_events' lista.
✅ Tabla 'airports' lista.
✅ 4,963 aeropuertos cargados en 6.3s
✅ Setup de Neo4j completado.
```

> Si `neo4j_setup_indexes.py` falla, significa que Víctor aún no levantó Neo4j. Esperar y volver a correr solo ese script.

### Cada sesión — Paso 3: Correr ingesta

```powershell
uv run python ingesta/opensky_to_cassandra.py
```

Debes ver cada 20 segundos:
```
Iter    1 | aeronaves:  8432 | insertadas:  8432 | total:    8,432 | 3.2s
Iter    2 | aeronaves:  8401 | insertadas:  8401 | total:   16,833 | 2.9s
```

Avisar a Regina cuando hayan pasado **al menos 5 minutos** de ingesta.

### Config relevante en tus scripts

`ingesta/opensky_to_cassandra.py`:
```python
CASSANDRA_NODE_IPS = ["localhost"]
CASSANDRA_PORT     = 9041
BATCH_SIZE         = 10       # no subir de 10
```

`setup/cassandra_schema_migration.py` y `setup/load_airports.py`:
```python
CASSANDRA_NODE_IPS = ["localhost"]
CASSANDRA_PORT     = 9041
```

`setup/neo4j_setup_indexes.py`:
```python
NEO4J_URI = "bolt://10.15.20.12:7687"   # IP de Víctor
```

---

## Víctor — Neo4j `10.15.20.12`

### Cada sesión — Paso 1: Levantar Neo4j

```powershell
docker start neo4j-instance
```

Verificar:
```powershell
docker ps | findstr neo4j
Test-NetConnection -ComputerName localhost -Port 7687
# TcpTestSucceeded: True
```

Confirmar al equipo que Neo4j está lista.

### Monitorear el grafo

Abre en el navegador: `http://localhost:7474`
- Usuario: `neo4j`
- Contraseña: `password`

Después de que Regina corra Spark, verifica que llegaron datos:

```cypher
// ¿Cuántas aeronaves?
MATCH (a:Aircraft) RETURN count(a) AS aeronaves

// ¿Cuántos países?
MATCH (c:Country) RETURN count(c) AS paises

// Top 10 países por flota
MATCH (c:Country)-[:OPERATES]->(a:Aircraft)
RETURN c.name AS pais, count(a) AS flota
ORDER BY flota DESC LIMIT 10

// Verificar relaciones
MATCH ()-[r]->() RETURN type(r), count(r) AS total ORDER BY total DESC
```

### Si necesitas resetear Neo4j manualmente

```cypher
MATCH (n) DETACH DELETE n
```

---

## Regina — Spark + Orquestador `10.15.20.13`

### Antes de empezar — Verificar conectividad

```powershell
# Cassandra (Andre)
Test-NetConnection -ComputerName 10.15.20.18 -Port 9041

# Neo4j (Víctor)
Test-NetConnection -ComputerName 10.15.20.12 -Port 7687
```

Ambos deben dar `TcpTestSucceeded: True`. Si no, el integrante correspondiente debe revisar sus contenedores.

### Verificar config antes de correr

Abre `procesamiento/cassandra_to_neo4j_spark.py` y confirma:

```python
CASSANDRA_HOST  = "10.15.20.18"              # IP de Andre
CASSANDRA_PORT  = 9041
NEO4J_URI       = "bolt://10.15.20.12:7687"  # IP de Víctor
NEO4J_USER      = "neo4j"
NEO4J_PASSWORD  = "password"
NEO4J_OVERWRITE = True
```

Abre `pipeline_orchestrator.py` y confirma:

```python
CASSANDRA_NODE_IPS = ["10.15.20.18"]             # IP de Andre
CASSANDRA_PORT     = 9041
NEO4J_URI          = "bolt://10.15.20.12:7687"   # IP de Víctor
NEO4J_USER         = "neo4j"
NEO4J_PASSWORD     = "password"
```

### Correr el orquestador

> ⚠️ Esperar confirmación de Andre de que la ingesta lleva al menos 5 minutos corriendo.

```powershell
uv run python pipeline_orchestrator.py
```

Al arrancar verás el reset automático:
```
RESET INICIAL — limpiando datos de ejecuciones anteriores
  ✅ state_vectors vaciada.
  ✅ flight_events vaciada.
  ✅ Neo4j limpio.
Reset completado. Iniciando pipeline...
```

Luego el primer job de Spark corre a los 5 minutos:
```
[23:24:18 UTC] Lanzando job Spark...
Job Spark completado exitosamente.
Próximo job Spark en 5 min.
```

Si ves `'spark-submit' no encontrado`, Spark no está en el PATH. Ejecutar desde la terminal donde Spark esté configurado o agregar `$SPARK_HOME/bin` al PATH:

```powershell
$env:PATH += ";C:\spark\bin"   # ajustar a tu ruta de instalación
```

### Monitorear logs

```powershell
# Ver log de Spark en tiempo real
Get-Content logs/spark.log -Wait -Tail 30

# Ver log de ingesta (que corre en paralelo)
Get-Content logs/ingesta.log -Wait -Tail 20
```

---

## Fernanda — API FastAPI `10.15.20.20`

### Antes de empezar

Verificar que Neo4j tiene datos (esperar al menos un run de Spark):

```powershell
Test-NetConnection -ComputerName 10.15.20.12 -Port 7687
```

### Configurar la API

Abre `api/queries.py` y confirma:

```python
NEO4J_URI      = "bolt://10.15.20.12:7687"   # IP de Víctor
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"
```

### Configurar tu API key

Crea o edita `api/keys.json`:

```json
{
  "andre":    "sk-andre-REEMPLAZA",
  "victor":   "sk-victor-REEMPLAZA",
  "regina":   "sk-regina-REEMPLAZA",
  "fernanda": "sk-fernanda-REEMPLAZA"
}
```

Generar cada key:
```powershell
python -c "import secrets; print('sk-andre-' + secrets.token_urlsafe(24))"
python -c "import secrets; print('sk-victor-' + secrets.token_urlsafe(24))"
python -c "import secrets; print('sk-regina-' + secrets.token_urlsafe(24))"
python -c "import secrets; print('sk-fernanda-' + secrets.token_urlsafe(24))"
```

### Levantar la API

```powershell
cd api
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

La API queda disponible para todo el equipo en:
```
http://10.15.20.20:8000/docs
```

### Probar los endpoints

```powershell
$KEY = "sk-andre-TU_KEY_AQUI"
$API = "http://10.15.20.20:8000"

# Sin auth — verificar que la API responde
curl $API/health

# Con auth — consultas analíticas
curl -H "X-API-Key: $KEY" "$API/analytics/top-countries"
curl -H "X-API-Key: $KEY" "$API/analytics/departure-hotspots"
curl -H "X-API-Key: $KEY" "$API/analytics/top-routes"
```

### Endpoints disponibles

| Endpoint | Descripción |
|---|---|
| `GET /health` | Estado del sistema y conteos del grafo |
| `GET /analytics/top-countries` | Top países por flota |
| `GET /analytics/top-speed` | Aeronaves más rápidas |
| `GET /analytics/proximity-hubs` | Aeronaves más cercanas entre sí |
| `GET /analytics/most-tracked` | Avión con más snapshots |
| `GET /analytics/trajectory/{icao24}` | Trayectoria de un avión |
| `GET /analytics/country-interactions` | Interacciones entre países |
| `GET /analytics/position-sources` | Distribución por tecnología ADS-B |
| `GET /analytics/departure-hotspots` | Aeropuertos con más despegues |
| `GET /analytics/arrival-hotspots` | Aeropuertos con más aterrizajes |
| `GET /analytics/top-routes` | Rutas más frecuentes |
| `GET /analytics/net-traffic` | Tráfico neto por aeropuerto |
| `GET /analytics/aircraft-history/{icao24}` | Historial de un avión |

Documentación interactiva completa: `http://10.15.20.20:8000/docs`

---

## Resolución de problemas comunes

| Error | Causa | Solución |
|---|---|---|
| `NoHostAvailable` | Cassandra no está corriendo | Andre: `docker start cassandra-node-1 cassandra-node-2 cassandra-node-3` |
| `TcpTestSucceeded: False` a `10.15.20.18:9041` | WireGuard no activo o Andre apagado | Verificar WireGuard y que Andre tiene Docker corriendo |
| `ServiceUnavailable` en Neo4j | Neo4j no está corriendo | Víctor: `docker start neo4j-instance` |
| `spark-submit no encontrado` | Spark no está en el PATH | Regina: agregar `$SPARK_HOME/bin` al PATH |
| `Batch size exceeding threshold` | Batch muy grande en ingesta | Andre: reducir `BATCH_SIZE = 5` en `opensky_to_cassandra.py` |
| `WriteTimeout` en Cassandra | Carga muy alta | Andre: reducir `BATCH_SIZE = 5` |
| API da `403 Forbidden` | Key incorrecta o faltante | Fernanda: verificar `api/keys.json` |
| Neo4j vacío después de Spark | Job de Spark falló | Regina: revisar `logs/spark.log` |
