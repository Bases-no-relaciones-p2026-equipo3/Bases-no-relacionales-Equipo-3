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
- [ ] Repositorio clonado y actualizado:

```powershell
# Windows
git clone https://github.com/Bases-no-relaciones-p2026-equipo3/Bases-no-relacionales-Equipo-3.git
cd Bases-no-relacionales-Equipo-3
git pull
```

```bash
# Mac
git clone https://github.com/Bases-no-relaciones-p2026-equipo3/Bases-no-relacionales-Equipo-3.git
cd Bases-no-relacionales-Equipo-3
git pull
```

- [ ] Dependencias instaladas:

```powershell
uv sync
```

- [ ] Crear tu archivo `.env` local a partir de la plantilla:

```powershell
# Windows
copy .env.example .env
notepad .env
```

```bash
# Mac
cp .env.example .env
nano .env
```

Cada quien edita su `.env` con las IPs del día según su rol (ver sección correspondiente). El `.env` nunca se sube al repo.

- [ ] Verificar conectividad antes de empezar:

```powershell
# Windows
Test-NetConnection -ComputerName 10.15.20.18 -Port 9041
Test-NetConnection -ComputerName 10.15.20.12 -Port 7687
```

```bash
# Mac
nc -zv 10.15.20.18 9041
nc -zv 10.15.20.12 7687
```

---

## Orden de arranque

```
1. Andre    → levantar Cassandra
2. Víctor   → levantar Neo4j
3. Andre    → correr setup (solo primera vez o tras reset)
4. Andre    → correr ingesta
5. Regina   → correr orquestador (esperar 5 min de ingesta)
6. Fernanda → levantar API (esperar al menos 1 run de Spark)
```

---

## Andre — Cassandra + Ingesta `10.15.20.18`

### Tu `.env`

```ini
CASSANDRA_HOST = localhost      # Cassandra corre en tu propia laptop
CASSANDRA_PORT = 9041
NEO4J_HOST     = 10.15.20.12   # IP de Víctor
NEO4J_PORT     = 7687
```

### Paso 1 — Levantar Cassandra

```powershell
docker start cassandra-node-1 cassandra-node-2 cassandra-node-3
```

Esperar 30 segundos y verificar:

```powershell
docker ps | findstr cassandra
# Los 3 nodos deben aparecer como (healthy)
```

Confirmar al equipo que Cassandra está lista.

### Paso 2 — Setup inicial

> Solo necesario la primera vez o tras un reset completo.

```powershell
uv run python setup/cassandra_schema_migration.py
uv run python setup/load_airports.py
uv run python setup/neo4j_setup_indexes.py
```

Salida esperada:
```
Configuración activa:
  Cassandra : localhost:9041 (keyspace: opensky)
  Neo4j     : bolt://10.15.20.12:7687
──────────────────────────────────────────────────
✅ Keyspace 'opensky' listo.
✅ Tabla 'flight_events' lista.
✅ Tabla 'airports' lista.
✅ 4,963 aeropuertos cargados en 6.3s
✅ Setup de Neo4j completado.
```

> Si `neo4j_setup_indexes.py` falla, Víctor aún no levantó Neo4j. Esperar y volver a correr solo ese script.

### Paso 3 — Correr ingesta

```powershell
uv run python ingesta/opensky_to_cassandra.py
```

Debes ver cada 20 segundos:
```
Iter    1 | aeronaves:  8432 | insertadas:  8432 | total:    8,432 | 3.2s
Iter    2 | aeronaves:  8401 | insertadas:  8401 | total:   16,833 | 2.9s
```

Avisar a Regina cuando lleven **al menos 5 minutos** corriendo.

---

## Víctor — Neo4j `10.15.20.12`

### Tu `.env`

```ini
CASSANDRA_HOST = 10.15.20.18   # IP de Andre
CASSANDRA_PORT = 9041
NEO4J_HOST     = localhost      # Neo4j corre en tu propia laptop
NEO4J_PORT     = 7687
```

### Paso 1 — Levantar Neo4j

```powershell
# Windows
docker start neo4j-instance
docker ps | findstr neo4j
Test-NetConnection -ComputerName localhost -Port 7687
```

```bash
# Mac
docker start neo4j-instance
docker ps | grep neo4j
nc -zv localhost 7687
```

Confirmar al equipo que Neo4j está lista.

### Monitorear el grafo

Abrir `http://localhost:7474` — usuario: `neo4j` / contraseña: `password`

Después de que Regina corra Spark:

```cypher
MATCH (a:Aircraft) RETURN count(a) AS aeronaves
MATCH (c:Country) RETURN count(c) AS paises
MATCH ()-[r]->() RETURN type(r), count(r) AS total ORDER BY total DESC
```

### Si necesitas resetear manualmente

```cypher
MATCH (n) DETACH DELETE n
```

---

## Regina — Spark + Orquestador `10.15.20.13`

### Tu `.env`

```ini
CASSANDRA_HOST   = 10.15.20.18   # IP de Andre
CASSANDRA_PORT   = 9041
NEO4J_HOST       = 10.15.20.12   # IP de Víctor
NEO4J_PORT       = 7687
SPARK_MASTER     = local[*]
SPARK_DRIVER_MEM = 2g
SPARK_EXEC_MEM   = 2g
```

### Antes de empezar — Verificar conectividad

```powershell
# Windows
Test-NetConnection -ComputerName 10.15.20.18 -Port 9041
Test-NetConnection -ComputerName 10.15.20.12 -Port 7687
```

```bash
# Mac
nc -zv 10.15.20.18 9041
nc -zv 10.15.20.12 7687
```

### Correr el orquestador

> ⚠️ Esperar confirmación de Andre de que la ingesta lleva al menos 5 minutos.

```powershell
uv run python pipeline_orchestrator.py
```

Al arrancar verás:
```
Configuración activa:
  Cassandra : 10.15.20.18:9041 (keyspace: opensky)
  Neo4j     : bolt://10.15.20.12:7687
──────────────────────────────────────────────────
RESET INICIAL — limpiando datos de ejecuciones anteriores
  ✅ state_vectors vaciada.
  ✅ flight_events vaciada.
  ✅ Neo4j limpio.
[23:24:18 UTC] Lanzando job Spark...
Job Spark completado exitosamente.
Próximo job Spark en 5 min.
```

### Si spark-submit no se encuentra

```powershell
# Windows — agregar Spark al PATH de la sesión actual
$env:PATH += ";C:\spark\bin"   # ajustar a tu ruta de instalación
spark-submit --version
```

```bash
# Mac
export PATH="$PATH:/opt/homebrew/opt/apache-spark/bin"
spark-submit --version
```

### Monitorear logs

```powershell
# Windows
Get-Content logs/spark.log -Wait -Tail 30
Get-Content logs/ingesta.log -Wait -Tail 20
```

```bash
# Mac
tail -f logs/spark.log
tail -f logs/ingesta.log
```

---

## Fernanda — API FastAPI `10.15.20.20`

### Tu `.env`

```ini
CASSANDRA_HOST = 10.15.20.18   # IP de Andre
CASSANDRA_PORT = 9041
NEO4J_HOST     = 10.15.20.12   # IP de Víctor
NEO4J_PORT     = 7687
```

### Paso 1 — Verificar que Neo4j tiene datos

```powershell
# Windows
Test-NetConnection -ComputerName 10.15.20.12 -Port 7687
```

```bash
# Mac
nc -zv 10.15.20.12 7687
```

### Paso 2 — Crear API keys

```powershell
# Windows / Mac — mismo comando
python -c "import secrets; print('sk-andre-'    + secrets.token_urlsafe(24))"
python -c "import secrets; print('sk-victor-'   + secrets.token_urlsafe(24))"
python -c "import secrets; print('sk-regina-'   + secrets.token_urlsafe(24))"
python -c "import secrets; print('sk-fernanda-' + secrets.token_urlsafe(24))"
```

Crear `api/keys.json` (no se sube al repo):

```json
{
  "andre":    "sk-andre-RESULTADO",
  "victor":   "sk-victor-RESULTADO",
  "regina":   "sk-regina-RESULTADO",
  "fernanda": "sk-fernanda-RESULTADO"
}
```

Compartir cada key por WhatsApp/chat, no por el repo.

### Paso 3 — Levantar la API

```powershell
# Windows / Mac — mismo comando
cd api
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Disponible para todo el equipo en: `http://10.15.20.20:8000/docs`

### Paso 4 — Probar

```powershell
# Windows
$KEY = "sk-andre-TU_KEY"
$API = "http://10.15.20.20:8000"
curl $API/health
curl -H "X-API-Key: $KEY" "$API/analytics/top-countries"
```

```bash
# Mac
KEY="sk-andre-TU_KEY"
API="http://10.15.20.20:8000"
curl $API/health
curl -H "X-API-Key: $KEY" "$API/analytics/top-countries"
```

### Endpoints disponibles

| Endpoint | Descripción |
|---|---|
| `GET /health` | Estado y conteos del grafo |
| `GET /analytics/top-countries` | Top países por flota |
| `GET /analytics/top-speed` | Aeronaves más rápidas |
| `GET /analytics/proximity-hubs` | Hubs de proximidad aérea |
| `GET /analytics/most-tracked` | Avión con más snapshots |
| `GET /analytics/trajectory/{icao24}` | Trayectoria de un avión |
| `GET /analytics/country-interactions` | Interacciones entre países |
| `GET /analytics/position-sources` | Distribución por tecnología |
| `GET /analytics/departure-hotspots` | Aeropuertos con más despegues |
| `GET /analytics/arrival-hotspots` | Aeropuertos con más aterrizajes |
| `GET /analytics/top-routes` | Rutas más frecuentes |
| `GET /analytics/net-traffic` | Tráfico neto por aeropuerto |
| `GET /analytics/aircraft-history/{icao24}` | Historial de un avión |

---

## Resolución de problemas

| Error | Causa | Solución |
|---|---|---|
| `NoHostAvailable` | Cassandra no corre | Andre: `docker start cassandra-node-1 cassandra-node-2 cassandra-node-3` |
| `TcpTestSucceeded: False` / `nc failed` | WireGuard o Docker caído | Verificar WireGuard y contenedores |
| `ServiceUnavailable` en Neo4j | Neo4j no corre | Víctor: `docker start neo4j-instance` |
| `spark-submit no encontrado` | Spark no está en PATH | Regina: agregar Spark al PATH (ver sección de Regina) |
| `Batch size exceeding threshold` | Batch muy grande | Andre: reducir `BATCH_SIZE = 5` en `opensky_to_cassandra.py` |
| `WriteTimeout` | Sobrecarga en Cassandra | Andre: reducir `BATCH_SIZE = 5` |
| Config muestra IPs incorrectas | `.env` mal configurado | Revisar `.env` — debe estar en la raíz del repo |
| API da `403 Forbidden` | Key incorrecta | Fernanda: verificar `api/keys.json` |
| Neo4j vacío tras Spark | Job de Spark falló | Regina: revisar logs |
