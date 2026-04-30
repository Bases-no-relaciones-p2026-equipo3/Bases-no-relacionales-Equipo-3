# Instructivo de Despliegue — Roles Intercambiables

Pipeline de tráfico aéreo en tiempo real sobre red WireGuard.

```
OpenSky API → Cassandra → Spark → Neo4j → FastAPI
  (Rol A)      (Rol A)   (Rol C)  (Rol B)  (Rol D)
```

Cualquier integrante puede tomar cualquier rol. La configuración se maneja
íntegramente desde el archivo `.env` — no hay que editar ningún script.

---

## IPs del equipo

| Integrante | IP WireGuard |
|---|---|
| Andre | `10.15.20.18` |
| Víctor | `10.15.20.12` |
| Regina | `10.15.20.13` |
| Fernanda | `10.15.20.20` |

---

## Roles disponibles

| Rol | Descripción | Requiere |
|---|---|---|
| **Rol A — Cassandra + Ingesta** | Levanta los 3 nodos de Cassandra y descarga datos de OpenSky | Docker |
| **Rol B — Neo4j** | Levanta la base de datos de grafos | Docker |
| **Rol C — Spark + Orquestador** | Procesa datos de Cassandra y los escribe en Neo4j | Python + Spark |
| **Rol D — API + Análisis** | Expone los datos del grafo vía REST | Python |

---

## Prerequisitos para todos

- [ ] WireGuard activo
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

- [ ] Definir roles del día y llenar esta tabla:

```
Rol A (Cassandra + Ingesta) → ____________  IP: 10.15.20.___
Rol B (Neo4j)               → ____________  IP: 10.15.20.___
Rol C (Spark + Orquestador) → ____________  IP: 10.15.20.___
Rol D (API + Análisis)      → ____________  IP: 10.15.20.___
```

- [ ] Cada quien crea su `.env` a partir de la plantilla:

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

---

## Orden de arranque

```
1. Rol A → levantar Cassandra
2. Rol B → levantar Neo4j
3. Rol A → correr setup (solo primera vez o tras reset)
4. Rol A → correr ingesta
5. Rol C → correr orquestador (esperar 5 min de ingesta)
6. Rol D → levantar API (esperar al menos 1 run de Spark)
```

---

## Rol A — Cassandra + Ingesta

### Tu `.env`

```ini
# Cassandra corre en tu propia laptop
CASSANDRA_HOST = localhost
CASSANDRA_PORT = 9041

# IP de quien tenga Rol B
NEO4J_HOST = <IP_ROL_B>
NEO4J_PORT = 7687
```

> Si Rol A y Rol B son la misma persona: `NEO4J_HOST = localhost`

### Paso 1 — Levantar Cassandra

```powershell
docker start cassandra-node-1 cassandra-node-2 cassandra-node-3
```

Esperar 30 segundos y verificar:

```powershell
# Windows
docker ps | findstr cassandra
```

```bash
# Mac
docker ps | grep cassandra
```

Los 3 nodos deben aparecer como `(healthy)`. Avisar al equipo.

### Paso 2 — Setup inicial

> Solo la primera vez o tras un reset completo.

```powershell
uv run python setup/cassandra_schema_migration.py
uv run python setup/load_airports.py
uv run python setup/neo4j_setup_indexes.py
```

Al inicio de cada script verás la configuración activa — verifica que las IPs sean correctas antes de continuar:
```
Configuración activa:
  Cassandra : localhost:9041 (keyspace: opensky)
  Neo4j     : bolt://<IP_ROL_B>:7687
```

### Paso 3 — Correr ingesta

```powershell
uv run python ingesta/opensky_to_cassandra.py
```

Debes ver cada 20 segundos:
```
Iter    1 | aeronaves:  8432 | insertadas:  8432 | total:    8,432 | 3.2s
```

Avisar a Rol C cuando lleven **al menos 5 minutos** corriendo.

### Errores comunes

| Error | Solución |
|---|---|
| `Batch size exceeding threshold` | Reducir `BATCH_SIZE = 5` en `opensky_to_cassandra.py` |
| `WriteTimeout` | Reducir `BATCH_SIZE = 5` |
| Nodos no aparecen como `healthy` | Esperar 60s más y verificar de nuevo |

---

## Rol B — Neo4j

### Tu `.env`

```ini
# IP de quien tenga Rol A
CASSANDRA_HOST = <IP_ROL_A>
CASSANDRA_PORT = 9041

# Neo4j corre en tu propia laptop
NEO4J_HOST = localhost
NEO4J_PORT = 7687
```

> Si Rol B y Rol A son la misma persona: `CASSANDRA_HOST = localhost`

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

Avisar al equipo que Neo4j está lista.

### Monitorear el grafo

Abrir `http://localhost:7474` — usuario: `neo4j` / contraseña: `password`

Después de que Rol C corra Spark:

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

## Rol C — Spark + Orquestador

### Tu `.env`

```ini
# IP de quien tenga Rol A
CASSANDRA_HOST   = <IP_ROL_A>
CASSANDRA_PORT   = 9041

# IP de quien tenga Rol B
NEO4J_HOST       = <IP_ROL_B>
NEO4J_PORT       = 7687

SPARK_MASTER     = local[*]
SPARK_DRIVER_MEM = 2g
SPARK_EXEC_MEM   = 2g
```

> Si Rol C y Rol A son la misma persona: `CASSANDRA_HOST = localhost`
> Si Rol C y Rol B son la misma persona: `NEO4J_HOST = localhost`

### Verificar conectividad

```powershell
# Windows
Test-NetConnection -ComputerName <IP_ROL_A> -Port 9041
Test-NetConnection -ComputerName <IP_ROL_B> -Port 7687
```

```bash
# Mac
nc -zv <IP_ROL_A> 9041
nc -zv <IP_ROL_B> 7687
```

### Correr el orquestador

> ⚠️ Esperar confirmación de Rol A de que la ingesta lleva al menos 5 minutos.

```powershell
uv run python pipeline_orchestrator.py
```

Al arrancar verás la config activa y el reset automático:
```
Configuración activa:
  Cassandra : <IP_ROL_A>:9041 (keyspace: opensky)
  Neo4j     : bolt://<IP_ROL_B>:7687
──────────────────────────────────────────────────
RESET INICIAL — limpiando datos de ejecuciones anteriores
  ✅ state_vectors vaciada.
  ✅ flight_events vaciada.
  ✅ Neo4j limpio.
```

### Si spark-submit no se encuentra

```powershell
# Windows
$env:PATH += ";C:\spark\bin"   # ajustar a tu ruta
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

## Rol D — API + Análisis

### Tu `.env`

```ini
# IP de quien tenga Rol A
CASSANDRA_HOST = <IP_ROL_A>
CASSANDRA_PORT = 9041

# IP de quien tenga Rol B
NEO4J_HOST = <IP_ROL_B>
NEO4J_PORT = 7687
```

> Si Rol D y Rol B son la misma persona: `NEO4J_HOST = localhost`

### Paso 1 — Verificar conectividad a Neo4j

```powershell
# Windows
Test-NetConnection -ComputerName <IP_ROL_B> -Port 7687
```

```bash
# Mac
nc -zv <IP_ROL_B> 7687
```

### Paso 2 — Crear API keys

```powershell
# Windows / Mac — mismo comando
python -c "import secrets; print('sk-nombre-' + secrets.token_urlsafe(24))"
```

Crear `api/keys.json` (no se sube al repo):

```json
{
  "nombre1": "sk-nombre1-RESULTADO",
  "nombre2": "sk-nombre2-RESULTADO",
  "nombre3": "sk-nombre3-RESULTADO",
  "nombre4": "sk-nombre4-RESULTADO"
}
```

Compartir cada key por WhatsApp/chat — nunca por el repo.

### Paso 3 — Levantar la API

```powershell
# Windows / Mac — mismo comando
cd api
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Disponible para todo el equipo en: `http://<IP_ROL_D>:8000/docs`

### Probar

```powershell
# Windows
$KEY = "sk-nombre-TU_KEY"
$API = "http://<IP_ROL_D>:8000"
curl $API/health
curl -H "X-API-Key: $KEY" "$API/analytics/top-countries"
curl -H "X-API-Key: $KEY" "$API/analytics/top-routes?limit=10"
```

```bash
# Mac
KEY="sk-nombre-TU_KEY"
API="http://<IP_ROL_D>:8000"
curl $API/health
curl -H "X-API-Key: $KEY" "$API/analytics/top-countries"
curl -H "X-API-Key: $KEY" "$API/analytics/top-routes?limit=10"
```

---

## Tabla de configuración rápida — llenar al inicio de cada sesión

```
Rol A (Cassandra) → nombre: ________  IP: 10.15.20.___
Rol B (Neo4j)     → nombre: ________  IP: 10.15.20.___
Rol C (Spark)     → nombre: ________  IP: 10.15.20.___
Rol D (API)       → nombre: ________  IP: 10.15.20.___
```

Compartir esta tabla por WhatsApp antes de empezar.
Cada quien reemplaza `<IP_ROL_X>` en su `.env` con los valores de la tabla.

---

## Resolución de problemas

| Error | Causa probable | Solución |
|---|---|---|
| `NoHostAvailable` | Cassandra no corre | Rol A: `docker start cassandra-node-1 cassandra-node-2 cassandra-node-3` |
| `TcpTestSucceeded: False` / `nc failed` | WireGuard o Docker caído | Verificar WireGuard y contenedores |
| `ServiceUnavailable` en Neo4j | Neo4j no corre | Rol B: `docker start neo4j-instance` |
| `spark-submit no encontrado` | Spark no está en PATH | Rol C: agregar Spark al PATH |
| `Batch size exceeding threshold` | Batch demasiado grande | Rol A: reducir `BATCH_SIZE = 5` |
| `WriteTimeout` | Sobrecarga en Cassandra | Rol A: reducir `BATCH_SIZE = 5` |
| Config muestra IPs incorrectas | `.env` mal configurado | Revisar `.env` en raíz del repo |
| `403 Forbidden` en API | Key incorrecta | Rol D: verificar `api/keys.json` |
| Neo4j vacío tras Spark | Job de Spark falló | Rol C: revisar `logs/spark.log` |
| Neo4j tiene datos de otro proyecto | Reset pendiente | Rol B: `MATCH (n) DETACH DELETE n` en `localhost:7474` |
