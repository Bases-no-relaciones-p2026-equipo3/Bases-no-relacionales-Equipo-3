# Instructivo de Despliegue — Roles Intercambiables

Pipeline de tráfico aéreo en tiempo real sobre red WireGuard.

```
OpenSky API → Cassandra → Spark → Neo4j → FastAPI
  (Rol A)      (Rol A)   (Rol C)  (Rol B)  (Rol D)
```

Cualquier integrante puede tomar cualquier rol. Solo hay que ajustar
el `.env` según quién haga qué en cada sesión. No se tocan IPs dentro de los scripts.

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
git clone https://github.com/Bases-no-relaciones-p2026-equipo3/Bases-no-relacionales-Equipo-3.git
cd Bases-no-relacionales-Equipo-3
git pull
```

- [ ] Dependencias instaladas:

```powershell
uv sync
```

- [ ] Definir quién toma cada rol al inicio de la sesión y anotar sus IPs:

```
Rol A (Cassandra + Ingesta) → ____________  IP: 10.15.20.___
Rol B (Neo4j)               → ____________  IP: 10.15.20.___
Rol C (Spark)               → ____________  IP: 10.15.20.___
Rol D (API)                 → ____________  IP: 10.15.20.___
```

- [ ] Cada integrante crea su `.env` con las IPs de la tabla de arriba (ver sección de cada Rol):

```powershell
copy .env.example .env
# Editar .env con los valores de tu rol
```

> `config.py` carga el `.env` automáticamente. No es necesario editar ningún script.

---

## Orden de arranque — respetar siempre este orden

```
1. Rol A → levantar Cassandra
2. Rol B → levantar Neo4j
3. Rol A → correr setup (solo primera vez o tras reset)
4. Rol A → correr ingesta
5. Rol C → correr orquestador (esperar 5 min de ingesta primero)
6. Rol D → levantar API (esperar al menos 1 run de Spark)
```

---

## Rol A — Cassandra + Ingesta

### Configurar tu `.env`

```env
OPENSKY_CLIENT_ID     = tu-client-id
OPENSKY_CLIENT_SECRET = tu-client-secret

CASSANDRA_HOST     = localhost       # Cassandra corre en tu propia máquina
CASSANDRA_PORT     = 9041
CASSANDRA_USER     = cassandra
CASSANDRA_PASSWORD = cassandra

NEO4J_HOST     = <IP_ROL_B>         # IP de quien tenga Neo4j
NEO4J_PORT     = 7687
NEO4J_USER     = neo4j
NEO4J_PASSWORD = password
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

Avisar al equipo que Cassandra está lista.

### Paso 2 — Setup inicial

> Solo necesario la primera vez o si se hizo un reset completo.

```powershell
uv run python setup/cassandra_schema_migration.py
uv run python setup/load_airports.py
uv run python setup/neo4j_setup_indexes.py
```

Salida esperada:
```
✅ Keyspace 'opensky' listo.
✅ Tabla 'flight_events' lista.
✅ Tabla 'airports' lista.
✅ 4,963 aeropuertos cargados en 6.3s
✅ Setup de Neo4j completado.
```

> Si `neo4j_setup_indexes.py` falla, Rol B aún no levantó Neo4j. Esperar y volver a correr solo ese script.

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
| Nodos no aparecen como `healthy` | Esperar 60s más y volver a verificar |

---

## Rol B — Neo4j

### Configurar tu `.env`

```env
CASSANDRA_HOST     = <IP_ROL_A>     # IP de quien tenga Cassandra
CASSANDRA_PORT     = 9041
CASSANDRA_USER     = cassandra
CASSANDRA_PASSWORD = cassandra

NEO4J_HOST     = localhost           # Neo4j corre en tu propia máquina
NEO4J_PORT     = 7687
NEO4J_USER     = neo4j
NEO4J_PASSWORD = password
```

### Paso 1 — Levantar Neo4j

```powershell
docker start neo4j-instance
```

Verificar:

```powershell
docker ps | findstr neo4j
Test-NetConnection -ComputerName localhost -Port 7687
# TcpTestSucceeded: True
```

Avisar al equipo que Neo4j está lista.

### Paso 2 — Monitorear el grafo

Abrir en el navegador: `http://localhost:7474`
- Usuario: `neo4j`
- Contraseña: `password`

Después de que Rol C corra Spark por primera vez, verificar:

```cypher
MATCH (a:Aircraft) RETURN count(a) AS aeronaves
MATCH (c:Country) RETURN count(c) AS paises
MATCH ()-[r]->() RETURN type(r), count(r) AS total ORDER BY total DESC
```

### Si necesitas resetear manualmente

```cypher
MATCH (n) DETACH DELETE n
```

> El orquestador de Rol C hace esto automáticamente al arrancar — no es necesario hacerlo a mano salvo emergencias.

---

## Rol C — Spark + Orquestador

### Configurar tu `.env`

```env
CASSANDRA_HOST     = <IP_ROL_A>     # IP de quien tenga Cassandra
CASSANDRA_PORT     = 9041
CASSANDRA_USER     = cassandra
CASSANDRA_PASSWORD = cassandra

NEO4J_HOST     = <IP_ROL_B>         # IP de quien tenga Neo4j
NEO4J_PORT     = 7687
NEO4J_USER     = neo4j
NEO4J_PASSWORD = password

SPARK_MASTER     = local[*]
SPARK_DRIVER_MEM = 2g
SPARK_EXEC_MEM   = 2g
```

> Si Rol C y Rol A son la misma persona: `CASSANDRA_HOST = localhost`
> Si Rol C y Rol B son la misma persona: `NEO4J_HOST = localhost`

### Paso 1 — Verificar conectividad

```powershell
Test-NetConnection -ComputerName <IP_ROL_A> -Port 9041   # Cassandra
Test-NetConnection -ComputerName <IP_ROL_B> -Port 7687   # Neo4j
```

Ambos deben dar `TcpTestSucceeded: True`.

### Paso 2 — Correr el orquestador

> ⚠️ Esperar confirmación de Rol A de que la ingesta lleva al menos 5 minutos.

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
Ingesta PID XXXXX corriendo.
Esperando 5 min antes del primer job Spark...
```

Tras 5 minutos:
```
[UTC] Lanzando job Spark...
Job Spark completado exitosamente.
Próximo job Spark en 5 min.
```

### Si spark-submit no se encuentra

```powershell
$env:PATH += ";C:\spark\bin"   # ajustar a tu ruta de instalación
spark-submit --version         # verificar
```

### Monitorear logs

```powershell
Get-Content logs/spark.log   -Wait -Tail 30
Get-Content logs/ingesta.log -Wait -Tail 20
```

---

## Rol D — API + Análisis

### Configurar tu `.env`

```env
CASSANDRA_HOST     = <IP_ROL_A>     # IP de quien tenga Cassandra
CASSANDRA_PORT     = 9041

NEO4J_HOST     = <IP_ROL_B>         # IP de quien tenga Neo4j
NEO4J_PORT     = 7687
NEO4J_USER     = neo4j
NEO4J_PASSWORD = password
```

> Si Rol D y Rol B son la misma persona: `NEO4J_HOST = localhost`

### Paso 1 — Crear API keys

Generar una key por integrante:

```powershell
python -c "import secrets; print('sk-nombre-' + secrets.token_urlsafe(24))"
```

Crear `api/keys.json`:

```json
{
  "nombre1": "sk-nombre1-RESULTADO",
  "nombre2": "sk-nombre2-RESULTADO",
  "nombre3": "sk-nombre3-RESULTADO",
  "nombre4": "sk-nombre4-RESULTADO"
}
```

> `api/keys.json` está en `.gitignore` — compartir las keys por otro medio (WhatsApp, etc.).

### Paso 2 — Levantar la API

```powershell
cd api
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

La API queda disponible para todo el equipo en:

```
http://<IP_ROL_D>:8000/docs
```

### Paso 3 — Probar

```powershell
$KEY = "sk-nombre-TU_KEY"
$API = "http://<IP_ROL_D>:8000"

curl $API/health
curl -H "X-API-Key: $KEY" "$API/analytics/top-countries"
curl -H "X-API-Key: $KEY" "$API/analytics/top-routes?limit=10"
```

---

## Tabla de configuración rápida

Llenar al inicio de cada sesión y compartir con el equipo:

```
Rol A (Cassandra) → nombre: ________  IP: 10.15.20.___
Rol B (Neo4j)     → nombre: ________  IP: 10.15.20.___
Rol C (Spark)     → nombre: ________  IP: 10.15.20.___
Rol D (API)       → nombre: ________  IP: 10.15.20.___
```

Con esa tabla, cada quien rellena su `.env` y ya puede correr cualquier script sin más cambios.

---

## Resolución de problemas

| Error | Causa probable | Solución |
|---|---|---|
| `NoHostAvailable` | Cassandra no corre | Rol A: `docker start cassandra-node-1 cassandra-node-2 cassandra-node-3` |
| `TcpTestSucceeded: False` a Cassandra | WireGuard o Docker caído | Verificar WireGuard y que Rol A tiene Docker corriendo |
| `ServiceUnavailable` en Neo4j | Neo4j no corre | Rol B: `docker start neo4j-instance` |
| `spark-submit no encontrado` | Spark no está en PATH | Rol C: agregar `$SPARK_HOME/bin` al PATH |
| `Batch size exceeding threshold` | Batch demasiado grande | Rol A: `BATCH_SIZE = 5` en `opensky_to_cassandra.py` |
| `WriteTimeout` | Sobrecarga en Cassandra | Rol A: `BATCH_SIZE = 5` |
| Neo4j vacío tras Spark | Job de Spark falló | Rol C: revisar `logs/spark.log` |
| `403 Forbidden` en API | Key incorrecta | Rol D: verificar `api/keys.json` |
| Neo4j tiene datos de otra sesión | Reset no ocurrió | Rol B: `MATCH (n) DETACH DELETE n` en Neo4j browser |
| Variables de entorno no cargadas | `.env` no existe o tiene errores | Verificar que copiaste `.env.example` a `.env` y rellenaste los valores |
