# Cómo correr el proyecto

Pipeline de tráfico aéreo en tiempo real: OpenSky → Cassandra → Spark → Neo4j → FastAPI

---

## Prerequisitos

| Herramienta | Versión requerida | Notas |
|---|---|---|
| Python | ≥ 3.13 | Administrado con `uv` |
| **PySpark** | **== 3.5.1** | Ver advertencia abajo |
| Java (JDK) | 17 ó 23 | Requerido por Spark |
| Docker | Cualquier versión reciente | Para Cassandra y Neo4j |
| `uv` | Cualquier versión reciente | `pip install uv` |

> [!CAUTION]
> **Usa exactamente `pyspark==3.5.1` — no instales Spark 4.x.**
>
> Los conectores del proyecto están compilados para **Spark 3.x / Scala 2.12**:
> ```
> com.datastax.spark:spark-cassandra-connector_2.12:3.5.1
> org.neo4j:neo4j-connector-apache-spark_2.12:5.3.2_for_spark_3
> ```
> Spark 4.x usa **Scala 2.13** e introduce incompatibilidades binarias que producen el error:
> ```
> java.lang.NoSuchMethodError: scala.jdk.CollectionConverters$.mapAsScalaMapConverter
> ```
> Si usas `brew install apache-spark` en macOS obtendrás Spark 4.1.1 — **no lo uses para correr el pipeline**. Usa siempre el `spark-submit` del ambiente virtual del proyecto.

---

## Paso 1 — Instalar el ambiente virtual

```bash
# Desde la raíz del repositorio
uv sync
```

Si ya tienes `pyspark` instalado con una versión incorrecta:

```bash
uv remove pyspark
uv add "pyspark==3.5.1"
```

---

## Paso 2 — Verificar que Spark es la versión correcta

```bash
# macOS / Linux
source .venv/bin/activate
export PATH="$PWD/.venv/bin:$PATH"

which spark-submit       # debe apuntar a .venv/bin/spark-submit
spark-submit --version   # debe mostrar: version 3.5.1, Using Scala version 2.12...
python -c "import pyspark; print(pyspark.__version__)"   # debe imprimir: 3.5.1
```

```powershell
# Windows (PowerShell)
.\.venv\Scripts\activate
python -c "import pyspark; print(pyspark.__version__)"   # debe imprimir: 3.5.1
```

> [!NOTE]
> El orquestador (`pipeline_orchestrator.py`) valida la versión al arrancar y termina con un mensaje claro si detecta una versión incorrecta. No es necesario verificarlo manualmente cada vez.

---

## Paso 3 — Configurar variables de entorno

Copia la plantilla y edítala con las IPs y credenciales de tu sesión:

```bash
# macOS / Linux
cp env.example .env

# Windows
copy env.example .env
```

El archivo `.env` **nunca se sube al repositorio** (está en `.gitignore`). Cada integrante del equipo configura su propio `.env` según su rol.

### 💡 Configuración de Red Automática (mDNS)

Para evitar reconfigurar IPs cada vez que cambias de red (ej. de casa al ITAM), el proyecto usa nombres de host `.local`.

1. **Obtén tu nombre de host**:
   - En **macOS / Linux**: corre `hostname` en la terminal.
   - En **Windows**: corre `hostname` en PowerShell.
   *Ejemplo: si tu comando devuelve `Laptop-Regina`, tu dirección es `Laptop-Regina.local`.*

2. **Configura el `.env`**:
   Reemplaza los valores con los nombres `.local` de tus compañeros:
   ```python
   CASSANDRA_HOST = CompuAndre.local
   NEO4J_HOST     = CompuAndre.local
   SPARK_MASTER   = spark://Laptop-Regina.local:6077
   ```

---

## Paso 4 — Levantar la Infraestructura (Docker)

En el modo distribuido, cada integrante levanta **solo** el componente que le corresponde. Desde la raíz del repositorio, ejecuta el comando según tu rol:

### Si eres Andre (Cassandra):
```bash
docker-compose -f infra/cassandra/cassandra-cluster.docker-compose.yml up -d
```

### Si eres Víctor (Neo4j):
```bash
docker-compose -f infra/neo4j/neo4j.docker-compose.yml up -d
```

### Si eres Regina (Spark):
```bash
docker-compose -f infra/spark/spark-cluster.docker-compose.yml up -d
```

### Si eres Fernanda (API):
No necesitas Docker local si vas a consumir los servicios de tus compañeros; solo asegúrate de que tu `.env` tenga las IPs de los demás.

> [!TIP]
> Puedes verificar que tu componente subió correctamente con `docker ps`. Si un contenedor se detiene, revisa los logs con `docker logs <nombre-contenedor>`.

---

## Paso 5 — Inicializar las bases de datos (solo la primera vez)

> Solo es necesario correr este paso la primera vez, o después de un reset completo de datos.

```bash
# 1. Crear tablas en Cassandra y cargar catálogo de aeropuertos (~5,000 aeropuertos)
uv run python setup/cassandra_schema_migration.py
uv run python setup/load_airports.py

# 2. Crear constraints e índices en Neo4j
uv run python setup/neo4j_setup_indexes.py
```

Salida esperada al finalizar:
```
✅ Keyspace 'opensky' listo.
✅ Tabla 'state_vectors' lista.
✅ Tabla 'flight_events' lista.
✅ Tabla 'airports' lista.
✅ 4,963 aeropuertos cargados.
✅ Setup de Neo4j completado.
```

---

## Paso 5 — Correr el pipeline

Con Cassandra y Neo4j activos (Docker), ejecutar desde la raíz del repositorio:

```bash
uv run python pipeline_orchestrator.py
```

El orquestador al arrancar:
1. Valida que `pyspark==3.5.1` esté instalado (falla rápido si no).
2. Limpia `state_vectors` y `flight_events` en Cassandra, y todos los nodos en Neo4j.
3. Lanza en paralelo dos hilos:
   - **Ingesta** — polling a OpenSky cada 20 s → inserta en Cassandra (corre de forma continua).
   - **Spark** — espera 5 min, luego lanza `spark-submit` periódicamente para procesar Cassandra → Neo4j.

Detener con `Ctrl+C` para un cierre limpio.

---

## Paso 6 — Levantar la API (opcional)

Una vez que el pipeline lleva al menos un ciclo de Spark (≥ 5 min), la API puede servir consultas.

Desde la **raíz del repositorio** (no desde `api/`):

```bash
uv run uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

Swagger UI disponible en `http://<tu-IP-WireGuard>:8000/docs`.

> [!NOTE]
> No uses `cd api && uvicorn main:app` — el path de `config.py` se resuelve
> relativo al archivo, así que funciona desde cualquier directorio.
> Las dependencias de `api/requirements.txt` ya están incluidas en el venv raíz;
> no es necesario instalarlas por separado.

Antes de levantarla, crea el archivo `api/keys.json`:

```bash
# Genera una key por integrante (ejecutar una vez por persona)
python -c "import secrets; print('sk-nombre-' + secrets.token_urlsafe(24))"
```

```json
{
  "andre":    "sk-andre-...",
  "victor":   "sk-victor-...",
  "regina":   "sk-regina-...",
  "fernanda": "sk-fernanda-..."
}
```

`keys.json` **no se sube al repo** (está en `.gitignore`). Distribúyelas por WhatsApp.

---

## Monitoreo de logs en tiempo real

```bash
# macOS / Linux
tail -f logs/orchestrator.log
tail -f logs/ingesta.log
tail -f logs/spark.log
```

```powershell
# Windows
Get-Content logs\orchestrator.log -Wait -Tail 30
Get-Content logs\ingesta.log      -Wait -Tail 20
Get-Content logs\spark.log        -Wait -Tail 50
```

---

## Resolución de problemas rápida

| Síntoma | Causa probable | Solución |
|---|---|---|
| `VERSION DE PYSPARK INCORRECTA` al arrancar | Spark 4.x instalado | `uv remove pyspark && uv add pyspark==3.5.1` |
| `NoHostAvailable` | Cassandra no corre | `docker start cassandra-node-1 cassandra-node-2 cassandra-node-3` |
| `ServiceUnavailable` en Neo4j | Neo4j no corre | `docker start neo4j-instance` |
| Job Spark termina con código ≠ 0 | Error interno del job | Revisar `logs/spark.log` |
| Neo4j vacío después de Spark | Problema de red Docker | Verificar que los contenedores Spark están en las redes `cassandra-net` y `neo4j-net` |
| `WriteTimeout` en Cassandra | Sobrecarga de batch | Reducir `BATCH_SIZE = 5` en `ingesta/opensky_to_cassandra.py` |
| API da `403 Forbidden` | Key incorrecta | Verificar `api/keys.json` |

Para problemas de despliegue distribuido (WireGuard, IPs, roles), ver `INSTRUCTIVO_DESPLIEGUE.md`.
---

## Paso 7 — Análisis y Consultas (Notebook)

El archivo `analisis/opensky_neo4j_queries.ipynb` permite ejecutar las consultas analíticas de forma interactiva.

1.  Asegúrate de que tu `.env` tenga la IP correcta de la persona que levantó Neo4j (`NEO4J_HOST`).
2.  Abre el notebook en VS Code o Jupyter Lab.
3.  La primera celda de configuración está diseñada para leer automáticamente de `config.py`. 
4.  Si cambias de host en el `.env`, solo necesitas reiniciar el kernel del notebook y volver a correr las celdas para conectar con el nuevo nodo.

---
---

## Paso 8 — Validación de Caudal (Stress Test)

Para demostrar que la base de datos de ingesta soporta el volumen de entrada sin pérdida de mensajes:

1.  Asegúrate de que tus contenedores de Cassandra estén corriendo.
2.  Ejecuta el script de prueba:
    ```powershell
    & .venv/Scripts/python.exe setup/test_load_cassandra.py
    ```
3.  El script insertará 5,000 registros y mostrará un resumen de rendimiento (Throughput) y tasa de error.

---
---

## Paso 9 — Ejecución de Consultas de Análisis

El sistema ofrece dos formas de consultar los hallazgos analíticos:

### A. Vía Jupyter Notebook (Exploración)
1.  Abre `analisis/opensky_neo4j_queries.ipynb`.
2.  Asegúrate de que el kernel de Python sea el de tu `.venv`.
3.  Ejecuta las celdas para ver visualizaciones de:
    - **Proximidad**: Aeronaves que estuvieron cerca en el aire.
    - **Rutas**: Aeropuertos de origen y destino detectados.
    - **Estadísticas**: Países con mayor tráfico en tiempo real.

### B. Vía API REST (Producción)
1.  Inicia la API: `uv run uvicorn api.main:app --host 0.0.0.0 --port 8000`.
2.  Usa un cliente como Postman o `curl`.
3.  **Importante**: Debes incluir una API Key válida de Cassandra en el header.
    - Ejemplo: `GET http://localhost:8000/analytics/top-countries`
    - Header: `X-API-Key: admin-key-2026`

---
