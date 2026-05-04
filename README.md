# Tráfico Aéreo en Tiempo Real con OpenSky
![flights_map](images/flights_map.png)
## Bases de Datos No Relacionales | Equipo 3
| Nombre | Clave Única |
| :--- | :---: |
| Irene Escudero Cazarez | 000215698 |
| Maria Fernanda Leon Hernandez | 000212576 |
| Regina Maria Cortés Vera | 000206138 |
| Ricardo André Gorostieta Jurado | 000217746 |
| Victor Manuel Benitez Renteria | 000207736 |

## Enlaces a la API y documentación del stream

Documentación y recursos oficiales:

- **Documentación oficial de la REST API:**  
  https://opensky-network.org/apidoc/rest.html

- **Sitio oficial de OpenSky Network:**  
  https://opensky-network.org

- **Repositorio de documentación y ejemplos de uso:**  
  https://github.com/openskynetwork

- **Biblioteca Python para consumir la API:**  
  https://github.com/openskynetwork/opensky-api



---

## Descripción del stream de datos


El stream de datos utilizado en este proyecto proviene de la **OpenSky Network**, una red global de sensores que recolecta información transmitida por aeronaves mediante tecnología **ADS-B (Automatic Dependent Surveillance–Broadcast)** y **Mode-S**. Estas señales son emitidas periódicamente por los transpondedores de los aviones y contienen información sobre su estado de vuelo.

La plataforma OpenSky recopila estas señales desde miles de receptores distribuidos en todo el mundo y las pone a disposición a través de una API pública que permite consultar el estado actual de las aeronaves.

Cada evento dentro del stream corresponde a un **state vector** de una aeronave en un momento determinado. Un state vector describe el estado del avión e incluye múltiples atributos, entre ellos:

- Identificador único del avión (`icao24`)
- Callsign del vuelo
- País de origen del registro de la aeronave
- Posición geográfica (latitud y longitud)
- Altitud
- Velocidad
- Dirección o rumbo del vuelo
- Tasa de ascenso o descenso
- Timestamps de posición y último contacto

Estos datos permiten reconstruir la dinámica del tráfico aéreo global en tiempo casi real. El propósito de este stream es facilitar el análisis del comportamiento del tráfico aéreo, el estudio de patrones de vuelo, la detección de anomalías en trayectorias y el análisis espacial y temporal de la densidad de aeronaves en diferentes regiones.

En este proyecto, cada state vector se considera un **evento del stream**, lo que permite modelar el flujo de datos como una serie de observaciones temporales de aeronaves que pueden ser almacenadas y analizadas utilizando una arquitectura de bases de datos NoSQL.


## Resumen
El flujo de datos seleccionado para este proyecto consiste en un stream de "State Vectors" (vectores de estado) provenientes de la red global OpenSky. Cada evento capturado representa una actualización en tiempo real de la situación física de una aeronave, incluyendo su posición tridimensional, velocidad e identidad. A diferencia de un conjunto de datos estático, este stream nos permite observar la dinámica del transporte aéreo como un sistema vivo y en constante cambio.

En el contexto de nuestro proyecto de Bases de Datos No Relacionales, el objetivo es utilizar este flujo masivo para diseñar una arquitectura de extremo a extremo que sea escalable. El stream de OpenSky es ideal para este propósito ya que genera más de un evento por segundo, permitiéndonos poner a prueba una capa de ingesta de alta disponibilidad con Cassandra y una capa de procesamiento analítico con Neo4j donde transformaremos estos eventos crudos en información estratégica. El enfoque del equipo será demostrar cómo estos datos, una vez ingestados y enriquecidos, pueden revelar patrones de tráfico, saturación de rutas y comportamientos operativos mediante consultas complejas.


## Origen y Autoría
La información utilizada en este proyecto es recolectada, procesada y distribuida por The OpenSky Network, una organización de investigación científica sin fines de lucro con sede en Suiza. Este proyecto surgió en 2012 como una colaboración académica entre la Universidad de Kaiserslautern (Alemania), la Universidad de Oxford (Reino Unido) y armasuisse (Suiza).

A diferencia de los radares comerciales cerrados, la autoría y obtención de estos datos es comunitaria y colaborativa. El pilar tecnológico de esta red es el sistema ADS-B (Automatic Dependent Surveillance-Broadcast), el cual permite que las aeronaves determinen su propia posición y velocidad mediante GPS para después transmitirla periódicamente en la frecuencia de radio de 1090 MHz.

Para capturar esta información a gran escala, OpenSky opera una infraestructura masiva de receptores distribuidos por todo el mundo. Esta red se mantiene gracias al apoyo de voluntarios, socios industriales y organizaciones gubernamentales o académicas que alojan los sensores en sus propias ubicaciones. OpenSky actúa como el nodo central que cosecha estos datos vía Internet, estandariza las señales recibidas y las transforma en el formato estructurado de "State Vectors" que consumimos a través de su API para este análisis.


### Diccionario de Datos
El stream de OpenSky Network transmite el estado de aeronaves detectadas por la red de sensores ADS-B. Cada registro representa la información más reciente de una aeronave en un momento determinado.
| Atributo | Definición técnica | Tipo de dato |
|-----------|-----------|-----------|
|Hex ID  | Identificador único ICAO de la aeronave    | String    |
|Callsign| Identificador del vuelo asignado por la aerolínea    | String    |
|Route   | Ruta estimada del vuelo entre aeropuerto origen y destino   |  String  |
|Registration  |   Matrícula oficial de la aeronave  | String   |
|Type  | Modelo o tipo de aeronave   |  String  |
|Squawk   | Código transponder asignado por control de tráfico aéreo  |  Integer  |
|Alt (ft)	| Altitud actual de la aeronave	| Numérico (pies) |
|Spd (kt)	| Velocidad horizontal del avión	| Numérico (knots) |
|V. Rate (ft/min) |	Tasa de ascenso o descenso del avión	| Numérico (pies/minuto) |
|Dist (nm)	| Distancia estimada respecto al receptor	| Numérico (millas náuticas) |
|Track |	Dirección de movimiento del avión	| Numérico (grados) |
|Messages	| Número de mensajes ADS-B recibidos	| Integer |
|Seen	| Tiempo desde el último mensaje recibido |	Numérico (segundos) |
|RSSI	| Intensidad de señal recibida	Numérico (dB) |
|Latitude |	Latitud de la aeronave	| Numérico (grados) |
|Longitude	| Longitud de la aeronave	| Numérico (grados) |
|Source	| Tipo de fuente de datos (ej. ADS-B)	| String |
|Mil.	| Indicador de aeronave militar	| Boolean |
|Wind D.	| Dirección del viento estimada |	Numérico (grados) |
|Wind (kt)	| Velocidad del viento	| Numérico (knots)|

### Variables Cuantitativas
Las variables cuantitativas corresponden a atributos numéricos que describen el estado físico y dinámico de las aeronaves.
Entre ellas se encuentran:
- Alt (ft) – altitud de vuelo
- Spd (kt) – velocidad del avión
- V. Rate (ft/min) – velocidad vertical
- Dist (nm) – distancia respecto al receptor
- Track – dirección de desplazamiento
- Latitude – coordenada geográfica de latitud
- Longitude – coordenada geográfica de longitud
- Messages – número de mensajes recibidos
- Seen – tiempo desde el último contacto
- RSSI – intensidad de señal
- Wind D. – dirección del viento
- Wind (kt) – velocidad del viento
- Estas variables permiten analizar patrones de tráfico aéreo, velocidad, altitud, trayectoria y condiciones ambientales del vuelo.

## Variables Cualitativas
Las variables cualitativas que corresponden a esta base de datos donde se representan grupos o identidades fijas son: 
- icao24 - identificador único y permanente de cada transpondedor de la aeronave
- callsign - código de identificación del vuelo (identifica la operación actual)
- origin_country - ubicación de registro de la aeronave
- on_ground - categoría dicotómica: true o false
- posicion_source - origen de los datos
- category - tipo de vehiculo según su peso

## Texto No Estructurado
Los sistemas de control de tráfico aereo requieren datos altamente estructurados y ligeros para ser transmitidos por radiofrecuencia. En general, el texto no estructurado es inexistente en la transmisión de estos datos en vivo. Existe un campo llamado 'sensors' que es una lista de IDs que se refieren a la red de receptores terrestres que escuchan las transmisiones de las aeronaves que podría ser lo más parecido a el texto no estructurado, sin embargo, este no se trata de un texto libre, si no, es una lista de códigos. 

## Series Temporales
La API-Rest de Open sky no cuenta con series temporales como tal. Funciona a base de snapshots, pequeñas capturas que reflejan el momento (timestamp) cuando la aeronave ha hecho contacto. Ademas de esto, la API cuenta propiamente con la opcion de filtrar mediante lapsos de tiempo, con un maximo de dos horas, donde filtra mediante los tiempos registrados en las snapshots que se empatan con el intervalo seleccionado.

## Consideraciones Éticas

El procesamiento de datos de vigilancia aérea, aunque se basa en señales públicas (ADS-B), conlleva responsabilidades éticas y riesgos de sesgo que el equipo debe considerar:
- Seguimiento de Individuos: El uso de icao24 para rastrear aeronaves privadas de forma persistente puede derivar en problemas de privacidad. Nuestro enfoque se limita al análisis de flujos agregados y patrones de tráfico, evitando el monitoreo de objetivos individuales.

- La red OpenSky depende de receptores voluntarios (crowdsourcing). Esto genera un sesgo de disponibilidad: las regiones con mayor infraestructura tecnológica (Europa y Norteamérica) presentan una densidad de datos artificialmente superior a la de regiones en desarrollo. Es éticamente necesario aclarar que la ausencia de datos en ciertas zonas no implica falta de tráfico, sino falta de sensores.

- Al ser una organización sin fines de lucro enfocada en la investigación, el uso ético de estos datos implica respetar los términos de servicio para fines académicos. Existe el riesgo de que análisis erróneos o interpretaciones simplistas de anomalías en el stream generen alarmas innecesarias sobre la seguridad aérea.

---

## 2. Infraestructura y Configuración

Para este proyecto desplegamos una arquitectura distribuida en cuatro máquinas conectadas mediante una red privada WireGuard. La idea general es separar la captura de datos en tiempo real del análisis posterior, usando una tecnología distinta para cada tarea según las necesidades del proyecto en cada momento.

El flujo del dato es el siguiente:

```
OpenSky API  →  Cassandra  →  Spark  →  Neo4j  →  FastAPI
```

OpenSky es la fuente externa que nos entrega los state vectors de las aeronaves. Un programa de Python en la máquina de ingesta se conecta cada 20 segundos y guarda lo recibido en Cassandra. Cada 5 minutos un job de Spark lee lo acumulado en Cassandra, lo limpia, detecta despegues y aterrizajes, y construye un grafo en Neo4j. Finalmente una API en FastAPI expone consultas analíticas a partir del grafo.

### Estructura del proyecto

El repositorio está organizado por capas del pipeline. Cada carpeta corresponde a una etapa del flujo de datos o a un componente del sistema.

```
BASES-NO-RELACIONALES-EQUIPO-3/
│
├── README.md                       ← Documentación principal del proyecto
├── config.py                       ← Configuración centralizada (lee del .env)
├── env.example                     ← Plantilla del archivo .env
├── pipeline_orchestrator.py        ← Punto de entrada: arranca ingesta + Spark
├── pyproject.toml                  ← Dependencias del proyecto
├── uv.lock                         ← Versiones exactas de las dependencias
├── .gitignore                      ← Archivos que no se suben al repo
│
├── Documentacion/                  ← Documentación operativa interna del equipo
│   ├── README.md
│   ├── ARQUITECTURA_TECNICA.md
│   └── COMO_CORRER.md
│
├── infra/                          ← Configuración de infraestructura
│   ├── cassandra/
│   │   ├── cassandra.yaml
│   │   └── cassandra-cluster.docker-compose.ylm
│   │   └──cassandra-rackdc.properties
│   │   └──cassandra_infra.ipynb
│   ├── neo4j/
│   |    ├── neo4j.docker-compose.yml
│   |    └── neo4j_infra.ipynb
│   └── spark/
│       ├── dockerfile.spark-job-venv
│       ├──dockerfile.spark-jupyter
│       ├── spark-cluster.docker-compose.yml
│       └──spark_infra.ipynb
│
├── setup/                          ← Scripts que se corren una sola vez al inicio
│   ├── cassandra_schema_migration.py
│   ├── load_airports.py
│   └── neo4j_setup_indexes.py
│
├── ingesta/                        ← Capa de ingesta (OpenSky → Cassandra)
│   └── opensky_to_cassandra.py
│
├── procesamiento/                  ← Capa OLAP (Cassandra → Neo4j vía Spark)
│   └── cassandra_to_neo4j_spark.py
│
├── analisis/                       ← Consultas analíticas sobre el grafo
│   └── opensky_neo4j_queries.ipynb
│
├── api/                            ← API REST que expone el grafo
│   ├── main.py
│   ├── auth.py
│   ├── queries.py
│   ├── keys.json
│   └── requirements.txt
│
├── logs/                           ← Logs de ejecución generados en runtime
│   ├── ingesta.log
│   ├── spark.log
│   └── orchestrator.log
│
└── images/                         ← Imágenes utilizadas en el README
```

#### Descripción de cada carpeta

| Carpeta | Función |
|---|---|
| **(raíz)** | Contiene el README principal, la configuración centralizada (`config.py`), la plantilla del entorno (`env.example`), el orquestador del pipeline (`pipeline_orchestrator.py`) y los archivos de gestión de dependencias. |
| **`Documentacion/`** | Documentación operativa interna del equipo. Incluye los instructivos de despliegue paso a paso usados durante las sesiones de trabajo. No forma parte de la entrega evaluable, sirve como guía de operación. |
| **`infra/`** | Configuración de infraestructura. La subcarpeta `cassandra/` contiene los archivos `cassandra.yaml` y `cassandra-rackdc.properties` que definen los parámetros del nodo Cassandra. La subcarpeta `docker/` contiene los Dockerfiles que construyen las imágenes de Spark utilizadas para procesamiento. |
| **`setup/`** | Scripts de inicialización que se ejecutan una sola vez al desplegar el proyecto. Crean el keyspace y las tablas en Cassandra, cargan el catálogo de aeropuertos y crean los constraints e índices en Neo4j. |
| **`ingesta/`** | Capa de ingesta. Contiene el script que se conecta a la API de OpenSky cada 20 segundos y guarda los state vectors en la tabla `state_vectors` de Cassandra. |
| **`procesamiento/`** | Capa OLAP. Contiene el job de Spark que lee los datos crudos de Cassandra, los limpia, detecta despegues y aterrizajes, y construye el grafo en Neo4j. Lo lanza el orquestador cada 5 minutos. |
| **`analisis/`** | Notebook de Jupyter con las consultas analíticas (Cypher) que se ejecutan sobre el grafo de Neo4j. |
| **`api/`** | API REST construida con FastAPI. Expone los resultados del análisis vía endpoints HTTP, con autenticación por API key (header `X-API-Key`). |
| **`logs/`** | Archivos de log generados en tiempo de ejecución por la ingesta, el job de Spark y el orquestador. Sirven como evidencia de operación y para diagnóstico. |
| **`images/`** | Imágenes utilizadas en el README (mapas, capturas, diagramas). |

#### Archivos clave en la raíz

| Archivo | Función |
|---|---|
| `README.md` | Documentación principal del proyecto. Contiene la descripción del stream, la documentación de la infraestructura, las decisiones de arquitectura y los hallazgos del análisis. |
| `config.py` | Carga centralizada de configuración. Todos los scripts importan de aquí los hosts, puertos y credenciales de Cassandra, Neo4j y Spark. La configuración se toma del archivo `.env` o de variables de entorno del sistema. |
| `env.example` | Plantilla del archivo `.env`. Cada miembro del equipo la copia a `.env` (excluido del repo) y la rellena con los valores correspondientes a su rol. |
| `pipeline_orchestrator.py` | Punto de entrada del sistema. Limpia las bases de datos al iniciar, arranca la ingesta como proceso continuo y lanza el job de Spark cada 5 minutos en paralelo. |
| `pyproject.toml` y `uv.lock` | Definición y bloqueo de dependencias del proyecto, gestionadas con `uv`. |
| `.gitignore` | Lista de archivos y carpetas que no se versionan (incluye `.env`, `keys.json` y los logs). |

### Capa de ingesta (operativa): Apache Cassandra

Para la capa de ingesta elegimos **Apache Cassandra**, una base de datos NoSQL de tipo columnar pensada para escritura intensiva en clusters distribuidos. La razón principal es que el stream de OpenSky genera ráfagas constantes de eventos y no podemos pedirle a la fuente que reenvíe lo que se pierda, así que el sistema tiene que aceptar escrituras siempre y de forma rápida. Cassandra está diseñada exactamente para eso: cada escritura se acepta primero en memoria y se escribe a disco después de manera asíncrona.

Cassandra también ofrece de manera nativa los dos requisitos del rubro:

- **Replicación** mediante el `replication_factor` del keyspace. Cada partición de datos se copia en varios nodos a la vez, así que si un nodo se cae el cluster sigue operando con los demás.
- **Sharding** automático mediante el particionador `Murmur3Partitioner` y *vnodes*. Los datos se reparten por hash entre los nodos sin que tengamos que hacer nada manual: Cassandra reasigna rangos cuando se agrega o quita un nodo.

#### Topología del cluster

Levantamos **tres nodos de Cassandra** (`cassandra-node-1`, `cassandra-node-2`, `cassandra-node-3`) en contenedores Docker, todos en el mismo datacenter lógico (`dc=dc1`, `rack=rack1`). El archivo `infra/cassandra/cassandra-rackdc.properties` define esa topología, y `infra/cassandra/cassandra.yaml` contiene los parámetros principales del nodo.

Los parámetros más relevantes son:

| Parámetro | Valor | Para qué sirve |
|---|---|---|
| `partitioner` | `Murmur3Partitioner` | Aplica un hash a la *partition key* para repartir los datos uniformemente entre nodos. |
| `num_tokens` | `16` | Cada nodo maneja 16 vnodes; permite un sharding fino y rebalanceo automático. |
| `endpoint_snitch` | `GossipingPropertyFileSnitch` | Permite definir datacenter y rack manualmente, requisito para usar `NetworkTopologyStrategy`. |
| `native_transport_port` | `9042` (mapeado a `9041` en el host) | Puerto CQL para conectarse desde el driver de Python. |
| `rpc_address` | `0.0.0.0` | Cassandra escucha en todas las interfaces, necesario para aceptar conexiones desde la red WireGuard. |

#### Replicación

El keyspace `opensky` se crea con `NetworkTopologyStrategy` y replication factor de 3, lo que significa que cada partición vive en los tres nodos. Esto permite tolerar la caída de un nodo sin pérdida de datos ni interrupción del servicio.

```cql
CREATE KEYSPACE IF NOT EXISTS opensky
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'dc1': 3
};
```

#### Tablas principales

**`state_vectors`** — almacena el estado más reciente de cada aeronave en cada captura. Es la tabla que recibe la ingesta directa desde OpenSky.

```cql
CREATE TABLE state_vectors (
    icao24          text,
    snapshot_time   timestamp,
    callsign        text,
    origin_country  text,
    longitude       double,
    latitude        double,
    baro_altitude   double,
    geo_altitude    double,
    velocity        double,
    true_track      double,
    vertical_rate   double,
    on_ground       boolean,
    squawk          text,
    spi             boolean,
    position_source int,
    category        int,
    time_position   bigint,
    last_contact    bigint,
    PRIMARY KEY (icao24, snapshot_time)
) WITH CLUSTERING ORDER BY (snapshot_time DESC)
  AND default_time_to_live = 604800;
```

La *partition key* es `icao24` (el código único del avión), lo que hace que todas las observaciones de la misma aeronave caigan en el mismo nodo y que la consulta "trayectoria del avión X" sea muy rápida. La *clustering column* `snapshot_time DESC` ordena las filas físicamente por tiempo descendente, así que la lectura "último estado conocido" es prácticamente instantánea. El TTL de 604 800 segundos (7 días) hace que los datos viejos se borren automáticamente.

**`flight_events`** — guarda los eventos TAKEOFF y LANDING que detecta el job de Spark.

```cql
CREATE TABLE flight_events (
    icao24       text,
    event_time   timestamp,
    event_type   text,
    latitude     double,
    longitude    double,
    airport_icao text,
    airport_name text,
    confidence   text,
    gap_seconds  int,
    batch_id     text,
    PRIMARY KEY (icao24, event_time, event_type)
) WITH CLUSTERING ORDER BY (event_time DESC, event_type ASC)
  AND default_time_to_live = 2592000;
```

**`airports`** — catálogo estático con los aeropuertos grandes y medianos del mundo (descargado de OurAirports.com). Sirve para enriquecer los eventos: cuando Spark detecta un despegue, busca el aeropuerto más cercano dentro de 15 km y lo asocia al evento.

#### Driver y consistencia

La ingesta usa el driver oficial `cassandra-driver` para Python, con `RoundRobinPolicy` como balanceador (reparte las inserciones entre los tres nodos) y un `RetryPolicy` por defecto. Las inserciones se hacen en `BatchStatement` con `BATCH_SIZE = 10` para no rebasar el límite de 5 KB por batch que tiene Cassandra.

El nivel de consistencia por defecto es `LOCAL_ONE` para escrituras (basta con que un nodo confirme, máximo throughput) y `LOCAL_QUORUM` para consultas que necesiten leer datos consistentes (dos de tres nodos deben coincidir).

### Capa de procesamiento analítico (OLAP): Neo4j + Apache Spark

Para la capa OLAP usamos **Neo4j** como base de datos analítica y **Apache Spark** como motor de transformación que mueve datos de Cassandra a Neo4j.

#### Por qué Neo4j

Las preguntas analíticas que queremos responder son inherentemente relacionales: rutas más frecuentes entre aeropuertos, países que comparten más tráfico aéreo, hubs de proximidad entre aeronaves, etc. Resolver estas preguntas en Cassandra requeriría cruces costosos entre tablas porque Cassandra no maneja joins. Neo4j sí: es una base de datos de grafos donde los nodos representan entidades (aviones, países, aeropuertos) y las aristas representan relaciones, y las consultas se expresan como travesías sobre ese grafo, lo cual es muy rápido.

#### Modelo del grafo

El grafo construido por Spark tiene cuatro tipos de nodos y cinco tipos de relaciones:

- Nodos: `Aircraft`, `Country`, `Airport`, `Snapshot`.
- Relaciones: `OPERATES` (un país opera un avión), `SNAPSHOT` (un avión tiene un snapshot), `NEAR` (dos aviones a 50 km o menos en el mismo instante), `DEPARTED_FROM` (un avión despegó de un aeropuerto), `ARRIVED_AT` (un avión aterrizó en un aeropuerto).

Para que las consultas sean rápidas, el script `setup/neo4j_setup_indexes.py` crea constraints de unicidad sobre `Aircraft.icao24`, `Country.name` y `Airport.icao`, además de índices secundarios sobre `Aircraft.callsign`, `Snapshot.snapshot_time`, `Airport.iata` y `Airport.country`.

#### Por qué Spark

Spark es un motor de procesamiento distribuido sin estado que puede leer de Cassandra y escribir en Neo4j gracias a dos conectores:

- `spark-cassandra-connector_2.12:3.5.1`
- `neo4j-connector-apache-spark_2.12:5.3.2_for_spark_3`

El job `procesamiento/cassandra_to_neo4j_spark.py` hace tres cosas: limpieza (filtra nulos, deduplica por `(icao24, snapshot_time)`, normaliza tipos numéricos), enriquecimiento (cruza posiciones con el catálogo de aeropuertos usando la fórmula de Haversine para distancia geográfica, y agrega columnas derivadas como `velocity_kmh`), y detección de eventos (usa una ventana ordenada por tiempo con la función `lag()` para identificar transiciones de `on_ground=true` a `on_ground=false`, que son despegues, y el caso contrario, que son aterrizajes; le asigna a cada evento un nivel de confianza HIGH/MEDIUM/LOW según qué tan cerca estaba del aeropuerto).

Toda la configuración de Spark (memoria, master, packages) está en `config.py`. Se corre en modo `local[*]` aprovechando todos los cores disponibles en la máquina de procesamiento.

### Implementación de Control de Accesos

*

### Justificación de Arquitectura: Teorema CAP

El teorema CAP dice que un sistema distribuido sólo puede garantizar al mismo tiempo dos de las tres propiedades: Consistencia, Disponibilidad y Tolerancia a Particiones. En cualquier despliegue real sobre red la tolerancia a particiones (P) es obligatoria porque las particiones de red ocurren tarde o temprano. La elección real entonces es entre C y A.

En este proyecto cada capa toma una decisión distinta porque los requisitos son distintos.

**Capa de ingesta (Cassandra): priorizamos AP.**

Lo más importante en la capa operativa es no perder eventos. OpenSky no nos puede repetir un mensaje que llegó hace cinco minutos, así que si un nodo se cae el sistema tiene que poder seguir aceptando escrituras en los demás nodos. Cassandra está diseñada exactamente con esa filosofía: es una base de datos masterless (todos los nodos son iguales y pueden aceptar lecturas y escrituras), y con replicación eventualmente consistente. El precio que se paga es que justo después de escribir, una lectura podría no ver el dato si toca un nodo donde la replicación todavía no llegó. Para nuestro caso eso es aceptable porque las consultas operativas toleran segundos de retraso, y la transformación analítica corre con minutos de retraso natural.

**Capa analítica (Neo4j): priorizamos CP.**

En la capa analítica preferimos consistencia sobre disponibilidad. Las respuestas de la API tienen que ser internamente coherentes; sería un error que el conteo de aviones de un país difiriera del listado real de aviones devuelto por otra consulta. Neo4j (en su versión Community como instancia única, o en Enterprise con Causal Cluster usando el protocolo Raft) garantiza consistencia ACID estricta. Si hay una partición de red, el sistema prefiere rechazar escrituras antes que aceptar valores divergentes. El costo es que durante una partición la capa analítica puede quedar temporalmente no escribible, pero eso no afecta la operación del sistema en general porque la captura sigue funcionando en Cassandra y Spark reconstruye el grafo cuando la conectividad regresa.

**Capa de procesamiento (Spark): no participa del eje CAP.**

Spark es un motor de cómputo sin estado persistente, así que no tiene que tomar partido. Lee de Cassandra y escribe a Neo4j. La idempotencia del job (el uso de MERGE en Cypher y de claves primarias en Cassandra) hace que se pueda reprocesar una ventana de tiempo sin problema, lo que convierte una falla del job en un retraso, no en una pérdida de datos.

**Resumen.**

| Capa | Tecnología | Prioridad CAP | Razón |
|---|---|---|---|
| Ingesta | Cassandra | AP | El stream no se puede repetir; la prioridad es no perder eventos. |
| Analítica | Neo4j | CP | Las consultas deben ser internamente consistentes. |
| Procesamiento | Spark | sin estado | Cómputo idempotente, no almacena nada. |





---

## 3. Evidencia de Rendimiento y Garantía de Caudal

Para validar los requerimientos de la entrega, se realizó una prueba de carga (Stress Test) sobre el cluster de Cassandra de 3 nodos. El objetivo fue demostrar que el sistema es capaz de soportar volúmenes masivos de datos sin pérdida de información, garantizando la resiliencia y el caudal.

### Resultados de la Prueba de Carga
La prueba consistió en la inserción de **5,000 registros** ficticios en batches de 50, simulando una ráfaga de tráfico aéreo.

| Métrica | Resultado |
|---|---|
| **Registros procesados** | 5,000 |
| **Errores encontrados** | 0 |
| **Tiempo total** | 6.91 segundos |
| **Caudal (Throughput)** | **723.9 registros/segundo** |
| **Pérdida de mensajes** | **0.00%** |

### Metodología del Test
El script `setup/test_load_cassandra.py` realiza las siguientes acciones para validar el sistema:
1.  **Generación de Datos Sintéticos**: Crea aeronaves ficticias con IDs únicos (`stress-XXXXXX`) y coordenadas aleatorias para evitar colisiones con datos reales.
2.  **Escritura en Lotes (Batching)**: Agrupa los registros en lotes de 50. Esto reduce el número de peticiones de red y optimiza el uso del protocolo binario de Cassandra.
3.  **Consistencia de Quórum**: Utiliza `LOCAL_QUORUM`, lo que obliga a que al menos 2 de los 3 nodos confirmen la recepción del dato antes de marcarlo como exitoso. Esto garantiza que la prueba no sea solo "de velocidad", sino también de integridad.
4.  **Cálculo de Métricas**: Mide el tiempo exacto entre el primer y el último lote para calcular el *Throughput* real (registros por segundo) y verifica que el contador de errores sea cero.

---
