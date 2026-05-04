# Documentación — Pipeline OpenSky

```
OpenSky API → Cassandra → Spark → Neo4j → FastAPI
```

---

## Índice

| Archivo | Contenido |
|---|---|
| [`COMO_CORRER.md`](./COMO_CORRER.md) | Prerequisitos, instalación del venv, advertencia de versión de Spark, pasos para correr el pipeline y la API, monitoreo de logs, resolución de problemas |
| [`ARQUITECTURA_TECNICA.md`](./ARQUITECTURA_TECNICA.md) | Descripción técnica de cada archivo del repositorio, esquemas de tablas en Cassandra, modelo del grafo en Neo4j, infraestructura Docker, dependencias y versiones |

---

## Advertencia crítica de versiones

> **PySpark debe ser exactamente `3.5.1`** (Scala 2.12).
>
> Los conectores de Cassandra y Neo4j son incompatibles con Spark 4.x (Scala 2.13).
> Ver `COMO_CORRER.md` para instrucciones de instalación correcta.
