# Pipeline Books

Pipeline ETL de análisis de libros y precios orquestado con **Apache Airflow**.

Obtiene información bibliográfica desde la API de Open Library, extrae precios y
ratings mediante scraping de Books to Scrape, procesa los datos a través de
**Kafka**, los almacena en **MongoDB** y los visualiza en un dashboard de
**Streamlit**.

---

## Arquitectura

```
   Open Library API ──┐
                      ├──► Kafka (books_api / books_scraping)
   Books to Scrape ───┘             │
                                    ▼
                       Consumo + transformación
                                    │
                                    ▼
                       MongoDB (books_db)
                       ├── books_api
                       ├── books_scraped
                       └── books_metricas
                                    │
                                    ▼
                            Streamlit Dashboard
```

Toda la orquestación corre en Airflow mediante el DAG `books_pipeline`.

## Stack

| Componente | Versión | Rol |
|------------|---------|-----|
| Apache Airflow | 2.8.1 | Orquestación del pipeline |
| Apache Kafka | confluentinc/cp-kafka 7.5.0 | Mensajería entre etapas |
| Zookeeper | confluentinc/cp-zookeeper 7.5.0 | Coordinación de Kafka |
| MongoDB | 7.0 | Almacenamiento de documentos |
| Streamlit | 1.32.0 | Dashboard de visualización |
| PostgreSQL | 13 | Backend de Airflow |
| Python | 3.8 (Airflow) / 3.11 (Streamlit) | Runtime |

## Fuentes de datos

- **Open Library API** — https://openlibrary.org/developers/api
  - Campos: título, autor, año de publicación, temas, identificadores.
- **Books to Scrape** — https://books.toscrape.com/
  - Campos: título, precio, disponibilidad, rating (1-5), categoría.

## Estructura del proyecto

```
Big-Data-Project/
├── dags/
│   └── books_dag.py              # DAG principal de Airflow
├── dashboard/
│   ├── app.py                    # Dashboard Streamlit
│   ├── Dockerfile
│   └── requirements.txt
├── scripts/
│   ├── extract_openlibrary.py    # Extracción API
│   ├── scraping_books.py         # Scraping web
│   ├── kafka_producer.py         # Publicación en Kafka
│   ├── kafka_consumer.py         # Consumo + transformación
│   ├── load_mongodb.py           # Carga a MongoDB
│   ├── calcular_metricas.py      # Cálculo de métricas
│   └── reset_pipeline.py         # Limpieza de estado (Mongo + Kafka)
├── docker-compose.yml
├── requirements.txt              # Dependencias para correr scripts en local
├── .env                          # Variables de entorno (KAFKA_BROKER, MONGO_HOST)
└── README.md
```

## DAG de Airflow

```
start
  │
  ├── extraer_openlibrary_api
  ├── scraping_books_to_scrape
  │
  ▼
enviar_books_kafka
  │
  ▼
consumo_transformacion_books
  │
  ▼
cargar_books_mongodb
  │
  ▼
calcular_metricas_books
  │
  ▼
actualizar_dashboard_streamlit
  │
  ▼
end
```

| Tarea | Descripción |
|-------|-------------|
| `extraer_openlibrary_api` | Obtiene libros de Open Library por temas |
| `scraping_books_to_scrape` | Scrapea libros (título, precio, rating, categoría) |
| `enviar_books_kafka` | Publica en `books_api` y `books_scraping` |
| `consumo_transformacion_books` | Drena los tópicos y normaliza los datos |
| `cargar_books_mongodb` | Inserta/upserta en `books_api` y `books_scraped` |
| `calcular_metricas_books` | Calcula stats y los guarda en `books_metricas` |
| `actualizar_dashboard_streamlit` | Marca el dataset como listo para el dashboard |

## Tópicos Kafka

- `books_api`
- `books_scraping`

## Colecciones MongoDB (DB `books_db`)

- `books_api` — datos crudos de Open Library
- `books_scraped` — datos crudos de Books to Scrape
- `books_metricas` — métricas calculadas (un solo documento)

---

## Cómo ejecutar

### Requisitos

- Docker + Docker Compose
- Python 3.11+ (solo si quieres correr scripts en local fuera de Docker)
- Puertos libres: `8080` (Airflow), `8501` (Streamlit), `9092` (Kafka), `27017` (Mongo), `2181` (Zookeeper)

### 1. Levantar el stack completo

```bash
docker-compose up -d
```

La primera vez, espera ~1 minuto a que Airflow termine de inicializarse.

### 2. Acceder a los servicios

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| Streamlit | http://localhost:8501 | — |
| MongoDB | `localhost:27017` | — |
| Kafka | `localhost:9092` | — |

### 3. Ejecutar el pipeline

1. Entra a Airflow ([http://localhost:8080](http://localhost:8080)).
2. Activa el DAG `books_pipeline` (toggle a la izquierda del nombre).
3. Dispáralo manualmente (botón ▶).
4. Cuando termine, abre [http://localhost:8501](http://localhost:8501) para ver el dashboard.

### 4. Resetear el estado

Para limpiar MongoDB y recrear los tópicos de Kafka:

```bash
.venv/bin/python scripts/reset_pipeline.py
```

---

## Desarrollo local — correr scripts individualmente

Cada script en `scripts/` tiene un bloque `__main__` que permite probarlo de
forma independiente desde tu terminal.

### Setup del entorno virtual

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### Variables de entorno para local

Como los scripts corren fuera de Docker, deben apuntar a los puertos expuestos
en tu host (no a los nombres internos de la red de Docker):

```bash
export KAFKA_BROKER=localhost:9092
export MONGO_HOST=localhost:27017
```

> El archivo `.env` solo lo lee `docker-compose` para los contenedores. **No
> lo modifiques** para correr scripts en local — usa `export`.

### Probar cada script

| # | Script | Necesita | Qué hace su `__main__` |
|---|--------|----------|------------------------|
| 1 | `extract_openlibrary.py` | Internet | Llama a la API y imprime el total |
| 2 | `scraping_books.py` | Internet | Scrapea 2 páginas de prueba |
| 3 | `reset_pipeline.py` | Kafka + Mongo | Limpia el estado |
| 4 | `kafka_producer.py` | Kafka | Publica 1 mensaje de prueba |
| 5 | `kafka_consumer.py` | Kafka con datos | Drena ambos tópicos |
| 6 | `load_mongodb.py` | Mongo | Inserta 1 doc de prueba |
| 7 | `calcular_metricas.py` | Mongo con datos | Calcula y guarda métricas |

```bash
.venv/bin/python scripts/extract_openlibrary.py
.venv/bin/python scripts/scraping_books.py
# ...
```

---

## Variables de entorno

| Variable | Default Docker | Default local |
|----------|----------------|---------------|
| `KAFKA_BROKER` | `kafka:29092` | `localhost:9092` |
| `MONGO_HOST` | `mongodb:27017` | `localhost:27017` |
| `MONGO_DB` | `books_db` | `books_db` |

## Dashboard

El dashboard de Streamlit incluye:

- Histograma de precios
- Libros por categoría
- Distribución de ratings
- Top 10 libros más caros
- Top 10 libros mejor calificados
- Tabla con filtros por título, autor o categoría

## Detener el stack

```bash
docker-compose down            # Mantiene los volúmenes de datos
docker-compose down -v         # Borra también los datos persistidos
```
