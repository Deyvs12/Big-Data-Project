"""Airflow DAG that orchestrates the books ETL pipeline."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from extract_openlibrary import extract_openlibrary
from scraping_books import scrape_books
from kafka_producer import publish_books
from kafka_consumer import consume_and_transform
from load_mongodb import load_to_mongodb
from calcular_metricas import calcular_metricas

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def task_extract_openlibrary(**context):
    books = extract_openlibrary()
    logger.info("Extracted %d books from Open Library", len(books))
    return books


def task_scrape_books(**context):
    books = scrape_books()
    logger.info("Scraped %d books from books.toscrape.com", len(books))
    return books


def task_publish_to_kafka(**context):
    ti = context["ti"]
    api_books = ti.xcom_pull(task_ids="extraer_openlibrary_api") or []
    scraped_books = ti.xcom_pull(task_ids="scraping_books_to_scrape") or []

    api_sent = publish_books(api_books, "books_api")
    scraped_sent = publish_books(scraped_books, "books_scraping")

    logger.info("Kafka publish: api=%d scraped=%d", api_sent, scraped_sent)
    return {"api_sent": api_sent, "scraped_sent": scraped_sent}


def task_consume_transform(**context):
    payload = consume_and_transform()
    logger.info(
        "Consumer drained: api=%d scraped=%d",
        len(payload["api"]),
        len(payload["scraped"]),
    )
    return payload


def task_load_mongodb(**context):
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="consumo_transformacion_books") or {"api": [], "scraped": []}

    api_loaded = load_to_mongodb(payload.get("api", []), "books_api")
    scraped_loaded = load_to_mongodb(payload.get("scraped", []), "books_scraped")

    logger.info("Mongo load: api=%d scraped=%d", api_loaded, scraped_loaded)
    return {"api_loaded": api_loaded, "scraped_loaded": scraped_loaded}


def task_compute_metrics(**context):
    metrics = calcular_metricas()
    logger.info("Metrics computed: totals=%s", metrics.get("totals"))
    return {"totals": metrics.get("totals"), "generated_at": metrics.get("generated_at")}


def task_refresh_dashboard(**context):
    """Streamlit reads MongoDB live; this task simply signals readiness."""
    logger.info("Dashboard data refreshed. Streamlit will reflect updates on next reload.")
    return "ok"


with DAG(
    dag_id="books_pipeline",
    description="ETL pipeline: Open Library + Books to Scrape -> Kafka -> MongoDB -> Streamlit",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 6),
    schedule_interval="@daily",
    catchup=False,
    tags=["books", "etl", "kafka", "mongodb"],
) as dag:

    start = EmptyOperator(task_id="start")

    extraer_openlibrary_api = PythonOperator(
        task_id="extraer_openlibrary_api",
        python_callable=task_extract_openlibrary,
    )

    scraping_books_to_scrape = PythonOperator(
        task_id="scraping_books_to_scrape",
        python_callable=task_scrape_books,
    )

    enviar_books_kafka = PythonOperator(
        task_id="enviar_books_kafka",
        python_callable=task_publish_to_kafka,
    )

    consumo_transformacion_books = PythonOperator(
        task_id="consumo_transformacion_books",
        python_callable=task_consume_transform,
    )

    cargar_books_mongodb = PythonOperator(
        task_id="cargar_books_mongodb",
        python_callable=task_load_mongodb,
    )

    calcular_metricas_books = PythonOperator(
        task_id="calcular_metricas_books",
        python_callable=task_compute_metrics,
    )

    actualizar_dashboard_streamlit = PythonOperator(
        task_id="actualizar_dashboard_streamlit",
        python_callable=task_refresh_dashboard,
    )

    end = EmptyOperator(task_id="end")

    start >> [extraer_openlibrary_api, scraping_books_to_scrape] >> enviar_books_kafka
    (
        enviar_books_kafka
        >> consumo_transformacion_books
        >> cargar_books_mongodb
        >> calcular_metricas_books
        >> actualizar_dashboard_streamlit
        >> end
    )
