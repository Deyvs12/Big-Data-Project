"""Kafka consumer that drains book messages and applies transformations."""
import json
import logging
import os
import uuid
from typing import List, Dict, Any

from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
CONSUMER_TIMEOUT_MS = 15000


def _transform_api_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a record sourced from the Open Library API."""
    return {
        "title": (record.get("title") or "").strip() or None,
        "author": (record.get("author") or "").strip() or None,
        "year": int(record["year"]) if record.get("year") else None,
        "subjects": record.get("subjects") or [],
        "ol_id": record.get("ol_id"),
        "source": "openlibrary",
    }


def _transform_scraped_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a record sourced from books.toscrape.com."""
    price = record.get("price")
    try:
        price_value = float(price) if price is not None else 0.0
    except (TypeError, ValueError):
        price_value = 0.0

    rating = record.get("rating")
    try:
        rating_value = int(rating) if rating is not None else 0
    except (TypeError, ValueError):
        rating_value = 0

    return {
        "title": (record.get("title") or "").strip() or None,
        "price": price_value,
        "availability": (record.get("availability") or "unknown").strip(),
        "rating": rating_value,
        "category": (record.get("category") or "unknown").strip().lower(),
        "source": "books_to_scrape",
    }


def _consume_topic(topic: str, transform) -> List[Dict[str, Any]]:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"books-pipeline-{uuid.uuid4()}",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
    )

    records: List[Dict[str, Any]] = []
    try:
        for message in consumer:
            try:
                records.append(transform(message.value))
            except Exception as exc:
                logger.warning("Transform failed for message in %s: %s", topic, exc)
    finally:
        consumer.close()

    logger.info("Consumed %d records from topic=%s", len(records), topic)
    return records


def consume_and_transform() -> Dict[str, List[Dict[str, Any]]]:
    """Drain both topics and return a dict with transformed payloads."""
    api_records = _consume_topic("books_api", _transform_api_record)
    scraped_records = _consume_topic("books_scraping", _transform_scraped_record)
    return {"api": api_records, "scraped": scraped_records}


if __name__ == "__main__":
    result = consume_and_transform()
    print(f"API records: {len(result['api'])}")
    print(f"Scraped records: {len(result['scraped'])}")
