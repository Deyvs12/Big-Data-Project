"""Kafka producer for publishing book records."""
import json
import logging
import os
from typing import List, Dict, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")


def _build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
    )


def publish_books(books: List[Dict[str, Any]], topic: str) -> int:
    """Publish each book as a JSON message to the given Kafka topic."""
    if not books:
        logger.warning("No books to publish to topic=%s", topic)
        return 0

    producer = _build_producer()
    sent = 0
    try:
        for book in books:
            try:
                producer.send(topic, value=book)
                sent += 1
            except KafkaError as exc:
                logger.error("Kafka send failed for topic=%s: %s", topic, exc)
        producer.flush()
        logger.info("Published %d/%d messages to topic=%s", sent, len(books), topic)
    finally:
        producer.close()

    return sent


if __name__ == "__main__":
    sample = [{"title": "Test Book", "author": "Test Author", "year": 2024}]
    publish_books(sample, "books_api")
