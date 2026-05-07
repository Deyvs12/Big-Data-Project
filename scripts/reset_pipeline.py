"""Reset pipeline state (Mongo collections + Kafka topics) for clean test runs.

Designed to run from the host machine (outside Docker), so the default
connection strings point to localhost.
"""
import os
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError
from pymongo import MongoClient

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost:27017")

MONGO_DB = os.getenv("MONGO_DB", "books_db")
MONGO_COLLECTIONS = ["books_api", "books_scraped", "books_metricas"]
KAFKA_TOPICS = ["books_api", "books_scraping"]


def reset_mongodb():
    print(f"[MongoDB] Conectando a mongodb://{MONGO_HOST} ...")
    try:
        client = MongoClient(f"mongodb://{MONGO_HOST}", serverSelectionTimeoutMS=10000)
        db = client[MONGO_DB]
        for collection_name in MONGO_COLLECTIONS:
            try:
                result = db[collection_name].delete_many({})
                print(
                    f"[MongoDB] Colección '{collection_name}': "
                    f"{result.deleted_count} documentos eliminados."
                )
            except Exception as exc:
                print(f"[MongoDB] ERROR limpiando '{collection_name}': {exc}")
        client.close()
        print("[MongoDB] Reset completado.")
    except Exception as exc:
        print(f"[MongoDB] ERROR de conexión: {exc}")


def reset_kafka_topics():
    print(f"[Kafka] Conectando a {KAFKA_BROKER} ...")
    admin = None
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=[KAFKA_BROKER],
            client_id="reset-pipeline",
        )
    except Exception as exc:
        print(f"[Kafka] ERROR de conexión: {exc}")
        return

    try:
        try:
            admin.delete_topics(topics=KAFKA_TOPICS, timeout_ms=10000)
            print(f"[Kafka] Tópicos eliminados: {KAFKA_TOPICS}")
        except UnknownTopicOrPartitionError:
            print(f"[Kafka] Algunos tópicos no existían (OK): {KAFKA_TOPICS}")
        except Exception as exc:
            print(f"[Kafka] ERROR eliminando tópicos: {exc}")

        print("[Kafka] Esperando 2s para que el broker procese la eliminación...")
        time.sleep(2)

        new_topics = [
            NewTopic(name=t, num_partitions=1, replication_factor=1)
            for t in KAFKA_TOPICS
        ]
        try:
            admin.create_topics(new_topics=new_topics, validate_only=False)
            print(f"[Kafka] Tópicos recreados: {KAFKA_TOPICS}")
        except TopicAlreadyExistsError:
            print(f"[Kafka] Algunos tópicos ya existen (OK): {KAFKA_TOPICS}")
        except Exception as exc:
            print(f"[Kafka] ERROR creando tópicos: {exc}")
    finally:
        try:
            admin.close()
        except Exception:
            pass

    print("[Kafka] Reset completado.")


def main():
    print("=== RESET DEL PIPELINE ===")
    reset_mongodb()
    print()
    reset_kafka_topics()
    print()
    print("=== RESET FINALIZADO ===")


if __name__ == "__main__":
    main()
