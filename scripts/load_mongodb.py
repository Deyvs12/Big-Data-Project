"""Load transformed book documents into MongoDB."""
import logging
import os
from typing import List, Dict, Any

from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb:27017")
DB_NAME = os.getenv("MONGO_DB", "books_db")


def _build_client() -> MongoClient:
    return MongoClient(f"mongodb://{MONGO_HOST}", serverSelectionTimeoutMS=10000)


def _dedupe_key(collection_name: str, doc: Dict[str, Any]):
    """Pick a stable key per collection to support upserts."""
    if collection_name == "books_api":
        if doc.get("ol_id"):
            return {"ol_id": doc["ol_id"]}
        return {"title": doc.get("title"), "author": doc.get("author")}
    if collection_name == "books_scraped":
        return {"title": doc.get("title")}
    return None


def load_to_mongodb(documents: List[Dict[str, Any]], collection_name: str) -> int:
    """Insert (or upsert) a list of documents into the given collection."""
    if not documents:
        logger.warning("No documents to load into collection=%s", collection_name)
        return 0

    client = _build_client()
    try:
        db = client[DB_NAME]
        collection = db[collection_name]

        operations = []
        for doc in documents:
            key = _dedupe_key(collection_name, doc)
            if key and all(v is not None for v in key.values()):
                operations.append(UpdateOne(key, {"$set": doc}, upsert=True))
            else:
                operations.append(UpdateOne({"_synthetic": id(doc)}, {"$set": doc}, upsert=True))

        try:
            result = collection.bulk_write(operations, ordered=False)
        except PyMongoError as exc:
            logger.error("Bulk write failed for %s: %s", collection_name, exc)
            return 0

        affected = (result.upserted_count or 0) + (result.modified_count or 0)
        logger.info(
            "Mongo write to %s: matched=%d modified=%d upserted=%d",
            collection_name,
            result.matched_count,
            result.modified_count,
            result.upserted_count or 0,
        )
        return affected
    finally:
        client.close()


if __name__ == "__main__":
    sample = [{"title": "Test Book", "author": "Test Author"}]
    load_to_mongodb(sample, "books_api")
