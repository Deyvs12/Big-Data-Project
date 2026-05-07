"""Compute metrics over the loaded book collections."""
import logging
import os
from datetime import datetime
from typing import Dict, Any, List

from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb:27017")
DB_NAME = os.getenv("MONGO_DB", "books_db")


def _build_client() -> MongoClient:
    return MongoClient(f"mongodb://{MONGO_HOST}", serverSelectionTimeoutMS=10000)


def _price_stats(scraped: List[Dict[str, Any]]) -> Dict[str, float]:
    prices = [b.get("price", 0.0) for b in scraped if isinstance(b.get("price"), (int, float))]
    if not prices:
        return {"avg_price": 0.0, "max_price": 0.0, "min_price": 0.0, "count": 0}
    return {
        "avg_price": round(sum(prices) / len(prices), 2),
        "max_price": max(prices),
        "min_price": min(prices),
        "count": len(prices),
    }


def _ratings_distribution(scraped: List[Dict[str, Any]]) -> Dict[str, int]:
    distribution = {str(i): 0 for i in range(1, 6)}
    for book in scraped:
        rating = book.get("rating")
        if isinstance(rating, int) and 1 <= rating <= 5:
            distribution[str(rating)] += 1
    return distribution


def _category_counts(scraped: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for book in scraped:
        category = book.get("category") or "unknown"
        counts[category] = counts.get(category, 0) + 1
    return counts


def _top_books_by(scraped: List[Dict[str, Any]], field: str, n: int = 10) -> List[Dict[str, Any]]:
    cleaned = [b for b in scraped if isinstance(b.get(field), (int, float))]
    cleaned.sort(key=lambda b: b[field], reverse=True)
    top = cleaned[:n]
    return [
        {
            "title": b.get("title"),
            "price": b.get("price"),
            "rating": b.get("rating"),
            "category": b.get("category"),
        }
        for b in top
    ]


def calcular_metricas() -> Dict[str, Any]:
    """Read scraped + api collections, compute metrics, and persist them."""
    client = _build_client()
    try:
        db = client[DB_NAME]

        scraped = list(db["books_scraped"].find({}, {"_id": 0}))
        api_books = list(db["books_api"].find({}, {"_id": 0}))

        metrics = {
            "generated_at": datetime.utcnow().isoformat(),
            "totals": {
                "books_api": len(api_books),
                "books_scraped": len(scraped),
            },
            "price_stats": _price_stats(scraped),
            "ratings_distribution": _ratings_distribution(scraped),
            "category_counts": _category_counts(scraped),
            "top_10_most_expensive": _top_books_by(scraped, "price", n=10),
            "top_10_best_rated": _top_books_by(scraped, "rating", n=10),
        }

        db["books_metricas"].delete_many({})
        db["books_metricas"].insert_one(dict(metrics))
        logger.info(
            "Metrics persisted: api=%d scraped=%d categories=%d",
            metrics["totals"]["books_api"],
            metrics["totals"]["books_scraped"],
            len(metrics["category_counts"]),
        )
        metrics.pop("_id", None)
        return metrics
    finally:
        client.close()


if __name__ == "__main__":
    result = calcular_metricas()
    print(result)
