"""Extract book data from the Open Library API."""
import logging
import time
from typing import List, Dict, Any

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

OPENLIBRARY_SEARCH_URL = "https://openlibrary.org/search.json"
DEFAULT_SUBJECTS = ["fiction", "science", "history", "fantasy", "mystery"]
DEFAULT_LIMIT_PER_SUBJECT = 50


def _normalize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    authors = doc.get("author_name") or []
    return {
        "title": doc.get("title"),
        "author": ", ".join(authors) if authors else None,
        "year": doc.get("first_publish_year"),
        "subjects": doc.get("subject", [])[:10] if doc.get("subject") else [],
        "ol_id": doc.get("key"),
    }


def fetch_books_by_subject(subject: str, limit: int = DEFAULT_LIMIT_PER_SUBJECT) -> List[Dict[str, Any]]:
    """Query Open Library search endpoint for a single subject."""
    params = {"subject": subject, "limit": limit}
    try:
        response = requests.get(OPENLIBRARY_SEARCH_URL, params=params, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Open Library request failed for subject=%s: %s", subject, exc)
        return []

    payload = response.json()
    docs = payload.get("docs", [])
    books = [_normalize_doc(d) for d in docs if d.get("title")]
    logger.info("Fetched %d books for subject=%s", len(books), subject)
    return books


def extract_openlibrary(
    subjects: List[str] = None,
    limit_per_subject: int = DEFAULT_LIMIT_PER_SUBJECT,
) -> List[Dict[str, Any]]:
    """Fetch books across several subjects and return a flat list."""
    subjects = subjects or DEFAULT_SUBJECTS
    all_books: List[Dict[str, Any]] = []
    for subject in subjects:
        books = fetch_books_by_subject(subject, limit=limit_per_subject)
        all_books.extend(books)
        time.sleep(0.5)
    logger.info("Total books extracted from Open Library: %d", len(all_books))
    return all_books


if __name__ == "__main__":
    data = extract_openlibrary()
    print(f"Extracted {len(data)} books. First record:")
    if data:
        print(data[0])
