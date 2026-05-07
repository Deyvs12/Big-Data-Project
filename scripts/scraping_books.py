"""Scrape book data from books.toscrape.com."""
import logging
import time
from typing import List, Dict, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://books.toscrape.com/"
CATALOGUE_URL = urljoin(BASE_URL, "catalogue/")
RATING_MAP = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
REQUEST_DELAY_SECONDS = 0.5


def _parse_price(price_text: str) -> float:
    cleaned = "".join(ch for ch in price_text if ch.isdigit() or ch == ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_rating(class_list: List[str]) -> int:
    for cls in class_list:
        if cls in RATING_MAP:
            return RATING_MAP[cls]
    return 0


def _fetch_book_detail(detail_url: str) -> str:
    """Return the category for a single book by visiting its detail page."""
    try:
        response = requests.get(detail_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Detail request failed for %s: %s", detail_url, exc)
        return "unknown"

    soup = BeautifulSoup(response.text, "html.parser")
    breadcrumbs = soup.select("ul.breadcrumb li a")
    if len(breadcrumbs) >= 3:
        return breadcrumbs[2].get_text(strip=True)
    return "unknown"


def _scrape_page(page_url: str) -> List[Dict[str, Any]]:
    response = requests.get(page_url, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    books: List[Dict[str, Any]] = []
    for article in soup.select("article.product_pod"):
        title_tag = article.select_one("h3 a")
        title = title_tag.get("title") if title_tag else None

        price_tag = article.select_one("p.price_color")
        price = _parse_price(price_tag.get_text()) if price_tag else 0.0

        availability_tag = article.select_one("p.availability")
        availability = availability_tag.get_text(strip=True) if availability_tag else "unknown"

        rating_tag = article.select_one("p.star-rating")
        rating = _parse_rating(rating_tag.get("class", []) if rating_tag else [])

        rel_url = title_tag.get("href") if title_tag else ""
        detail_url = urljoin(page_url, rel_url)
        category = _fetch_book_detail(detail_url)
        time.sleep(REQUEST_DELAY_SECONDS)

        books.append({
            "title": title,
            "price": price,
            "availability": availability,
            "rating": rating,
            "category": category,
        })

    return books


def scrape_books(max_pages: int = None) -> List[Dict[str, Any]]:
    """Scrape every paginated page of books.toscrape.com.

    max_pages limits the crawl for testing; None means scrape all available pages.
    """
    all_books: List[Dict[str, Any]] = []
    page_num = 1

    while True:
        if page_num == 1:
            page_url = BASE_URL
        else:
            page_url = urljoin(CATALOGUE_URL, f"page-{page_num}.html")

        try:
            response = requests.get(page_url, timeout=30)
        except requests.RequestException as exc:
            logger.error("Page request failed for %s: %s", page_url, exc)
            break

        if response.status_code == 404:
            logger.info("No more pages found at page %d. Stopping.", page_num)
            break
        response.raise_for_status()

        try:
            books = _scrape_page(page_url)
        except Exception as exc:
            logger.error("Failed to parse page %s: %s", page_url, exc)
            break

        if not books:
            logger.info("Empty page at %s. Stopping.", page_url)
            break

        all_books.extend(books)
        logger.info("Scraped page %d: %d books (total=%d)", page_num, len(books), len(all_books))

        if max_pages and page_num >= max_pages:
            logger.info("Reached max_pages=%d. Stopping.", max_pages)
            break

        page_num += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    logger.info("Total books scraped: %d", len(all_books))
    return all_books


if __name__ == "__main__":
    data = scrape_books(max_pages=2)
    print(f"Scraped {len(data)} books. First record:")
    if data:
        print(data[0])
