"""Phase 1: Discover all document IDs via search API pagination."""

import asyncio
import logging
from urllib.parse import urlencode

import aiohttp

from crawler.config import (
    BASE_URL,
    DISCOVERY_CONCURRENCY,
    MAX_RETRIES,
    REQUEST_DELAY,
    RETRY_BASE_DELAY,
    ROWS_PER_PAGE,
    SEARCH_VBHN_URL,
    SEARCH_VBPQ_URL,
)
from crawler.parser import parse_search_results
from crawler.state import CrawlState

logger = logging.getLogger(__name__)


async def fetch_search_page(
    session: aiohttp.ClientSession,
    url: str,
    page: int,
) -> str:
    """Fetch a single search results page via POST."""
    params = {
        "SearchIn": "",
        "DivID": "divResultSearch" if "HopNhat" not in url else "divResultSearchHN",
        "IsVietNamese": "True",
        "type": "0",
        "s": "0",
        "DonVi": "",
        "RowPerPage": str(ROWS_PER_PAGE),
        "page": str(page),
    }

    for attempt in range(MAX_RETRIES):
        try:
            async with session.post(
                url,
                data=urlencode(params),
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": BASE_URL + "/pages/vbpq-timkiem.aspx",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    return await resp.text()
                logger.warning("Search page %d returned status %d (attempt %d)", page, resp.status, attempt + 1)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning("Search page %d error: %s (attempt %d)", page, e, attempt + 1)

        if attempt < MAX_RETRIES - 1:
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            await asyncio.sleep(delay)

    return ""


async def discover_page(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    url: str,
    page: int,
    doc_type: str,
    state: CrawlState,
) -> int:
    """Discover documents from a single search page."""
    async with sem:
        await asyncio.sleep(REQUEST_DELAY)
        html = await fetch_search_page(session, url, page)

    if not html:
        logger.error("Empty response for %s page %d", doc_type, page)
        return 0

    documents, _ = parse_search_results(html)

    if documents:
        batch = [(doc["item_id"], doc["scope"], doc_type) for doc in documents]
        state.add_discovered_batch(batch)

    state.update_last_page(doc_type, page)

    count = len(documents)
    if page % 100 == 0:
        logger.info("[%s] Page %d: found %d documents", doc_type, page, count)

    return count


async def run_discovery(
    session: aiohttp.ClientSession,
    state: CrawlState,
    doc_type: str = "vbpq",
) -> int:
    """Run full discovery for a document type.

    Returns total documents discovered.
    """
    url = SEARCH_VBPQ_URL if doc_type == "vbpq" else SEARCH_VBHN_URL
    label = "Văn bản pháp quy" if doc_type == "vbpq" else "Văn bản hợp nhất"

    logger.info("Starting discovery for %s...", label)

    # Fetch first page to get total pages
    html = await fetch_search_page(session, url, 1)
    if not html:
        logger.error("Cannot fetch first page for %s", doc_type)
        return 0

    documents, total_pages = parse_search_results(html)
    logger.info("[%s] Total pages: %d", doc_type, total_pages)

    if total_pages == 0:
        logger.warning("[%s] No pages found — trying to estimate from document count", doc_type)
        total_pages = 1

    state.set_total_pages(doc_type, total_pages)

    # Save first page results
    if documents:
        batch = [(doc["item_id"], doc["scope"], doc_type) for doc in documents]
        state.add_discovered_batch(batch)
    state.update_last_page(doc_type, 1)

    # Resume from last completed page
    start_page = state.get_last_page(doc_type) + 1
    if start_page > total_pages:
        total = state.get_total_discovered(doc_type)
        logger.info("[%s] Discovery already complete. %d documents in DB.", doc_type, total)
        return total

    logger.info("[%s] Resuming from page %d / %d", doc_type, start_page, total_pages)

    # Paginate with controlled concurrency
    sem = asyncio.Semaphore(DISCOVERY_CONCURRENCY)

    # Process in batches to avoid creating too many tasks at once
    batch_size = 100
    total_found = len(documents)

    for batch_start in range(start_page, total_pages + 1, batch_size):
        batch_end = min(batch_start + batch_size, total_pages + 1)
        tasks = [
            discover_page(session, sem, url, page, doc_type, state)
            for page in range(batch_start, batch_end)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                logger.error("Discovery task error: %s", r)
            elif isinstance(r, int):
                total_found += r

        logger.info(
            "[%s] Progress: pages %d-%d / %d done | Total discovered so far: %d",
            doc_type, batch_start, batch_end - 1, total_pages,
            state.get_total_discovered(doc_type),
        )

    total = state.get_total_discovered(doc_type)
    logger.info("[%s] Discovery complete! Total: %d documents", doc_type, total)
    return total
