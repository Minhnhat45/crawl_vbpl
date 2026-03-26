"""Phase 2: Crawl document detail pages for full text and metadata."""

import asyncio
import json
import logging
from datetime import datetime, timezone

import aiohttp

from crawler.config import (
    ATTRS_URL_TEMPLATE,
    BACKOFF_ON_CONSECUTIVE_ERRORS,
    BACKOFF_PAUSE_SECONDS,
    DETAIL_CONCURRENCY,
    DETAIL_URL_TEMPLATE,
    FALLBACK_SCOPE,
    MAX_RETRIES,
    REQUEST_DELAY,
    RETRY_BASE_DELAY,
    VBHN_ATTRS_URL_TEMPLATE,
    VBHN_DETAIL_URL_TEMPLATE,
)
from crawler.parser import parse_attributes_page, parse_detail_page
from crawler.state import CrawlState
from crawler.writer import write_document

logger = logging.getLogger(__name__)

# Track consecutive errors for backoff
_consecutive_errors = 0


async def _fetch_url(
    session: aiohttp.ClientSession,
    url: str,
    item_id: int,
) -> str | None:
    """Fetch a URL with retries and backoff."""
    global _consecutive_errors

    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    # Skip ASP.NET error pages (typically ~1937 bytes with "resource cannot be found")
                    if "resource cannot be found" in text.lower() or "không tìm thấy" in text.lower():
                        return None
                    _consecutive_errors = 0
                    return text
                if resp.status == 404:
                    return None
                logger.warning(
                    "Detail ItemID=%d status=%d (attempt %d)",
                    item_id, resp.status, attempt + 1,
                )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning("Detail ItemID=%d error: %s %s (attempt %d)", item_id, type(e).__name__, e, attempt + 1)

        _consecutive_errors += 1
        if _consecutive_errors >= BACKOFF_ON_CONSECUTIVE_ERRORS:
            logger.warning(
                "Too many consecutive errors (%d), pausing %ds...",
                _consecutive_errors, BACKOFF_PAUSE_SECONDS,
            )
            await asyncio.sleep(BACKOFF_PAUSE_SECONDS)
            _consecutive_errors = 0

        if attempt < MAX_RETRIES - 1:
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            await asyncio.sleep(delay)

    return None


async def fetch_detail(
    session: aiohttp.ClientSession,
    item_id: int,
    scope: str,
    doc_type: str = "vbpq",
) -> str | None:
    """Fetch a document detail page, with fallback scope for TW."""
    if doc_type == "vbhn":
        url_template = VBHN_DETAIL_URL_TEMPLATE
    else:
        url_template = DETAIL_URL_TEMPLATE

    url = url_template.format(scope=scope, item_id=item_id)
    result = await _fetch_url(session, url, item_id)

    # Fallback: if scope returned 404, try fallback scope
    if result is None and scope.lower() == "tw":
        fallback_url = url_template.format(scope=FALLBACK_SCOPE, item_id=item_id)
        result = await _fetch_url(session, fallback_url, item_id)

    return result


async def fetch_attrs(
    session: aiohttp.ClientSession,
    item_id: int,
    scope: str,
    doc_type: str = "vbpq",
) -> str | None:
    """Fetch the attributes (thuoctinh) page for structured metadata."""
    if doc_type == "vbhn":
        url_template = VBHN_ATTRS_URL_TEMPLATE
    else:
        url_template = ATTRS_URL_TEMPLATE

    effective_scope = FALLBACK_SCOPE if scope.lower() == "tw" else scope
    url = url_template.format(scope=effective_scope, item_id=item_id)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status == 200:
                return await resp.text()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass
    return None


async def crawl_single_detail(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    item_id: int,
    scope: str,
    doc_type: str,
    state: CrawlState,
    counter: dict,
) -> None:
    """Fetch and parse one document, save to state and JSONL."""
    async with sem:
        await asyncio.sleep(REQUEST_DELAY)
        html = await fetch_detail(session, item_id, scope, doc_type)

    if html is None:
        state.mark_failed(item_id, scope, "fetch_failed_or_404")
        counter["failed"] += 1
        return

    try:
        doc = parse_detail_page(html)
    except Exception as e:
        logger.error("Parse error ItemID=%d: %s", item_id, e)
        state.mark_failed(item_id, scope, f"parse_error: {e}")
        counter["failed"] += 1
        return

    # Fetch attributes page for richer metadata
    async with sem:
        attrs_html = await fetch_attrs(session, item_id, scope, doc_type)
    if attrs_html:
        try:
            attrs_meta = parse_attributes_page(attrs_html)
            # Merge — attributes page has more structured data
            attrs_meta.update(doc["metadata"])  # detail page values take priority if both exist
            doc["metadata"] = attrs_meta
        except Exception as e:
            logger.debug("Attrs parse error ItemID=%d: %s", item_id, e)

    # Enrich metadata
    doc["metadata"]["scope"] = scope
    doc["metadata"]["doc_type"] = "van_ban_phap_quy" if doc_type == "vbpq" else "van_ban_hop_nhat"
    doc["metadata"]["source_url"] = DETAIL_URL_TEMPLATE.format(scope=scope, item_id=item_id)

    # Save to state DB (for resume & attachment phase)
    detail_json = json.dumps(doc, ensure_ascii=False)
    state.mark_detail_done(item_id, scope, detail_json)

    # Write to JSONL
    record = {
        "id": f"VBPL-{item_id}",
        "text": doc["text"],
        "metadata": doc["metadata"],
        "attachments": doc["attachment_urls"],
        "created_timestamp": doc["metadata"].get("ngay_ban_hanh"),
        "downloaded_timestamp": datetime.now(timezone.utc).isoformat(),
        "url": doc["metadata"]["source_url"],
    }
    await write_document(record, doc_type)

    counter["done"] += 1
    total = counter["done"] + counter["failed"]
    if total % 500 == 0:
        logger.info(
            "[%s] Detail progress: %d done, %d failed / %d total",
            doc_type, counter["done"], counter["failed"], counter["total"],
        )


async def run_detail_crawl(
    session: aiohttp.ClientSession,
    state: CrawlState,
    doc_type: str = "vbpq",
) -> None:
    """Run Phase 2: crawl all discovered documents for details."""
    pending = state.get_pending_details(doc_type)
    if not pending:
        logger.info("[%s] No pending documents for detail crawl.", doc_type)
        return

    logger.info("[%s] Starting detail crawl for %d documents...", doc_type, len(pending))

    sem = asyncio.Semaphore(DETAIL_CONCURRENCY)
    counter = {"done": 0, "failed": 0, "total": len(pending)}

    # Process in batches
    batch_size = 500
    for i in range(0, len(pending), batch_size):
        batch = pending[i : i + batch_size]
        tasks = [
            crawl_single_detail(session, sem, doc["item_id"], doc["scope"], doc_type, state, counter)
            for doc in batch
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info(
            "[%s] Batch complete: %d/%d done, %d failed",
            doc_type, counter["done"], counter["total"], counter["failed"],
        )

    logger.info(
        "[%s] Detail crawl complete! %d done, %d failed",
        doc_type, counter["done"], counter["failed"],
    )
