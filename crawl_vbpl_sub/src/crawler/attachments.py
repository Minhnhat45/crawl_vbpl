"""Phase 3: Download document attachments (doc/pdf files)."""

import asyncio
import json
import logging
from pathlib import Path

import aiofiles
import aiohttp

from crawler.config import (
    ATTACHMENT_CONCURRENCY,
    ATTACHMENTS_DIR,
    MAX_RETRIES,
    REQUEST_DELAY,
    RETRY_BASE_DELAY,
)
from crawler.state import CrawlState

logger = logging.getLogger(__name__)


async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
) -> bool:
    """Download a single file with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    async with aiofiles.open(dest, "wb") as f:
                        await f.write(content)
                    return True
                if resp.status == 404:
                    logger.debug("Attachment 404: %s", url)
                    return False
                logger.warning("Attachment %s status=%d (attempt %d)", url, resp.status, attempt + 1)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning("Attachment download error: %s (attempt %d)", e, attempt + 1)

        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))

    return False


async def download_doc_attachments(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    item_id: int,
    scope: str,
    attachment_urls: list[dict],
    state: CrawlState,
    counter: dict,
) -> None:
    """Download all attachments for a single document."""
    if not attachment_urls:
        state.mark_attachments_done(item_id, scope)
        counter["done"] += 1
        return

    dest_dir = ATTACHMENTS_DIR / str(item_id)
    dest_dir.mkdir(parents=True, exist_ok=True)

    all_ok = True
    for att in attachment_urls:
        dest = dest_dir / att["filename"]
        if dest.exists() and dest.stat().st_size > 0:
            continue  # Already downloaded

        async with sem:
            await asyncio.sleep(REQUEST_DELAY)
            ok = await download_file(session, att["url"], dest)

        if not ok:
            all_ok = False
            logger.warning("Failed to download: %s for ItemID=%d", att["filename"], item_id)

    if all_ok:
        state.mark_attachments_done(item_id, scope)
    counter["done"] += 1

    total = counter["done"] + counter["failed"]
    if total % 500 == 0:
        logger.info(
            "Attachment progress: %d / %d done",
            counter["done"], counter["total"],
        )


async def run_attachment_downloads(
    session: aiohttp.ClientSession,
    state: CrawlState,
    doc_type: str = "vbpq",
) -> None:
    """Run Phase 3: download attachments for all detail-crawled documents."""
    pending = state.get_pending_attachments(doc_type)
    if not pending:
        logger.info("[%s] No pending attachments to download.", doc_type)
        return

    logger.info("[%s] Starting attachment downloads for %d documents...", doc_type, len(pending))

    sem = asyncio.Semaphore(ATTACHMENT_CONCURRENCY)
    counter = {"done": 0, "failed": 0, "total": len(pending)}

    batch_size = 200
    for i in range(0, len(pending), batch_size):
        batch = pending[i : i + batch_size]
        tasks = []
        for doc in batch:
            detail = json.loads(doc["detail_json"]) if doc["detail_json"] else {}
            att_urls = detail.get("attachment_urls", [])
            tasks.append(
                download_doc_attachments(
                    session, sem, doc["item_id"], doc["scope"], att_urls, state, counter,
                )
            )
        await asyncio.gather(*tasks, return_exceptions=True)

    logger.info(
        "[%s] Attachment downloads complete! %d done, %d failed",
        doc_type, counter["done"], counter["failed"],
    )
