"""CLI entry point and orchestrator for the VBPL crawler."""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone

import aiohttp

from crawler.config import (
    HEADERS,
    LOGS_DIR,
    STATE_DB_PATH,
)
from crawler.state import CrawlState


def setup_logging() -> None:
    """Configure logging to both file and console."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / f"crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    # Quiet noisy libs
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("charset_normalizer").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl vbpl.vn legal documents")
    parser.add_argument(
        "--phase",
        choices=["discovery", "details", "attachments", "all"],
        default="all",
        help="Which phase to run (default: all)",
    )
    parser.add_argument(
        "--doc-type",
        choices=["vbpq", "vbhn", "all"],
        default="all",
        help="Document type to crawl (default: all)",
    )
    parser.add_argument(
        "--no-attachments",
        action="store_true",
        help="Skip attachment downloads (text only)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=0,
        help="Override detail crawl concurrency (default: use config)",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show crawl stats and exit",
    )
    return parser.parse_args()


async def run_crawl(args: argparse.Namespace) -> None:
    """Main crawl orchestrator."""
    # Lazy imports to avoid circular deps and keep startup fast
    from crawler.attachments import run_attachment_downloads
    from crawler.detail import run_detail_crawl
    from crawler.discovery import run_discovery

    if args.concurrency > 0:
        import crawler.config as cfg
        cfg.DETAIL_CONCURRENCY = args.concurrency

    state = CrawlState(STATE_DB_PATH)
    doc_types = ["vbpq", "vbhn"] if args.doc_type == "all" else [args.doc_type]

    connector = aiohttp.TCPConnector(
        limit=20,
        limit_per_host=10,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    timeout = aiohttp.ClientTimeout(total=60, connect=10)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=HEADERS,
    ) as session:
        for doc_type in doc_types:
            label = "VBPQ" if doc_type == "vbpq" else "VBHN"

            # Phase 1: Discovery
            if args.phase in ("discovery", "all"):
                logging.info("=" * 60)
                logging.info("PHASE 1: Discovery [%s]", label)
                logging.info("=" * 60)
                total = await run_discovery(session, state, doc_type)
                logging.info("Discovered %d documents for %s", total, label)

            # Phase 2: Detail crawl
            if args.phase in ("details", "all"):
                logging.info("=" * 60)
                logging.info("PHASE 2: Detail Crawl [%s]", label)
                logging.info("=" * 60)
                await run_detail_crawl(session, state, doc_type)

            # Phase 3: Attachments
            if args.phase in ("attachments", "all") and not args.no_attachments:
                logging.info("=" * 60)
                logging.info("PHASE 3: Attachment Downloads [%s]", label)
                logging.info("=" * 60)
                await run_attachment_downloads(session, state, doc_type)

    # Final stats
    stats = state.get_stats()
    logging.info("=" * 60)
    logging.info("CRAWL COMPLETE")
    logging.info("=" * 60)
    for dt, counts in stats.items():
        logging.info("[%s] %s", dt, counts)


def show_stats() -> None:
    """Print current crawl statistics."""
    state = CrawlState(STATE_DB_PATH)
    stats = state.get_stats()

    if not stats:
        print("No crawl data found.")
        return

    print("\n📊 Crawl Statistics")
    print("=" * 50)
    for doc_type, counts in stats.items():
        label = "Văn bản pháp quy" if doc_type == "vbpq" else "Văn bản hợp nhất"
        print(f"\n{label} ({doc_type}):")
        total = sum(counts.values())
        for status, count in sorted(counts.items()):
            pct = (count / total * 100) if total > 0 else 0
            print(f"  {status:20s}: {count:>8,d} ({pct:.1f}%)")
        print(f"  {'TOTAL':20s}: {total:>8,d}")

    print()


def main() -> None:
    args = parse_args()

    if args.stats:
        show_stats()
        return

    setup_logging()
    logging.info("Starting VBPL crawler at %s", datetime.now(timezone.utc).isoformat())
    logging.info("Args: phase=%s doc_type=%s no_attachments=%s", args.phase, args.doc_type, args.no_attachments)

    try:
        asyncio.run(run_crawl(args))
    except KeyboardInterrupt:
        logging.info("Interrupted by user. State saved — resume with same command.")
        sys.exit(1)


if __name__ == "__main__":
    main()
