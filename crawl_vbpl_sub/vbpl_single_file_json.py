"""Single-file VBPL crawler with SQLite state and JSON export.

This script merges the existing multi-module logic into one file and adds a
native SQLite -> JSON export mode.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

try:
    import aiofiles
except ImportError:  # pragma: no cover - runtime environment dependent
    aiofiles = None  # type: ignore[assignment]

try:
    import aiohttp
except ImportError:  # pragma: no cover - runtime environment dependent
    aiohttp = None  # type: ignore[assignment]

try:
    from bs4 import BeautifulSoup, Tag
except ImportError:  # pragma: no cover - runtime environment dependent
    BeautifulSoup = None  # type: ignore[assignment]
    Tag = object  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://vbpl.vn"

# AJAX search endpoints (POST)
SEARCH_VBPQ_URL = BASE_URL + "/VBQPPL_UserControls/Publishing_22/TimKiem/p_KetQuaTimKiemVanBan.aspx"
SEARCH_VBHN_URL = BASE_URL + "/VBQPPL_UserControls/Publishing_22/Timkiem/p_KetQuaTimKiemHopNhat.aspx"

# Document detail page patterns
DETAIL_URL_TEMPLATE = BASE_URL + "/{scope}/Pages/vbpq-toanvan.aspx?ItemID={item_id}"
ATTRS_URL_TEMPLATE = BASE_URL + "/{scope}/Pages/vbpq-thuoctinh.aspx?ItemID={item_id}"
VBHN_DETAIL_URL_TEMPLATE = BASE_URL + "/{scope}/Pages/vbpq-van-ban-goc-hopnhat.aspx?ItemID={item_id}"
VBHN_ATTRS_URL_TEMPLATE = BASE_URL + "/{scope}/Pages/vbpq-thuoctinh-hopnhat.aspx?ItemID={item_id}"

FALLBACK_SCOPE = "botaichinh"

DISCOVERY_CONCURRENCY = 5
DETAIL_CONCURRENCY = 5
ATTACHMENT_CONCURRENCY = 5
ROWS_PER_PAGE = 50

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0
REQUEST_DELAY = 0.3
BACKOFF_ON_CONSECUTIVE_ERRORS = 5
BACKOFF_PAUSE_SECONDS = 60

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
DOCUMENTS_DIR = DATA_DIR / "documents"
ATTACHMENTS_DIR = DATA_DIR / "attachments"
LOGS_DIR = DATA_DIR / "logs"
STATE_DB_PATH = DATA_DIR / "crawl_state.db"

VBPQ_JSON = DOCUMENTS_DIR / "van_ban_phap_quy.json"
VBHN_JSON = DOCUMENTS_DIR / "van_ban_hop_nhat.json"
STATE_EXPORT_JSON = DOCUMENTS_DIR / "crawl_state_export.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Referer": BASE_URL + "/pages/vbpq-timkiem.aspx",
    "X-Requested-With": "XMLHttpRequest",
}

logger = logging.getLogger("vbpl_single")


# ---------------------------------------------------------------------------
# SQLite state
# ---------------------------------------------------------------------------


class CrawlState:
    """SQLite-backed crawl state for resume capability."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    item_id INTEGER NOT NULL,
                    scope TEXT NOT NULL,
                    doc_type TEXT NOT NULL,
                    status TEXT DEFAULT 'discovered',
                    detail_json TEXT,
                    error_message TEXT,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (item_id, scope)
                );

                CREATE TABLE IF NOT EXISTS crawl_progress (
                    doc_type TEXT PRIMARY KEY,
                    total_pages INTEGER DEFAULT 0,
                    last_completed_page INTEGER DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_documents_status
                    ON documents(status, doc_type);
                """
            )

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def get_last_page(self, doc_type: str) -> int:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT last_completed_page FROM crawl_progress WHERE doc_type = ?",
                (doc_type,),
            ).fetchone()
            return row["last_completed_page"] if row else 0

    def set_total_pages(self, doc_type: str, total: int) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO crawl_progress (doc_type, total_pages, last_completed_page)
                VALUES (?, ?, 0)
                ON CONFLICT(doc_type) DO UPDATE SET total_pages = ?
                """,
                (doc_type, total, total),
            )

    def update_last_page(self, doc_type: str, page: int) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE crawl_progress SET last_completed_page = ? WHERE doc_type = ?",
                (page, doc_type),
            )

    def add_discovered_batch(self, docs: list[tuple[int, str, str]]) -> None:
        with self._conn() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO documents (item_id, scope, doc_type, status)
                VALUES (?, ?, ?, 'discovered')
                """,
                docs,
            )

    def mark_detail_done(self, item_id: int, scope: str, detail_json: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE documents
                SET status = 'detail_done', detail_json = ?, updated_at = CURRENT_TIMESTAMP
                WHERE item_id = ? AND scope = ?
                """,
                (detail_json, item_id, scope),
            )

    def mark_attachments_done(self, item_id: int, scope: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE documents
                SET status = 'attachments_done', updated_at = CURRENT_TIMESTAMP
                WHERE item_id = ? AND scope = ?
                """,
                (item_id, scope),
            )

    def mark_failed(self, item_id: int, scope: str, error: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE documents
                SET status = 'failed', error_message = ?, updated_at = CURRENT_TIMESTAMP
                WHERE item_id = ? AND scope = ?
                """,
                (error, item_id, scope),
            )

    def get_pending_details(self, doc_type: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT item_id, scope
                FROM documents
                WHERE status = 'discovered' AND doc_type = ?
                ORDER BY item_id
                """,
                (doc_type,),
            ).fetchall()
            return [{"item_id": r["item_id"], "scope": r["scope"]} for r in rows]

    def get_pending_attachments(self, doc_type: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT item_id, scope, detail_json
                FROM documents
                WHERE status = 'detail_done' AND doc_type = ?
                ORDER BY item_id
                """,
                (doc_type,),
            ).fetchall()
            return [
                {"item_id": r["item_id"], "scope": r["scope"], "detail_json": r["detail_json"]}
                for r in rows
            ]

    def get_total_discovered(self, doc_type: str) -> int:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM documents WHERE doc_type = ?",
                (doc_type,),
            ).fetchone()
            return row["cnt"] if row else 0

    def get_stats(self) -> dict[str, dict[str, int]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT doc_type, status, COUNT(*) AS cnt FROM documents GROUP BY doc_type, status"
            ).fetchall()
            stats: dict[str, dict[str, int]] = {}
            for r in rows:
                stats.setdefault(r["doc_type"], {})[r["status"]] = r["cnt"]
            return stats


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


LABEL_MAP = {
    "số hiệu": "so_hieu",
    "số ký hiệu": "so_hieu",
    "loại văn bản": "loai_van_ban",
    "cơ quan ban hành": "co_quan_ban_hanh",
    "chức danh": "chuc_danh",
    "người ký": "nguoi_ky",
    "ngày ban hành": "ngay_ban_hanh",
    "ngày có hiệu lực": "ngay_hieu_luc",
    "ngày hiệu lực": "ngay_hieu_luc",
    "ngày hết hiệu lực": "ngay_het_hieu_luc",
    "tình trạng hiệu lực": "tinh_trang",
    "tình trạng": "tinh_trang",
    "lĩnh vực": "linh_vuc",
    "ngành": "nganh",
    "trích yếu": "trich_yeu",
    "nơi ban hành": "noi_ban_hanh",
    "ngày đăng công báo": "ngay_cong_bao",
    "số công báo": "so_cong_bao",
    "nguồn thu thập": "nguon_thu_thap",
    "phạm vi": "pham_vi",
}


def parse_search_results(html: str) -> tuple[list[dict], int]:
    soup = BeautifulSoup(html, "lxml")
    documents: list[dict] = []
    seen_ids: set[int] = set()

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "ItemID=" not in href:
            continue
        if not any(pat in href for pat in ("vbpq-toanvan", "ivbpq-toanvan", "vbpq-van-ban-goc")):
            continue

        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        item_id_str = qs.get("ItemID", [None])[0]
        if not item_id_str:
            continue

        try:
            item_id = int(item_id_str)
        except ValueError:
            continue

        path_parts = parsed.path.strip("/").split("/")
        scope = path_parts[0] if len(path_parts) >= 2 else "TW"

        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)

        documents.append(
            {
                "item_id": item_id,
                "scope": scope,
                "title": link.get_text(strip=True),
            }
        )

    total_pages = parse_total_pages(soup)
    return documents, total_pages


def parse_total_pages(soup: BeautifulSoup) -> int:
    paging_div = soup.find("div", class_="paging")
    if not paging_div:
        return 1

    max_page = 1
    for link in paging_div.find_all("a", href=True):
        href = link.get("href", "")
        page_match = re.search(r"Nexpage\w*\([^,]*,\s*'(\d+)'\)", href)
        if page_match:
            max_page = max(max_page, int(page_match.group(1)))
    return max_page


def parse_detail_page(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    return {
        "text": extract_full_text(soup),
        "metadata": extract_metadata(soup),
        "attachment_urls": extract_attachments(soup),
    }


def parse_attributes_page(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    return extract_metadata(soup)


def extract_full_text(soup: BeautifulSoup) -> str:
    content_selectors = [
        {"id": "toanvancontent"},
        {"id": "toanvan"},
        {"class_": "toanvancontent"},
        {"class_": "content1"},
        {"id": "divContentDoc"},
        {"class_": "fulltext"},
    ]

    content_div = None
    for selector in content_selectors:
        content_div = soup.find("div", **selector)
        if content_div:
            break

    if not content_div:
        content_div = find_largest_content_div(soup)
    if not content_div:
        return ""

    return html_to_clean_text(content_div)


def find_largest_content_div(soup: BeautifulSoup) -> Tag | None:
    best: Tag | None = None
    best_len = 0

    for div in soup.find_all("div"):
        text = div.get_text(strip=True)
        div_id = div.get("id", "")
        div_class = " ".join(div.get("class", []))
        combined = (div_id + " " + div_class).lower()
        if any(skip in combined for skip in ("menu", "nav", "header", "footer", "sidebar")):
            continue
        if len(text) > best_len:
            best = div
            best_len = len(text)
    return best


def html_to_clean_text(element: Tag) -> str:
    for tag in element.find_all(["script", "style", "nav"]):
        tag.decompose()

    chunks: list[str] = []
    for child in element.descendants:
        if isinstance(child, str):
            text = child.strip()
            if text:
                chunks.append(text)
        elif isinstance(child, Tag):
            if child.name in ("br",):
                chunks.append("\n")
            elif child.name in ("p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "tr", "li"):
                chunks.append("\n")
            elif child.name == "td":
                chunks.append("\t")

    text = "".join(chunks)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_metadata(soup: BeautifulSoup) -> dict[str, str]:
    metadata: dict[str, str] = {}

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if not cells:
            continue

        first_text = cells[0].get_text(strip=True).lower()
        if "cơ quan ban hành" in first_text and "người ký" in first_text:
            if len(cells) >= 4:
                metadata["co_quan_ban_hanh"] = cells[1].get_text(strip=True)
                metadata["chuc_danh"] = cells[2].get_text(strip=True)
                metadata["nguoi_ky"] = cells[3].get_text(strip=True)
            continue

        if "tình trạng hiệu lực" in first_text:
            value = first_text.split(":", 1)[-1].strip() if ":" in first_text else ""
            if value:
                metadata["tinh_trang"] = value
            continue

        i = 0
        while i < len(cells):
            cell = cells[i]
            cell_text = cell.get_text(strip=True).lower()
            cell_classes = cell.get("class", [])
            if "label" in cell_classes or any(label in cell_text for label in LABEL_MAP):
                if i + 1 < len(cells):
                    value = cells[i + 1].get_text(strip=True)
                    for vn_label, key in LABEL_MAP.items():
                        if vn_label in cell_text and value and value != "...":
                            metadata[key] = value
                            break
                    i += 2
                    continue
            i += 1

    for li in soup.find_all("li"):
        span = li.find("span")
        if not span:
            continue
        label = span.get_text(strip=True).lower().rstrip(":")
        value = li.get_text(strip=True)
        value = value[len(span.get_text(strip=True)) :].strip().lstrip(":")
        for vn_label, key in LABEL_MAP.items():
            if vn_label in label and value:
                metadata.setdefault(key, value)
                break

    if "trich_yeu" not in metadata:
        title_el = soup.find(["h1", "h2"], class_=re.compile(r"(title|tieude)", re.I))
        if title_el:
            metadata["trich_yeu"] = title_el.get_text(strip=True)

    return metadata


def extract_attachments(soup: BeautifulSoup) -> list[dict]:
    attachments: list[dict] = []
    seen_urls: set[str] = set()

    for link in soup.find_all("a", href=True):
        href = link["href"]
        lower_href = href.lower()
        if not any(lower_href.endswith(ext) for ext in (".doc", ".docx", ".pdf", ".zip", ".rar")):
            continue
        if href in seen_urls:
            continue
        seen_urls.add(href)

        filename = href.rsplit("/", maxsplit=1)[-1]
        if lower_href.endswith(".pdf"):
            file_type = "pdf"
        elif lower_href.endswith((".doc", ".docx")):
            file_type = "doc"
        else:
            file_type = "other"

        if href.startswith("/"):
            href = f"{BASE_URL}{href}"
        elif not href.startswith("http"):
            href = f"{BASE_URL}/{href}"

        attachments.append({"filename": filename, "url": href, "type": file_type})

    return attachments


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


async def fetch_search_page(
    session: aiohttp.ClientSession,
    url: str,
    page: int,
) -> str:
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
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp:
                if resp.status == 200:
                    return await resp.text()
                logger.warning("Search page %d returned status %d (attempt %d)", page, resp.status, attempt + 1)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("Search page %d error: %s (attempt %d)", page, exc, attempt + 1)

        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_BASE_DELAY * (2**attempt))
    return ""


async def discover_page(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    url: str,
    page: int,
    doc_type: str,
    state: CrawlState,
) -> int:
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
    doc_type: str,
    discovery_concurrency: int,
) -> int:
    url = SEARCH_VBPQ_URL if doc_type == "vbpq" else SEARCH_VBHN_URL
    label = "Văn bản pháp quy" if doc_type == "vbpq" else "Văn bản hợp nhất"
    logger.info("Starting discovery for %s...", label)

    html = await fetch_search_page(session, url, 1)
    if not html:
        logger.error("Cannot fetch first page for %s", doc_type)
        return 0

    documents, total_pages = parse_search_results(html)
    logger.info("[%s] Total pages: %d", doc_type, total_pages)
    if total_pages == 0:
        total_pages = 1

    state.set_total_pages(doc_type, total_pages)
    if documents:
        state.add_discovered_batch([(d["item_id"], d["scope"], doc_type) for d in documents])
    state.update_last_page(doc_type, 1)

    start_page = state.get_last_page(doc_type) + 1
    if start_page > total_pages:
        total = state.get_total_discovered(doc_type)
        logger.info("[%s] Discovery already complete. %d documents in DB.", doc_type, total)
        return total

    logger.info("[%s] Resuming from page %d / %d", doc_type, start_page, total_pages)
    sem = asyncio.Semaphore(discovery_concurrency)
    batch_size = 100

    for batch_start in range(start_page, total_pages + 1, batch_size):
        batch_end = min(batch_start + batch_size, total_pages + 1)
        tasks = [
            discover_page(session, sem, url, page, doc_type, state)
            for page in range(batch_start, batch_end)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error("Discovery task error: %s", result)
        logger.info(
            "[%s] Progress: pages %d-%d / %d done | Total discovered so far: %d",
            doc_type,
            batch_start,
            batch_end - 1,
            total_pages,
            state.get_total_discovered(doc_type),
        )

    total = state.get_total_discovered(doc_type)
    logger.info("[%s] Discovery complete! Total: %d documents", doc_type, total)
    return total


# ---------------------------------------------------------------------------
# Detail crawl
# ---------------------------------------------------------------------------


_consecutive_errors = 0


async def _fetch_url(
    session: aiohttp.ClientSession,
    url: str,
    item_id: int,
) -> str | None:
    global _consecutive_errors
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    if "resource cannot be found" in text.lower() or "không tìm thấy" in text.lower():
                        return None
                    _consecutive_errors = 0
                    return text
                if resp.status == 404:
                    return None
                logger.warning("Detail ItemID=%d status=%d (attempt %d)", item_id, resp.status, attempt + 1)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning(
                "Detail ItemID=%d error: %s %s (attempt %d)",
                item_id,
                type(exc).__name__,
                exc,
                attempt + 1,
            )

        _consecutive_errors += 1
        if _consecutive_errors >= BACKOFF_ON_CONSECUTIVE_ERRORS:
            logger.warning(
                "Too many consecutive errors (%d), pausing %ds...",
                _consecutive_errors,
                BACKOFF_PAUSE_SECONDS,
            )
            await asyncio.sleep(BACKOFF_PAUSE_SECONDS)
            _consecutive_errors = 0

        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_BASE_DELAY * (2**attempt))
    return None


async def fetch_detail(
    session: aiohttp.ClientSession,
    item_id: int,
    scope: str,
    doc_type: str,
) -> str | None:
    url_template = VBHN_DETAIL_URL_TEMPLATE if doc_type == "vbhn" else DETAIL_URL_TEMPLATE
    url = url_template.format(scope=scope, item_id=item_id)
    result = await _fetch_url(session, url, item_id)
    if result is None and scope.lower() == "tw":
        fallback = url_template.format(scope=FALLBACK_SCOPE, item_id=item_id)
        result = await _fetch_url(session, fallback, item_id)
    return result


async def fetch_attrs(
    session: aiohttp.ClientSession,
    item_id: int,
    scope: str,
    doc_type: str,
) -> str | None:
    url_template = VBHN_ATTRS_URL_TEMPLATE if doc_type == "vbhn" else ATTRS_URL_TEMPLATE
    effective_scope = FALLBACK_SCOPE if scope.lower() == "tw" else scope
    url = url_template.format(scope=effective_scope, item_id=item_id)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status == 200:
                return await resp.text()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return None
    return None


async def crawl_single_detail(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    item_id: int,
    scope: str,
    doc_type: str,
    state: CrawlState,
    counter: dict[str, int],
) -> None:
    async with sem:
        await asyncio.sleep(REQUEST_DELAY)
        html = await fetch_detail(session, item_id, scope, doc_type)

    if html is None:
        state.mark_failed(item_id, scope, "fetch_failed_or_404")
        counter["failed"] += 1
        return

    try:
        doc = parse_detail_page(html)
    except Exception as exc:  # noqa: BLE001
        state.mark_failed(item_id, scope, f"parse_error: {exc}")
        counter["failed"] += 1
        return

    async with sem:
        attrs_html = await fetch_attrs(session, item_id, scope, doc_type)
    if attrs_html:
        try:
            attrs_meta = parse_attributes_page(attrs_html)
            attrs_meta.update(doc["metadata"])  # detail page keys win
            doc["metadata"] = attrs_meta
        except Exception as exc:  # noqa: BLE001
            logger.debug("Attrs parse error ItemID=%d: %s", item_id, exc)

    doc["metadata"]["scope"] = scope
    doc["metadata"]["doc_type"] = "van_ban_phap_quy" if doc_type == "vbpq" else "van_ban_hop_nhat"
    url_template = VBHN_DETAIL_URL_TEMPLATE if doc_type == "vbhn" else DETAIL_URL_TEMPLATE
    doc["metadata"]["source_url"] = url_template.format(scope=scope, item_id=item_id)
    doc["metadata"]["downloaded_timestamp"] = datetime.now(timezone.utc).isoformat()

    state.mark_detail_done(item_id, scope, json.dumps(doc, ensure_ascii=False))

    counter["done"] += 1
    total = counter["done"] + counter["failed"]
    if total % 500 == 0:
        logger.info(
            "[%s] Detail progress: %d done, %d failed / %d total",
            doc_type,
            counter["done"],
            counter["failed"],
            counter["total"],
        )


async def run_detail_crawl(
    session: aiohttp.ClientSession,
    state: CrawlState,
    doc_type: str,
    detail_concurrency: int,
) -> None:
    pending = state.get_pending_details(doc_type)
    if not pending:
        logger.info("[%s] No pending documents for detail crawl.", doc_type)
        return

    logger.info("[%s] Starting detail crawl for %d documents...", doc_type, len(pending))
    sem = asyncio.Semaphore(detail_concurrency)
    counter = {"done": 0, "failed": 0, "total": len(pending)}
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
            doc_type,
            counter["done"],
            counter["total"],
            counter["failed"],
        )

    logger.info("[%s] Detail crawl complete! %d done, %d failed", doc_type, counter["done"], counter["failed"])


# ---------------------------------------------------------------------------
# Attachments
# ---------------------------------------------------------------------------


async def download_file(session: aiohttp.ClientSession, url: str, dest: Path) -> bool:
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    async with aiofiles.open(dest, "wb") as handle:
                        await handle.write(content)
                    return True
                if resp.status == 404:
                    return False
                logger.warning("Attachment %s status=%d (attempt %d)", url, resp.status, attempt + 1)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("Attachment download error: %s (attempt %d)", exc, attempt + 1)

        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_BASE_DELAY * (2**attempt))
    return False


async def download_doc_attachments(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    item_id: int,
    scope: str,
    attachment_urls: list[dict],
    state: CrawlState,
    counter: dict[str, int],
) -> None:
    if not attachment_urls:
        state.mark_attachments_done(item_id, scope)
        counter["done"] += 1
        return

    dest_dir = ATTACHMENTS_DIR / str(item_id)
    dest_dir.mkdir(parents=True, exist_ok=True)

    all_ok = True
    for attachment in attachment_urls:
        dest = dest_dir / attachment["filename"]
        if dest.exists() and dest.stat().st_size > 0:
            continue

        async with sem:
            await asyncio.sleep(REQUEST_DELAY)
            ok = await download_file(session, attachment["url"], dest)
        if not ok:
            all_ok = False
            logger.warning("Failed to download: %s for ItemID=%d", attachment["filename"], item_id)

    if all_ok:
        state.mark_attachments_done(item_id, scope)
    counter["done"] += 1

    total = counter["done"] + counter["failed"]
    if total % 500 == 0:
        logger.info("Attachment progress: %d / %d done", counter["done"], counter["total"])


async def run_attachment_downloads(
    session: aiohttp.ClientSession,
    state: CrawlState,
    doc_type: str,
    attachment_concurrency: int,
) -> None:
    pending = state.get_pending_attachments(doc_type)
    if not pending:
        logger.info("[%s] No pending attachments to download.", doc_type)
        return

    logger.info("[%s] Starting attachment downloads for %d documents...", doc_type, len(pending))
    sem = asyncio.Semaphore(attachment_concurrency)
    counter = {"done": 0, "failed": 0, "total": len(pending)}
    batch_size = 200

    for i in range(0, len(pending), batch_size):
        batch = pending[i : i + batch_size]
        tasks = []
        for doc in batch:
            detail = json.loads(doc["detail_json"]) if doc["detail_json"] else {}
            tasks.append(
                download_doc_attachments(
                    session,
                    sem,
                    doc["item_id"],
                    doc["scope"],
                    detail.get("attachment_urls", []),
                    state,
                    counter,
                )
            )
        await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("[%s] Attachment downloads complete! %d done, %d failed", doc_type, counter["done"], counter["failed"])


# ---------------------------------------------------------------------------
# SQLite -> JSON export
# ---------------------------------------------------------------------------


def build_output_record(row: sqlite3.Row, detail: dict) -> dict:
    item_id = row["item_id"]
    scope = row["scope"]
    doc_type = row["doc_type"]
    metadata = dict(detail.get("metadata") or {})

    metadata.setdefault("scope", scope)
    metadata.setdefault("doc_type", "van_ban_phap_quy" if doc_type == "vbpq" else "van_ban_hop_nhat")
    detail_template = VBHN_DETAIL_URL_TEMPLATE if doc_type == "vbhn" else DETAIL_URL_TEMPLATE
    metadata.setdefault("source_url", detail_template.format(scope=scope, item_id=item_id))

    downloaded_timestamp = metadata.get("downloaded_timestamp") or row["updated_at"]

    return {
        "id": f"VBPL-{item_id}",
        "text": detail.get("text", ""),
        "metadata": metadata,
        "attachments": detail.get("attachment_urls", []),
        "created_timestamp": metadata.get("ngay_ban_hanh"),
        "downloaded_timestamp": downloaded_timestamp,
        "url": metadata["source_url"],
    }


def export_doc_type_json(state: CrawlState, doc_type: str, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    exported = 0

    with state._conn() as conn, output_path.open("w", encoding="utf-8") as handle:
        handle.write("[\n")
        first = True
        rows = conn.execute(
            """
            SELECT item_id, scope, doc_type, status, detail_json, updated_at
            FROM documents
            WHERE doc_type = ? AND detail_json IS NOT NULL AND detail_json <> ''
            ORDER BY item_id
            """,
            (doc_type,),
        )
        for row in rows:
            try:
                detail = json.loads(row["detail_json"])
            except json.JSONDecodeError:
                continue
            record = build_output_record(row, detail)
            if not first:
                handle.write(",\n")
            handle.write(json.dumps(record, ensure_ascii=False))
            first = False
            exported += 1
        handle.write("\n]\n")

    return exported


def export_full_state_json(state: CrawlState, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0

    with state._conn() as conn, output_path.open("w", encoding="utf-8") as handle:
        progress = [dict(row) for row in conn.execute("SELECT * FROM crawl_progress ORDER BY doc_type")]
        header = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "db_path": str(state.db_path),
            "crawl_progress": progress,
            "documents": [],
        }

        # Start object without materializing all documents in memory.
        prefix = json.dumps(
            {
                "generated_at": header["generated_at"],
                "db_path": header["db_path"],
                "crawl_progress": header["crawl_progress"],
            },
            ensure_ascii=False,
        )
        # Convert {"a":..., "b":...} into {"a":..., "b":..., "documents":[...]}
        handle.write(prefix[:-1])
        handle.write(', "documents": [\n')

        first = True
        rows = conn.execute(
            """
            SELECT item_id, scope, doc_type, status, detail_json, error_message,
                   discovered_at, updated_at
            FROM documents
            ORDER BY doc_type, item_id
            """
        )
        for row in rows:
            document = {
                "item_id": row["item_id"],
                "scope": row["scope"],
                "doc_type": row["doc_type"],
                "status": row["status"],
                "error_message": row["error_message"],
                "discovered_at": row["discovered_at"],
                "updated_at": row["updated_at"],
                "detail": None,
                "record": None,
            }

            if row["detail_json"]:
                try:
                    detail = json.loads(row["detail_json"])
                    document["detail"] = detail
                    document["record"] = build_output_record(row, detail)
                except json.JSONDecodeError:
                    document["detail"] = row["detail_json"]

            if not first:
                handle.write(",\n")
            handle.write(json.dumps(document, ensure_ascii=False))
            first = False
            row_count += 1

        handle.write("\n]}\n")

    return row_count


def show_stats(state: CrawlState) -> None:
    stats = state.get_stats()
    if not stats:
        print("No crawl data found.")
        return

    print("\nCrawl Statistics")
    print("=" * 50)
    for doc_type, counts in stats.items():
        label = "Văn bản pháp quy" if doc_type == "vbpq" else "Văn bản hợp nhất"
        print(f"\n{label} ({doc_type}):")
        total = sum(counts.values())
        for status, count in sorted(counts.items()):
            pct = (count / total * 100) if total else 0
            print(f"  {status:20s}: {count:>8,d} ({pct:.1f}%)")
        print(f"  {'TOTAL':20s}: {total:>8,d}")
    print()


def setup_logging(verbose: bool) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / f"crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")],
    )
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("charset_normalizer").setLevel(logging.WARNING)


def ensure_crawl_dependencies() -> None:
    missing: list[str] = []
    if aiohttp is None:
        missing.append("aiohttp")
    if aiofiles is None:
        missing.append("aiofiles")
    if BeautifulSoup is None:
        missing.append("beautifulsoup4")

    if missing:
        raise RuntimeError(
            "Missing crawl dependencies: "
            + ", ".join(missing)
            + ". Install with: pip install aiohttp aiofiles beautifulsoup4 lxml"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Single-file VBPL crawler with JSON output")
    parser.add_argument(
        "--phase",
        choices=["discovery", "details", "attachments", "all", "export"],
        default="all",
        help="Which phase to run (default: all)",
    )
    parser.add_argument(
        "--doc-type",
        choices=["vbpq", "vbhn", "all"],
        default="all",
        help="Document type to process (default: all)",
    )
    parser.add_argument("--no-attachments", action="store_true", help="Skip attachment downloads")
    parser.add_argument("--stats", action="store_true", help="Show crawl stats and exit")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--db-path", type=Path, default=STATE_DB_PATH, help="SQLite state database path")
    parser.add_argument("--concurrency", type=int, default=0, help="Override detail crawl concurrency")
    parser.add_argument(
        "--export-json",
        type=Path,
        default=None,
        help="Optional custom path for full SQLite export JSON",
    )
    return parser.parse_args()


async def run_crawl(args: argparse.Namespace) -> None:
    ensure_crawl_dependencies()
    state = CrawlState(args.db_path)
    doc_types = ["vbpq", "vbhn"] if args.doc_type == "all" else [args.doc_type]

    detail_concurrency = args.concurrency if args.concurrency > 0 else DETAIL_CONCURRENCY

    connector = aiohttp.TCPConnector(limit=20, limit_per_host=10, ttl_dns_cache=300, enable_cleanup_closed=True)
    timeout = aiohttp.ClientTimeout(total=60, connect=10)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=HEADERS) as session:
        for doc_type in doc_types:
            label = "VBPQ" if doc_type == "vbpq" else "VBHN"

            if args.phase in ("discovery", "all"):
                logger.info("=" * 60)
                logger.info("PHASE 1: Discovery [%s]", label)
                logger.info("=" * 60)
                total = await run_discovery(session, state, doc_type, DISCOVERY_CONCURRENCY)
                logger.info("Discovered %d documents for %s", total, label)

            if args.phase in ("details", "all"):
                logger.info("=" * 60)
                logger.info("PHASE 2: Detail Crawl [%s]", label)
                logger.info("=" * 60)
                await run_detail_crawl(session, state, doc_type, detail_concurrency)

            if args.phase in ("attachments", "all") and not args.no_attachments:
                logger.info("=" * 60)
                logger.info("PHASE 3: Attachment Downloads [%s]", label)
                logger.info("=" * 60)
                await run_attachment_downloads(session, state, doc_type, ATTACHMENT_CONCURRENCY)

    logger.info("=" * 60)
    logger.info("CRAWL COMPLETE")
    logger.info("=" * 60)
    for doc_type, counts in state.get_stats().items():
        logger.info("[%s] %s", doc_type, counts)


def run_exports(args: argparse.Namespace) -> None:
    state = CrawlState(args.db_path)

    if args.doc_type in ("all", "vbpq"):
        count = export_doc_type_json(state, "vbpq", VBPQ_JSON)
        logger.info("Exported %d VBPQ records to %s", count, VBPQ_JSON)

    if args.doc_type in ("all", "vbhn"):
        count = export_doc_type_json(state, "vbhn", VBHN_JSON)
        logger.info("Exported %d VBHN records to %s", count, VBHN_JSON)

    full_export_path = args.export_json if args.export_json else STATE_EXPORT_JSON
    total_rows = export_full_state_json(state, full_export_path)
    logger.info("Exported %d SQLite rows to %s", total_rows, full_export_path)


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)

    state = CrawlState(args.db_path)
    if args.stats:
        show_stats(state)
        return

    logger.info("Starting single-file VBPL crawler at %s", datetime.now(timezone.utc).isoformat())
    logger.info(
        "Args: phase=%s doc_type=%s no_attachments=%s db_path=%s",
        args.phase,
        args.doc_type,
        args.no_attachments,
        args.db_path,
    )

    try:
        if args.phase == "export":
            run_exports(args)
        else:
            asyncio.run(run_crawl(args))
            run_exports(args)  # Always export JSON after crawl phases.
    except KeyboardInterrupt:
        logger.info("Interrupted by user. State saved — resume with same command.")
        sys.exit(1)


if __name__ == "__main__":
    main()
