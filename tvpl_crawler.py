"""
Full crawler for thuvienphapluat.vn document pages.

Features:
- Discover document URLs from sitemap index (`SiteMap.aspx` / `sitemap.xml`)
- Crawl document HTML and extract metadata + main text content
- Multiprocess document crawling (`--workers`)
- Save streamed JSONL output to local disk (sharded by sitemap)
- Resume from checkpoint after interruption
- Retry with adaptive backoff for throttling/challenge responses
- Progress logging with ETA
- Retry mode for failed URLs from `errors.jsonl`

Usage examples:
    python3 tvpl_crawler.py --max-documents 10
    python3 tvpl_crawler.py --start-sitemap 1 --end-sitemap 20 --sleep 2.0
    python3 tvpl_crawler.py --resume --output-dir tvpl_dump
    python3 tvpl_crawler.py --retry-errors-file tvpl_output/errors.jsonl --workers 1 --sleep 4
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import random
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    import cloudscraper
except Exception:  # pragma: no cover - optional dependency
    cloudscraper = None


DEFAULT_SITEMAP_INDEX_URL = "https://thuvienphapluat.vn/SiteMap.aspx"
RETRYABLE_STATUS_CODES = {403, 429, 500, 502, 503, 504}


def _create_http_session(transport: str):
    if transport == "cloudscraper":
        if cloudscraper is None:
            raise RuntimeError(
                "transport=cloudscraper requested but package is not installed. "
                "Install with: pip install cloudscraper"
            )
        session = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
    elif transport == "requests":
        session = requests.Session()
    elif transport == "auto":
        if cloudscraper is not None:
            session = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
        else:
            session = requests.Session()
    else:
        raise ValueError(f"Unsupported transport: {transport}")

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
        }
    )
    return session

# Map display labels on thuvienphapluat.vn to canonical field names.
LABEL_FIELD_MAP = {
    "số hiệu": "so_hieu",
    "loại văn bản": "loai_van_ban",
    "nơi ban hành": "noi_ban_hanh",
    "người ký": "nguoi_ky",
    "ngày ban hành": "ngay_ban_hanh",
    "ngày hiệu lực": "ngay_hieu_luc",
    "ngày công báo": "ngay_cong_bao",
    "số công báo": "so_cong_bao",
    "tình trạng": "tinh_trang",
}

METADATA_LABEL_PATTERN = re.compile(
    r"(Số\s*hiệu|Loại\s*văn\s*bản|Nơi\s*ban\s*hành|Người\s*ký|Ngày\s*ban\s*hành|"
    r"Ngày\s*hiệu\s*lực|Ngày\s*công\s*báo|Số\s*công\s*báo|Tình\s*trạng)\s*:",
    re.IGNORECASE,
)

CONTENT_SELECTORS: Sequence[str] = (
    "#divNoiDung",
    ".content1",
    "#divPrint",
    ".contentDoc .content1",
    ".contentDoc",
    "article",
    "main",
)


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _strip_bom_prefix(text: str) -> str:
    if text.startswith("\ufeff"):
        return text[1:]
    if text.startswith("ï»¿"):
        return text[3:]
    return text


def _normalize_label(label: str) -> str:
    return _normalize_whitespace(label).lower().rstrip(":")


def parse_sitemap(xml_string: str) -> List[str]:
    """Parse sitemap XML and return all <loc> values."""
    xml_string = _strip_bom_prefix(xml_string.strip())

    leading = xml_string[:200].lower()
    if "<html" in leading or "<!doctype html" in leading:
        raise ValueError(
            "Expected XML sitemap but received HTML. "
            "This usually means Cloudflare challenge or throttling."
        )

    root = ET.fromstring(xml_string)
    urls: List[str] = []
    for elem in root.iter():
        tag = elem.tag.split("}", 1)[-1].lower()
        if tag == "loc" and elem.text:
            loc = elem.text.strip()
            if loc:
                urls.append(loc)
    return urls


def parse_sitemap_links_from_html(html_string: str, *, base_url: str) -> List[str]:
    """
    Parse an HTML sitemap page (e.g., SiteMap.aspx) and return linked XML sitemap URLs.
    """
    soup = BeautifulSoup(html_string, "html.parser")
    urls: List[str] = []
    seen = set()

    for link in soup.select("a[href]"):
        href = str(link.get("href", "")).strip()
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        path = urlparse(abs_url).path.lower()
        if not path.endswith(".xml"):
            continue
        if abs_url in seen:
            continue
        seen.add(abs_url)
        urls.append(abs_url)

    return urls


@dataclass
class DocumentAttributes:
    so_hieu: Optional[str] = None
    loai_van_ban: Optional[str] = None
    noi_ban_hanh: Optional[str] = None
    nguoi_ky: Optional[str] = None
    ngay_ban_hanh: Optional[str] = None
    ngay_hieu_luc: Optional[str] = None
    ngay_cong_bao: Optional[str] = None
    so_cong_bao: Optional[str] = None
    tinh_trang: Optional[str] = None
    title: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Optional[str]]) -> "DocumentAttributes":
        return cls(**data)


def _extract_metadata_fields(text: str) -> Dict[str, str]:
    text = _normalize_whitespace(text)
    matches = list(METADATA_LABEL_PATTERN.finditer(text))
    if not matches:
        return {}

    extracted: Dict[str, str] = {}
    for idx, match in enumerate(matches):
        label = _normalize_label(match.group(1))
        field = LABEL_FIELD_MAP.get(label)
        if not field:
            continue

        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        value = _normalize_whitespace(text[start:end]).strip(":- ")
        if value:
            extracted[field] = value
    return extracted


def parse_document_attributes(html_string: str) -> DocumentAttributes:
    """
    Extract metadata from document HTML.

    The current site layout commonly stores metadata inside #divThuocTinh,
    but this parser also falls back to broader containers for older pages.
    """
    soup = BeautifulSoup(html_string, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else None

    attributes: Dict[str, Optional[str]] = {
        "so_hieu": None,
        "loai_van_ban": None,
        "noi_ban_hanh": None,
        "nguoi_ky": None,
        "ngay_ban_hanh": None,
        "ngay_hieu_luc": None,
        "ngay_cong_bao": None,
        "so_cong_bao": None,
        "tinh_trang": None,
        "title": title,
    }

    candidate_nodes = [
        soup.select_one("#divThuocTinh"),
        soup.select_one("#tab-1"),
        soup.select_one(".contentDoc"),
        soup.body,
    ]
    seen_texts = set()
    for node in candidate_nodes:
        if node is None:
            continue
        text = _normalize_whitespace(node.get_text(" ", strip=True))
        if not text or text in seen_texts:
            continue
        seen_texts.add(text)

        fields = _extract_metadata_fields(text)
        for key, value in fields.items():
            if not attributes.get(key):
                attributes[key] = value
        if attributes["so_hieu"] and attributes["loai_van_ban"]:
            break

    return DocumentAttributes.from_dict(attributes)


def extract_document_content(html_string: str) -> str:
    """Extract main document text from common content containers."""
    soup = BeautifulSoup(html_string, "html.parser")
    best_text = ""

    for selector in CONTENT_SELECTORS:
        for node in soup.select(selector):
            text = _normalize_whitespace(node.get_text(" ", strip=True))
            if len(text) > len(best_text):
                best_text = text

    if best_text:
        return best_text

    body = soup.body if soup.body else soup
    return _normalize_whitespace(body.get_text(" ", strip=True))


def _decode_response_text(response: requests.Response, *, prefer_utf8_sig: bool = False) -> str:
    raw = response.content
    if prefer_utf8_sig:
        try:
            return raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            pass

    encoding = response.encoding or response.apparent_encoding or "utf-8"
    try:
        return raw.decode(encoding, errors="replace")
    except LookupError:
        return raw.decode("utf-8", errors="replace")


def _looks_like_challenge_page(text: str) -> bool:
    sample = text[:1200].lower()
    return ("just a moment" in sample and "cloudflare" in sample) or "cf-challenge" in sample


def _parse_retry_after_seconds(retry_after_header: Optional[str]) -> Optional[float]:
    if not retry_after_header:
        return None

    value = retry_after_header.strip()
    if not value:
        return None

    try:
        return max(0.0, float(int(value)))
    except ValueError:
        pass

    try:
        retry_dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None

    if retry_dt.tzinfo is None:
        retry_dt = retry_dt.replace(tzinfo=timezone.utc)
    seconds = (retry_dt - datetime.now(timezone.utc)).total_seconds()
    return max(0.0, seconds)


def _compute_retry_sleep_seconds(
    *,
    status_code: Optional[int],
    retry_after_header: Optional[str],
    attempt: int,
    base_sleep: float,
) -> float:
    retry_after = _parse_retry_after_seconds(retry_after_header)
    if retry_after is not None:
        floor = max(base_sleep, retry_after)
    elif status_code == 429:
        floor = max(base_sleep, 8.0)
    elif status_code == 403:
        floor = max(base_sleep, 5.0)
    else:
        floor = max(base_sleep, 1.0)

    sleep_for = min(120.0, floor * (2 ** max(0, attempt - 1)))
    sleep_for += random.uniform(0.2, max(0.8, floor * 0.35))
    return sleep_for


@dataclass
class CrawlCheckpoint:
    current_sitemap_index: int = 1  # 1-based index in discovered sitemap list
    next_url_index: int = 0  # 0-based offset in current sitemap's URL list
    documents_saved: int = 0
    errors: int = 0


_WORKER_SESSION = None
_WORKER_TIMEOUT = 45.0
_WORKER_RETRIES = 5
_WORKER_SLEEP_SECONDS = 0.0
_WORKER_TRANSPORT = "auto"


def _init_worker(timeout: float, retries: int, sleep_seconds: float, transport: str) -> None:
    global _WORKER_SESSION, _WORKER_TIMEOUT, _WORKER_RETRIES, _WORKER_SLEEP_SECONDS, _WORKER_TRANSPORT
    _WORKER_TIMEOUT = timeout
    _WORKER_RETRIES = retries
    _WORKER_SLEEP_SECONDS = sleep_seconds
    _WORKER_TRANSPORT = transport
    random.seed(time.time() + os.getpid())
    _WORKER_SESSION = _create_http_session(transport)


def _rotate_worker_session() -> None:
    global _WORKER_SESSION
    if _WORKER_SESSION is not None:
        try:
            _WORKER_SESSION.close()
        except Exception:
            pass
    _WORKER_SESSION = _create_http_session(_WORKER_TRANSPORT)


def _worker_fetch_url(url: str, *, prefer_utf8_sig: bool = False) -> str:
    global _WORKER_SESSION
    if _WORKER_SESSION is None:
        _WORKER_SESSION = _create_http_session(_WORKER_TRANSPORT)

    last_error: Optional[Exception] = None
    retry_status: Optional[int] = None
    retry_after_header: Optional[str] = None

    for attempt in range(1, _WORKER_RETRIES + 2):
        retry_status = None
        retry_after_header = None
        try:
            response = _WORKER_SESSION.get(url, timeout=_WORKER_TIMEOUT, allow_redirects=True)
            status = response.status_code

            if status == 200:
                text = _decode_response_text(response, prefer_utf8_sig=prefer_utf8_sig)
                if _looks_like_challenge_page(text):
                    raise RuntimeError("Cloudflare challenge page returned with status 200")
                return text

            if status in RETRYABLE_STATUS_CODES:
                retry_status = status
                retry_after_header = response.headers.get("Retry-After")
                last_error = RuntimeError(f"Retryable status code {status}")
            else:
                response.raise_for_status()

        except Exception as exc:
            last_error = exc

        if attempt <= _WORKER_RETRIES:
            _rotate_worker_session()
            sleep_for = _compute_retry_sleep_seconds(
                status_code=retry_status,
                retry_after_header=retry_after_header,
                attempt=attempt,
                base_sleep=_WORKER_SLEEP_SECONDS,
            )
            time.sleep(sleep_for)

    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def _crawl_document_worker(task: Tuple[int, str, bool]) -> Dict[str, Any]:
    doc_idx, url, include_html = task
    try:
        html = _worker_fetch_url(url, prefer_utf8_sig=False)
        attrs = parse_document_attributes(html)
        content = extract_document_content(html)

        doc: Dict[str, Any] = {
            "url": url,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "title": attrs.title,
            "attributes": asdict(attrs),
            "content_text": content,
            "content_length": len(content),
        }
        if include_html:
            doc["html"] = html
        return {
            "document_index": doc_idx,
            "url": url,
            "ok": True,
            "doc": doc,
        }
    except Exception as exc:
        return {
            "document_index": doc_idx,
            "url": url,
            "ok": False,
            "error": str(exc),
        }
    finally:
        if _WORKER_SLEEP_SECONDS > 0:
            sleep_for = _WORKER_SLEEP_SECONDS + random.uniform(
                0.05, max(0.2, _WORKER_SLEEP_SECONDS * 0.35)
            )
            time.sleep(sleep_for)


class TVPLCrawler:
    def __init__(
        self,
        output_dir: Path,
        *,
        timeout: float = 45.0,
        retries: int = 5,
        sleep_seconds: float = 3.0,
        sitemap_sleep_seconds: float = 0.5,
        transport: str = "auto",
        progress_every: int = 20,
        workers: Optional[int] = None,
        worker_chunksize: int = 2,
    ) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.documents_dir = self.output_dir / "documents"
        self.documents_dir.mkdir(parents=True, exist_ok=True)

        self.errors_file = self.output_dir / "errors.jsonl"
        self.checkpoint_file = self.output_dir / "checkpoint.json"
        self.sitemap_cache_file = self.output_dir / "sitemap_urls.json"

        self.timeout = timeout
        self.retries = retries
        self.sleep_seconds = sleep_seconds
        self.sitemap_sleep_seconds = sitemap_sleep_seconds
        self.transport = transport
        self.progress_every = max(1, progress_every)
        default_workers = max(1, min(2, os.cpu_count() or 2))
        configured_workers = workers if workers is not None else default_workers
        self.workers = max(1, configured_workers)
        self.worker_chunksize = max(1, worker_chunksize)

        self.session = self._new_session()
        self._start_time = time.time()

    def _new_session(self):
        return _create_http_session(self.transport)

    def _rotate_session(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self.session = self._new_session()

    @staticmethod
    def _decode_response_text(response: requests.Response, *, prefer_utf8_sig: bool = False) -> str:
        return _decode_response_text(response, prefer_utf8_sig=prefer_utf8_sig)

    @staticmethod
    def _looks_like_challenge_page(text: str) -> bool:
        return _looks_like_challenge_page(text)

    def fetch_url(self, url: str, *, prefer_utf8_sig: bool = False) -> str:
        """Fetch URL with retries, backoff, and session rotation on throttling."""
        last_error: Optional[Exception] = None
        retry_status: Optional[int] = None
        retry_after_header: Optional[str] = None

        for attempt in range(1, self.retries + 2):
            retry_status = None
            retry_after_header = None
            try:
                response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
                status = response.status_code

                if status == 200:
                    text = self._decode_response_text(response, prefer_utf8_sig=prefer_utf8_sig)
                    if self._looks_like_challenge_page(text):
                        raise RuntimeError("Cloudflare challenge page returned with status 200")
                    return text

                if status in RETRYABLE_STATUS_CODES:
                    retry_status = status
                    retry_after_header = response.headers.get("Retry-After")
                    last_error = RuntimeError(f"Retryable status code {status}")
                else:
                    response.raise_for_status()

            except Exception as exc:
                last_error = exc

            if attempt <= self.retries:
                self._rotate_session()
                sleep_for = _compute_retry_sleep_seconds(
                    status_code=retry_status,
                    retry_after_header=retry_after_header,
                    attempt=attempt,
                    base_sleep=self.sleep_seconds,
                )
                time.sleep(sleep_for)

        raise RuntimeError(f"Failed to fetch {url}: {last_error}")

    @staticmethod
    def _is_sitemap_url(url: str) -> bool:
        path = urlparse(url).path.lower()
        return path.endswith(".xml")

    def _read_sitemap_cache(self) -> List[str]:
        if not self.sitemap_cache_file.exists():
            return []
        try:
            payload = json.loads(self.sitemap_cache_file.read_text(encoding="utf-8"))
        except Exception:
            return []
        if not isinstance(payload, list):
            return []
        return [str(item) for item in payload if isinstance(item, str) and item.strip()]

    def _write_sitemap_cache(self, sitemap_urls: Sequence[str]) -> None:
        if not sitemap_urls:
            return
        self.sitemap_cache_file.write_text(
            json.dumps(list(sitemap_urls), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _build_fallback_sitemap_urls(
        sitemap_index_url: str,
        *,
        fallback_sitemap_count: Optional[int],
    ) -> List[str]:
        if fallback_sitemap_count is None or fallback_sitemap_count < 1:
            return []

        parsed = urlparse(sitemap_index_url)
        scheme = parsed.scheme or "https"
        netloc = parsed.netloc
        if not netloc:
            return []

        base = f"{scheme}://{netloc}"
        return [f"{base}/resitemap{i}.xml" for i in range(1, fallback_sitemap_count + 1)]

    def discover_sitemaps(
        self,
        sitemap_index_url: str,
        *,
        fallback_sitemap_count: Optional[int] = None,
    ) -> List[str]:
        try:
            content = self.fetch_url(sitemap_index_url, prefer_utf8_sig=True)
            parse_error: Optional[Exception] = None
            try:
                locs = parse_sitemap(content)
            except Exception as exc:
                parse_error = exc
                locs = parse_sitemap_links_from_html(content, base_url=sitemap_index_url)

            if not locs:
                if parse_error is None:
                    raise RuntimeError(f"No sitemap/document URLs found in: {sitemap_index_url}")
                raise RuntimeError(
                    f"No sitemap/document URLs found in: {sitemap_index_url}. "
                    f"XML parse error: {parse_error}"
                )

            xml_locs = [url for url in locs if self._is_sitemap_url(url)]
            sitemap_urls = xml_locs if xml_locs else [sitemap_index_url]
            self._write_sitemap_cache(sitemap_urls)
            return sitemap_urls
        except Exception as exc:
            cached_urls = self._read_sitemap_cache()
            if cached_urls:
                print(
                    f"Warning: failed to fetch sitemap index ({exc}). "
                    f"Using cached sitemap list: {self.sitemap_cache_file} "
                    f"({len(cached_urls)} URLs)."
                )
                return cached_urls

            fallback_urls = self._build_fallback_sitemap_urls(
                sitemap_index_url,
                fallback_sitemap_count=fallback_sitemap_count,
            )
            if fallback_urls:
                print(
                    f"Warning: failed to fetch sitemap index ({exc}). "
                    f"Using generated fallback sitemap URLs (1..{len(fallback_urls)})."
                )
                return fallback_urls

            raise RuntimeError(
                f"Failed to discover sitemap URLs from {sitemap_index_url}: {exc}. "
                "No cached sitemap list is available."
            )

    def read_checkpoint(self, *, start_sitemap: int, resume: bool) -> CrawlCheckpoint:
        if not resume or not self.checkpoint_file.exists():
            return CrawlCheckpoint(current_sitemap_index=max(1, start_sitemap))

        data = json.loads(self.checkpoint_file.read_text(encoding="utf-8"))
        checkpoint = CrawlCheckpoint(
            current_sitemap_index=max(start_sitemap, int(data.get("current_sitemap_index", start_sitemap))),
            next_url_index=int(data.get("next_url_index", 0)),
            documents_saved=int(data.get("documents_saved", 0)),
            errors=int(data.get("errors", 0)),
        )
        return checkpoint

    def write_checkpoint(self, checkpoint: CrawlCheckpoint) -> None:
        self.checkpoint_file.write_text(
            json.dumps(asdict(checkpoint), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def _sitemap_output_path(self, sitemap_url: str, sitemap_index: int) -> Path:
        match = re.search(r"resitemap(\d+)\.xml", sitemap_url, flags=re.IGNORECASE)
        if match:
            name = f"resitemap_{int(match.group(1)):04d}.jsonl"
        else:
            name = f"sitemap_{sitemap_index:04d}.jsonl"
        return self.documents_dir / name

    def _append_error(self, payload: Dict[str, object]) -> None:
        with self.errors_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def crawl_document(self, url: str, *, include_html: bool = False) -> Dict[str, object]:
        html = self.fetch_url(url, prefer_utf8_sig=False)
        attrs = parse_document_attributes(html)
        content = extract_document_content(html)

        result: Dict[str, object] = {
            "url": url,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "title": attrs.title,
            "attributes": asdict(attrs),
            "content_text": content,
            "content_length": len(content),
        }
        if include_html:
            result["html"] = html
        return result

    @staticmethod
    def _format_duration(seconds: Optional[float]) -> str:
        if seconds is None:
            return "unknown"
        if seconds <= 0:
            return "00:00"
        total_seconds = int(seconds)
        hours, rem = divmod(total_seconds, 3600)
        minutes, secs = divmod(rem, 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"

    def _log_progress(
        self,
        *,
        checkpoint: CrawlCheckpoint,
        attempts_this_run: int,
        documents_saved_this_run: int,
        remaining_in_sitemap: int,
        max_documents: Optional[int],
    ) -> None:
        elapsed = time.time() - self._start_time
        attempt_rate = attempts_this_run / elapsed if elapsed > 0 else 0.0
        saved_rate = documents_saved_this_run / elapsed if elapsed > 0 else 0.0

        if max_documents is not None:
            remaining_docs = max(0, max_documents - documents_saved_this_run)
            eta_rate = saved_rate
            eta_scope = "max_documents"
        else:
            remaining_docs = max(0, remaining_in_sitemap)
            eta_rate = attempt_rate
            eta_scope = "current sitemap"

        eta_seconds = (remaining_docs / eta_rate) if eta_rate > 0 else None
        print(
            f"  progress: saved={checkpoint.documents_saved}, "
            f"errors={checkpoint.errors}, rate={attempt_rate:.2f} urls/s, "
            f"eta({eta_scope})={self._format_duration(eta_seconds)}"
        )

    def _iter_document_results(
        self,
        *,
        doc_urls: List[str],
        start_url_offset: int,
        include_html: bool,
    ) -> Iterator[Dict[str, Any]]:
        if self.workers <= 1:
            for doc_idx in range(start_url_offset, len(doc_urls)):
                url = doc_urls[doc_idx]
                try:
                    doc = self.crawl_document(url, include_html=include_html)
                    yield {
                        "document_index": doc_idx,
                        "url": url,
                        "ok": True,
                        "doc": doc,
                    }
                except Exception as exc:
                    yield {
                        "document_index": doc_idx,
                        "url": url,
                        "ok": False,
                        "error": str(exc),
                    }
                if self.sleep_seconds > 0:
                    sleep_for = self.sleep_seconds + random.uniform(
                        0.05, max(0.2, self.sleep_seconds * 0.35)
                    )
                    time.sleep(sleep_for)
            return

        tasks = (
            (doc_idx, doc_urls[doc_idx], include_html)
            for doc_idx in range(start_url_offset, len(doc_urls))
        )
        ctx = mp.get_context("spawn")
        with ctx.Pool(
            processes=self.workers,
            initializer=_init_worker,
            initargs=(self.timeout, self.retries, self.sleep_seconds, self.transport),
        ) as pool:
            for payload in pool.imap(_crawl_document_worker, tasks, chunksize=self.worker_chunksize):
                yield payload

    def crawl(
        self,
        *,
        sitemap_index_url: str,
        start_sitemap: int = 1,
        end_sitemap: Optional[int] = None,
        fallback_sitemap_count: Optional[int] = None,
        max_documents: Optional[int] = None,
        resume: bool = True,
        include_html: bool = False,
    ) -> None:
        start_sitemap = max(1, start_sitemap)
        checkpoint = self.read_checkpoint(start_sitemap=start_sitemap, resume=resume)

        effective_fallback_count = fallback_sitemap_count
        if effective_fallback_count is None:
            if end_sitemap is not None:
                effective_fallback_count = max(1, end_sitemap)
            else:
                effective_fallback_count = max(start_sitemap, checkpoint.current_sitemap_index)

        sitemap_urls = self.discover_sitemaps(
            sitemap_index_url,
            fallback_sitemap_count=effective_fallback_count,
        )
        total_sitemaps = len(sitemap_urls)

        final_sitemap = min(total_sitemaps, end_sitemap or total_sitemaps)
        if start_sitemap > final_sitemap:
            raise ValueError(
                f"Invalid sitemap range: start={start_sitemap}, end={final_sitemap}, "
                f"available={total_sitemaps}"
            )

        self.write_checkpoint(checkpoint)

        print(
            f"Discovered {total_sitemaps} sitemap files. "
            f"Crawling range {start_sitemap}..{final_sitemap}. "
            f"Starting at sitemap #{checkpoint.current_sitemap_index}, "
            f"url offset {checkpoint.next_url_index}. "
            f"workers={self.workers}."
        )

        documents_saved_this_run = 0
        attempts_this_run = 0
        for sitemap_idx in range(start_sitemap, final_sitemap + 1):
            if sitemap_idx < checkpoint.current_sitemap_index:
                continue

            sitemap_url = sitemap_urls[sitemap_idx - 1]
            try:
                sitemap_xml = self.fetch_url(sitemap_url, prefer_utf8_sig=True)
                doc_urls = parse_sitemap(sitemap_xml)
            except Exception as exc:
                checkpoint.errors += 1
                self._append_error(
                    {
                        "when_utc": datetime.now(timezone.utc).isoformat(),
                        "stage": "sitemap_fetch",
                        "sitemap_index": sitemap_idx,
                        "sitemap_url": sitemap_url,
                        "error": str(exc),
                    }
                )
                self.write_checkpoint(checkpoint)
                print(f"[sitemap {sitemap_idx}] failed to load sitemap: {exc}")
                time.sleep(max(1.0, self.sitemap_sleep_seconds))
                continue

            output_path = self._sitemap_output_path(sitemap_url, sitemap_idx)
            start_url_offset = checkpoint.next_url_index if sitemap_idx == checkpoint.current_sitemap_index else 0
            write_mode = "a" if start_url_offset > 0 else "w"
            run_mode = "multiprocess" if self.workers > 1 else "single-process"

            print(
                f"[sitemap {sitemap_idx}/{final_sitemap}] "
                f"URLs: {len(doc_urls)} | output: {output_path.name} | "
                f"start at {start_url_offset} | mode={run_mode}"
            )

            with output_path.open(write_mode, encoding="utf-8") as out_f:
                processed_since_log = 0
                for payload in self._iter_document_results(
                    doc_urls=doc_urls,
                    start_url_offset=start_url_offset,
                    include_html=include_html,
                ):
                    doc_idx = int(payload["document_index"])
                    url = str(payload["url"])
                    attempts_this_run += 1
                    processed_since_log += 1

                    if bool(payload.get("ok")):
                        out_f.write(json.dumps(payload["doc"], ensure_ascii=False) + "\n")
                        checkpoint.documents_saved += 1
                        documents_saved_this_run += 1
                    else:
                        checkpoint.errors += 1
                        self._append_error(
                            {
                                "when_utc": datetime.now(timezone.utc).isoformat(),
                                "stage": "document_fetch",
                                "sitemap_index": sitemap_idx,
                                "document_index": doc_idx,
                                "url": url,
                                "error": str(payload.get("error", "unknown error")),
                            }
                        )

                    checkpoint.current_sitemap_index = sitemap_idx
                    checkpoint.next_url_index = doc_idx + 1
                    self.write_checkpoint(checkpoint)

                    if processed_since_log >= self.progress_every:
                        remaining_in_sitemap = max(0, len(doc_urls) - (doc_idx + 1))
                        self._log_progress(
                            checkpoint=checkpoint,
                            attempts_this_run=attempts_this_run,
                            documents_saved_this_run=documents_saved_this_run,
                            remaining_in_sitemap=remaining_in_sitemap,
                            max_documents=max_documents,
                        )
                        processed_since_log = 0

                    if max_documents is not None and documents_saved_this_run >= max_documents:
                        remaining_in_sitemap = max(0, len(doc_urls) - (doc_idx + 1))
                        if processed_since_log > 0:
                            self._log_progress(
                                checkpoint=checkpoint,
                                attempts_this_run=attempts_this_run,
                                documents_saved_this_run=documents_saved_this_run,
                                remaining_in_sitemap=remaining_in_sitemap,
                                max_documents=max_documents,
                            )
                        print(
                            f"Reached max_documents={max_documents}. "
                            f"Checkpoint saved at sitemap {checkpoint.current_sitemap_index}, "
                            f"offset {checkpoint.next_url_index}."
                        )
                        return

            checkpoint.current_sitemap_index = sitemap_idx + 1
            checkpoint.next_url_index = 0
            self.write_checkpoint(checkpoint)

            if self.sitemap_sleep_seconds > 0:
                time.sleep(self.sitemap_sleep_seconds)

        print(
            "Crawl completed. "
            f"saved={checkpoint.documents_saved}, errors={checkpoint.errors}, "
            f"checkpoint={self.checkpoint_file}"
        )

    def retry_from_errors_file(
        self,
        *,
        errors_file: Path,
        sitemap_index_url: str,
        include_html: bool = False,
        max_retry_urls: Optional[int] = None,
    ) -> None:
        if not errors_file.exists():
            raise FileNotFoundError(f"Errors file not found: {errors_file}")

        retry_items: List[Tuple[int, Optional[int], str]] = []
        seen_urls = set()

        with errors_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if payload.get("stage") != "document_fetch":
                    continue

                url = str(payload.get("url", "")).strip()
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                try:
                    sitemap_idx = int(payload.get("sitemap_index", 0))
                except (TypeError, ValueError):
                    sitemap_idx = 0

                raw_doc_idx = payload.get("document_index")
                try:
                    doc_idx = int(raw_doc_idx) if raw_doc_idx is not None else None
                except (TypeError, ValueError):
                    doc_idx = None

                retry_items.append((sitemap_idx, doc_idx, url))
                if max_retry_urls is not None and len(retry_items) >= max_retry_urls:
                    break

        total = len(retry_items)
        if total == 0:
            print(f"No document URLs found to retry in {errors_file}.")
            return

        print(
            f"Retrying {total} failed document URLs from {errors_file}. "
            "Running sequentially with throttled sleeps to reduce 429/403."
        )

        start_time = time.time()
        successes = 0
        failures = 0
        output_files: Dict[Path, Any] = {}

        try:
            for idx, (sitemap_idx, doc_idx, url) in enumerate(retry_items, start=1):
                output_path = (
                    self.documents_dir / f"resitemap_{sitemap_idx:04d}.jsonl"
                    if sitemap_idx >= 1
                    else self.documents_dir / "retry_unmapped.jsonl"
                )

                try:
                    doc = self.crawl_document(url, include_html=include_html)
                    writer = output_files.get(output_path)
                    if writer is None:
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        writer = output_path.open("a", encoding="utf-8")
                        output_files[output_path] = writer
                    writer.write(json.dumps(doc, ensure_ascii=False) + "\n")
                    successes += 1
                except Exception as exc:
                    failures += 1
                    self._append_error(
                        {
                            "when_utc": datetime.now(timezone.utc).isoformat(),
                            "stage": "document_retry",
                            "source_errors_file": str(errors_file),
                            "sitemap_index": sitemap_idx,
                            "document_index": doc_idx,
                            "url": url,
                            "error": str(exc),
                        }
                    )

                if idx % self.progress_every == 0 or idx == total:
                    elapsed = time.time() - start_time
                    rate = idx / elapsed if elapsed > 0 else 0.0
                    eta_seconds = ((total - idx) / rate) if rate > 0 else None
                    print(
                        f"  retry-progress: processed={idx}/{total}, "
                        f"success={successes}, failed={failures}, "
                        f"rate={rate:.2f} urls/s, eta={self._format_duration(eta_seconds)}"
                    )

                if self.sleep_seconds > 0:
                    sleep_for = self.sleep_seconds + random.uniform(
                        0.05, max(0.2, self.sleep_seconds * 0.35)
                    )
                    time.sleep(sleep_for)
        finally:
            for writer in output_files.values():
                try:
                    writer.close()
                except Exception:
                    pass

        print(
            f"Retry completed. total={total}, success={successes}, failed={failures}. "
            "Failed retries were appended to errors.jsonl with stage=document_retry."
        )


def fetch_url(url: str, *, timeout: float = 30.0) -> str:
    """
    Backward-compatible helper.

    Prefer using TVPLCrawler.fetch_url() for retry/session logic.
    """
    session = _create_http_session("auto")
    try:
        response = session.get(url, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        text = _decode_response_text(response, prefer_utf8_sig=url.lower().endswith(".xml"))
        if _looks_like_challenge_page(text):
            raise RuntimeError(
                "Cloudflare challenge page returned. "
                "Use TVPLCrawler with retries/backoff instead of fetch_url()."
            )
        return text
    finally:
        try:
            session.close()
        except Exception:
            pass


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crawl law documents from thuvienphapluat.vn")
    default_workers = 10
    parser.add_argument(
        "--sitemap-index-url",
        default=DEFAULT_SITEMAP_INDEX_URL,
        help="Sitemap index URL (default: https://thuvienphapluat.vn/SiteMap.aspx)",
    )
    parser.add_argument("--output-dir", default="tvpl_output", help="Directory to write crawl output")
    parser.add_argument("--start-sitemap", type=int, default=1, help="1-based sitemap start index")
    parser.add_argument("--end-sitemap", type=int, default=None, help="1-based sitemap end index")
    parser.add_argument(
        "--fallback-sitemap-count",
        type=int,
        default=None,
        help="If sitemap index is blocked, generate fallback resitemap URLs from 1..N",
    )
    parser.add_argument("--max-documents", type=int, default=None, help="Stop after N documents (smoke test)")
    parser.add_argument("--timeout", type=float, default=45.0, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=5, help="Retry count per request")
    parser.add_argument("--sleep", type=float, default=3.0, help="Delay between document requests")
    parser.add_argument("--sitemap-sleep", type=float, default=0.5, help="Delay between sitemap requests")
    parser.add_argument(
        "--transport",
        choices=("auto", "requests", "cloudscraper"),
        default="auto",
        help="HTTP transport mode",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume from checkpoint if available",
    )
    parser.add_argument(
        "--include-html",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Store raw HTML in output JSONL (larger files)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=default_workers,
        help=f"Number of document worker processes (default: {default_workers})",
    )
    parser.add_argument(
        "--worker-chunksize",
        type=int,
        default=2,
        help="Chunk size for distributing document URLs to workers",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=20,
        help="Print progress/ETA every N processed URLs",
    )
    parser.add_argument(
        "--retry-errors-file",
        default=None,
        help="Retry document URLs from a previous errors.jsonl file",
    )
    parser.add_argument(
        "--retry-max-urls",
        type=int,
        default=None,
        help="Optional cap on number of URLs retried from --retry-errors-file",
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()

    crawler = TVPLCrawler(
        output_dir=Path(args.output_dir),
        timeout=args.timeout,
        retries=args.retries,
        sleep_seconds=args.sleep,
        sitemap_sleep_seconds=args.sitemap_sleep,
        transport=args.transport,
        progress_every=args.progress_every,
        workers=args.workers,
        worker_chunksize=args.worker_chunksize,
    )

    if args.retry_errors_file:
        crawler.retry_from_errors_file(
            errors_file=Path(args.retry_errors_file),
            sitemap_index_url=args.sitemap_index_url,
            include_html=args.include_html,
            max_retry_urls=args.retry_max_urls,
        )
        return

    crawler.crawl(
        sitemap_index_url=args.sitemap_index_url,
        start_sitemap=args.start_sitemap,
        end_sitemap=args.end_sitemap,
        fallback_sitemap_count=args.fallback_sitemap_count,
        max_documents=args.max_documents,
        resume=args.resume,
        include_html=args.include_html,
    )


if __name__ == "__main__":
    main()
