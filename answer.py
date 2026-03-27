from __future__ import annotations

import csv
import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from lxml import html
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DOMAIN = "https://vbpl.vn"
TXT_OUTPUT = Path("vbpl_detail_urls.txt")
CSV_OUTPUT = Path("vbpl_detail_urls.csv")
JSONL_OUTPUT = Path("vbpl_detail_urls.jsonl")
STATE_OUTPUT = Path("vbpl_crawl_state.json")
LOG_OUTPUT = Path("vbpl_crawl.log")
REQUEST_TIMEOUT = 30
REQUEST_DELAY_SECONDS = 0.4

SLUGS = [
    # Central and ministries
    "tw",
    "bocongan",
    "bocongthuong",
    "bogiaoducdaotao",
    "bogiaothong",
    "bokehoachvadautu",
    "bokhoahoccongnghe",
    "bolaodong",
    "bovanhoathethao",
    "bonongnghiep",
    "bonoivu",
    "bongoaigiao",
    "boquocphong",
    "botaichinh",
    "botainguyen",
    "botuphap",
    "bothongtin",
    "boxaydung",
    "boyte",
    "nganhangnhanuoc",
    "thanhtrachinhphu",
    "uybandantoc",
    "vanphongchinhphu",
    "kiemtoannhanuoc",
    "toaannhandantoicao",
    "vienkiemsatnhandantoicao",
    # Municipalities
    "hanoi",
    "thanhphohochiminh",
    "danang",
    "haiphong",
    "cantho",
    # Provinces
    "angiang",
    "bariavungtau",
    "bacgiang",
    "backan",
    "baclieu",
    "bacninh",
    "bentre",
    "binhdinh",
    "binhduong",
    "binhphuoc",
    "binhthuan",
    "camau",
    "caobang",
    "daklak",
    "daknong",
    "dienbien",
    "dongnai",
    "dongthap",
    "gialai",
    "hagiang",
    "hanam",
    "hatinh",
    "haiduong",
    "haugiang",
    "hoabinh",
    "hungyen",
    "khanhhoa",
    "kiengiang",
    "kontum",
    "laichau",
    "lamdong",
    "langson",
    "laocai",
    "longan",
    "namdinh",
    "nghean",
    "ninhbinh",
    "ninhthuan",
    "phutho",
    "phuyen",
    "quangbinh",
    "quangnam",
    "quangngai",
    "quangninh",
    "quangtri",
    "soctrang",
    "sonla",
    "tayninh",
    "thaibinh",
    "thainguyen",
    "thanhhoa",
    "thuathienhue",
    "tiengiang",
    "travinh",
    "tuyenquang",
    "vinhlong",
    "vinhphuc",
    "yenbai",
]

DOC_TYPE_NAME_MAP = {
    15: "Hiến pháp",
    16: "Bộ luật",
    17: "Luật",
    19: "Pháp lệnh",
    2: "Lệnh",
    18: "Nghị quyết",
    3: "Nghị quyết liên tịch",
    20: "Nghị định",
    21: "Quyết định",
    22: "Thông tư",
    23: "Thông tư liên tịch",
}


@dataclass(slots=True)
class CrawlStats:
    slugs_total: int = 0
    slugs_done: int = 0
    jobs_total: int = 0
    jobs_done: int = 0
    pages_visited: int = 0
    candidate_links_found: int = 0
    unique_detail_urls: int = 0
    duplicates_skipped: int = 0
    failures: int = 0


@dataclass(slots=True)
class Job:
    slug: str
    dvid: str
    doc_type_id: str
    source_page: str

    @property
    def key(self) -> str:
        return f"{self.slug}|{self.dvid}|{self.doc_type_id}"

    @property
    def doc_type_name(self) -> str:
        try:
            return DOC_TYPE_NAME_MAP.get(int(self.doc_type_id), "")
        except ValueError:
            return ""


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.data = {
            "completed_jobs": [],
            "job_last_page": {},
        }
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                logging.warning("Could not parse existing state file; starting fresh")
                self.data = {"completed_jobs": [], "job_last_page": {}}

        self.completed_jobs: Set[str] = set(self.data.get("completed_jobs", []))
        self.job_last_page: Dict[str, int] = {
            k: int(v) for k, v in self.data.get("job_last_page", {}).items()
        }

    def mark_page_done(self, job_key: str, page_number: int) -> None:
        self.job_last_page[job_key] = page_number
        self._flush()

    def mark_job_done(self, job_key: str) -> None:
        self.completed_jobs.add(job_key)
        self._flush()

    def _flush(self) -> None:
        payload = {
            "completed_jobs": sorted(self.completed_jobs),
            "job_last_page": self.job_last_page,
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class IncrementalWriter:
    def __init__(self, txt_path: Path, csv_path: Path, jsonl_path: Path) -> None:
        self.txt_path = txt_path
        self.csv_path = csv_path
        self.jsonl_path = jsonl_path
        self.seen: Set[str] = set()
        self._load_existing()

        self.txt_file = self.txt_path.open("a", encoding="utf-8", buffering=1)
        self.csv_file = self.csv_path.open("a", encoding="utf-8", newline="", buffering=1)
        self.jsonl_file = self.jsonl_path.open("a", encoding="utf-8", buffering=1)
        self.csv_writer = csv.DictWriter(
            self.csv_file,
            fieldnames=["url", "source_section", "discovery_method", "parent_page", "title"],
        )
        if self.csv_path.stat().st_size == 0:
            self.csv_writer.writeheader()
            self.csv_file.flush()

    def _load_existing(self) -> None:
        for path in (self.txt_path, self.csv_path, self.jsonl_path):
            path.parent.mkdir(parents=True, exist_ok=True)

        if self.txt_path.exists():
            with self.txt_path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self.seen.add(line)

    def add(
        self,
        *,
        url: str,
        source_section: str,
        discovery_method: str,
        parent_page: str,
        title: str,
    ) -> bool:
        if url in self.seen:
            return False
        self.seen.add(url)

        row = {
            "url": url,
            "source_section": source_section,
            "discovery_method": discovery_method,
            "parent_page": parent_page,
            "title": title,
        }
        self.txt_file.write(url + "\n")
        self.csv_writer.writerow(row)
        self.jsonl_file.write(json.dumps(row, ensure_ascii=False) + "\n")

        self.txt_file.flush()
        self.csv_file.flush()
        self.jsonl_file.flush()
        return True

    def close(self) -> None:
        self.txt_file.close()
        self.csv_file.close()
        self.jsonl_file.close()


def setup_logging() -> None:
    handlers = [logging.StreamHandler(), logging.FileHandler(LOG_OUTPUT, encoding="utf-8")]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return session


def sleep_briefly() -> None:
    time.sleep(REQUEST_DELAY_SECONDS)


def fetch(session: requests.Session, url: str, *, referer: Optional[str] = None) -> requests.Response:
    headers = {}
    if referer:
        headers["Referer"] = referer
    response = session.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
    response.raise_for_status()
    sleep_briefly()
    return response


def normalize_url(url: str) -> str:
    return url if url.startswith("http") else urljoin(BASE_DOMAIN, url)


def is_detail_url(url: str) -> bool:
    low = url.lower()
    return "vbpq-toanvan.aspx" in low or "vbpq-van-ban-goc.aspx" in low


def parse_listing_jobs(session: requests.Session, slug: str) -> List[Job]:
    """Discover all (dvid, idLoaiVanBan) combinations from a section's vanban page."""
    url = f"{BASE_DOMAIN}/{slug}/Pages/vanban.aspx"
    response = fetch(session, url, referer=f"{BASE_DOMAIN}/pages/vbpq-timkiem.aspx")
    tree = html.fromstring(response.content)

    jobs: Dict[Tuple[str, str], Job] = {}
    for anchor in tree.xpath("//a[@href]"):
        href = anchor.get("href") or ""
        if "idLoaiVanBan=" not in href or "dvid=" not in href:
            continue
        full = normalize_url(href)
        parsed = urlparse(full)
        params = parse_qs(parsed.query)
        dvid = (params.get("dvid") or [""])[0]
        doc_type_id = (params.get("idLoaiVanBan") or [""])[0]
        if not dvid or not doc_type_id:
            continue
        key = (dvid, doc_type_id)
        if key not in jobs:
            jobs[key] = Job(slug=slug, dvid=dvid, doc_type_id=doc_type_id, source_page=url)

    # Fallback: regex scan in raw HTML for badly formed links
    if not jobs:
        text = response.text
        for dvid, doc_type_id in re.findall(r"idLoaiVanBan=(\d+).*?dvid=(\d+)|dvid=(\d+).*?idLoaiVanBan=(\d+)", text):
            pairs = [x for x in (dvid, doc_type_id) if x]
            if len(pairs) != 2:
                continue
        # intentionally no-op; xpath path is usually enough

    result = sorted(jobs.values(), key=lambda j: (int(j.doc_type_id), int(j.dvid)))
    logging.info("[%s] discovered %d document-type jobs", slug, len(result))
    for job in result:
        logging.info(
            "[%s] job doc_type=%s (%s), dvid=%s",
            slug,
            job.doc_type_id,
            job.doc_type_name or "unknown",
            job.dvid,
        )
    return result


def build_listing_url(job: Job, page_number: int) -> str:
    return (
        f"{BASE_DOMAIN}/{job.slug}/Pages/vanban.aspx?"
        f"idLoaiVanBan={job.doc_type_id}&dvid={job.dvid}&Page={page_number}"
    )


def extract_detail_links(page_content: bytes) -> List[Tuple[str, str]]:
    tree = html.fromstring(page_content)
    anchors = tree.xpath('//*[@id="tabVB_lv1"]/div[2]/ul/li/div/p/a')
    results: List[Tuple[str, str]] = []
    for a in anchors:
        href = (a.get("href") or "").strip()
        title = " ".join(a.text_content().split())
        if not href:
            continue
        full = normalize_url(href)
        if is_detail_url(full):
            results.append((full, title))
    return results


def count_list_items(page_content: bytes) -> int:
    tree = html.fromstring(page_content)
    return len(tree.xpath('//*[@id="tabVB_lv1"]/div[2]/ul/li'))


def page_has_results(page_content: bytes) -> bool:
    return bool(extract_detail_links(page_content))


def crawl_job(
    session: requests.Session,
    job: Job,
    writer: IncrementalWriter,
    state: StateStore,
    stats: CrawlStats,
) -> None:
    start_page = state.job_last_page.get(job.key, 0) + 1
    logging.info(
        "[%s] start job doc_type=%s (%s), dvid=%s from page %d",
        job.slug,
        job.doc_type_id,
        job.doc_type_name or "unknown",
        job.dvid,
        start_page,
    )

    empty_streak = 0
    page_number = start_page
    while True:
        page_url = build_listing_url(job, page_number)
        try:
            response = fetch(session, page_url, referer=f"{BASE_DOMAIN}/{job.slug}/Pages/vanban.aspx")
            stats.pages_visited += 1
        except Exception as exc:
            stats.failures += 1
            logging.exception("[%s] failed to fetch page %s: %s", job.slug, page_url, exc)
            break

        links = extract_detail_links(response.content)
        li_count = count_list_items(response.content)
        logging.info(
            "[%s] page %d | doc_type=%s dvid=%s | li=%d | detail_links=%d | url=%s",
            job.slug,
            page_number,
            job.doc_type_id,
            job.dvid,
            li_count,
            len(links),
            page_url,
        )

        if not links:
            empty_streak += 1
            # stop immediately on first empty page; this matches your own logic
            logging.info(
                "[%s] stop job doc_type=%s dvid=%s at page %d (no result links)",
                job.slug,
                job.doc_type_id,
                job.dvid,
                page_number,
            )
            break

        empty_streak = 0
        stats.candidate_links_found += len(links)

        new_on_page = 0
        for detail_url, title in links:
            added = writer.add(
                url=detail_url,
                source_section=job.slug,
                discovery_method=f"listing:idLoaiVanBan={job.doc_type_id}&dvid={job.dvid}&Page={page_number}",
                parent_page=page_url,
                title=title,
            )
            if added:
                new_on_page += 1
                stats.unique_detail_urls += 1
                logging.info("[%s] new URL #%d: %s", job.slug, stats.unique_detail_urls, detail_url)
            else:
                stats.duplicates_skipped += 1

        logging.info(
            "[%s] page %d complete | new=%d | duplicates_total=%d | unique_total=%d",
            job.slug,
            page_number,
            new_on_page,
            stats.duplicates_skipped,
            stats.unique_detail_urls,
        )
        state.mark_page_done(job.key, page_number)
        page_number += 1

    state.mark_job_done(job.key)
    stats.jobs_done += 1
    logging.info(
        "[%s] completed job doc_type=%s dvid=%s | jobs_done=%d/%d",
        job.slug,
        job.doc_type_id,
        job.dvid,
        stats.jobs_done,
        stats.jobs_total,
    )


def crawl_all() -> None:
    setup_logging()
    session = build_session()
    writer = IncrementalWriter(TXT_OUTPUT, CSV_OUTPUT, JSONL_OUTPUT)
    state = StateStore(STATE_OUTPUT)
    stats = CrawlStats(slugs_total=len(SLUGS), unique_detail_urls=len(writer.seen))

    logging.info("Loaded %d existing URLs from previous output files", len(writer.seen))
    logging.info("State file: %s", STATE_OUTPUT.resolve())

    try:
        for slug_index, slug in enumerate(SLUGS, start=1):
            logging.info("========== slug %d/%d: %s ==========" , slug_index, len(SLUGS), slug)
            try:
                jobs = parse_listing_jobs(session, slug)
            except Exception as exc:
                stats.failures += 1
                logging.exception("[%s] failed to discover listing jobs: %s", slug, exc)
                continue

            pending_jobs = [job for job in jobs if job.key not in state.completed_jobs]
            stats.jobs_total += len(pending_jobs)
            if not pending_jobs:
                logging.info("[%s] nothing to do; all jobs already completed", slug)
                stats.slugs_done += 1
                continue

            for job in pending_jobs:
                crawl_job(session, job, writer, state, stats)

            stats.slugs_done += 1
            logging.info(
                "[%s] slug complete | slugs_done=%d/%d | pages=%d | unique=%d | failures=%d",
                slug,
                stats.slugs_done,
                stats.slugs_total,
                stats.pages_visited,
                stats.unique_detail_urls,
                stats.failures,
            )
    finally:
        writer.close()
        session.close()

    logging.info("========== crawl finished ==========")
    logging.info("pages_visited=%d", stats.pages_visited)
    logging.info("candidate_links_found=%d", stats.candidate_links_found)
    logging.info("unique_detail_urls=%d", stats.unique_detail_urls)
    logging.info("duplicates_skipped=%d", stats.duplicates_skipped)
    logging.info("failures=%d", stats.failures)
    logging.info("outputs: %s | %s | %s", TXT_OUTPUT, CSV_OUTPUT, JSONL_OUTPUT)


if __name__ == "__main__":
    crawl_all()
