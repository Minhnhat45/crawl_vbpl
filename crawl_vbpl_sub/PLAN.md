# Crawl VBPL.vn - Implementation Plan

## Goal

Crawl **all** legal documents from https://vbpl.vn (both "Văn bản pháp quy" and "Văn bản hợp nhất") for AI training purposes.

**Scale:** ~153,556+ documents across 63 provinces + ministries + central government.

---

## Output Format

### Primary: JSONL (AI Training)

One JSON object per line, following [Pile of Law](https://huggingface.co/datasets/pile-of-law/pile-of-law) conventions with extended Vietnamese legal metadata:

```jsonl
{
  "id": "VBPL-116287",
  "text": "QUYẾT ĐỊNH\nVề việc ban hành Quy định...\n\nChương I\nQUY ĐỊNH CHUNG\n...",
  "metadata": {
    "so_hieu": "3900/2000/QĐ-UB",
    "loai_van_ban": "Quyết định",
    "co_quan_ban_hanh": "UBND tỉnh Bà Rịa - Vũng Tàu",
    "nguoi_ky": "Nguyễn Văn A",
    "ngay_ban_hanh": "2000-08-29",
    "ngay_hieu_luc": "2000-09-13",
    "ngay_het_hieu_luc": "2004-03-11",
    "tinh_trang": "Hết hiệu lực toàn bộ",
    "linh_vuc": "Tài nguyên - Môi trường",
    "scope": "bariavungtau",
    "doc_type": "van_ban_phap_quy",
    "source_url": "https://vbpl.vn/bariavungtau/Pages/vbpq-toanvan.aspx?ItemID=116287"
  },
  "attachments": [
    {"filename": "3900.2000.QĐ.UB.doc", "url": "https://vbpl.vn/...doc", "type": "doc"},
    {"filename": "VanBanGoc_3900.2000.QĐ.UB.pdf", "url": "https://vbpl.vn/...pdf", "type": "pdf"}
  ],
  "created_timestamp": "2000-08-29T00:00:00",
  "downloaded_timestamp": "2026-03-26T15:30:00",
  "url": "https://vbpl.vn/bariavungtau/Pages/vbpq-toanvan.aspx?ItemID=116287"
}
```

### File Organization

```
data/
├── crawl_state.db              # SQLite - resume state tracking
├── documents/
│   ├── van_ban_phap_quy.jsonl  # All VBPQ documents
│   └── van_ban_hop_nhat.jsonl  # All VBHN documents
├── attachments/
│   ├── 116287/
│   │   ├── 3900.2000.QĐ.UB.doc
│   │   └── VanBanGoc_3900.2000.QĐ.UB.pdf
│   └── .../
└── logs/
    └── crawl.log
```

---

## Architecture

### Tech Stack

| Component | Library | Why |
|-----------|---------|-----|
| HTTP client | `aiohttp` | Async, connection pooling, handles cookies |
| HTML parsing | `beautifulsoup4` + `lxml` | Fast, reliable for ASP.NET HTML |
| Concurrency | `asyncio.Semaphore` | Control concurrent connections |
| State/Resume | `sqlite3` (stdlib) | Track crawl progress, atomic updates |
| CLI | `argparse` | Simple, no extra deps |
| Logging | `logging` | Stdlib, structured output |
| File writing | `aiofiles` | Non-blocking file I/O |

### Concurrency Model

```
                    ┌─────────────────────────┐
                    │      Main Orchestrator   │
                    │   (asyncio event loop)   │
                    └─────────┬───────────────┘
                              │
              ┌───────────────┼───────────────┐
              │               │               │
     ┌────────▼──────┐ ┌─────▼──────┐ ┌──────▼─────┐
     │  Phase 1      │ │  Phase 2   │ │  Phase 3   │
     │  Discover IDs │ │  Crawl     │ │  Download  │
     │  (search API) │ │  Details   │ │  Attach.   │
     │  sem=10       │ │  sem=30    │ │  sem=20    │
     └───────────────┘ └────────────┘ └────────────┘
              │               │               │
         asyncio.Queue   asyncio.Queue   asyncio.Queue
              │               │               │
         N workers       N workers       N workers
```

- **Phase 1 (Discovery):** 10 concurrent search requests — paginate all results
- **Phase 2 (Details):** 30 concurrent detail page fetches — parse metadata + full text
- **Phase 3 (Attachments):** 20 concurrent file downloads — save doc/pdf files

---

## Detailed Steps

### Step 1: Project Setup

Create project structure:

```
crawl_vbpl/
├── pyproject.toml
├── src/
│   └── crawler/
│       ├── __init__.py
│       ├── main.py          # CLI entry point & orchestrator
│       ├── config.py         # Constants, URLs, settings
│       ├── state.py          # SQLite state management (resume)
│       ├── discovery.py      # Phase 1: search API pagination
│       ├── detail.py         # Phase 2: document detail scraping
│       ├── attachments.py    # Phase 3: file downloads
│       ├── parser.py         # HTML parsing helpers
│       └── writer.py         # JSONL file writer
└── data/                     # Output directory
```

### Step 2: Config & Constants

```python
# config.py
BASE_URL = "https://vbpl.vn"

# AJAX search endpoints
SEARCH_VBPQ_URL = "/VBQPPL_UserControls/Publishing_22/TimKiem/p_KetQuaTimKiemVanBan.aspx"
SEARCH_VBHN_URL = "/VBQPPL_UserControls/Publishing_22/Timkiem/p_KetQuaTimKiemHopNhat.aspx"

# Document detail page pattern
DETAIL_URL = "/{scope}/Pages/vbpq-toanvan.aspx?ItemID={item_id}"

# Concurrency limits
DISCOVERY_CONCURRENCY = 10
DETAIL_CONCURRENCY = 30
ATTACHMENT_CONCURRENCY = 20

# Rate limiting
REQUEST_DELAY = 0.05  # 50ms between requests per worker
ROWS_PER_PAGE = 50    # Max allowed by the site

# Retry
MAX_RETRIES = 3
RETRY_DELAY = 2.0     # seconds, with exponential backoff
```

### Step 3: State Management (Resume)

SQLite database tracks every document's crawl state:

```sql
CREATE TABLE documents (
    item_id INTEGER PRIMARY KEY,
    scope TEXT NOT NULL,
    doc_type TEXT NOT NULL,        -- 'vbpq' or 'vbhn'
    discovery_page INTEGER,
    status TEXT DEFAULT 'discovered', -- discovered | detail_done | attachments_done | failed
    detail_json TEXT,              -- cached parsed metadata+text
    error_message TEXT,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE crawl_progress (
    doc_type TEXT NOT NULL,
    total_pages INTEGER,
    last_completed_page INTEGER DEFAULT 0,
    PRIMARY KEY (doc_type)
);
```

**Resume logic:** On restart, skip pages already in `crawl_progress`, skip documents already at `detail_done` or `attachments_done` status.

### Step 4: Phase 1 — Discovery

**Strategy:** POST to search endpoints, paginate through ALL results.

```python
# Pseudocode for discovery
async def discover_all(session, doc_type):
    """Paginate through search results to collect all ItemIDs."""
    endpoint = SEARCH_VBPQ_URL if doc_type == "vbpq" else SEARCH_VBHN_URL

    # First request to get total pages
    page_1 = await fetch_search_page(session, endpoint, page=1)
    total_pages = parse_total_pages(page_1)  # ~5,119 for VBPQ

    # Resume from last completed page
    start_page = state.get_last_page(doc_type) + 1

    # Paginate with semaphore-controlled concurrency
    sem = asyncio.Semaphore(DISCOVERY_CONCURRENCY)
    tasks = []
    for page in range(start_page, total_pages + 1):
        tasks.append(discover_page(session, sem, endpoint, page, doc_type))

    await asyncio.gather(*tasks)
```

**Parse search results HTML** to extract:
- `ItemID` from links like `vbpq-toanvan.aspx?ItemID=32801`
- `scope` from link path like `/bariavungtau/Pages/...`

### Step 5: Phase 2 — Detail Crawling

For each discovered ItemID, fetch the full document page:

```python
async def crawl_detail(session, sem, item_id, scope, doc_type):
    """Fetch and parse a single document detail page."""
    url = f"{BASE_URL}/{scope}/Pages/vbpq-toanvan.aspx?ItemID={item_id}"

    async with sem:
        html = await fetch_with_retry(session, url)

    doc = parse_detail_page(html)
    # Returns: {text, metadata, attachment_urls}

    state.save_detail(item_id, doc)
```

**Parse from detail page:**

| Field | HTML Location |
|-------|---------------|
| Full text | Main content div (rendered HTML → clean text) |
| Số hiệu | Metadata table/div |
| Loại VB | Metadata table/div |
| Cơ quan ban hành | Metadata table/div |
| Ngày ban hành | Metadata table/div |
| Ngày hiệu lực | Metadata table/div |
| Tình trạng | Metadata table/div |
| Người ký | Metadata table/div |
| Lĩnh vực | Metadata table/div |
| Attachment URLs | Download tab links (.doc, .pdf) |

**HTML → Clean Text conversion:**
- Strip HTML tags but preserve paragraph structure
- Keep headings (Chương, Điều, Khoản, Mục) as structural markers
- Preserve tables as text
- Remove navigation/UI elements

### Step 6: Phase 3 — Attachment Downloads

```python
async def download_attachments(session, sem, item_id, attachment_urls):
    """Download doc/pdf files for a document."""
    dest_dir = Path(f"data/attachments/{item_id}")
    dest_dir.mkdir(parents=True, exist_ok=True)

    for att in attachment_urls:
        filepath = dest_dir / att["filename"]
        if filepath.exists():
            continue  # Resume support

        async with sem:
            content = await fetch_binary(session, att["url"])

        async with aiofiles.open(filepath, "wb") as f:
            await f.write(content)
```

### Step 7: JSONL Writer

```python
async def write_jsonl(item_id, doc, doc_type):
    """Append one document to the appropriate JSONL file."""
    record = {
        "id": f"VBPL-{item_id}",
        "text": doc["text"],
        "metadata": doc["metadata"],
        "attachments": doc["attachments"],
        "created_timestamp": doc["metadata"].get("ngay_ban_hanh"),
        "downloaded_timestamp": datetime.utcnow().isoformat(),
        "url": doc["source_url"],
    }

    filepath = f"data/documents/van_ban_{'phap_quy' if doc_type == 'vbpq' else 'hop_nhat'}.jsonl"
    async with aiofiles.open(filepath, "a", encoding="utf-8") as f:
        await f.write(json.dumps(record, ensure_ascii=False) + "\n")
```

### Step 8: CLI Interface

```bash
# Full crawl (all phases)
python -m crawler

# Resume interrupted crawl
python -m crawler --resume

# Discovery only (just collect IDs)
python -m crawler --phase discovery

# Crawl details only (skip discovery)
python -m crawler --phase details

# Download attachments only
python -m crawler --phase attachments

# Crawl specific doc type only
python -m crawler --doc-type vbpq
python -m crawler --doc-type vbhn

# Override concurrency
python -m crawler --concurrency 50

# Skip attachments (text only — faster)
python -m crawler --no-attachments
```

---

## Error Handling & Resilience

| Scenario | Strategy |
|----------|----------|
| HTTP 429 (rate limited) | Exponential backoff: 2s → 4s → 8s → 16s |
| HTTP 500 (server error) | Retry 3x with 2s delay, then mark `failed` |
| Connection timeout | 30s timeout, retry 3x |
| Malformed HTML | Log warning, save raw HTML, continue |
| Disk full | Check disk space periodically, abort gracefully |
| IP ban | Detect via consecutive failures, pause 60s |
| Duplicate ItemIDs | SQLite UPSERT — idempotent |
| Interrupted crawl | Resume from SQLite state on restart |

---

## Estimated Performance

| Phase | Items | Concurrency | Est. Time |
|-------|-------|-------------|-----------|
| Discovery (VBPQ) | ~5,119 pages | 10 workers | ~10 min |
| Discovery (VBHN) | ~100-500 pages | 10 workers | ~2 min |
| Detail crawl | ~153,000 docs | 30 workers | ~3-5 hours |
| Attachments | ~200,000 files | 20 workers | ~8-12 hours |
| **Total (with attachments)** | | | **~12-18 hours** |
| **Total (text only)** | | | **~3-6 hours** |

---

## Dependencies

```toml
[project]
name = "crawl-vbpl"
requires-python = ">=3.11"
dependencies = [
    "aiohttp>=3.9",
    "aiofiles>=24.1",
    "beautifulsoup4>=4.12",
    "lxml>=5.0",
]
```

Zero heavy dependencies. All lightweight and well-maintained.

---

## Implementation Order

| Step | Task | Files |
|------|------|-------|
| 1 | Project scaffolding + deps | `pyproject.toml`, package structure |
| 2 | Config constants | `config.py` |
| 3 | State management (SQLite) | `state.py` |
| 4 | HTML parser helpers | `parser.py` |
| 5 | Phase 1: Discovery | `discovery.py` |
| 6 | Phase 2: Detail crawler | `detail.py` |
| 7 | Phase 3: Attachment downloader | `attachments.py` |
| 8 | JSONL writer | `writer.py` |
| 9 | CLI orchestrator | `main.py` |
| 10 | Test with small scope first | Manual test with 1 page |

---

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| IP ban from aggressive crawling | Medium | High | Implement backoff detection, add `--delay` flag |
| ASP.NET ViewState/session issues | Medium | Medium | Maintain session cookies, extract `__REQUESTDIGEST` |
| HTML structure varies by scope | Medium | Medium | Build flexible parser, test across multiple scopes |
| Some ItemIDs return 404 | High | Low | Log and skip, track in state DB |
| Site goes down during crawl | Low | Medium | Resume capability handles this |
| Attachment URLs broken | Medium | Low | Log failures, don't block main crawl |
| Total doc count higher than 153K | Low | Low | Dynamic pagination handles any count |
