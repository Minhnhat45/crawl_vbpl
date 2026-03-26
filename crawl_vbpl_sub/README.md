# VBPL.vn Crawler

Async Python crawler for Vietnam's National Legal Document Database ([vbpl.vn](https://vbpl.vn)). Crawls all **Van ban phap quy** (regulatory documents) and **Van ban hop nhat** (consolidated documents) for AI training.

## Quick Start

```bash
# Install dependencies
pip install aiohttp aiofiles beautifulsoup4 lxml

# Run full crawl (text only, no attachments)
PYTHONPATH=src python -m crawler.main --no-attachments

# Check progress
PYTHONPATH=src python -m crawler.main --stats
```

## How It Works

The crawler runs in 3 phases:

### Phase 1: Discovery

POSTs to the site's AJAX search endpoints, paginates through all results, and collects every `ItemID` + scope into a local SQLite database.

- **VBPQ endpoint:** `/VBQPPL_UserControls/Publishing_22/TimKiem/p_KetQuaTimKiemVanBan.aspx`
- **VBHN endpoint:** `/VBQPPL_UserControls/Publishing_22/Timkiem/p_KetQuaTimKiemHopNhat.aspx`
- ~3,072 pages for VBPQ (~50,000 unique docs), ~40 pages for VBHN (~1,900 docs)

### Phase 2: Detail Crawl

For each discovered ItemID, fetches two pages:

1. **Full text page** (`vbpq-toanvan.aspx`) -- the rendered legal document
2. **Attributes page** (`vbpq-thuoctinh.aspx`) -- structured metadata table

Parses both into a single JSONL record and appends to the output file.

### Phase 3: Attachments (optional)

Downloads `.doc` and `.pdf` files linked from each document into `data/attachments/{ItemID}/`.

## CLI Usage

```bash
# Full crawl (all phases, all doc types, with attachments)
PYTHONPATH=src python -m crawler.main

# Text only (skip Phase 3 -- much faster)
PYTHONPATH=src python -m crawler.main --no-attachments

# Discovery only (just collect IDs, don't fetch content)
PYTHONPATH=src python -m crawler.main --phase discovery

# Detail crawl only (skip discovery, use existing state DB)
PYTHONPATH=src python -m crawler.main --phase details

# Download attachments only
PYTHONPATH=src python -m crawler.main --phase attachments

# Crawl specific doc type
PYTHONPATH=src python -m crawler.main --doc-type vbpq
PYTHONPATH=src python -m crawler.main --doc-type vbhn

# Override concurrency (default: 5)
PYTHONPATH=src python -m crawler.main --concurrency 10

# Show stats
PYTHONPATH=src python -m crawler.main --stats
```

## Resume Capability

All progress is tracked in `data/crawl_state.db` (SQLite). If the crawl is interrupted (Ctrl+C, crash, network failure), just run the same command again -- it picks up exactly where it stopped.

## Output Format

### JSONL (one JSON object per line)

Follows the [Pile of Law](https://huggingface.co/datasets/pile-of-law/pile-of-law) conventions with extended Vietnamese legal metadata:

```json
{
  "id": "VBPL-116287",
  "text": "QUYET DINH\nVe viec ban hanh Quy dinh...\n\nChuong I\nQUY DINH CHUNG\n...",
  "metadata": {
    "so_hieu": "3900/2000/QD-UB",
    "loai_van_ban": "Quyet dinh",
    "co_quan_ban_hanh": "UBND tinh Ba Ria - Vung Tau",
    "nguoi_ky": "Tran Minh Sanh",
    "chuc_danh": "Pho Chu tich",
    "ngay_ban_hanh": "29/08/2000",
    "ngay_hieu_luc": "13/09/2000",
    "ngay_het_hieu_luc": "11/03/2004",
    "tinh_trang": "het hieu luc toan bo",
    "linh_vuc": "Tai nguyen khoang san, dia chat",
    "nganh": "Tai nguyen va Moi truong",
    "pham_vi": "Tinh Ba Ria-Vung Tau",
    "scope": "bariavungtau",
    "doc_type": "van_ban_phap_quy",
    "source_url": "https://vbpl.vn/bariavungtau/Pages/vbpq-toanvan.aspx?ItemID=116287"
  },
  "attachments": [
    {"filename": "3900.2000.QD.UB.doc", "url": "https://vbpl.vn/...", "type": "doc"}
  ],
  "created_timestamp": "29/08/2000",
  "downloaded_timestamp": "2026-03-26T15:30:00+00:00",
  "url": "https://vbpl.vn/bariavungtau/Pages/vbpq-toanvan.aspx?ItemID=116287"
}
```

### Metadata Fields

| Field | Description | Example |
|-------|-------------|---------|
| `so_hieu` | Document number | `3900/2000/QD-UB` |
| `loai_van_ban` | Document type | Luat, Nghi dinh, Quyet dinh, Thong tu, ... |
| `co_quan_ban_hanh` | Issuing body | Quoc hoi, Thu tuong, Bo Tai chinh, ... |
| `nguoi_ky` | Signer name | Nguyen Van A |
| `chuc_danh` | Signer title | Chu tich, Thu tuong, Bo truong |
| `ngay_ban_hanh` | Issue date | `29/08/2000` |
| `ngay_hieu_luc` | Effective date | `13/09/2000` |
| `ngay_het_hieu_luc` | Expiry date | `11/03/2004` |
| `tinh_trang` | Legal status | Con hieu luc, Het hieu luc toan bo, ... |
| `linh_vuc` | Subject area | Tai nguyen khoang san, Giao duc, ... |
| `nganh` | Sector | Tai nguyen va Moi truong, ... |
| `pham_vi` | Jurisdiction scope | Toan quoc, Tinh X, ... |
| `nguon_thu_thap` | Collection source | Cong bao so X |
| `scope` | Site scope path | TW, hanoi, botaichinh, ... |
| `doc_type` | Document category | van_ban_phap_quy, van_ban_hop_nhat |

### File Structure

```
data/
├── crawl_state.db              # SQLite -- resume state
├── documents/
│   ├── van_ban_phap_quy.jsonl  # ~50,000 regulatory documents
│   └── van_ban_hop_nhat.jsonl  # ~1,900 consolidated documents
├── attachments/                # Optional (--no-attachments to skip)
│   ├── 116287/
│   │   └── 3900.2000.QD.UB.doc
│   └── .../
└── logs/
    └── crawl_20260326_225525.log
```

## Configuration

All settings are in [`src/crawler/config.py`](src/crawler/config.py):

| Setting | Default | Description |
|---------|---------|-------------|
| `DETAIL_CONCURRENCY` | 5 | Concurrent detail page requests |
| `DISCOVERY_CONCURRENCY` | 5 | Concurrent search pagination requests |
| `ATTACHMENT_CONCURRENCY` | 5 | Concurrent file downloads |
| `REQUEST_DELAY` | 0.3s | Delay between requests per worker |
| `MAX_RETRIES` | 3 | Retry count on failure |
| `BACKOFF_PAUSE_SECONDS` | 60 | Pause on consecutive errors |
| `FALLBACK_SCOPE` | `botaichinh` | Fallback when TW scope 404s |

### Tuning Concurrency

The site is a SharePoint-based government server. Too many concurrent requests cause timeouts.

| Profile | Concurrency | Delay | Est. Time (text only) |
|---------|-------------|-------|-----------------------|
| **Conservative** | 3 | 0.5s | ~24-36 hrs |
| **Default** | 5 | 0.3s | ~15-20 hrs |
| **Moderate** | 10 | 0.2s | ~8-12 hrs |
| **Aggressive** | 20 | 0.1s | ~4-6 hrs (risk timeouts) |

Override at runtime: `--concurrency 10`

## Known Issues

- **TW scope returns 404** -- The `/TW/Pages/...` URL path is broken on the site. The crawler automatically falls back to `/botaichinh/Pages/...` which serves the same documents regardless of scope.
- **50,000 unique docs vs 153K total** -- The search API returns ~153K results across 3,072 pages, but many are duplicates across scopes. After deduplication, ~50,000 unique documents remain.
- **VBHN text can be short** -- Some consolidated documents only have summary text on the detail page, with the full content in PDF attachments.

## Project Structure

```
src/crawler/
├── main.py          # CLI entry point & orchestrator
├── config.py        # URLs, concurrency settings, paths
├── state.py         # SQLite state management (resume)
├── discovery.py     # Phase 1: search API pagination
├── detail.py        # Phase 2: document detail scraping
├── attachments.py   # Phase 3: file downloads
├── parser.py        # HTML parsing (search, detail, attributes)
└── writer.py        # JSONL file writer (async, locked)
```

## Dependencies

- `aiohttp` -- async HTTP client
- `aiofiles` -- async file I/O
- `beautifulsoup4` + `lxml` -- HTML parsing
- Python 3.11+
