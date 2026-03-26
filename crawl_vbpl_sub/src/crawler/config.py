"""Configuration constants for the VBPL crawler."""

from pathlib import Path

# Base
BASE_URL = "https://vbpl.vn"

# AJAX search endpoints (POST)
SEARCH_VBPQ_URL = BASE_URL + "/VBQPPL_UserControls/Publishing_22/TimKiem/p_KetQuaTimKiemVanBan.aspx"
SEARCH_VBHN_URL = BASE_URL + "/VBQPPL_UserControls/Publishing_22/Timkiem/p_KetQuaTimKiemHopNhat.aspx"

# Document detail page pattern
DETAIL_URL_TEMPLATE = BASE_URL + "/{scope}/Pages/vbpq-toanvan.aspx?ItemID={item_id}"

# Attributes page (structured metadata)
ATTRS_URL_TEMPLATE = BASE_URL + "/{scope}/Pages/vbpq-thuoctinh.aspx?ItemID={item_id}"

# VBHN detail pages use different URL patterns
VBHN_DETAIL_URL_TEMPLATE = BASE_URL + "/{scope}/Pages/vbpq-van-ban-goc-hopnhat.aspx?ItemID={item_id}"
VBHN_ATTRS_URL_TEMPLATE = BASE_URL + "/{scope}/Pages/vbpq-thuoctinh-hopnhat.aspx?ItemID={item_id}"

# Fallback scope when TW returns 404 (TW scope is broken on this site)
FALLBACK_SCOPE = "botaichinh"

# Concurrency
DISCOVERY_CONCURRENCY = 5
DETAIL_CONCURRENCY = 5
ATTACHMENT_CONCURRENCY = 5

# Pagination
ROWS_PER_PAGE = 50

# Timeouts & retries
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0
REQUEST_DELAY = 0.3  # 300ms between requests per worker

# Backoff on suspected rate-limiting
BACKOFF_ON_CONSECUTIVE_ERRORS = 5
BACKOFF_PAUSE_SECONDS = 60

# Paths
DATA_DIR = Path("data")
DOCUMENTS_DIR = DATA_DIR / "documents"
ATTACHMENTS_DIR = DATA_DIR / "attachments"
LOGS_DIR = DATA_DIR / "logs"
STATE_DB_PATH = DATA_DIR / "crawl_state.db"

# Output files
VBPQ_JSONL = DOCUMENTS_DIR / "van_ban_phap_quy.jsonl"
VBHN_JSONL = DOCUMENTS_DIR / "van_ban_hop_nhat.jsonl"

# HTTP headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Referer": BASE_URL + "/pages/vbpq-timkiem.aspx",
    "X-Requested-With": "XMLHttpRequest",
}
