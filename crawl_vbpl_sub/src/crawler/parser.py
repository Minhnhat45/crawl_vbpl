"""HTML parsing helpers for VBPL pages."""

import re
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup, Tag


def parse_search_results(html: str) -> tuple[list[dict], int]:
    """Parse search result page HTML.

    Returns:
        Tuple of (list of {item_id, scope, title}, total_pages).
    """
    soup = BeautifulSoup(html, "lxml")
    documents: list[dict] = []

    # Extract document links — they contain ItemID in the URL
    seen_ids: set[int] = set()
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "ItemID=" not in href:
            continue
        # Match both VBPQ and VBHN page patterns
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

        # Extract scope from path: /{scope}/Pages/...
        path_parts = parsed.path.strip("/").split("/")
        scope = path_parts[0] if len(path_parts) >= 2 else "TW"

        # Deduplicate by item_id (multiple links per doc in results)
        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)

        title = link.get_text(strip=True)

        documents.append({
            "item_id": item_id,
            "scope": scope,
            "title": title,
        })

    # Extract total pages from pagination
    total_pages = _parse_total_pages(soup)

    return documents, total_pages


def _parse_total_pages(soup: BeautifulSoup) -> int:
    """Extract total page count from pagination controls.

    The pagination HTML looks like:
    <div class="paging">
        <a class="current">1</a>
        <a href="javascript:Nexpage('divResultSearch','2');">2</a>
        ...
        <a href="javascript:Nexpage('divResultSearch','3072');">Cuối »</a>
    </div>
    """
    # Find the "Cuối »" (Last) link which contains the total page count
    # Pattern: Nexpage('divID','NUMBER') or NexpageHN('divID','NUMBER')
    paging_div = soup.find("div", class_="paging")
    if not paging_div:
        return 1

    max_page = 1
    for link in paging_div.find_all("a", href=True):
        href = link.get("href", "")
        # Match: javascript:Nexpage('divResultSearch','3072')
        # or: javascript:NexpageHN('divResultSearchHN','40')
        page_match = re.search(r"Nexpage\w*\([^,]*,\s*'(\d+)'\)", href)
        if page_match:
            page_num = int(page_match.group(1))
            max_page = max(max_page, page_num)

    return max_page


def parse_detail_page(html: str) -> dict:
    """Parse a document detail (toàn văn) page.

    Returns dict with: text, metadata, attachment_urls.
    """
    soup = BeautifulSoup(html, "lxml")
    result: dict = {
        "text": "",
        "metadata": {},
        "attachment_urls": [],
    }

    # Extract full text content
    result["text"] = _extract_full_text(soup)

    # Extract metadata from the page
    result["metadata"] = _extract_metadata(soup)

    # Extract attachment download links
    result["attachment_urls"] = _extract_attachments(soup)

    return result


def _extract_full_text(soup: BeautifulSoup) -> str:
    """Extract the main document text content."""
    # The full text is typically in a div with specific class/id
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
        # Fallback: look for the largest text block
        content_div = _find_largest_content_div(soup)

    if not content_div:
        return ""

    return _html_to_clean_text(content_div)


def _find_largest_content_div(soup: BeautifulSoup) -> Tag | None:
    """Find the div with the most text content as fallback."""
    best: Tag | None = None
    best_len = 0

    for div in soup.find_all("div"):
        text = div.get_text(strip=True)
        # Filter out navigation/menu divs
        div_id = div.get("id", "")
        div_class = " ".join(div.get("class", []))
        if any(skip in div_id.lower() + div_class.lower() for skip in ["menu", "nav", "header", "footer", "sidebar"]):
            continue
        if len(text) > best_len:
            best_len = len(text)
            best = div

    return best


def _html_to_clean_text(element: Tag) -> str:
    """Convert HTML element to clean text preserving structure."""
    # Remove script and style tags
    for tag in element.find_all(["script", "style", "nav"]):
        tag.decompose()

    lines: list[str] = []

    for child in element.descendants:
        if isinstance(child, str):
            text = child.strip()
            if text:
                lines.append(text)
        elif isinstance(child, Tag):
            if child.name in ("br",):
                lines.append("\n")
            elif child.name in ("p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "tr", "li"):
                lines.append("\n")
            elif child.name == "td":
                lines.append("\t")

    text = "".join(lines)
    # Normalize whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_attributes_page(html: str) -> dict:
    """Parse the thuoctinh (attributes) page for structured metadata.

    The page has a table with td.label cells followed by value cells.
    Layout is 4 columns: label | value | label | value per row.
    """
    soup = BeautifulSoup(html, "lxml")
    return _extract_metadata(soup)


# Mapping from Vietnamese labels to field keys
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


def _extract_metadata(soup: BeautifulSoup) -> dict:
    """Extract structured metadata from the page.

    Handles the table layout where label cells have class="label"
    and value cells follow them.
    """
    metadata: dict[str, str] = {}

    # Strategy 1: Parse table rows with td.label pattern (most reliable)
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if not cells:
            continue

        # Handle the special "Cơ quan ban hành/ Chức danh / Người ký" row
        # which has: label | co_quan | chuc_danh | nguoi_ky
        first_text = cells[0].get_text(strip=True).lower()
        if "cơ quan ban hành" in first_text and "người ký" in first_text:
            if len(cells) >= 4:
                metadata["co_quan_ban_hanh"] = cells[1].get_text(strip=True)
                metadata["chuc_danh"] = cells[2].get_text(strip=True)
                metadata["nguoi_ky"] = cells[3].get_text(strip=True)
            continue

        # Handle "Tình trạng hiệu lực: ..." in a single colspan cell
        if "tình trạng hiệu lực" in first_text:
            value = first_text.split(":", 1)[-1].strip() if ":" in first_text else ""
            if value:
                metadata["tinh_trang"] = value
            continue

        # Standard 4-column layout: label | value | label | value
        i = 0
        while i < len(cells):
            cell = cells[i]
            cell_classes = cell.get("class", [])
            cell_text = cell.get_text(strip=True).lower()

            if "label" in cell_classes or any(lbl in cell_text for lbl in LABEL_MAP):
                # This is a label cell — next cell is the value
                if i + 1 < len(cells):
                    value_cell = cells[i + 1]
                    value = value_cell.get_text(strip=True)

                    # Match to our known labels
                    for vn_label, key in LABEL_MAP.items():
                        if vn_label in cell_text:
                            if value and value != "...":
                                metadata[key] = value
                            break
                    i += 2
                    continue
            i += 1

    # Strategy 2: Extract from span-based metadata in detail page sidebar
    for li in soup.find_all("li"):
        span = li.find("span")
        if not span:
            continue
        label = span.get_text(strip=True).lower().rstrip(":")
        # Value is the text after the span
        value = li.get_text(strip=True)
        value = value[len(span.get_text(strip=True)):].strip().lstrip(":")

        for vn_label, key in LABEL_MAP.items():
            if vn_label in label and value:
                metadata.setdefault(key, value)
                break

    # Extract title/trich yeu from page title or heading
    if "trich_yeu" not in metadata:
        title_el = soup.find(["h1", "h2"], class_=re.compile(r"(title|tieude)", re.I))
        if title_el:
            metadata["trich_yeu"] = title_el.get_text(strip=True)

    return metadata


def _extract_attachments(soup: BeautifulSoup) -> list[dict]:
    """Extract attachment download URLs."""
    attachments: list[dict] = []
    seen_urls: set[str] = set()

    for link in soup.find_all("a", href=True):
        href = link["href"]
        lower_href = href.lower()

        # Match doc/pdf/zip attachments
        if not any(lower_href.endswith(ext) for ext in (".doc", ".docx", ".pdf", ".zip", ".rar")):
            continue

        # Skip duplicates
        if href in seen_urls:
            continue
        seen_urls.add(href)

        filename = href.rsplit("/", maxsplit=1)[-1]
        # Determine file type
        if lower_href.endswith(".pdf"):
            file_type = "pdf"
        elif lower_href.endswith((".doc", ".docx")):
            file_type = "doc"
        else:
            file_type = "other"

        # Make URL absolute
        if href.startswith("/"):
            href = f"https://vbpl.vn{href}"
        elif not href.startswith("http"):
            href = f"https://vbpl.vn/{href}"

        attachments.append({
            "filename": filename,
            "url": href,
            "type": file_type,
        })

    return attachments
