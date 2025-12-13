from urllib.parse import urljoin, urlparse
from collections import deque
import requests
from bs4 import BeautifulSoup
import os
import json
from tqdm import tqdm
SITEMAP_URL = "https://vbpl.vn/Pages/sitemap.aspx"
BASE_DOMAIN = "vbpl.vn"


def get_ministry_home_urls():
    """
    Lấy danh sách 25 mục 'Văn bản pháp luật Bộ, ban ngành' từ sitemap:
    trả về list[(tên_bộ, url_home)]
    """
    resp = requests.get(SITEMAP_URL, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Tìm đoạn text 'Văn bản pháp luật Bộ, ban ngành'
    label = soup.find(string=lambda t: t and "Văn bản pháp luật Bộ, ban ngành" in t)
    if not label:
        raise RuntimeError("Không tìm thấy mục 'Văn bản pháp luật Bộ, ban ngành' trong sitemap")

    li = label.find_parent("li")
    if not li:
        raise RuntimeError("Không tìm thấy thẻ <li> cha chứa mục 'Văn bản pháp luật Bộ, ban ngành'")

    ministry_links = li.find_all("a", href=True)
    results = []
    for a in ministry_links:
        name = " ".join(a.get_text(strip=True).split())
        url = urljoin(SITEMAP_URL, a["href"])
        results.append((name, url))

    return results


def same_ministry(url, base_prefix):
    """
    Kiểm tra URL có thuộc cùng 1 Bộ (cùng prefix /bocongan, /bocongthuong, ...) không
    """
    parsed = urlparse(url)
    if parsed.netloc != BASE_DOMAIN:
        return False
    return parsed.geturl().startswith(base_prefix)


def parse_document_page(ministry_name, url, soup):
    """
    Parse 1 trang văn bản chi tiết (vbpq-toanvan.aspx)
    Trả về dict đơn giản, bạn chỉnh thêm field nếu cần.
    """
    # Tiêu đề: ưu tiên h1
    title_tag = soup.find("h1")
    if title_tag:
        title_text = " ".join(title_tag.get_text(strip=True).split())
    else:
        title_text = ""
        # fallback: tìm strong/b/h2 có chứa từ khóa loại VB
        for tag in soup.find_all(["h2", "strong", "b"]):
            txt = tag.get_text(strip=True)
            if any(kw in txt for kw in ["Thông tư", "Nghị định", "Quyết định", "Luật"]):
                title_text = " ".join(txt.split())
                break

    # Lấy toàn bộ text (bạn có thể refine lại sau)
    full_text = soup.get_text(separator="\n", strip=True)

    return {
        "ministry": ministry_name,
        "url": url,
        "title": title_text,
        "text": full_text,
    }


def crawl_ministry(ministry_name, home_url, max_pages=500):
    """
    Crawler BFS cho 1 Bộ:
    - Xuất phát từ home_url (…/SomeMinistry/Pages/Home.aspx)
    - Chỉ theo các link cùng prefix /SomeMinistry
    - Mọi URL chứa 'vbpq-toanvan.aspx' sẽ được parse và lưu
    """
    parsed = urlparse(home_url)
    parts = parsed.path.strip("/").split("/")
    if not parts:
        return []

    slug = parts[0]  # vd: 'bocongan'
    base_prefix = f"{parsed.scheme}://{parsed.netloc}/{slug}"
    print(f"Crawling {ministry_name} ({base_prefix})")

    q = deque([home_url])
    visited = set([home_url])
    docs = []

    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (compatible; vbpl-crawler/1.0)"

    while q and len(visited) <= max_pages:
        print(len(visited))
        url = q.popleft()
        try:
            r = session.get(url, timeout=15)
            r.raise_for_status()
        except Exception as e:
            print("  [!] Lỗi tải", url, ":", e)
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        # Nếu là trang văn bản chi tiết
        if "vbpq-toanvan.aspx" in url.lower():
            doc = parse_document_page(ministry_name, url, soup)
            docs.append(doc)
            continue

        # Nếu là các trang khác (Home, tìm kiếm, danh sách, phân trang,...)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            new_url = urljoin(url, href)

            if new_url in visited:
                continue
            if not same_ministry(new_url, base_prefix):
                continue

            visited.add(new_url)

            # Ưu tiên trang chi tiết văn bản
            if "vbpq-toanvan.aspx" in new_url.lower():
                q.appendleft(new_url)
            else:
                q.append(new_url)

    print(f"  -> Thu được {len(docs)} văn bản từ {ministry_name}")
    return docs


def load_existing_ministries(path):
    """
    Đọc file JSONL đầu ra để biết Bộ nào đã được crawl trước đó.
    """
    if not os.path.exists(path):
        return set()

    existed = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                continue
            ministry = doc.get("ministry")
            if ministry:
                existed.add(ministry)
    return existed


def main():
    ministries = get_ministry_home_urls()
    print("Tìm thấy", len(ministries), "mục Bộ, ban ngành:")
    for name, url in ministries:
        print(" -", name, "=>", url)

    out_file = "vbpl_ministry_docs.jsonl"
    existed_ministries = load_existing_ministries(out_file)
    if existed_ministries:
        print("Bỏ qua các Bộ đã có sẵn trong file:", ", ".join(sorted(existed_ministries)))

    total_docs = 0
    with open(out_file, "a", encoding="utf-8") as f:
        for name, url in tqdm(ministries):
            if name in existed_ministries:
                print(f"⏭️  Bỏ qua {name} (đã crawl trước đó)")
                continue
            # Tùy server, bạn có thể tăng/giảm max_pages
            docs = crawl_ministry(name, url, max_pages=800)
            for doc in docs:
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")
                f.flush()  # Ghi ngay để không mất dữ liệu nếu tiến trình dừng đột ngột
                total_docs += 1

    print(f"\n✅ Đã ghi thêm {total_docs} văn bản vào {out_file}")


if __name__ == "__main__":
    main()
