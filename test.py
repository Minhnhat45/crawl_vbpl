from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import glob
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
