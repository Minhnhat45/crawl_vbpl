import requests
from lxml import html
import pdb
import itertools
from tqdm import tqdm
import json
def check_url(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        if response.status_code == 200:
            print("URL exists:", url)
        else:
            print(f"URL returned status {response.status_code}")
    except requests.RequestException as e:
        print("URL not reachable:", e)


def get_page_info(url):
    response = requests.get(url)
    if response.status_code == 200:
        # Assume `html_content` contains the HTML string
        tree = html.fromstring(response.content)
        xpath = '//*[@id="tabVB_lv1"]/div[2]/div'
        # Use the XPath directly
        elements = tree.xpath(xpath)
        # If the element exists, get its content
        
        if elements:
            while "Cuối" in elements[0].text_content():
                print(elements[0].text_content())


def check_valid_page(url="https://vbpl.vn/bokhoahoccongnghe/Pages/vanban.aspx?idLoaiVanBan=17&dvid=213&Page=2"):
    response = requests.get(url)
    if response.status_code == 200:
        tree = html.fromstring(response.content)
        article_xpath = '//*[@id="tabVB_lv1"]/div[2]/ul/li[1]/div/p/a' # //*[@id="tabVB_lv1"]/div[2]/ul/li[1]/div/p/a
        elements = tree.xpath(article_xpath)
        if elements:
            return True

        else:
            return False
    else:
        return False


def crawl_url(department, document_type):
    
    base_url = f"https://vbpl.vn/{department}/Pages/vanban.aspx?"
    suffix_url = f"idLoaiVanBan={document_type}&dvid=213&"
    
    url = base_url + suffix_url
    
    
    def count_li(url_page):
        """
        Count how many <li> nodes are inside
        //*[@id="tabVB_lv1"]/div[2]/ul   on the given page.
        """
        resp = requests.get(url_page, timeout=10)
        resp.raise_for_status()

        tree = html.fromstring(resp.content)

        # all <li> children of the UL you care about
        li_nodes = tree.xpath('//*[@id="tabVB_lv1"]/div[2]/ul/li')
        return len(li_nodes)
    
    def get_url_in_page(url_page, num_of_li_tag):
        resp = requests.get(url_page, timeout=10)
        resp.raise_for_status()
        tree = html.fromstring(resp.content)
        results = {}
        for i in range(1, num_of_li_tag +1):
            xpath = f'//*[@id="tabVB_lv1"]/div[2]/ul/li[{i}]/div/p/a'
            anchors = tree.xpath(xpath)
            for a in anchors:
                href = a.get('href')
                url_for_document = "https://vbpl.vn" + href
                text = (a.text_content() or '').strip()
                results.setdefault(str(i), {'document_url': url_for_document, 'document_title': text})
        return results
    
    all_url_in_document_type = {}
    for page_number in itertools.count(start=1):
        suffix_page_url = f"Page={page_number}"
        url_page = url + suffix_page_url
        if not check_valid_page(url_page):
            break
        # pdb.set_trace()
        num_of_li_tag = count_li(url_page)
        url_in_page = get_url_in_page(url_page, num_of_li_tag)
        all_url_in_document_type.setdefault(f"Page_{page_number}", url_in_page)
    
    return all_url_in_document_type

document_type = {
    "Hien_Phap": 15,
    "Bo_luat": 16,
    "Luat": 17,
    "Phap_lenh": 19,
    "Lenh": 2,
    "Nghi_quyet": 18,
    "Nghi_quyet_lien_tinh": 3,
    "Nghi_dinh": 20,
    "Quyet_dinh": 21,
    "Thong_tu": 22,
    "Thong_tu_lien_tich": 23
}

all_departments = ['bocongan', 'bocongthuong', 'bogiaoducdaotao', 'bogiaothong', 'bokehoachvadautu', 'bokhoahoccongnghe', 'bolaodong', 'bonoivu', 'bonongnghiep', 'bongoaigiao', 'boquocphong', 'botaichinh', 'botainguyen', 'botuphap', 'bothongtin', 'bovanhoathethao', 'boxaydung', 'boyte', 'kiemtoannhanuoc', 'nganhangnhanuoc', 'toaannhandantoicao', 'thanhtrachinhphu', 'uybandantoc', 'vanphongchinhphu', 'vienkiemsatnhandantoicao']

for department in all_departments:
    print("Crawling department:", department)
    if department in ['bocongan', 'bokhoahoccongnghe']:
        continue
    url_department = {}
    for k, v in tqdm(document_type.items()):
        url_department.setdefault(k, crawl_url(department=department, document_type=v))
    with open(f"all_url/url_{department}.json", "w") as f:
        json.dump(url_department, f, indent=4, ensure_ascii=False)