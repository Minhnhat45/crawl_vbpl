import requests
from lxml import html
import pdb
url = "https://vbpl.vn/TW/Pages/vanban.aspx?idLoaiVanBan=20&dvid=13&Page=2"

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

document_type = {
    "Bo_luat": 16,
    "Luat": 17,
    "Phap_lenh": 19,
    "Nghi_quyet": 18,
    "Nghi_dinh": 20,
    "Quyet_dinh": 21,
    "Thong_tu": 22,
    "Thong_tu_lien_tich": 23
}
def generate_url(department="bokhoahoccongnghe"):
    base_url = f"https://vbpl.vn/{department}/Page/vanban.aspx?"
    suffix_url = ""
    
    return

def check_valid_page(url=""):
    url = "https://vbpl.vn/bokhoahoccongnghe/Pages/vanban.aspx?idLoaiVanBan=17&dvid=213&Page=2"
    response = requests.get(url)
    if response.status_code == 200:
        tree = html.fromstring(response.content)
        article_xpath = '//*[@id="tabVB_lv1"]/div[2]/ul/li[1]/div/p/a'
        elements = tree.xpath(article_xpath)
        if elements:
            return True

        else:
            return False
    else:
        return False
    


print(check_valid_page())