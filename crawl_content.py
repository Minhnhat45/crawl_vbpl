import json
import requests
from lxml import html
import pdb
def read_json(file_path):
    f = open(file_path)
    data = json.load(f)
    return data

def check_feasible_header(url):
    """
    Kiểm tra xem trong văn bản pháp luật óc bao nhiêu header có thể crawl
    """
    def count_li(url):
        """
        Count how many <li> nodes are inside
        """
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()

        tree = html.fromstring(resp.content)

        # all <li> children of the UL you care about
        li_nodes = tree.xpath('//*[@id="ctl00_ctl37_g_5776a891_1cc3_4081_9adc_12779c8ef8b3"]/div[1]/div/div[2]/ul/li')
        # //*[@id="ctl00_ctl37_g_5776a891_1cc3_4081_9adc_12779c8ef8b3"]/div[1]/div/div[2]/ul/li[1]/a
        # //*[@id="ctl00_ctl37_g_5776a891_1cc3_4081_9adc_12779c8ef8b3"]/div[1]/div/div[2]/ul/li[2]/a/span/b
        return len(li_nodes)
    num_of_li = count_li(url)
    def crawl_header_url(url, num_of_li):
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        tree = html.fromstring(resp.content)
        result = {}
        for i in range(num_of_li+1):
            xpath = f'//*[@id="ctl00_ctl37_g_5776a891_1cc3_4081_9adc_12779c8ef8b3"]/div[1]/div/div[2]/ul/li[{i}]/a'
            anchors = tree.xpath(xpath)
            spans = tree.xpath(xpath + "/span/b")
            for a in anchors:
                href = a.get('href')
                print(href)
            for sp in spans:
                context = sp.text_content().strip()
                print(context)
            # result.setdefault(context, href)
        return result
    # print(count_li(url))
    result = crawl_header_url(url, num_of_li)
    print(result)
    return


def crawl_content():
    
    return

if __name__ == "__main__":
    # url_data = read_json("./url_bokhoahoccongnghe.json")
    # for category, page_value in url_data.items():
    #     for page, docs in page_value.items():
    #         for doc_idx, doc in docs.items():
    #             print(doc["document_url"])
    #             print(doc["document_title"])
    check_feasible_header(url="https://vbpl.vn/bokhoahoccongnghe/Pages/vbpq-toanvan.aspx?ItemID=157722")