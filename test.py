import glob
import json


all_doc = 0
miss_toan_van = 0
for url_file in glob.glob("./all_url/url_*.json"):
    with open(url_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    for document_type, pages in data.items():
        # pages is now a list of {"Page_X": [{"1": {...}}, {"2": {...}}, ...]}
        for page_entry in pages:
            for page_name, documents in page_entry.items():
                for doc in documents:
                    for _, info in doc.items():
                        all_doc += 1
                        document_url =  info.get('document_url')
                        if "toanvan" not in document_url:
                            miss_toan_van += 1
                        print(
                            # f"File: {url_file}, "
                            # f"Document type: {document_type}, "
                            # f"Page: {page_name}, "
                            f"Document URL: {document_url}"
                        )

print(all_doc)
print(miss_toan_van)