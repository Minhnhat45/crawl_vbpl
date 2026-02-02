import json

with open("./out_vbpl_179271/vbpl_179271.json", "r", encoding="utf-8") as f:
    data = json.load(f)
    
for block in data["texts"]:
    print(f"{block["self_ref"]}: {block["text"]}")