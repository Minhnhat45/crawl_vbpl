# pip install -U docling

from pathlib import Path
from docling.document_converter import DocumentConverter

URL = "https://vbpl.vn/bogiaoducdaotao/Pages/vbpq-toanvan.aspx?ItemID=179271"

out_dir = Path("out_vbpl_179271")
out_dir.mkdir(parents=True, exist_ok=True)

converter = DocumentConverter()
result = converter.convert(URL)          # URL or local file path
doc = result.document                    # DoclingDocument

# Export to Markdown (nice for RAG ingestion)
# (out_dir / "vbpl_179271.md").write_text(
#     doc.export_to_markdown(),
#     encoding="utf-8",
# )

# Export to JSON (structured)
(out_dir / "vbpl_179271.json").write_text(
    doc.model_dump_json(indent=2, ensure_ascii=False),
    encoding="utf-8",
)

print("Saved to:", out_dir)