import json
from pathlib import Path

# Directory containing url_*.json files
BASE_DIR = Path(__file__).resolve().parent
ALL_URL_DIR = BASE_DIR / "all_url"


def is_document_entry(value: dict) -> bool:
    """Check whether the dict looks like a document leaf node."""
    return (
        isinstance(value, dict)
        and "document_url" in value
        and "document_title" in value
    )


def dict_to_list(node):
    """
    Convert a mapping to a list of single-key dicts, skipping leaf document nodes.
    Example: {"1": {...}, "2": {...}} -> [{"1": {...}}, {"2": {...}}]
    """
    if isinstance(node, dict) and not is_document_entry(node):
        return [{key: dict_to_list(value)} for key, value in node.items()]
    return node


def convert_subject(subject_value):
    """
    Convert subject data from dict of pages to a list structure.
    Leaves already-converted (non-dict) values untouched.
    """
    if not isinstance(subject_value, dict):
        return subject_value
    return [{page_name: dict_to_list(items)} for page_name, items in subject_value.items()]


def convert_file(path: Path):
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        converted = {subject: convert_subject(value) for subject, value in data.items()}
    else:
        converted = data

    with path.open("w", encoding="utf-8") as f:
        json.dump(converted, f, ensure_ascii=False, indent=4)
        f.write("\n")


def main():
    for json_file in sorted(ALL_URL_DIR.glob("url_*.json")):
        convert_file(json_file)
        print(f"Converted {json_file.name}")


if __name__ == "__main__":
    main()
