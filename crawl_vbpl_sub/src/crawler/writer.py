"""JSONL file writer with async I/O and file locking."""

import asyncio
import json
import logging

import aiofiles

from crawler.config import VBHN_JSONL, VBPQ_JSONL

logger = logging.getLogger(__name__)

# Locks to prevent concurrent writes to the same file
_write_locks: dict[str, asyncio.Lock] = {}


def _get_lock(key: str) -> asyncio.Lock:
    if key not in _write_locks:
        _write_locks[key] = asyncio.Lock()
    return _write_locks[key]


async def write_document(record: dict, doc_type: str) -> None:
    """Append a single document record to the appropriate JSONL file."""
    filepath = VBPQ_JSONL if doc_type == "vbpq" else VBHN_JSONL
    filepath.parent.mkdir(parents=True, exist_ok=True)

    line = json.dumps(record, ensure_ascii=False) + "\n"

    lock = _get_lock(doc_type)
    async with lock:
        async with aiofiles.open(filepath, "a", encoding="utf-8") as f:
            await f.write(line)
