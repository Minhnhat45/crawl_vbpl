"""SQLite-based crawl state management for resume capability."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path


class CrawlState:
    """Track crawl progress in SQLite for crash recovery."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS documents (
                    item_id INTEGER NOT NULL,
                    scope TEXT NOT NULL,
                    doc_type TEXT NOT NULL,
                    status TEXT DEFAULT 'discovered',
                    detail_json TEXT,
                    error_message TEXT,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (item_id, scope)
                );

                CREATE TABLE IF NOT EXISTS crawl_progress (
                    doc_type TEXT PRIMARY KEY,
                    total_pages INTEGER DEFAULT 0,
                    last_completed_page INTEGER DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_documents_status
                    ON documents(status, doc_type);
            """)

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    # -- Discovery progress --

    def get_last_page(self, doc_type: str) -> int:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT last_completed_page FROM crawl_progress WHERE doc_type = ?",
                (doc_type,),
            ).fetchone()
            return row["last_completed_page"] if row else 0

    def set_total_pages(self, doc_type: str, total: int) -> None:
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO crawl_progress (doc_type, total_pages, last_completed_page)
                   VALUES (?, ?, 0)
                   ON CONFLICT(doc_type) DO UPDATE SET total_pages = ?""",
                (doc_type, total, total),
            )

    def update_last_page(self, doc_type: str, page: int) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE crawl_progress SET last_completed_page = ? WHERE doc_type = ?",
                (page, doc_type),
            )

    # -- Document tracking --

    def add_discovered(self, item_id: int, scope: str, doc_type: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO documents (item_id, scope, doc_type, status)
                   VALUES (?, ?, ?, 'discovered')""",
                (item_id, scope, doc_type),
            )

    def add_discovered_batch(self, docs: list[tuple[int, str, str]]) -> None:
        with self._conn() as conn:
            conn.executemany(
                """INSERT OR IGNORE INTO documents (item_id, scope, doc_type, status)
                   VALUES (?, ?, ?, 'discovered')""",
                docs,
            )

    def mark_detail_done(self, item_id: int, scope: str, detail_json: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """UPDATE documents
                   SET status = 'detail_done', detail_json = ?, updated_at = CURRENT_TIMESTAMP
                   WHERE item_id = ? AND scope = ?""",
                (detail_json, item_id, scope),
            )

    def mark_attachments_done(self, item_id: int, scope: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """UPDATE documents
                   SET status = 'attachments_done', updated_at = CURRENT_TIMESTAMP
                   WHERE item_id = ? AND scope = ?""",
                (item_id, scope),
            )

    def mark_failed(self, item_id: int, scope: str, error: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """UPDATE documents
                   SET status = 'failed', error_message = ?, updated_at = CURRENT_TIMESTAMP
                   WHERE item_id = ? AND scope = ?""",
                (error, item_id, scope),
            )

    def get_pending_details(self, doc_type: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT item_id, scope FROM documents WHERE status = 'discovered' AND doc_type = ?",
                (doc_type,),
            ).fetchall()
            return [{"item_id": r["item_id"], "scope": r["scope"]} for r in rows]

    def get_pending_attachments(self, doc_type: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT item_id, scope, detail_json FROM documents
                   WHERE status = 'detail_done' AND doc_type = ?""",
                (doc_type,),
            ).fetchall()
            return [
                {"item_id": r["item_id"], "scope": r["scope"], "detail_json": r["detail_json"]}
                for r in rows
            ]

    # -- Stats --

    def get_stats(self) -> dict:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT doc_type, status, COUNT(*) as cnt FROM documents GROUP BY doc_type, status"
            ).fetchall()
            stats: dict[str, dict[str, int]] = {}
            for r in rows:
                stats.setdefault(r["doc_type"], {})[r["status"]] = r["cnt"]
            return stats

    def get_total_discovered(self, doc_type: str) -> int:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM documents WHERE doc_type = ?",
                (doc_type,),
            ).fetchone()
            return row["cnt"] if row else 0
