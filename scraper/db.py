"""Minimal SQLite persistence (Option 1).

Purpose: lightweight dedupe + ability to resume without re-scraping.
We only store canonical_url, an optional primary topic label, the content hash, and timestamps.
No relational expansion; JSONL remains source-of-truth for rich data.
"""

import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = Path("data/state.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS guides (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_url TEXT UNIQUE NOT NULL,
    primary_topic TEXT,
    content_hash TEXT,
    version INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS topics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic_slug TEXT UNIQUE NOT NULL,
    topic_url TEXT NOT NULL,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_visited_at TIMESTAMP,
    pages_crawled INTEGER DEFAULT 0,
    guides_crawled INTEGER DEFAULT 0,
    last_page_number INTEGER DEFAULT 0,
    is_complete INTEGER DEFAULT 0,
    total_pages_detected INTEGER
);
CREATE TABLE IF NOT EXISTS topic_guides (
    topic_slug TEXT NOT NULL,
    guide_url TEXT NOT NULL,
    PRIMARY KEY (topic_slug, guide_url)
);
"""


def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        # lightweight migration for added columns (idempotent)
        cur = conn.execute("PRAGMA table_info(topics)")
        cols = {row[1] for row in cur.fetchall()}
        alter_stmts = []
        if "guides_crawled" not in cols:
            alter_stmts.append("ALTER TABLE topics ADD COLUMN guides_crawled INTEGER DEFAULT 0")
        if "last_page_number" not in cols:
            alter_stmts.append("ALTER TABLE topics ADD COLUMN last_page_number INTEGER DEFAULT 0")
        if "is_complete" not in cols:
            alter_stmts.append("ALTER TABLE topics ADD COLUMN is_complete INTEGER DEFAULT 0")
        if "total_pages_detected" not in cols:
            alter_stmts.append("ALTER TABLE topics ADD COLUMN total_pages_detected INTEGER")
        # topic_guides table (create if missing)
        conn.execute("CREATE TABLE IF NOT EXISTS topic_guides (topic_slug TEXT NOT NULL, guide_url TEXT NOT NULL, PRIMARY KEY (topic_slug, guide_url))")
        for stmt in alter_stmts:
            conn.execute(stmt)
        if alter_stmts:
            conn.commit()


def seen_canonical(url: str) -> bool:
    with get_conn() as conn:
        cur = conn.execute("SELECT 1 FROM guides WHERE canonical_url=?", (url,))
        return cur.fetchone() is not None


def insert_or_update_guide(canonical_url: str, content_hash: Optional[str], primary_topic: Optional[str]) -> dict:
    """Upsert guide row; detect content change.

    Returns dict with keys:
      - is_new: bool
      - is_updated: bool (content hash changed)
      - version: int (incremented on change)
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT content_hash, version FROM guides WHERE canonical_url=?", (canonical_url,))
        row = cur.fetchone()
        if not row:
            cur.execute(
                "INSERT INTO guides(canonical_url, primary_topic, content_hash, version) VALUES(?,?,?,1)",
                (canonical_url, primary_topic, content_hash),
            )
            conn.commit()
            return {"is_new": True, "is_updated": False, "version": 1}
        old_hash, old_version = row
        is_updated = content_hash is not None and content_hash != old_hash and content_hash != ""
        if is_updated:
            new_version = (old_version or 1) + 1
            cur.execute(
                "UPDATE guides SET content_hash=?, version=?, primary_topic=? WHERE canonical_url=?",
                (content_hash, new_version, primary_topic, canonical_url),
            )
            conn.commit()
            return {"is_new": False, "is_updated": True, "version": new_version}
        # no change
        cur.execute(
            "UPDATE guides SET primary_topic=COALESCE(?, primary_topic) WHERE canonical_url=?",
            (primary_topic, canonical_url),
        )
        conn.commit()
        return {"is_new": False, "is_updated": False, "version": old_version or 1}


# --- Topic helpers ---
def upsert_topic(topic_url: str) -> dict:
    """Insert a topic row if new. Returns dict with is_new + slug.

    Topic slug is derived from last non-empty path segment.
    """
    from urllib.parse import urlparse
    parts = urlparse(topic_url)
    slug = parts.path.rstrip('/').split('/')[-1]
    if not slug:
        slug = parts.path.strip('/').replace('/', '_') or 'root'
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM topics WHERE topic_slug=?", (slug,))
        row = cur.fetchone()
        if row:
            return {"is_new": False, "topic_slug": slug}
        cur.execute(
            "INSERT INTO topics(topic_slug, topic_url) VALUES(?, ?)",
            (slug, topic_url),
        )
        conn.commit()
        return {"is_new": True, "topic_slug": slug}


def mark_topic_visit(slug: str, pages_crawled_increment: int = 1):
    with get_conn() as conn:
        conn.execute(
            "UPDATE topics SET last_visited_at=CURRENT_TIMESTAMP, pages_crawled=pages_crawled + ? WHERE topic_slug=?",
            (pages_crawled_increment, slug),
        )
        conn.commit()


def seen_topic(slug: str) -> bool:
    with get_conn() as conn:
        cur = conn.execute("SELECT 1 FROM topics WHERE topic_slug=?", (slug,))
        return cur.fetchone() is not None


def update_topic_progress(slug: str, guides_inc: int = 0, last_page_number: int | None = None, mark_complete: bool = False):
    sets = []
    params = []
    if guides_inc:
        sets.append("guides_crawled = guides_crawled + ?")
        params.append(guides_inc)
    if last_page_number is not None:
        sets.append("last_page_number = ?")
        params.append(last_page_number)
    if mark_complete:
        sets.append("is_complete = 1")
    if not sets:
        return
    sql = f"UPDATE topics SET {', '.join(sets)} WHERE topic_slug=?"
    params.append(slug)
    with get_conn() as conn:
        conn.execute(sql, tuple(params))
        conn.commit()


def get_topic_progress(slug: str) -> dict | None:
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT topic_slug, pages_crawled, guides_crawled, last_page_number, is_complete FROM topics WHERE topic_slug=?",
            (slug,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "topic_slug": row[0],
            "pages_crawled": row[1],
            "guides_crawled": row[2],
            "last_page_number": row[3],
            "is_complete": bool(row[4]),
        }


def record_topic_guides(slug: str, guide_urls: list[str]):
    """Persist observed (non-video) guide URLs for a topic to derive total available.

    Idempotent: uses INSERT OR IGNORE on composite PK.
    """
    if not guide_urls:
        return
    with get_conn() as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO topic_guides(topic_slug, guide_url) VALUES(?, ?)",
            [(slug, u) for u in guide_urls],
        )
        conn.commit()


def topic_guide_counts() -> list[tuple[str, int, int]]:
    """Return list of (topic_slug, scraped_guides, total_non_video_observed)."""
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT t.topic_slug,
                   t.guides_crawled,
                   COALESCE((SELECT COUNT(*) FROM topic_guides tg WHERE tg.topic_slug = t.topic_slug), 0) AS total_available
            FROM topics t
            ORDER BY t.topic_slug
            """
        )
        return cur.fetchall()


def set_topic_total_pages(slug: str, total_pages: int):
    """Persist detected total pages for a topic (idempotent overwrite)."""
    with get_conn() as conn:
        conn.execute(
            "UPDATE topics SET total_pages_detected=? WHERE topic_slug=?",
            (total_pages, slug),
        )
        conn.commit()


def get_topic_total_pages(slug: str) -> int | None:
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT total_pages_detected FROM topics WHERE topic_slug=?",
            (slug,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return row[0]
