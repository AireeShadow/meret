import json
import logging
import re
import sqlite3
import threading
import contextlib
from pathlib import Path

from meret.core import tagpaths
from meret.core.logger import traced
from meret.core.models import FileEntry, FileType, Tag, TagNode
from meret.core.repository import AbstractRepository

SCHEMA_PATH = Path(__file__).parent / "schema.sql"
logger = logging.getLogger(__name__)
_traced = traced(logger)  # DEBUG: every public call with args, result size and time
_TAG_SEP = "\x1f"  # separator for GROUP_CONCAT
_WORD = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")


def _iter_statements(script: str):
    """Splits an SQL script into individual statements.

    Respects string literals, comments and BEGIN...END in triggers.
    Important: execute() on a concatenation silently runs only the first statement,
    so we must split them strictly one by one.
    """
    buf: list[str] = []
    quote: str | None = None
    depth = 0  # BEGIN...END nesting depth
    i, n = 0, len(script)

    while i < n:
        if quote is None:
            if script.startswith("--", i):
                j = script.find("\n", i)
                j = n if j == -1 else j
                buf.append(script[i:j])
                i = j
                continue
            if script.startswith("/*", i):
                j = script.find("*/", i + 2)
                j = n if j == -1 else j + 2
                buf.append(script[i:j])
                i = j
                continue

        ch = script[i]

        if quote is not None:
            buf.append(ch)
            if ch == quote:
                if i + 1 < n and script[i + 1] == quote:  # '' is an escaped quote
                    buf.append(script[i + 1])
                    i += 2
                    continue
                quote = None
            i += 1
            continue

        if ch in "'\"`":
            quote = ch
            buf.append(ch)
            i += 1
            continue

        if ch.isalpha() or ch == "_":
            word = _WORD.match(script, i).group(0)
            buf.append(word)
            i += len(word)
            upper = word.upper()
            if upper == "BEGIN":
                depth += 1
            elif upper == "END":
                depth = max(0, depth - 1)
            continue

        buf.append(ch)
        i += 1

        if ch == ";" and depth == 0:
            stmt = "".join(buf).strip()
            if stmt:
                yield stmt
            buf = []

    tail = "".join(buf).strip()
    if tail:
        yield tail


class SQLiteRepository(AbstractRepository):
    _SAVE_FILE_SQL = """
        INSERT INTO files (filepath, filename, rating, type, metadata, size_bytes, mtime)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(filepath) DO UPDATE SET
            filename = excluded.filename,
            rating = excluded.rating,
            type = excluded.type,
            metadata = excluded.metadata,
            size_bytes = excluded.size_bytes,
            mtime = excluded.mtime,
            updated_at = CURRENT_TIMESTAMP
    """
    _EXPECTED_TABLES = ("files", "tags", "file_tags")

    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        self.fts_enabled = True
        self._watch: sqlite3.Connection | None = None  # for data_version()
        self._watch_lock = threading.Lock()
        self._init_tables()

    #----------- helpers -------------
    @staticmethod
    def _fingerprint(entry: FileEntry) -> tuple[int, float]:
        """(size, mtime) from the extracted metadata — the incremental scan key."""
        meta = entry.metadata or {}
        try:
            size = int(meta.get("size_bytes") or 0)
        except (TypeError, ValueError):
            size = 0
        try:
            mtime = float(meta.get("modified_at") or 0.0)
        except (TypeError, ValueError):
            mtime = 0.0
        return size, mtime
    
    # ---------- connection ----------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 5000")  # Wait up to 5 seconds
        return conn

    @contextlib.contextmanager
    def _session(self):
        """One operation: commit on success, roll back on error, always close.

        sqlite3.Connection.__exit__ commits but does NOT close, so the old
        `with self._connect()` left every connection to the GC. This does both,
        in the right order, and keeps _connect() for the long-lived watch handle.
        """
        conn = self._connect()
        try:
            with conn:  # commit on success, rollback on exception
                yield conn
        finally:
            conn.close()

    def _init_tables(self) -> None:
        schema = SCHEMA_PATH.read_text(encoding="utf-8")
        with self._session() as conn:
            for stmt in _iter_statements(schema):
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError as e:
                    if "fts5" in stmt.lower():  # build without FTS5 — don't crash
                        self.fts_enabled = False
                        logger.warning(f"FTS5 unavailable: {e}")
                    else:
                        raise
            self._migrate_files(conn)
            self._migrate_tags(conn)
            conn.commit()
            self._check_schema(conn)
        logger.debug("opened %s (fts=%s)", self.db_path, self.fts_enabled)

    def _check_schema(self, conn) -> None:
        """The schema must be created in full.

        execute() on concatenated statements silently runs only the first one,
        so we check the result instead of hoping.
        """
        have = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        missing = [t for t in self._EXPECTED_TABLES if t not in have]
        if missing:
            raise RuntimeError(
                f"schema was only partially created, missing tables: {', '.join(missing)}. "
                "Looks like a statement in schema.sql lost its ';'"
            )

    def _migrate_tags(self, conn) -> None:
        """Brings an old tags table up to the current schema.

        1. Adds name_key and fills it in (COLLATE NOCASE only folds
           ASCII, so we handle case-insensitivity ourselves).
        2. Drops the global uniqueness of name_key.
        3. Adds uniqueness among siblings. COALESCE(parent_id, 0) is needed
           because SQLite treats NULLs in a unique index as distinct,
           and two root-level "mystery" tags would happily coexist.
        """
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(tags)")}
        if "name_key" not in cols:
            conn.execute("ALTER TABLE tags ADD COLUMN name_key TEXT")

        rows = conn.execute(
            "SELECT id, name FROM tags WHERE name_key IS NULL OR name_key = ''"
        ).fetchall()
        for row in rows:
            conn.execute(
                "UPDATE tags SET name_key = ? WHERE id = ?",
                (row["name"].casefold(), row["id"]),
            )

        conn.execute("DROP INDEX IF EXISTS idx_tags_name_key")

        try:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_tags_sibling "
                "ON tags(COALESCE(parent_id, 0), name_key)"
            )
        except sqlite3.OperationalError as e:
            logger.warning(f"same-name siblings found, unique index not created: {e}")

    # ---------- conversion ----------
    @staticmethod
    def _tags_from_row(row) -> list[str]:
        """Tag names of a row, empty when the query has no tag_names column.

        `in` on a sqlite3.Row tests VALUES, not column names (cpython#100450), so
        `"tag_names" in row` is False for any real listing and every file would come
        back untagged. keys() is the honest question, and holding it in a local keeps
        ruff's SIM118 and SIM401 from "simplifying" it back into the bug.
        """
        columns = row.keys()
        raw = row["tag_names"] if "tag_names" in columns else ""
        return raw.split(_TAG_SEP) if raw else []

    def _row_to_file(self, row) -> FileEntry:
        return FileEntry(
            id=row["id"],
            filename=row["filename"],
            filepath=row["filepath"],
            labels=self._tags_from_row(row),
            rating=row["rating"],
            type=FileType(row["type"]),
            metadata=json.loads(row["metadata"]) if row["metadata"] else None,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    _SELECT_FILES = """
        SELECT f.*, COALESCE(GROUP_CONCAT(t.name, char(31)), '') AS tag_names
        FROM files f
        LEFT JOIN file_tags ft ON ft.file_id = f.id
        LEFT JOIN tags t ON t.id = ft.tag_id
    """

    # ---------- files ----------
    def _migrate_files(self, conn) -> None:
        """Adds the size/mtime columns used by the incremental scan."""
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(files)")}
        if "size_bytes" not in cols:
            conn.execute("ALTER TABLE files ADD COLUMN size_bytes INTEGER DEFAULT 0")
        if "mtime" not in cols:
            conn.execute("ALTER TABLE files ADD COLUMN mtime REAL DEFAULT 0")

    @_traced
    def get_file_by_id(self, file_id: int) -> FileEntry | None:
        with self._session() as conn:
            row = conn.execute(
                f"{self._SELECT_FILES} WHERE f.id = ? GROUP BY f.id", (file_id,)
            ).fetchone()
            return self._row_to_file(row) if row else None

    @_traced
    def get_file_by_path(self, path: str) -> FileEntry | None:
        with self._session() as conn:
            row = conn.execute(
                f"{self._SELECT_FILES} WHERE f.filepath = ? GROUP BY f.id", (path,)
            ).fetchone()
            return self._row_to_file(row) if row else None

    @_traced
    def save_file(self, file: FileEntry) -> int:
        with self._session() as conn:
            size, mtime = self._fingerprint(file)
            cur = conn.execute(
                self._SAVE_FILE_SQL,
                (
                    file.filepath,
                    file.filename,
                    file.rating,
                    file.type.value,
                    json.dumps(file.metadata, ensure_ascii=False) if file.metadata else None,
                    size,
                    mtime,
                ),
            )
            conn.commit()
            if cur.lastrowid:
                return cur.lastrowid
            row = conn.execute(
                "SELECT id FROM files WHERE filepath = ?", (file.filepath,)
            ).fetchone()
            return row["id"]

    @_traced
    def save_files(
        self,
        files: list[FileEntry],
        chunk_size: int = 1000,
        on_progress=None,
    ) -> dict[str, int]:
        """Batch of files: one connection, a transaction per chunk, executemany."""
        stats = {"indexed": 0, "updated": 0, "errors": 0}
        if not files:
            return stats

        total = len(files)
        with self._session() as conn:
            known = {row[0] for row in conn.execute("SELECT filepath FROM files")}

            for start in range(0, total, chunk_size):
                part = files[start : start + chunk_size]
                rows = []
                for entry in part:
                    try:
                        meta = (
                            json.dumps(entry.metadata, ensure_ascii=False)
                            if entry.metadata
                            else None
                        )
                    except (TypeError, ValueError) as e:
                        stats["errors"] += 1
                        meta = None
                        logger.error(f"metadata is not serializable: {entry.filepath}: {e}")
                    size, mtime = self._fingerprint(entry)
                    rows.append(
                        (
                            entry.filepath,
                            entry.filename,
                            entry.rating,
                            entry.type.value,
                            meta,
                            size,
                            mtime,
                        )
                    )

                conn.executemany(self._SAVE_FILE_SQL, rows)
                conn.commit()

                new = sum(1 for e in part if e.filepath not in known)
                stats["indexed"] += new
                stats["updated"] += len(part) - new
                known.update(e.filepath for e in part)

                if on_progress:
                    on_progress(min(start + chunk_size, total), total)

        return stats

    @_traced
    def delete_file(self, file_id: int) -> bool:
        with self._session() as conn:
            cur = conn.execute("DELETE FROM files WHERE id = ?", (file_id,))
            conn.commit()
            return cur.rowcount > 0

    # ---------- tags: lookup ----------

    _TAG_COLS = "id, name, name_key, colour, parent_id"

    _FIND_CHILD_SQL = f"""
        SELECT {_TAG_COLS} FROM tags
        WHERE COALESCE(parent_id, 0) = COALESCE(?, 0) AND name_key = ?
    """

    def _child_row(self, conn, parent_id: int | None, chunk: str):
        return conn.execute(self._FIND_CHILD_SQL, (parent_id, chunk.casefold())).fetchone()

    def _tag_strict(self, conn, ref: str):
        """Tag strictly by path from the root. None if any segment is not found."""
        chunks = tagpaths.parts(ref)
        if not chunks:
            return None
        row = None
        parent_id: int | None = None
        for chunk in chunks:
            row = self._child_row(conn, parent_id, chunk)
            if row is None:
                return None
            parent_id = row["id"]
        return row

    def _tag_row(self, conn, ref: str):
        """Tag by path ("genre/mystery") or by a single name.

        The path is strict. A single name is looked up as a root tag first, then
        anywhere: namesakes in different branches are now allowed, but the choice
        is deterministic (root first, then the lowest id).
        """
        chunks = tagpaths.parts(ref)
        if not chunks:
            return None

        row = self._tag_strict(conn, ref)
        if row is not None or len(chunks) > 1:
            return row

        return conn.execute(
            f"SELECT {self._TAG_COLS} FROM tags WHERE name_key = ? "
            "ORDER BY (parent_id IS NOT NULL), id LIMIT 1",
            (chunks[0].casefold(),),
        ).fetchone()

    def _tag_id(self, conn, ref: str) -> int | None:
        row = self._tag_row(conn, ref)
        return row["id"] if row else None

    def _ensure_tag(
        self, conn, ref: str, colour: str | None = None, reuse_any: bool = True
    ) -> int:
        """Returns the tag id, creating it and any missing parents along the path.

        reuse_any=True: a single name is first looked up anywhere, so we don't
        multiply namesakes. reuse_any=False (for create_tag): the path is strict.
        """
        chunks = tagpaths.parts(ref)
        if not chunks:
            raise ValueError(f"empty tag path: {ref!r}")

        if reuse_any and len(chunks) == 1:
            row = self._tag_row(conn, chunks[0])
            if row is not None:
                return row["id"]

        parent_id: int | None = None
        tag_id: int | None = None
        for i, chunk in enumerate(chunks):
            row = self._child_row(conn, parent_id, chunk)
            if row is None:
                conn.execute(
                    "INSERT INTO tags (name, name_key, colour, parent_id) VALUES (?, ?, ?, ?)",
                    (chunk, chunk.casefold(), colour if i == len(chunks) - 1 else None, parent_id),
                )
                row = self._child_row(conn, parent_id, chunk)
            tag_id = row["id"]
            parent_id = tag_id
        return tag_id

    def _would_cycle(self, conn, tag_id: int, parent_id: int) -> bool:
        """True if parent_id is tag_id itself or one of its descendants.

        Walk up FROM THE PARENT looking for the tag: if we hit it, the parent lies
        inside the tag's subtree, and reparenting would create a loop.
        """
        seen: set[int] = set()
        cur: int | None = parent_id
        while cur is not None and cur not in seen:
            if cur == tag_id:
                return True
            seen.add(cur)
            row = conn.execute("SELECT parent_id FROM tags WHERE id = ?", (cur,)).fetchone()
            cur = row["parent_id"] if row else None
        return False

    @_traced
    def add_tag(self, file_id: int, tag: str, colour: str | None = None) -> int:
        with self._session() as conn:
            tag_id = self._ensure_tag(conn, tag, colour)
            conn.execute(
                "INSERT OR IGNORE INTO file_tags (file_id, tag_id) VALUES (?, ?)",
                (file_id, tag_id),
            )
            conn.commit()
            return tag_id

    @_traced
    def remove_tag(self, file_id: int, tag: str) -> bool:
        with self._session() as conn:
            tag_id = self._tag_id(conn, tag)
            if tag_id is None:
                return False
            cur = conn.execute(
                "DELETE FROM file_tags WHERE file_id = ? AND tag_id = ?",
                (file_id, tag_id),
            )
            conn.commit()
            return cur.rowcount > 0

    @_traced
    def set_file_tags(self, file_id: int, tags: list[str]) -> bool:
        with self._session() as conn:
            conn.execute("DELETE FROM file_tags WHERE file_id = ?", (file_id,))
            for name in dict.fromkeys(tags):  # dedupe while preserving order
                tag_id = self._ensure_tag(conn, name)
                conn.execute(
                    "INSERT OR IGNORE INTO file_tags (file_id, tag_id) VALUES (?, ?)",
                    (file_id, tag_id),
                )
            conn.commit()
            return True

    @_traced
    def rename_tag(self, old: str, new: str) -> bool:
        """Renaming = ONE row. If a sibling with that name exists, they are merged."""
        with self._session() as conn:
            src = self._tag_row(conn, old)
            if src is None:
                return False

            chunks = tagpaths.parts(new)
            if not chunks:
                return False
            new_name = chunks[-1]

            dst = self._child_row(conn, src["parent_id"], new_name)
            if dst is not None and dst["id"] != src["id"]:
                # src's children move up to src's parent, links move to dst
                conn.execute(
                    "UPDATE tags SET parent_id = ? WHERE parent_id = ?",
                    (src["parent_id"], src["id"]),
                )
                conn.execute(
                    """INSERT OR IGNORE INTO file_tags (file_id, tag_id)
                       SELECT file_id, ? FROM file_tags WHERE tag_id = ?""",
                    (dst["id"], src["id"]),
                )
                conn.execute("DELETE FROM file_tags WHERE tag_id = ?", (src["id"],))
                conn.execute("DELETE FROM tags WHERE id = ?", (src["id"],))
            else:
                conn.execute(
                    "UPDATE tags SET name = ?, name_key = ? WHERE id = ?",
                    (new_name, new_name.casefold(), src["id"]),
                )
            conn.commit()
            return True

    # ---------- tags: creation and hierarchy ----------

    @_traced
    def create_tag(self, name: str, colour: str | None = None) -> bool:
        """Create a tag by path, building any missing parents.

        The path is strict: create_tag("a") creates a root "a" even if a nested
        namesake exists somewhere. False if a tag already exists at this path.
        """
        with self._session() as conn:
            if self._tag_strict(conn, name) is not None:
                return False
            self._ensure_tag(conn, name, colour, reuse_any=False)
            conn.commit()
            return True

    @_traced
    def delete_tag(self, name: str) -> bool:
        """Deletes a tag: children move up to its parent, links go away via cascade."""
        with self._session() as conn:
            row = self._tag_row(conn, name)
            if row is None:
                return False
            conn.execute(
                "UPDATE tags SET parent_id = ? WHERE parent_id = ?",
                (row["parent_id"], row["id"]),
            )
            conn.execute("DELETE FROM tags WHERE id = ?", (row["id"],))
            conn.commit()
            return True

    @_traced
    def set_tag_parent(self, tag: str, parent: str | None) -> bool:
        with self._session() as conn:
            row = self._tag_row(conn, tag)
            if row is None:
                return False

            if parent is None:
                parent_id = None
            else:
                prow = self._tag_row(conn, parent)
                if prow is None:
                    return False
                parent_id = prow["id"]
                if self._would_cycle(conn, row["id"], parent_id):
                    return False  # it would be a cycle

            conn.execute("UPDATE tags SET parent_id = ? WHERE id = ?", (parent_id, row["id"]))
            conn.commit()
            return True

    # ---------- tags: reading ----------
    @_traced
    def get_all_tags(self) -> list[Tag]:
        with self._session() as conn:
            rows = conn.execute(
                "SELECT id, name, colour, parent_id FROM tags ORDER BY name_key"
            ).fetchall()
            return [Tag(**dict(r)) for r in rows]

    @_traced
    def get_tag_usage(self, limit: int = 100) -> list[Tag]:
        with self._session() as conn:
            rows = conn.execute(
                """SELECT t.id, t.name, t.colour, t.parent_id,
                          COUNT(ft.file_id) AS usage_count
                   FROM tags t LEFT JOIN file_tags ft ON ft.tag_id = t.id
                   GROUP BY t.id ORDER BY usage_count DESC, t.name_key LIMIT ?""",
                (limit,),
            ).fetchall()
            return [Tag(**dict(r)) for r in rows]

    @_traced
    def get_tag_tree(self) -> list[TagNode]:
        tags = self.get_all_tags()
        nodes = {t.id: TagNode(**t.model_dump()) for t in tags}
        roots: list[TagNode] = []
        for node in nodes.values():
            parent = nodes.get(node.parent_id)
            if parent is None or parent is node:
                roots.append(node)
            else:
                parent.children.append(node)

        # guard against cycles: also show whatever isn't reachable from the roots
        reachable: set[int] = set()
        stack = list(roots)
        while stack:
            n = stack.pop()
            if n.id in reachable:
                continue
            reachable.add(n.id)
            stack.extend(n.children)
        for node in nodes.values():
            if node.id not in reachable:
                roots.append(node)
        return roots

    @_traced
    def prune_orphan_tags(self) -> int:
        """Deletes tags with no files AND no children.

        A section tag ("genre") usually has no files of its own, only children,
        so it must not be touched.
        """
        with self._session() as conn:
            cur = conn.execute(
                """DELETE FROM tags
                   WHERE id NOT IN (SELECT DISTINCT tag_id FROM file_tags)
                     AND id NOT IN (SELECT parent_id FROM tags
                                    WHERE parent_id IS NOT NULL)"""
            )
            conn.commit()
            return cur.rowcount

    # ---------- search ----------
    @_traced
    def search_by_tags(
        self,
        tags: list[str],
        match_all: bool = False,
        file_type: str | None = None,
        limit: int = 100,
    ) -> list[FileEntry]:
        if not tags:
            sql = f"{self._SELECT_FILES} WHERE 1=1"
            params: list = []
            if file_type:
                sql += " AND f.type = ?"
                params.append(file_type)
            sql += " GROUP BY f.id LIMIT ?"
            params.append(limit)
            with self._session() as conn:
                return [self._row_to_file(r) for r in conn.execute(sql, params).fetchall()]

        placeholders = ",".join("?" for _ in tags)

        # t and ft are already joined inside _SELECT_FILES — no extra JOIN needed
        sql = f"{self._SELECT_FILES} WHERE t.name_key IN ({placeholders})"
        params = [t.casefold() for t in tags]

        if file_type:
            sql += " AND f.type = ?"
            params.append(file_type)

        sql += " GROUP BY f.id"
        if match_all:
            sql += " HAVING COUNT(DISTINCT t.id) = ?"
            params.append(len(tags))

        sql += " LIMIT ?"
        params.append(limit)

        with self._session() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._row_to_file(r) for r in rows]

    @_traced
    def search_by_tag_ids(
        self,
        groups: list[list[int]],
        match_all: bool = False,
        file_type: str | None = None,
        limit: int | None = None,
    ) -> list[FileEntry]:
        groups = [g for g in groups if g]
        if not groups:
            return []

        clauses: list[str] = []
        params: list = []
        for group in groups:
            holes = ",".join("?" for _ in group)
            clauses.append(
                f"EXISTS (SELECT 1 FROM file_tags x "
                f"WHERE x.file_id = f.id AND x.tag_id IN ({holes}))"
            )
            params.extend(group)

        # parentheses matter: OR would otherwise swallow the type filter
        joiner = " AND " if match_all else " OR "
        sql = f"{self._SELECT_FILES} WHERE ({joiner.join(clauses)})"
        if file_type:
            sql += " AND f.type = ?"
            params.append(file_type)
        sql += " GROUP BY f.id"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        with self._session() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._row_to_file(r) for r in rows]

    @_traced
    def search_by_name(self, query: str, limit: int = 100) -> list[FileEntry]:
        with self._session() as conn:
            if self.fts_enabled and query.strip():
                # FTS5: prefix search by words
                safe = " ".join(f'"{w}"*' for w in query.split())
                rows = conn.execute(
                    f"""{self._SELECT_FILES}
                        JOIN files_fts fts ON fts.rowid = f.id
                        WHERE files_fts MATCH ?
                        GROUP BY f.id LIMIT ?""",
                    (safe, limit),
                ).fetchall()
                if rows:
                    return [self._row_to_file(r) for r in rows]

            # fallback: substring
            rows = conn.execute(
                f"{self._SELECT_FILES} WHERE f.filename LIKE ? GROUP BY f.id LIMIT ?",
                (f"%{query}%", limit),
            ).fetchall()
            return [self._row_to_file(r) for r in rows]

    @_traced
    def get_stats(self) -> dict:
        with self._session() as conn:
            total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            by_type = dict(conn.execute("SELECT type, COUNT(*) FROM files GROUP BY type"))
            tags = conn.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
            return {"total_files": total, "by_type": by_type, "total_tags": tags}

    @_traced
    def known_paths(self) -> set[str]:
        with self._session() as conn:
            return {row[0] for row in conn.execute("SELECT filepath FROM files")}

    @_traced
    def known_fingerprints(self) -> dict[str, tuple[int, float]]:
        """{filepath: (size, mtime)} — one query for the whole incremental scan."""
        with self._session() as conn:
            return {
                row["filepath"]: (row["size_bytes"] or 0, row["mtime"] or 0.0)
                for row in conn.execute("SELECT filepath, size_bytes, mtime FROM files")
            }

    # ---------- change tracking ----------
    def data_version(self) -> int | None:
        """PRAGMA data_version on a dedicated long-lived connection.

        The pragma only moves for commits made by OTHER connections. That covers
        everything: our own writes go through a fresh _connect() each time, and so
        do other processes (`meret tag` while the drive is mounted).
        The connection lives as long as the repository; reads from many threads.
        """
        with self._watch_lock:
            if self._watch is None:
                self._watch = sqlite3.connect(self.db_path, check_same_thread=False)
            return self._watch.execute("PRAGMA data_version").fetchone()[0]
