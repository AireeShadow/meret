import json
import sqlite3
from pathlib import Path
from typing import List, Optional

from meret.core.logger import Logger
from meret.core.models import FileEntry, FileType, Tag, TagNode
from meret.core.repository import AbstractRepository

SCHEMA_PATH = Path(__file__).parent / "schema.sql"
_TAG_SEP = "\x1f"  # разделитель для GROUP_CONCAT


class SQLiteRepository(AbstractRepository, Logger):
    def __init__(self, db_path: str):
        Logger.__init__(self)
        self.db_path = str(db_path)
        self.fts_enabled = True
        self._init_tables()

    # ---------- соединение ----------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")   # меньше блокировок
        return conn

    def _init_tables(self) -> None:
        schema = SCHEMA_PATH.read_text(encoding="utf-8")
        with self._connect() as conn:
            for statement in schema.split(";"):
                stmt = statement.strip()
                if not stmt:
                    continue
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError as e:
                    # FTS5 может отсутствовать в сборке Python — не падаем
                    if "fts5" in stmt.lower():
                        self.fts_enabled = False
                        self.logger.warning(f"FTS5 unavailable: {e}")
                    else:
                        raise
            conn.commit()

    # ---------- конвертация ----------
    @staticmethod
    def _tags_from_row(row) -> List[str]:
        raw = row["tag_names"] if "tag_names" in row.keys() else ""
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

    _SELECT_FILES = f"""
        SELECT f.*, COALESCE(GROUP_CONCAT(t.name, char(31)), '') AS tag_names
        FROM files f
        LEFT JOIN file_tags ft ON ft.file_id = f.id
        LEFT JOIN tags t ON t.id = ft.tag_id
    """

    # ---------- файлы ----------
    def get_file_by_id(self, file_id: int) -> Optional[FileEntry]:
        with self._connect() as conn:
            row = conn.execute(
                f"{self._SELECT_FILES} WHERE f.id = ? GROUP BY f.id", (file_id,)
            ).fetchone()
            return self._row_to_file(row) if row else None

    def get_file_by_path(self, path: str) -> Optional[FileEntry]:
        with self._connect() as conn:
            row = conn.execute(
                f"{self._SELECT_FILES} WHERE f.filepath = ? GROUP BY f.id", (path,)
            ).fetchone()
            return self._row_to_file(row) if row else None

    def save_file(self, file: FileEntry) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO files (filepath, filename, rating, type, metadata)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(filepath) DO UPDATE SET
                    filename = excluded.filename,
                    rating = excluded.rating,
                    type = excluded.type,
                    metadata = excluded.metadata,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    file.filepath,
                    file.filename,
                    file.rating,
                    file.type.value,
                    json.dumps(file.metadata) if file.metadata else None,
                ),
            )
            conn.commit()
            if cur.lastrowid:
                return cur.lastrowid
            row = conn.execute(
                "SELECT id FROM files WHERE filepath = ?", (file.filepath,)
            ).fetchone()
            return row["id"]

    def delete_file(self, file_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM files WHERE id = ?", (file_id,))
            conn.commit()
            return cur.rowcount > 0

    # ---------- теги ----------
    def _ensure_tag(self, conn, name: str, colour: Optional[str] = None) -> int:
        """Возвращает id тега, создавая при необходимости."""
        conn.execute(
            "INSERT OR IGNORE INTO tags (name, colour) VALUES (?, ?)", (name, colour)
        )
        row = conn.execute("SELECT id FROM tags WHERE name = ?", (name,)).fetchone()
        return row["id"]

    def add_tag(self, file_id: int, tag: str, colour: Optional[str] = None) -> int:
        with self._connect() as conn:
            tag_id = self._ensure_tag(conn, tag, colour)
            conn.execute(
                "INSERT OR IGNORE INTO file_tags (file_id, tag_id) VALUES (?, ?)",
                (file_id, tag_id),
            )
            conn.commit()
            return tag_id

    def remove_tag(self, file_id: int, tag: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                """DELETE FROM file_tags
                   WHERE file_id = ?
                     AND tag_id = (SELECT id FROM tags WHERE name = ?)""",
                (file_id, tag),
            )
            conn.commit()
            return cur.rowcount > 0

    def set_file_tags(self, file_id: int, tags: List[str]) -> bool:
        with self._connect() as conn:
            conn.execute("DELETE FROM file_tags WHERE file_id = ?", (file_id,))
            for name in dict.fromkeys(tags):  # без дублей, с сохранением порядка
                tag_id = self._ensure_tag(conn, name)
                conn.execute(
                    "INSERT INTO file_tags (file_id, tag_id) VALUES (?, ?)",
                    (file_id, tag_id),
                )
            conn.commit()
            return True

    def rename_tag(self, old: str, new: str) -> bool:
        """Переименование = ОДНА строка. Если new существует — слияние."""
        with self._connect() as conn:
            src = conn.execute("SELECT id FROM tags WHERE name = ?", (old,)).fetchone()
            if not src:
                return False
            dst = conn.execute("SELECT id FROM tags WHERE name = ?", (new,)).fetchone()

            if dst and dst["id"] != src["id"]:
                conn.execute(
                    """INSERT OR IGNORE INTO file_tags (file_id, tag_id)
                       SELECT file_id, ? FROM file_tags WHERE tag_id = ?""",
                    (dst["id"], src["id"]),
                )
                conn.execute("DELETE FROM file_tags WHERE tag_id = ?", (src["id"],))
                conn.execute("DELETE FROM tags WHERE id = ?", (src["id"],))
            else:
                conn.execute("UPDATE tags SET name = ? WHERE id = ?", (new, src["id"]))
            conn.commit()
            return True

    def set_tag_parent(self, tag: str, parent: Optional[str]) -> bool:
        with self._connect() as conn:
            if parent:
                p = conn.execute("SELECT id FROM tags WHERE name = ?", (parent,)).fetchone()
                if not p:
                    return False
                parent_id = p["id"]
            else:
                parent_id = None
            cur = conn.execute(
                "UPDATE tags SET parent_id = ? WHERE name = ?", (parent_id, tag)
            )
            conn.commit()
            return cur.rowcount > 0

    def get_all_tags(self) -> List[Tag]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, name, colour, parent_id FROM tags ORDER BY name COLLATE NOCASE"
            ).fetchall()
            return [Tag(**dict(r)) for r in rows]

    def get_tag_usage(self, limit: int = 100) -> List[Tag]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT t.id, t.name, t.colour, t.parent_id, COUNT(ft.file_id) AS usage_count
                   FROM tags t LEFT JOIN file_tags ft ON ft.tag_id = t.id
                   GROUP BY t.id ORDER BY usage_count DESC, t.name LIMIT ?""",
                (limit,),
            ).fetchall()
            return [Tag(**dict(r)) for r in rows]

    def get_tag_tree(self) -> List[TagNode]:
        tags = self.get_all_tags()
        nodes = {t.id: TagNode(**t.model_dump()) for t in tags}
        roots: List[TagNode] = []
        for node in nodes.values():
            parent = nodes.get(node.parent_id)
            (parent.children if parent else roots).append(node)
        return roots

    def prune_orphan_tags(self) -> int:
        """Удаляет теги без файлов."""
        with self._connect() as conn:
            cur = conn.execute(
                """DELETE FROM tags WHERE id NOT IN
                   (SELECT DISTINCT tag_id FROM file_tags)"""
            )
            conn.commit()
            return cur.rowcount

    # ---------- поиск ----------
    def search_by_tags(
        self,
        tags: List[str],
        match_all: bool = False,
        file_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[FileEntry]:
        placeholders = ",".join("?" for _ in tags)
        having = "HAVING COUNT(DISTINCT t.id) = ?" if match_all else ""

        sql = f"""
            {self._SELECT_FILES}
            JOIN tags t ON t.id = ft.tag_id
            WHERE t.name IN ({placeholders})
        """
        params: list = list(tags)
        if file_type:
            sql += " AND f.type = ?"
            params.append(file_type)
        sql += " GROUP BY f.id"
        if having:
            sql += " " + having
            params.append(len(tags))
        sql += " LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._row_to_file(r) for r in rows]

    def search_by_name(self, query: str, limit: int = 100) -> List[FileEntry]:
        with self._connect() as conn:
            if self.fts_enabled and query.strip():
                # FTS5: префиксный поиск по словам
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

            # fallback: подстрока
            rows = conn.execute(
                f"{self._SELECT_FILES} WHERE f.filename LIKE ? GROUP BY f.id LIMIT ?",
                (f"%{query}%", limit),
            ).fetchall()
            return [self._row_to_file(r) for r in rows]

    def get_stats(self) -> dict:
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            by_type = dict(conn.execute("SELECT type, COUNT(*) FROM files GROUP BY type"))
            tags = conn.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
            return {"total_files": total, "by_type": by_type, "total_tags": tags}