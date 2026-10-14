import json
import sqlite3
from pathlib import Path
from typing import List, Optional

from meret.core.logger import Logger
from meret.core.models import FileEntry, FileType, Tag, TagNode
from meret.core.repository import AbstractRepository

SCHEMA_PATH = Path(__file__).parent / "schema.sql"
_TAG_SEP = "\x1f"  # разделитель для GROUP_CONCAT


def _iter_statements(script: str):
    """Режет SQL-скрипт на операторы, уважая BEGIN...END в триггерах."""
    buffer = ""
    for line in script.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            yield buffer.strip()
            buffer = ""
    if buffer.strip():
        yield buffer.strip()

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
            for stmt in _iter_statements(schema):
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError as e:
                    if "fts5" in stmt.lower():          # сборка без FTS5 — не падаем
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
    def _tag_id(self, conn, name: str) -> Optional[int]:
        row = conn.execute("SELECT id FROM tags WHERE name = ?", (name,)).fetchone()
        return row["id"] if row else None

    def _is_ancestor(self, conn, candidate: int, tag_id: int) -> bool:
        """True, если candidate — сам tag_id или кто-то из его предков."""
        seen: set[int] = set()
        cur: Optional[int] = tag_id
        while cur is not None and cur not in seen:
            if cur == candidate:
                return True
            seen.add(cur)
            row = conn.execute("SELECT parent_id FROM tags WHERE id = ?", (cur,)).fetchone()
            cur = row["parent_id"] if row else None
        return False

    def create_tag(self, name: str, colour: Optional[str] = None) -> bool:
        with self._connect() as conn:
            if self._tag_id(conn, name) is not None:
                return False
            conn.execute("INSERT INTO tags (name, colour) VALUES (?, ?)", (name, colour))
            conn.commit()
            return True

    def delete_tag(self, name: str) -> bool:
        """Удаляет тег: дети поднимаются на уровень его родителя, ссылки уходят каскадом."""
        with self._connect() as conn:
            tag_id = self._tag_id(conn, name)
            if tag_id is None:
                return False
            row = conn.execute("SELECT parent_id FROM tags WHERE id = ?", (tag_id,)).fetchone()
            conn.execute(
                "UPDATE tags SET parent_id = ? WHERE parent_id = ?",
                (row["parent_id"], tag_id),
            )
            conn.execute("DELETE FROM tags WHERE id = ?", (tag_id,))
            conn.commit()
            return True

    def set_tag_parent(self, tag: str, parent: Optional[str]) -> bool:
        with self._connect() as conn:
            tag_id = self._tag_id(conn, tag)
            if tag_id is None:
                return False

            if parent is None:
                parent_id = None
            else:
                parent_id = self._tag_id(conn, parent)
                if parent_id is None:
                    return False
                if self._is_ancestor(conn, parent_id, tag_id):
                    return False          # был бы цикл

            conn.execute(
                "UPDATE tags SET parent_id = ? WHERE id = ?", (parent_id, tag_id)
            )
            conn.commit()
            return True

    def get_tag_tree(self) -> List[TagNode]:
        tags = self.get_all_tags()
        nodes = {t.id: TagNode(**t.model_dump()) for t in tags}
        roots: List[TagNode] = []
        for node in nodes.values():
            parent = nodes.get(node.parent_id)
            if parent is None or parent is node:
                roots.append(node)
            else:
                parent.children.append(node)

        # страховка от циклов: что не достижимо от корней — тоже показываем
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

    def prune_orphan_tags(self) -> int:
        """Удаляет теги без файлов И без детей.

        Тег-раздел («жанр») обычно сам файлов не имеет — только детей,
        поэтому его трогать нельзя.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """DELETE FROM tags
                   WHERE id NOT IN (SELECT DISTINCT tag_id FROM file_tags)
                     AND id NOT IN (SELECT parent_id FROM tags
                                    WHERE parent_id IS NOT NULL)"""
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
        if not tags:
            sql = f"{self._SELECT_FILES} WHERE 1=1"
            params: list = []
            if file_type:
                sql += " AND f.type = ?"
                params.append(file_type)
            sql += " GROUP BY f.id LIMIT ?"
            params.append(limit)
            with self._connect() as conn:
                return [self._row_to_file(r) for r in conn.execute(sql, params).fetchall()]

        placeholders = ",".join("?" for _ in tags)

        # t и ft уже приджойнены внутри _SELECT_FILES — добавлять JOIN не нужно
        sql = f"{self._SELECT_FILES} WHERE t.name IN ({placeholders})"
        params = list(tags)

        if file_type:
            sql += " AND f.type = ?"
            params.append(file_type)

        sql += " GROUP BY f.id"
        if match_all:
            sql += " HAVING COUNT(DISTINCT t.id) = ?"
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