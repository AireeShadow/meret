import re
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Set, Optional

from meret.core import tagpaths
from meret.core.logger import Logger
from meret.core.models import FileEntry, FileType, Tag, TagNode
from meret.core.repository import AbstractRepository

SCHEMA_PATH = Path(__file__).parent / "schema.sql"
_TAG_SEP = "\x1f"  # разделитель для GROUP_CONCAT
_WORD = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")


def _iter_statements(script: str):
    """Режет SQL-скрипт на отдельные операторы.

    Уважает строковые литералы, комментарии и BEGIN...END в триггерах.
    Важно: execute() на склейке молча выполняет только первый оператор,
    поэтому резать надо строго по одному.
    """
    buf: list[str] = []
    quote: Optional[str] = None
    depth = 0                      # вложенность BEGIN...END
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
                if i + 1 < n and script[i + 1] == quote:   # '' — экранирование
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


class SQLiteRepository(AbstractRepository, Logger):
    def __init__(self, db_path: str):
        Logger.__init__(self)
        self.db_path = str(db_path)
        self.fts_enabled = True
        self._init_tables()
        self._SAVE_FILE_SQL = """
            INSERT INTO files (filepath, filename, rating, type, metadata)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(filepath) DO UPDATE SET
                filename = excluded.filename,
                rating = excluded.rating,
                type = excluded.type,
                metadata = excluded.metadata,
                updated_at = CURRENT_TIMESTAMP
        """
        self._EXPECTED_TABLES = ("files", "tags", "file_tags")

    # ---------- соединение ----------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")   # меньше блокировок
        conn.execute("PRAGMA synchronous = NORMAL")  # fsync не на каждый commit
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
            self._migrate_tags(conn)
            conn.commit()
            self._check_schema(conn)

    def _check_schema(self, conn) -> None:
        """Схема обязана создаться целиком.

        execute() на склеенных операторах молча выполняет только первый,
        поэтому проверяем результат, а не надеемся.
        """
        have = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        missing = [t for t in self._EXPECTED_TABLES if t not in have]
        if missing:
            raise RuntimeError(
                f"схема создана не полностью, нет таблиц: {', '.join(missing)}. "
                "Похоже, в schema.sql у оператора потерялась ';'"
            )
        

    def _migrate_tags(self, conn) -> None:
        """Доводит старую таблицу тегов до текущей схемы.

        1. Добавляет name_key и заполняет (COLLATE NOCASE сворачивает только
           ASCII, регистронезависимость делаем сами).
        2. Снимает глобальную уникальность name_key.
        3. Ставит уникальность среди братьев. COALESCE(parent_id, 0) нужен
           потому, что NULL'ы в уникальном индексе SQLite считаются разными,
           и два корневых «детектив» спокойно бы ужились.
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
            self.logger.warning(f"тёзки среди братьев, уникальный индекс не создан: {e}")

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
                self._SAVE_FILE_SQL,
                (
                    file.filepath,
                    file.filename,
                    file.rating,
                    file.type.value,
                    json.dumps(file.metadata, ensure_ascii=False) if file.metadata else None,
                ),
            )
            conn.commit()
            if cur.lastrowid:
                return cur.lastrowid
            row = conn.execute(
                "SELECT id FROM files WHERE filepath = ?", (file.filepath,)
            ).fetchone()
            return row["id"]

    def save_files(
        self,
        files: List[FileEntry],
        chunk_size: int = 1000,
        on_progress=None,
    ) -> Dict[str, int]:
        """Пачка файлов: одно соединение, транзакция на чанк, executemany."""
        stats = {"indexed": 0, "updated": 0, "errors": 0}
        if not files:
            return stats

        total = len(files)
        with self._connect() as conn:
            known = {row[0] for row in conn.execute("SELECT filepath FROM files")}

            for start in range(0, total, chunk_size):
                part = files[start:start + chunk_size]
                rows = []
                for entry in part:
                    try:
                        meta = (
                            json.dumps(entry.metadata, ensure_ascii=False)
                            if entry.metadata else None
                        )
                    except (TypeError, ValueError) as e:
                        stats["errors"] += 1
                        meta = None
                        self.logger.error(f"metadata не сериализуется: {entry.filepath}: {e}")
                    rows.append(
                        (entry.filepath, entry.filename, entry.rating, entry.type.value, meta)
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

    def delete_file(self, file_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM files WHERE id = ?", (file_id,))
            conn.commit()
            return cur.rowcount > 0

    # ---------- теги: поиск ----------

    _TAG_COLS = "id, name, name_key, colour, parent_id"

    _FIND_CHILD_SQL = f"""
        SELECT {_TAG_COLS} FROM tags
        WHERE COALESCE(parent_id, 0) = COALESCE(?, 0) AND name_key = ?
    """

    def _child_row(self, conn, parent_id: Optional[int], chunk: str):
        return conn.execute(self._FIND_CHILD_SQL, (parent_id, chunk.casefold())).fetchone()

    def _tag_strict(self, conn, ref: str):
        """Тег строго по пути от корня. None — если хоть одно звено не найдено."""
        chunks = tagpaths.parts(ref)
        if not chunks:
            return None
        row = None
        parent_id: Optional[int] = None
        for chunk in chunks:
            row = self._child_row(conn, parent_id, chunk)
            if row is None:
                return None
            parent_id = row["id"]
        return row

    def _tag_row(self, conn, ref: str):
        """Тег по пути («жанр/детектив») или по одиночному имени.

        Путь строгий. Одиночное имя ищется сначала как корневой тег, потом
        вообще: тёзки в разных ветках теперь законны, но выбор детерминирован
        (корневой, затем минимальный id).
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

    def _tag_id(self, conn, ref: str) -> Optional[int]:
        row = self._tag_row(conn, ref)
        return row["id"] if row else None

    def _ensure_tag(
        self, conn, ref: str, colour: Optional[str] = None, reuse_any: bool = True
    ) -> int:
        """Возвращает id тега, создавая его и недостающих родителей по пути.

        reuse_any=True: одиночное имя сначала ищется где угодно — тёзок не
        размножаем. reuse_any=False (для create_tag): путь строгий.
        """
        chunks = tagpaths.parts(ref)
        if not chunks:
            raise ValueError(f"пустой путь тега: {ref!r}")

        if reuse_any and len(chunks) == 1:
            row = self._tag_row(conn, chunks[0])
            if row is not None:
                return row["id"]

        parent_id: Optional[int] = None
        tag_id: Optional[int] = None
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
        """True, если parent_id — сам tag_id или его потомок.

        Идём вверх ОТ РОДИТЕЛЯ и ищем тег: если наткнулись — родитель лежит
        внутри поддерева тега, и перевешивание даст петлю.
        """
        seen: set[int] = set()
        cur: Optional[int] = parent_id
        while cur is not None and cur not in seen:
            if cur == tag_id:
                return True
            seen.add(cur)
            row = conn.execute(
                "SELECT parent_id FROM tags WHERE id = ?", (cur,)
            ).fetchone()
            cur = row["parent_id"] if row else None
        return False

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
            tag_id = self._tag_id(conn, tag)
            if tag_id is None:
                return False
            cur = conn.execute(
                "DELETE FROM file_tags WHERE file_id = ? AND tag_id = ?",
                (file_id, tag_id),
            )
            conn.commit()
            return cur.rowcount > 0

    def set_file_tags(self, file_id: int, tags: List[str]) -> bool:
        with self._connect() as conn:
            conn.execute("DELETE FROM file_tags WHERE file_id = ?", (file_id,))
            for name in dict.fromkeys(tags):  # без дублей, с сохранением порядка
                tag_id = self._ensure_tag(conn, name)
                conn.execute(
                    "INSERT OR IGNORE INTO file_tags (file_id, tag_id) VALUES (?, ?)",
                    (file_id, tag_id),
                )
            conn.commit()
            return True

    def rename_tag(self, old: str, new: str) -> bool:
        """Переименование = ОДНА строка. Если брат с таким именем есть — слияние."""
        with self._connect() as conn:
            src = self._tag_row(conn, old)
            if src is None:
                return False

            chunks = tagpaths.parts(new)
            if not chunks:
                return False
            new_name = chunks[-1]

            dst = self._child_row(conn, src["parent_id"], new_name)
            if dst is not None and dst["id"] != src["id"]:
                # дети переезжают к родителю src, ссылки уходят на dst
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

    # ---------- теги: создание и иерархия ----------

    def create_tag(self, name: str, colour: Optional[str] = None) -> bool:
        """Создать тег по пути, достроив недостающих родителей.

        Путь строгий: create_tag("a") создаст корневой «a», даже если где-то
        есть вложенный тёзка. False — если тег по этому пути уже есть.
        """
        with self._connect() as conn:
            if self._tag_strict(conn, name) is not None:
                return False
            self._ensure_tag(conn, name, colour, reuse_any=False)
            conn.commit()
            return True

    def delete_tag(self, name: str) -> bool:
        """Удаляет тег: дети поднимаются на уровень его родителя, ссылки уходят каскадом."""
        with self._connect() as conn:
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

    def set_tag_parent(self, tag: str, parent: Optional[str]) -> bool:
        with self._connect() as conn:
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
                    return False          # был бы цикл

            conn.execute(
                "UPDATE tags SET parent_id = ? WHERE id = ?", (parent_id, row["id"])
            )
            conn.commit()
            return True

    # ---------- теги: чтение ----------
    def get_all_tags(self) -> List[Tag]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, name, colour, parent_id FROM tags ORDER BY name_key"
            ).fetchall()
            return [Tag(**dict(r)) for r in rows]

    def get_tag_usage(self, limit: int = 100) -> List[Tag]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT t.id, t.name, t.colour, t.parent_id,
                          COUNT(ft.file_id) AS usage_count
                   FROM tags t LEFT JOIN file_tags ft ON ft.tag_id = t.id
                   GROUP BY t.id ORDER BY usage_count DESC, t.name_key LIMIT ?""",
                (limit,),
            ).fetchall()
            return [Tag(**dict(r)) for r in rows]

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

    def known_paths(self) -> Set[str]:
        with self._connect() as conn:
            return {row[0] for row in conn.execute("SELECT filepath FROM files")}