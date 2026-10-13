import json
import sqlite3
from typing import List, Optional
from meret.core.models import FileEntry, Tag, FileType
from meret.core.repository import AbstractRepository


class SQLiteRepository(AbstractRepository):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_tables()
    
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _init_tables(self):
        """Создаём таблицы, если их нет"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Таблица файлов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    filepath TEXT UNIQUE NOT NULL,
                    labels TEXT DEFAULT '[]',
                    rating INTEGER DEFAULT 0,
                    type TEXT DEFAULT 'unknown',
                    label_group TEXT,
                    label_subgroup TEXT,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Индексы
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_filepath ON files(filepath)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_type ON files(type)')
            
            conn.commit()
    
    def _row_to_file(self, row: sqlite3.Row) -> FileEntry:
        """Конвертируем строку БД в модель"""
        return FileEntry(
            id=row['id'],
            filename=row['filename'],
            filepath=row['filepath'],
            labels=json.loads(row['labels']) if row['labels'] else [],
            rating=row['rating'],
            type=FileType(row['type']),
            label_group=row['label_group'],
            label_subgroup=row['label_subgroup'],
            metadata=json.loads(row['metadata']) if row['metadata'] else None,
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )
    
    def get_file_by_id(self, file_id: int) -> Optional[FileEntry]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM files WHERE id = ?", (file_id,))
            row = cursor.fetchone()
            return self._row_to_file(row) if row else None
    
    def get_file_by_path(self, path: str) -> Optional[FileEntry]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM files WHERE filepath = ?", (path,))
            row = cursor.fetchone()
            return self._row_to_file(row) if row else None
    
    def save_file(self, file: FileEntry) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # UPSERT через INSERT OR REPLACE или UPDATE
            cursor.execute('''
                INSERT INTO files 
                (filepath, filename, labels, rating, type, label_group, label_subgroup, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(filepath) DO UPDATE SET
                    labels = excluded.labels,
                    rating = excluded.rating,
                    label_group = excluded.label_group,
                    label_subgroup = excluded.label_subgroup,
                    metadata = excluded.metadata,
                    updated_at = CURRENT_TIMESTAMP
            ''', (
                file.filepath,
                file.filename,
                json.dumps(file.labels),
                file.rating,
                file.type.value,
                file.label_group,
                file.label_subgroup,
                json.dumps(file.metadata) if file.metadata else None
            ))
            
            conn.commit()
            return cursor.lastrowid or file.id or 0
    
    def search_by_tags(
        self, 
        tags: List[str], 
        match_all: bool = False,
        file_type: Optional[str] = None
    ) -> List[FileEntry]:
        """Поиск по тегам"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Простой поиск через LIKE (для начала)
            # Потом можно сделать через JSON_EACH если SQLite поддерживает
            conditions = []
            params = []
            
            for tag in tags:
                conditions.append("labels LIKE ?")
                params.append(f'%"{tag}"%')
            
            where_clause = " AND ".join(conditions) if match_all else " OR ".join(conditions)
            
            if file_type:
                where_clause = f"({where_clause}) AND type = ?"
                params.append(file_type)
            
            cursor.execute(f"SELECT * FROM files WHERE {where_clause}", params)
            rows = cursor.fetchall()
            
            return [self._row_to_file(row) for row in rows]
    
    def get_all_tags(self) -> List[Tag]:
        """Получить все уникальные теги"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT labels FROM files WHERE labels IS NOT NULL")
            
            all_tags: set = set()
            for row in cursor.fetchall():
                labels = json.loads(row['labels'])
                all_tags.update(labels)
            
            return [Tag(name=tag) for tag in sorted(all_tags)]
    
    def update_file_labels(self, file_id: int, labels: List[str]) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE files SET labels = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (json.dumps(labels), file_id)
            )
            conn.commit()
            return cursor.rowcount > 0
    
    def delete_file(self, file_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM files WHERE id = ?", (file_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    def get_stats(self) -> dict:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM files")
            total = cursor.fetchone()[0]
            
            cursor.execute("SELECT type, COUNT(*) FROM files GROUP BY type")
            by_type = dict(cursor.fetchall())
            
            return {
                "total_files": total,
                "by_type": by_type
            }