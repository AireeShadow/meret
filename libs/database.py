import json
import sqlite3 as sql
import os
import shutil
from typing import List, Tuple, Dict, Any, Optional
from contextlib import contextmanager
from libs.logger import Logger


class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass


class InitializeDb(Logger):
    """
    Creates database and tables, backups DB, deletes DB
    """
    def __init__(self, db_location: str) -> None:
        super().__init__()
        self.db_location = os.path.join(db_location, "meret.db")
        if not isinstance(self.db_location, str):
            self.logger.critical(f'DB location should be str type, not {type(self.db_location)}!')
            raise ValueError("db_location must be a string")

    @staticmethod
    def _table_list() -> List[str]:
        """
        Returns: a list of SQL statements as strings
        """
        # Основная таблица файлов
        files_table = '''
        CREATE TABLE IF NOT EXISTS Files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE NOT NULL,
            filepath TEXT NOT NULL,
            labels TEXT,  -- JSON array
            rating INTEGER DEFAULT 0,
            type TEXT,
            label_group TEXT,
            label_subgroup TEXT,
            metadata TEXT,  -- JSON
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        '''
        
        # Таблица групп тегов
        groups_table = '''
        CREATE TABLE IF NOT EXISTS Groups (
            label_group TEXT PRIMARY KEY,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        '''
        
        # Таблица подгрупп
        subgroups_table = '''
        CREATE TABLE IF NOT EXISTS Subgroups (
            label_subgroup TEXT PRIMARY KEY,
            label_group TEXT,
            labels TEXT,  -- JSON array
            colour TEXT,
            FOREIGN KEY (label_group) REFERENCES Groups(label_group)
        );
        '''
        
        # Таблица тегов с обратными ссылками на файлы
        labels_table = '''
        CREATE TABLE IF NOT EXISTS Labels (
            label TEXT PRIMARY KEY,
            description TEXT,
            usage_count INTEGER DEFAULT 0
        );
        '''
        
        # Связующая таблица many-to-many
        file_labels_table = '''
        CREATE TABLE IF NOT EXISTS FileLabels (
            file_id INTEGER,
            label TEXT,
            FOREIGN KEY (file_id) REFERENCES Files(id),
            FOREIGN KEY (label) REFERENCES Labels(label),
            PRIMARY KEY (file_id, label)
        );
        '''
        
        # Индексы для быстрого поиска
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_files_type ON Files(type);',
            'CREATE INDEX IF NOT EXISTS idx_files_rating ON Files(rating);',
            'CREATE INDEX IF NOT EXISTS idx_files_group ON Files(label_group);',
            'CREATE INDEX IF NOT EXISTS idx_files_filepath ON Files(filepath);',
        ]
        
        return [files_table, groups_table, subgroups_table, labels_table, file_labels_table] + indexes

    def create_tables(self) -> bool:
        """Creates initial tables"""
        if not os.path.exists(self.db_location):
            self.logger.info(f'Creating database "meret.db" in {self.db_location}')
            try:
                with sql.connect(self.db_location) as conn:
                    cursor = conn.cursor()
                    for table_sql in self._table_list():
                        cursor.execute(table_sql)
                    conn.commit()
                return True
            except sql.Error as e:
                self.logger.error(f"Failed to create database: {e}")
                return False
        else:
            self.logger.warning(f'Database already exists at {self.db_location}')
            # Проверим и создадим недостающие таблицы
            with sql.connect(self.db_location) as conn:
                cursor = conn.cursor()
                for table_sql in self._table_list():
                    try:
                        cursor.execute(table_sql)
                    except sql.OperationalError:
                        pass  # Таблица/индекс уже существует
                conn.commit()
            return True

    def backup_db(self) -> bool:
        """Creates a backup for DB file"""
        self.logger.info('Creating DB backup...')
        try:
            shutil.copyfile(self.db_location, f'{self.db_location}.bak')
            self.logger.info('Backup completed!')
            return True
        except Exception as ex:
            self.logger.warning(f'Backup failed: {ex}')
            return False

    def restore_db(self) -> bool:
        """Restores DB from backup"""
        self.logger.info('Restoring DB from backup...')
        backup_path = f'{self.db_location}.bak'
        if not os.path.exists(backup_path):
            self.logger.error('No backup file found!')
            return False
        try:
            shutil.copyfile(backup_path, self.db_location)
            self.logger.info('Restore completed!')
            return True
        except Exception as ex:
            self.logger.warning(f'Restore failed: {ex}')
            return False

    def delete_db(self) -> bool:
        """Permanently deletes the DB"""
        if os.path.exists(self.db_location):
            self.logger.info('Deleting DB...')
            try:
                os.remove(self.db_location)
                self.logger.info('Deletion completed!')
                return True
            except Exception as ex:
                self.logger.warning(f'Deletion failed: {ex}')
                return False
        else:
            self.logger.warning('DB not found, nothing to delete')
            return False


class CommonDbOperations(Logger):
    def __init__(self, db_location: str) -> None:
        super().__init__()
        self.db_location = os.path.join(db_location, "meret.db")
        self._local = threading.local()
        
    @contextmanager
    def _get_connection(self):
        """Context manager for database connections"""
        if not hasattr(self._local, 'connection'):
            self._local.connection = sql.connect(self.db_location)
            self._local.connection.row_factory = sql.Row
        try:
            yield self._local.connection
        except Exception as e:
            self._local.connection.rollback()
            raise DatabaseError(f"Database error: {e}")
    
    def _execute_query(
        self, 
        query: str, 
        params: tuple = (),
        fetch: bool = False
    ) -> Optional[List[Tuple]]:
        """Execute query with parameterized values"""
        self.logger.debug(f'Executing: {query[:50]}... with params: {params}')
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params)
                if fetch:
                    result = cursor.fetchall()
                else:
                    conn.commit()
                    result = True
                return result
            except sql.Error as e:
                self.logger.error(f"Query failed: {e}")
                conn.rollback()
                raise DatabaseError(f"Query failed: {e}")

    def insert(self, table: str, data: Dict[str, Any]) -> Optional[int]:
        """
        Insert data with proper parameterization.
        Returns: lastrowid or None
        """
        # Сериализуем JSON-поля
        json_fields = {'labels', 'metadata'}
        processed_data = {}
        for key, value in data.items():
            if key in json_fields and value is not None:
                processed_data[key] = json.dumps(value)
            else:
                processed_data[key] = value
        
        columns = ', '.join(processed_data.keys())
        placeholders = ', '.join(['?' for _ in processed_data])
        values = tuple(processed_data.values())
        
        query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, values)
                conn.commit()
                return cursor.lastrowid
        except sql.IntegrityError as e:
            self.logger.debug(f'Row already exists: {e}')
            return None
        except sql.Error as e:
            self.logger.error(f'Insert failed: {e}')
            raise DatabaseError(f"Insert failed: {e}")

    def insert_or_update(self, table: str, data: Dict[str, Any], unique_key: str) -> bool:
        """
        Insert or update (UPSERT) for SQLite 3.24+
        """
        json_fields = {'labels', 'metadata'}
        processed_data = {}
        for key, value in data.items():
            if key in json_fields and value is not None:
                processed_data[key] = json.dumps(value)
            else:
                processed_data[key] = value
        
        columns = ', '.join(processed_data.keys())
        placeholders = ', '.join(['?' for _ in processed_data])
        updates = ', '.join([f"{col} = excluded.{col}" for col in processed_data.keys()])
        
        query = f'''
            INSERT INTO {table} ({columns}) 
            VALUES ({placeholders})
            ON CONFLICT({unique_key}) 
            DO UPDATE SET {updates}
        '''
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, tuple(processed_data.values()))
                conn.commit()
                return True
        except sql.Error as e:
            self.logger.error(f'Upsert failed: {e}')
            return False

    def select(
        self,
        table: str,
        columns: str = '*',
        condition: Optional[str] = None,
        params: tuple = (),
        limit: Optional[int] = None
    ) -> List[Dict]:
        """Select with proper parameterization"""
        query = f'SELECT {columns} FROM {table}'
        if condition:
            query += f' WHERE {condition}'
        if limit:
            query += f' LIMIT {limit}'
        query += ';'
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            # Конвертируем в dict
            return [dict(row) for row in rows]

    def update(
        self,
        table: str,
        data: Dict[str, Any],
        condition: str,
        condition_params: tuple = ()
    ) -> int:
        """
        Update rows. Returns number of affected rows.
        """
        json_fields = {'labels', 'metadata'}
        processed_data = {}
        for key, value in data.items():
            if key in json_fields and value is not None:
                processed_data[key] = json.dumps(value)
            else:
                processed_data[key] = value
        
        set_clause = ', '.join([f"{col} = ?" for col in processed_data.keys()])
        values = tuple(processed_data.values()) + condition_params
        
        query = f'UPDATE {table} SET {set_clause} WHERE {condition};'
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, values)
            conn.commit()
            return cursor.rowcount

    def delete(self, table: str, condition: str, params: tuple = ()) -> int:
        """Delete rows. Returns number of deleted rows."""
        query = f'DELETE FROM {table} WHERE {condition};'
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount

    def close(self):
        """Close connection if exists"""
        if hasattr(self._local, 'connection'):
            self._local.connection.close()
            del self._local.connection