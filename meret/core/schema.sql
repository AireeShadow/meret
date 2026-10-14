-- meret/core/schema.sql

PRAGMA foreign_keys = ON;

-- Файлы. Метаданные — JSON-блоб (их не ищем), теги — отдельно.
CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    filename    TEXT NOT NULL,
    filepath    TEXT UNIQUE NOT NULL,
    rating      INTEGER DEFAULT 0,
    type        TEXT DEFAULT 'unknown',
    metadata    TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_files_type ON files(type);
CREATE INDEX IF NOT EXISTS idx_files_rating ON files(rating);

-- Теги. parent_id даёт иерархию → "папки" из шага 3.
-- name_key = name.casefold(): COLLATE NOCASE в SQLite сворачивает только ASCII,
-- а теги у нас кириллицей. Поиск и уникальность — всегда по name_key.
CREATE TABLE IF NOT EXISTS tags (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    name_key    TEXT NOT NULL,
    colour      TEXT,
    parent_id   INTEGER REFERENCES tags(id) ON DELETE SET NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tags_name_key ON tags(name_key);
CREATE INDEX IF NOT EXISTS idx_tags_parent ON tags(parent_id);

-- Связь многие-ко-многим.
CREATE TABLE IF NOT EXISTS file_tags (
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    tag_id  INTEGER NOT NULL REFERENCES tags(id)  ON DELETE CASCADE,
    PRIMARY KEY (file_id, tag_id)
);
CREATE INDEX IF NOT EXISTS idx_file_tags_tag ON file_tags(tag_id);

-- Полнотекстовый поиск по именам (внешний контент = таблица files).
CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
    filename, filepath,
    content='files', content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
    INSERT INTO files_fts(rowid, filename, filepath)
    VALUES (new.id, new.filename, new.filepath);
END;
CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
    INSERT INTO files_fts(files_fts, rowid, filename, filepath)
    VALUES ('delete', old.id, old.filename, old.filepath);
END;
CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files BEGIN
    INSERT INTO files_fts(files_fts, rowid, filename, filepath)
    VALUES ('delete', old.id, old.filename, old.filepath);
    INSERT INTO files_fts(rowid, filename, filepath)
    VALUES (new.id, new.filename, new.filepath);
END;