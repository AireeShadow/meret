# The core

`meret/core` is the part that would still work if the UI disappeared. It imports
neither `vfs` nor `cli`, and nothing from PySide6; `tests/vfs/test_architecture.py`
guards that.

## Models

`models.py` holds four types and no logic worth the name:

| Type | What it is |
|------|------------|
| `FileType` | `image`, `video`, `audio`, `unknown` — an enum, because the column stores a string and a typo must be impossible |
| `FileEntry` | one indexed file: `id`, `filename`, `filepath`, `labels`, `rating`, `type`, `metadata` |
| `Tag` | `id`, `name`, `colour`, `parent_id`, `usage_count` |
| `TagNode` | a `Tag` with `children`, for the tree |

`id` is `int | None` on purpose: a model is built before it is saved, and pretending
otherwise would mean two classes or a sentinel.

## Schema

`core/schema.sql`, applied by the repository on every open — the statements are
`CREATE ... IF NOT EXISTS`, so opening an old database is the migration.

- **`files.metadata` is a JSON blob.** It is written whole and read whole; nothing
  searches inside it. Anything that has to be searched or filtered lives in a column
  (`type`, `rating`) or in `file_tags`.
- **`size_bytes` and `mtime` are a fingerprint**, not metadata: the incremental scan
  compares them and re-indexes only what changed. They are duplicated out of the JSON
  because a scan needs them for every file without parsing every blob.
- **`tags.name_key = name.casefold()`.** `COLLATE NOCASE` in SQLite folds ASCII only,
  and tag names are often Cyrillic — so case-insensitivity is done in the application,
  and every lookup and uniqueness check uses `name_key`.
- **Uniqueness is among siblings**: the index is on `(COALESCE(parent_id, 0), name_key)`.
  `COALESCE` is required because SQLite treats NULLs in a unique index as distinct, so
  two root-level `mystery` tags would otherwise coexist. `genre/mystery` and
  `studio/mystery` are different tags by design.
- **`files_fts` is an external-content FTS5 table over `filename` and `filepath`**, kept
  in sync by three triggers. A build without FTS5 sets `fts_enabled = False` instead of
  failing, and `search_by_name` falls back to `LIKE`.

## The repository contract

`repository.py` is an ABC with no storage in it. Two things in it are worth knowing:

- **`iter_files()` is not abstract.** A paged generator with a default that raises
  `NotImplementedError` at the moment someone walks it. A storage that cannot enumerate
  its files — a test double, a backend that only answers lookups — must not become
  uninstantiable because of a feature it never uses. Making it abstract broke every mock
  in the suite once.
- **`count_files()` and `add_tags()` have working defaults** in terms of the existing
  methods, and exist to be overridden. The auto-tagger needs a total before it starts,
  and it writes a handful of tags per file over a whole library: the difference between
  one query and ten thousand is the difference between seconds and minutes.
- `data_version()` returns `None` by default: readers that cache compare it, and a
  backend that cannot tell falls back to a TTL.

## SQLite specifics

`sqlite_repo.py` opens **one connection per operation**, through a `_session()`
context manager that commits on success, rolls back on error and always closes.

- That is the right shape for a GUI where each call arrives on a different thread, and
  the wrong shape for a batch — hence `add_tags()`, which does one session and one
  commit per file.
- `PRAGMA journal_mode = WAL`, `synchronous = NORMAL`, `busy_timeout = 5000`. WAL means
  a reader does not block a writer; the timeout means a short lock is a wait, not an
  error.
- **Opening and wiping retry with a short sleep**, because a scan in flight can hold the
  file for a moment. They run on the pool, so the sleep is a wait and never a freeze.
- **Wiping drops tables instead of deleting the file.** A brand-new database is in
  `delete` journal mode, and switching that to WAL wants an exclusive lock that
  `PRAGMA journal_mode` does not wait for — that was the `database is locked`.
- **`_iter_statements()` exists** because `execute()` on a concatenated script silently
  runs only the first statement. It splits on `;` while respecting string literals,
  comments and `BEGIN ... END` in triggers, and `_check_schema()` verifies the result
  instead of trusting it.
- `_traced` logs every public call at DEBUG with its arguments, result size and
  duration. It is the first thing to read when something is slow.

## The indexer

`indexer.py` is the scan orchestrator and knows nothing about formats.

1. **collect** — walk the library, keep what the registry recognises. Fast, no metadata.
2. **extract** — a `ThreadPoolExecutor`, one entry per file. Pure: no database access,
   because a connection per thread would be worse than useless.
3. **index** — one transaction per `chunk_size` files, through `save_files()`.

- `--skip-existing` compares `(size, mtime)` against `known_fingerprints()`, one query
  for the whole scan. A file that cannot be `stat`ed is **not** counted as unchanged:
  `(None, None)` used to compare equal to `(None, None)` and the file was never
  indexed.
- **Ctrl+C and Cancel are the same path**: `self.interrupted`, set by
  `KeyboardInterrupt` or by `cancel()`. Extraction stops between futures and cancels the
  rest; writing stops between chunks, so a half-written transaction is never left
  behind. What was written stays written — a scan is not a transaction.

## Extractors

`extractors.py` is a registry keyed by extension. Each extractor answers
`can_extract()` and `extract()`, and returns a dict that becomes `files.metadata`.

- **A failure is data, not an exception**: a file that cannot be read is still indexed,
  with `{"error": "..."}` in its metadata and a line in the log. A library with one
  corrupt video must not lose the other ten thousand files.
- Images are read with Pillow (size, mode, format, EXIF flattened to strings). Video and
  audio go through `ffprobe`; a missing `ffprobe` is reported **once**, not once per
  file, and the file still gets its base metadata.

## Configuration

`config.py` is pydantic models plus a manager for one TOML file.

- `model_config = {"extra": "allow"}` on the root: a config written by a newer meret
  must not be destroyed by an older one.
- `set()` converts `"true"`, `"-5"` and `"5.0"` before validating, and re-validates the
  whole model — a value that the model rejects is an error, not a silent default.
- `save()` falls back to JSON when `tomli-w` is missing, and `load()` reads that sibling
  back. Otherwise a save would be lost on the next start.
- **Validators are where a value is normalised**, not at the call sites:
  `autotag.root` is stripped of separators once, in the model, so `/metadata/`,
  `metadata` and ` metadata ` are one spelling everywhere.

## Debug flags

`debug.py` is a registry of names (`watchdog`, `watch_fs`, `settings_modeless`,
`media_log`, `no_tooltips`), and `enabled(name)` answers from the CLI flags with the
environment as a fallback.

The point is not tidiness. `MERET_WATCHDOGG=1` was a silent no-op for as long as it
existed; `--debug watchdogg` is a `BadParameter` listing the real names. A diagnostic
that can be enabled silently and disabled silently is the worst kind.

## Locale and logging

`locale.py` parses `meret/locales/<lang>/LC_MESSAGES/messages.po` directly — no compile
step, so a translation is a reviewable diff. `_()` looks the string up and returns it
unchanged when there is no translation.

`logger.py` writes one rotated file, keeps crashes with a full traceback, and turns
console logging off for the TUI and the GUI — a window has no console, and the log is
the only place left.
