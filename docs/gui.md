# The Qt window

Why `meret/gui` looks the way it does. Almost every shape here was paid for with a
freeze or a lost drive letter, so read this before changing any of it.

## Boundaries

- **Qt lives only in `meret/gui`.** `core`, `vfs` and `cli` stay importable without
  PySide6; `tests/gui/test_architecture.py` guards it.
- **The GUI never imports a mount backend.** Mounting shells out to `meret mount` /
  `meret umount`, so opening a window does not drag FUSE or WinFsp in.
- **Nothing touches SQLite or decodes an image on the GUI thread.** Both go through
  `TaskRunner`.

## Threads: TaskRunner

`meret/gui/workers.py` runs a callable in a `QThreadPool` and delivers the result on
the GUI thread through a `QueuedConnection`.

- Every task carries a **generation**. A panel that re-asks (another tag, another
  folder) calls `bump()`, and results from an older generation are dropped instead of
  painting over fresher rows.
- **`sticky=True`** exempts a task from that: a version probe, a scan or a database
  switch must still report even though the user kept browsing while it ran.
- **Lifetime rules, learned the hard way.** `QRunnable` is not a `QObject` and the
  pool does not own it: module-level `_INFLIGHT` keeps every task alive from `start()`
  until its result is delivered, a task keeps its runner alive, and `clear()` releases
  dropped runnables itself — otherwise the safety net would hold them and their
  closures for the whole run.

## The tag tree

`TagTree` reads children only when a row is expanded, and off the GUI thread:
`listdir` can rebuild the whole repository snapshot, and doing that on a library with
thousands of tags freezes the window mid-click.

- A row that is being read shows `…`; a row that failed shows the error text. A row
  that looks empty is a bug.
- After a listing lands — or fails — the row is marked `FILLED`. That is what stops
  the next expand from queueing the same broken listing again: **F5 (`reload`) is the
  retry.**
- The tree keeps its own generation, so a listing still in flight when the tree is
  rebuilt never touches a row that is already gone.
- `set_tree(None)` is not a detail: the old tree holds the old repository, and a
  database file cannot be deleted while a connection to it is open.

## The real filesystem panel

`RealFsPanel` is a plain `QTreeView` over Qt's own `QFileSystemModel` — no database,
no pool thread. Three decisions in it are load-bearing:

- **The watcher is off** (`DontWatchForChanges`). `QFileSystemModel` watches every
  directory it has ever listed, and a change anywhere in one of them makes it re-read
  that directory on the GUI thread with a `stat` per entry — measured at **6.4
  seconds inside a single `QEvent`**. `DontResolveSymlinks` and
  `DontUseCustomDirectoryIcons` (an `SHGetFileInfo` call per entry on Windows) go with
  it. `MERET_WATCH_FS=1` restores the stock model for an A/B run.
- **The view is rooted at an invalid index, never at a folder.** That is the model's
  own root: the drive list on Windows, `/` elsewhere. `setRootPath("")` with a path it
  already has returns early, so it is called once; `refresh()` steps through
  `QDir.homePath()` and back to force a real re-read.
- **Navigation *reveals*, it does not re-root.** `reveal()` tries `index()` first (a
  known path is not listed again) and falls back to `setRootPath()`, then expands the
  ancestors top down and scrolls the row into view. Ancestors that are already open
  are skipped: expanding an open row repeats the whole `stat` cost, and `%TEMP%` alone
  is tens of thousands of entries.

## Switching the database

`_use_db()` puts the work on the pool and returns: opening and wiping both retry with
a `time.sleep` while a task in flight still holds the file — up to half a second of a
frozen window. `_switch_to()` runs in the callback and does the Qt part (repository,
tree, thumbnail scope, re-read); `_on_db_failed()` writes a status line and leaves the
old database in place. The task is sticky, so a switch is not eaten by browsing.

## The settings dialog

`_edit_config()` calls `open()`, never `exec()`: `exec()` runs a nested event loop and
the poll, the pool and the dialog end up fighting over the same window. The
`raise_()` / `activateWindow()` pair is **deferred by one turn** — on Windows the owner
can take the foreground back in the same turn, and the dialog ends up behind a window
that modality has already blocked, which reads as a dead app.
`MERET_SETTINGS_MODELESS=1` removes native modality for an A/B run.

While the dialog is up, `_paused` stops the thumbnail pump: the grid underneath is not
looked at, and every icon that lands repaints it. `finished()` resumes the pump.

## Thumbnails

Decoding happens in the pool, in batches of `BATCH`. The next batch is asked for by
the **last landing result**, not by a zero-timer — a zero-timer refills the queue
immediately, so the whole folder ends up in the pool and `BATCH` limits nothing. The
preview takes its own token per request, so a slow decode cannot overwrite a newer
selection.

## Diagnostics

All three are off by default.

| Variable | Effect |
|----------|--------|
| `MERET_WATCHDOG=1` | dumps every thread's Python stack every 3 s and runs `TracedApplication`: an event rate, the slowest event and its widget chain, the focus owner |
| `MERET_WATCH_FS=1` | keeps the filesystem watcher (stock `QFileSystemModel`) |
| `MERET_SETTINGS_MODELESS=1` | settings as a modeless window |

The watchdog report is printed from a **daemon thread**, so it keeps coming while the
GUI thread is the thing that is stuck. Read it as: `0 events/s` means the thread is
blocked outside event handling; thousands of events per second means a layout or paint
storm. The two need opposite fixes, and the rate tells them apart.

## Testing rules

- The suite runs headless (`QT_QPA_PLATFORM=offscreen`).
- Every modal dialog is stubbed: a dialog runs its own event loop, and nothing clicks
  in a headless run.
- `pump()` is a nested `QEventLoop` with a timer. `processEvents()` without a limit
  does not return while the event stream is alive, which is how the suite used to
  hang.
- Background threads must print to the file descriptor (`os.write(2, ...)`), never to
  `sys.stderr`: pytest replaces and closes it, and the thread then raises
  `OSError: [WinError 6]`.
- A test that waits for async work waits for the **observable effect** (file created,
  tags gone), not for a path that never changed.

## Traps already paid for

- **`in` on a `sqlite3.Row` tests values, not column names** (cpython#100450). `row`
  is not a dict and has no `get()`. `_tags_from_row` holds `row.keys()` in a local for
  exactly this reason — otherwise ruff's SIM118 "simplifies" it back and every file
  comes back untagged.
- `QFileSystemModel` populates on its own thread: `rowCount()` is 0 until the listing
  lands, so tests wait instead of asserting straight away.
- A `QTimer.singleShot` outlives the widget it was started from: callbacks touch
  `self` inside `try/except RuntimeError`.