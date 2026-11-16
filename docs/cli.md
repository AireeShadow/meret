# The CLI and the TUI

## The Typer app

`cli/app.py` is one `typer.Typer` with `cls=TranslatedGroup`, and every command is
registered in it. Two things there look odd and are not:

- **`--lang` is read from `sys.argv` while the module is being imported.**
  Typer freezes every `help=` string the moment the decorators run, which is at import
  time — long before the callback could set the locale. Without that, `meret --lang ru
  --help` prints English.
- **`TranslatedGroup` replaces `resolve_command`.** There is no `command_not_found`
  hook, and the message is built with the stdlib gettext rather than meret's catalogs,
  so the method itself is what gets replaced. A stray `app = typer.Typer(...)` later in
  the module would drop the class and silently un-translate the error;
  `tests/test_locale_catalog.py` guards that.

`--debug` and `--log-level` also go before the command name, because they are options
of the group, not of the subcommands.

## Which database

`cli/deps.py` resolves it once, in one order: `--db` (a directory with `meret.db`, or a
path to a `.db` file), then `$MERET_DB`, then `paths.default_db`, then `./meret.db`.
Only `meret init` creates one; every other command raises `DatabaseNotFound`, which the
commands turn into a sentence instead of a traceback.

## The TUI

`cli/tui.py` is a Textual app: the real filesystem on the left, the tag tree on the
right, a preview and a path bar below.

- **The bindings and the palette are the same list as the GUI's command table.**
  `tests/gui/test_commands.py` compares them, so a command added to one front-end and
  forgotten in the other is a red test — which is exactly what happened when
  `autotag` was added.
- **`VirtualFileTree` loads children lazily**, on expand, and shows `…` while it works.
  A library with thousands of tags must not be walked to draw one screen.
- **`SortedDirectoryTree` sorts through `DirectoryTree`'s own hooks** and adds a `..`
  row on top of a non-root listing. Everything stays in the widget's worker, so
  expanding and reloading keep working.
- **`DriveSelector` asks the Windows API** (`GetLogicalDriveStringsA`) instead of
  `QDir.drives()`-style enumeration: it returns names, and names cannot fault the way a
  volume query can.
- **Long commands shell out** (`scan`, `mount`, `autotag`) instead of importing the
  work: one process, one exit code, and the TUI stays responsive.

## Output

Rich is used for tables and trees, and the rule for translations is one line: keep
markup and values out of the message.

```python
msg = _("Database created: {path}").format(path=db_file)
console.print(f"[green]✓ {msg}[/green]")
```

A message with a `[bold]` inside it cannot be translated without translating the markup,
and a `{path}` that a translator renames is a `KeyError` at runtime —
`tests/test_locale_catalog.py` fails on both.

`python scripts/extract_strings.py` writes `meret/locales/messages.pot`, which is the
authoritative list of what the code asks for. Run it after adding a string, not after
the tests fail: it names the missing and the orphaned entries at once.
