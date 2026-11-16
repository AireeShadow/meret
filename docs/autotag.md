# Auto-tagging

A rule is a tag path plus conditions over a file's **already indexed** metadata. No
disk is touched, no EXIF is re-read: everything comes from `files.metadata`, which is
why a run over a large library is a database walk and not a scan.

## Why rules are a file, not the config

Rules are a list of lists. The settings dialog shows exactly one widget per config
field — a checkbox, a spin box, a drop-down or a path — and a rules table is not
something a form row can hold. `test_every_config_field_has_a_widget` is what settled
that argument: a field without a widget is a setting the dialog silently cannot change.

So the config holds the policy (`enabled`, `only_untagged`, `root`, `rules_file`) and
the file holds the rules. Nine rules ship with meret, used **only** when nothing is
configured: a configured file that is missing stays missing, with a warning. Falling
back to a default policy would tag a library nobody asked it to tag.

## Facts

`build_facts()` flattens the metadata into dotted keys and adds what can be derived:

| Fact | Where it comes from |
|------|---------------------|
| `exif.Make`, `tags.artist`, `creation_time` | nested dicts in the metadata |
| `type`, `filename`, `stem`, `extension` | the entry itself |
| `orientation`, `aspect`, `megapixels` | width and height |
| `duration_s`, `duration_min` | duration |
| `year` | EXIF date, then `creation_time`, then the file mtime |

Lists (`streams`) are dropped: a condition over a list has no single answer. Strings
are stripped, so a padded `"Canon "` becomes `camera/Canon` and not `camera/Canon `.

## Conditions

`field`, `op`, `value`, and `ci` (case folding, on by default — tag names are often
Cyrillic, so it is `casefold()`, not `lower()`).

| Operators | Note |
|-----------|------|
| `eq ne` | case-folded strings |
| `in not_in` | membership in a list |
| `contains startswith endswith` | substring and prefix/suffix |
| `regex` | `re.search` with `IGNORECASE`; a bad pattern logs and never matches |
| `gt gte lt lte` | floats; a fact that is not a number never matches |
| `exists missing` | presence, not value |

`match = "all"` (default) or `"any"` per rule, `colour` is optional, and `enabled` lets
a rule be parked without deleting it. Operators are a dispatch table in code, one row
each, so adding one is a row and not another `if`.

## Templates and the root

`{field}` in a tag is filled from the facts. **A rule whose placeholder has no value is
skipped** — `camera/{exif.Make}` never produces a bare `camera/`, which would then be
cleaned up by `tag-prune` and blamed on the user.

`autotag.root` (default `metadata`) is the parent every produced tag is filed under, so
auto-tags never mix with hand-made ones at the top of the tree. An empty root files them
bare, and a rule that already spells the root out is not prefixed twice — both spellings
of the same rule agree.

## Running

```bash
meret autotag --dry-run --limit 50    # what would change, nothing written
meret autotag                         # the whole library
meret autotag --rules ~/Media/rules.toml --root library --only-untagged
```

`AutoTagger.run()` walks `repo.iter_files()`, plans the tags for each entry and writes
them. Three things in it are load-bearing:

- **The total is counted up front** (`repo.count_files()`, or `len(paths)` when a scan
  narrows the walk), because a progress bar without a total stays in its busy state and
  reads as "nothing is happening" while the run works.
- **`should_stop()` is asked before each file**, so a cancel lands on a file boundary
  and every processed file is fully tagged. The result reports `interrupted`.
- **Writes go through `repo.add_tags()`**, one transaction per file. `add_tag()` opens a
  connection and commits per call — right for a GUI where each call arrives on a
  different thread, catastrophic for a batch: a 10 000-file library with three tags each
  was 30 000 connections and 30 000 commits, each waiting on fsync.

`plan()` skips tags the file already has, comparing the **final** path with the root
included — otherwise `--root` would change what gets written while the "already tagged"
check looked at a different name, and a second run would add duplicates.

## Front-ends

The CLI runs it directly, the TUI shells out to `meret autotag` (same policy, one
place), and the window runs it on the task pool with a progress dialog and a Cancel
button. All three read the same config and the same rules file.
