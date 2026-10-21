"""scripts/prune_msgids.py

Delete catalog entries that no _() / ngettext() / translate() call asks for.

The used-key set comes from the same AST walk the tests use, so the script and
tests/test_locale_catalog.py cannot drift apart. Dry run by default:

    python scripts/prune_msgids.py            # list what would go
    python scripts/prune_msgids.py --write    # rewrite the catalogs
"""

import argparse
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from meret.core.locale import DEFAULT_LOCALE_DIR, _parse_po  # noqa: E402

MERET = ROOT / "meret"


def used_keys() -> set:
    """Keys with a literal call site, plus the header."""
    found = {""}
    for py in MERET.rglob("*.py"):
        for node in ast.walk(ast.parse(py.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call):
                continue
            name = (
                node.func.id if isinstance(node.func, ast.Name)
                else getattr(node.func, "attr", "")
            )
            args = [a.value for a in node.args if isinstance(a, ast.Constant)]
            context = next(
                (k.value.value for k in node.keywords
                 if k.arg == "context" and isinstance(k.value, ast.Constant)),
                None,
            )
            if name in ("_", "translate") and args and isinstance(args[0], str):
                found.add(f"{context}\x04{args[0]}" if context else args[0])
            elif name in ("ngettext", "translate_plural") and len(args) >= 2:
                found.add(f"{args[0]}\0{args[1]}")
    return found


def key_of(block: str) -> str | None:
    """The key a .po block defines — parsed, so continuations and msgctxt work.

    None when the block is skipped anyway (untranslated, fuzzy, no msgid).
    """
    return next(iter(_parse_po(block + "\n")), None)


def prune(path: Path, keys: set, write: bool) -> list:
    """Drop whole entries whose key nothing uses, and comments left over them."""
    blocks = path.read_text(encoding="utf-8").split("\n\n")
    kept, dropped = [], []
    for block in blocks:
        key = key_of(block)
        if key is not None and key not in keys:
            dropped.append(key)
            if kept and key_of(kept[-1]) is None:
                kept.pop()  # the comment that introduced this entry
            continue
        kept.append(block)
    if write and dropped:
        path.write_text("\n\n".join(kept), encoding="utf-8")
    return dropped


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="rewrite the catalogs")
    args = parser.parse_args()

    keys, total = used_keys(), 0
    for po in sorted(DEFAULT_LOCALE_DIR.glob("*/LC_MESSAGES/*.po")):
        dropped = prune(po, keys, args.write)
        total += len(dropped)
        print(f"{po.relative_to(ROOT)}: {len(dropped)} unused")
        for key in dropped:
            print(f"    {key!r}")
    print(f"{'removed' if args.write else 'would remove'}: {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())