"""Keep the README version badge in step with meret/__init__.py.

The version has exactly one source: `meret/__init__.py`, which hatch reads for the
build. The README badge spells the same number out a second time, and it drifts
silently - nobody looks at a badge until a release page disagrees with the package.

    python scripts/sync_version.py          # rewrite the badge
    python scripts/sync_version.py --check  # write nothing, exit 1 on a drift

--check also wants a changelog section for the current version: a badge can be
rewritten for you, a heading with a date and prose cannot. Both hooks in
.pre-commit-config.yaml call this script, which is why it needs nothing but the
stdlib - a pytest-based hook would want the whole test dependency set in its own
virtualenv.
"""

import argparse
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
INIT = REPO / "meret" / "__init__.py"
README = REPO / "README.md"
CHANGELOG = REPO / "CHANGELOG.md"

#: The badge URL, split so the number can be replaced without touching the rest of
#: the line. Anchored on `badge/version-` so another shields.io URL is left alone.
BADGE = re.compile(r"(badge/version-)(?P<version>[^)\s]+?)(-blue\))")

VERSION_LINE = re.compile(r'^__version__\s*=\s*"(?P<version>[^"]+)"', re.M)


def version() -> str:
    """`__version__` from meret/__init__.py, without importing the package."""
    found = VERSION_LINE.search(INIT.read_text(encoding="utf-8"))
    if found is None:
        raise SystemExit(f"no __version__ in {INIT}")
    return found.group("version")


def badge_version() -> str | None:
    """What the README badge says right now. None when there is no badge."""
    found = BADGE.search(README.read_text(encoding="utf-8"))
    return found.group("version") if found else None


def announces(wanted: str, changelog: str) -> bool:
    """True when a `## [x.y.z]` heading announces `wanted`.

    The brackets are escaped in the file (`## \\[0.7.32\\]`), so the line is asked for
    the version instead of being matched against a pattern that would have to spell
    the escaping out. A version-log line (`- 0.7.32 - ...`) is not a heading and must
    not count.
    """
    return any(line.startswith("## ") and wanted in line for line in changelog.splitlines())


def problems() -> list[str]:
    """Everything that disagrees with the version. Empty when it is all in step."""
    current = version()
    found: list[str] = []

    badge = badge_version()
    if badge is None:
        found.append("README.md has no version badge")
    elif badge != current:
        found.append(f"README.md badge says {badge}, meret/__init__.py says {current}")

    if not announces(current, CHANGELOG.read_text(encoding="utf-8")):
        found.append(f"CHANGELOG.md has no `## [{current}]` section")

    return found


def sync() -> bool:
    """Rewrite the badge to the current version. False when it was already right."""
    current = version()
    text = README.read_text(encoding="utf-8")
    replaced, count = BADGE.subn(rf"\g<1>{current}\g<3>", text)
    if not count or replaced == text:
        return False
    README.write_text(replaced, encoding="utf-8")
    return True


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="write nothing, exit 1 on a drift")
    args = parser.parse_args(argv)

    if not args.check:
        print(f"README badge set to {version()}" if sync() else "README badge already in step")
        return 0

    found = problems()
    if not found:
        print(f"README badge and CHANGELOG.md match meret/__init__.py ({version()})")
        return 0
    for problem in found:
        print(f"! {problem}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
