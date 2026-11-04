"""The version is spelled out twice - `meret/__init__.py` and the README badge.

`meret/__init__.py` is the source: hatch reads it for the build, and the CLI reports
it. The badge is the copy, and a copy drifts silently. `scripts/sync_version.py`
rewrites it and `--check` refuses a drift; these tests run the same check.

    python scripts/sync_version.py
"""

from scripts.sync_version import announces, badge_version, problems, version


def test_the_version_is_badged_and_announced():
    """The badge is rewritten for you; the changelog heading is not."""
    found = problems()
    assert not found, "\n".join(found) + (
        f"\nmeret/__init__.py is {version()} - run: python scripts/sync_version.py"
    )


def test_there_is_a_badge_to_check():
    """A missing badge must not read as "in step"."""
    assert badge_version() is not None


def test_the_heading_detector_takes_the_escaped_brackets():
    """The file writes `## \\[0.7.32\\]`; the check must not depend on that escaping."""
    assert announces("0.7.32", "## \\[0.7.32\\] — 2026-10-29\n")
    assert announces("0.7.32", "## [0.7.32] — 2026-10-29\n")


def test_a_version_log_line_is_not_a_heading():
    """`- 0.7.32 - something` announces nothing: it has no date and no section."""
    assert not announces("0.7.32", "- 0.7.32 - a version-log line\n")


def test_another_version_does_not_count():
    assert not announces("0.7.32", "## \\[0.7.31\\] — 2026-10-28\n")
