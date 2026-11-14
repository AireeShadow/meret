"""Metadata-driven auto-tagging.

A rule is a tag path plus a set of conditions over a file's extracted metadata.
Conditions see a flat view of that metadata (dotted keys, e.g. ``exif.Make``)
plus a few derived facts: ``type``, ``orientation``, ``megapixels``, ``year``,
``duration_s``.

Rules are data, not code: they live in the config (``[autotag]``) or in a
separate TOML/JSON file, so a library can carry its own tagging policy without
touching the user's config.
"""

import json
import logging
import re
from collections.abc import Callable, Iterable
from datetime import datetime
from pathlib import Path
from string import Formatter
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from meret.core.models import FileEntry
from meret.core.repository import AbstractRepository

logger = logging.getLogger(__name__)

Operator = Literal[
    "eq",
    "ne",
    "in",
    "not_in",
    "contains",
    "startswith",
    "endswith",
    "regex",
    "gte",
    "lte",
    "gt",
    "lt",
    "exists",
    "missing",
]

# operators that do not need a value
_NO_VALUE_OPS = frozenset({"exists", "missing"})
_LIST_OPS = frozenset({"in", "not_in"})

_DATE_RE = re.compile(r"(\d{4})[-:](\d{2})[-:](\d{2})")
_FORMATTER = Formatter()


class Condition(BaseModel):
    """One test over one fact.

    ``ci`` folds case for string comparison (tag names are often Cyrillic, so
    casefold, not lower). Numeric operators compare as floats and ignore it.
    """

    field: str = Field(min_length=1, description="Fact name: width, exif.Make, tags.artist")
    op: Operator = "eq"
    value: Any = None
    ci: bool = True

    @field_validator("field")
    @classmethod
    def _strip_metadata_prefix(cls, value: str) -> str:
        """``metadata.width`` and ``width`` mean the same thing — one spelling."""
        return value.strip().removeprefix("metadata.")

    @model_validator(mode="after")
    def _check_value(self) -> "Condition":
        if self.op not in _NO_VALUE_OPS and self.value is None:
            raise ValueError(f"operator {self.op!r} needs a value")
        if self.op in _LIST_OPS and not isinstance(self.value, list):
            raise ValueError(f"operator {self.op!r} needs a list")
        return self


class AutoTagRule(BaseModel):
    """A tag to add when the conditions hold.

    ``tag`` may use ``{placeholders}``: the rule is skipped when a placeholder
    has no value, so ``camera/{exif.Make}`` never produces a bare ``camera/``.
    """

    tag: str = Field(min_length=1)
    conditions: list[Condition] = Field(min_length=1)
    match: Literal["all", "any"] = "all"
    colour: str | None = None
    enabled: bool = True


# ---------- facts ----------


def _as_float(value: Any) -> float | None:
    """A number or None. bool is excluded: True is not a duration."""
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> Any:
    """EXIF strings come padded ('Canon '), and a padded tag looks broken."""
    return value.strip() if isinstance(value, str) else value


def normalize_root(root: str | None) -> str:
    """A tag path with no stray separators. Empty means "no parent tag".

    Accepts "/media/", "media", " media " and backslashes: the value comes from a
    config file a human typed into, and a leading slash is not a hierarchy.
    """
    if not root:
        return ""
    parts = (part.strip() for part in str(root).replace("\\", "/").split("/"))
    return "/".join(part for part in parts if part)


def _year_from(facts: dict[str, Any]) -> int | None:
    """The year a photo/video was taken, falling back to the file mtime."""
    for key in ("exif.DateTimeOriginal", "exif.DateTime", "creation_time", "tags.date"):
        raw = facts.get(key)
        if isinstance(raw, str):
            found = _DATE_RE.search(raw)
            if found:
                return int(found.group(1))
    stamp = _as_float(facts.get("modified_at"))
    if stamp:
        try:
            return datetime.fromtimestamp(stamp).year
        except (OverflowError, OSError, ValueError):
            return None
    return None


def build_facts(entry: FileEntry) -> dict[str, Any]:
    """Flatten an entry's metadata into dotted keys plus derived facts.

    Nested dicts (``exif``, audio ``tags``) become ``parent.child`` keys; lists
    (``streams``) are dropped — a condition over a list has no single answer.
    """
    facts: dict[str, Any] = {"type": entry.type.value, "filename": entry.filename}

    for key, value in (entry.metadata or {}).items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                facts[f"{key}.{sub_key}"] = _clean(sub_value)
        elif isinstance(value, (list, tuple)):
            continue
        else:
            facts[key] = _clean(value)

    facts["stem"] = Path(entry.filename).stem
    facts["extension"] = str(facts.get("extension") or Path(entry.filename).suffix).lower()

    width = _as_float(facts.get("width"))
    height = _as_float(facts.get("height"))
    if width and height:
        if width > height:
            facts["orientation"] = "landscape"
        elif height > width:
            facts["orientation"] = "portrait"
        else:
            facts["orientation"] = "square"
        facts["aspect"] = round(width / height, 2)
        facts["megapixels"] = round(width * height / 1_000_000, 1)

    duration = _as_float(facts.get("duration"))
    if duration:
        facts["duration_s"] = round(duration, 1)
        facts["duration_min"] = round(duration / 60, 1)

    year = _year_from(facts)
    if year is not None:
        facts["year"] = year

    return facts


# ---------- matching ----------


def _norm(value: Any, ci: bool) -> Any:
    return value.casefold() if isinstance(value, str) and ci else value


def _compare_numeric(condition: Condition, actual: Any) -> bool:
    """Numeric comparison; a fact that is not a number never matches."""
    left = _as_float(actual)
    right = _as_float(condition.value)
    if left is None or right is None:
        return False
    if condition.op == "gt":
        return left > right
    if condition.op == "gte":
        return left >= right
    if condition.op == "lt":
        return left < right
    return left <= right


def _compare_list(condition: Condition, actual: Any) -> bool:
    """Membership in the rule's list, case-folded when `ci` is on."""
    wanted = [_norm(item, condition.ci) for item in condition.value]
    hit = _norm(actual, condition.ci) in wanted
    return hit if condition.op == "in" else not hit


def _compare_regex(condition: Condition, actual: Any) -> bool:
    """Regex search. The pattern is not case-folded — IGNORECASE does that without
    mangling escapes."""
    try:
        flags = re.IGNORECASE if condition.ci else 0
        return re.search(str(condition.value), str(actual), flags) is not None
    except re.error as e:
        logger.warning("bad regex in auto-tag rule: %r (%s)", condition.value, e)
        return False


def _compare_text(condition: Condition, actual: Any) -> bool:
    """String comparison, case-folded when `ci` is on."""
    left = _norm(actual, condition.ci)
    right = _norm(condition.value, condition.ci)

    if condition.op == "eq":
        return left == right
    if condition.op == "ne":
        return left != right
    if condition.op == "contains":
        return str(right) in str(left)
    if condition.op == "startswith":
        return str(left).startswith(str(right))
    if condition.op == "endswith":
        return str(left).endswith(str(right))
    return _compare_regex(condition, actual)


# One entry per operator, so adding an operator is a row here and nothing else.
_COMPARATORS: dict[str, Callable[[Condition, Any], bool]] = {
    "eq": _compare_text,
    "ne": _compare_text,
    "contains": _compare_text,
    "startswith": _compare_text,
    "endswith": _compare_text,
    "regex": _compare_regex,
    "in": _compare_list,
    "not_in": _compare_list,
    "gt": _compare_numeric,
    "gte": _compare_numeric,
    "lt": _compare_numeric,
    "lte": _compare_numeric,
}


def matches_condition(condition: Condition, facts: dict[str, Any]) -> bool:
    """True when one condition holds for the given facts."""
    actual = facts.get(condition.field)
    present = actual is not None and actual != ""

    if condition.op == "exists":
        return present
    if condition.op == "missing":
        return not present
    if not present:
        return False

    return _COMPARATORS[condition.op](condition, actual)


def rule_matches(rule: AutoTagRule, facts: dict[str, Any]) -> bool:
    """All conditions (``match: all``) or any of them (``match: any``)."""
    results = (matches_condition(condition, facts) for condition in rule.conditions)
    return all(results) if rule.match == "all" else any(results)


def render_tag(template: str, facts: dict[str, Any]) -> str | None:
    """Fill ``{placeholders}`` from facts; None when one of them has no value.

    Format specs and conversions are ignored on purpose: a tag is a name, not a
    formatted number, and ``{width:d}`` in a tag would be a surprise.
    """
    parts: list[str] = []
    for literal, field_name, _spec, _conversion in _FORMATTER.parse(template):
        parts.append(literal)
        if field_name is None:
            continue
        value = facts.get(field_name)
        if value is None or value == "":
            return None
        parts.append(str(value))
    return "".join(parts)


# ---------- loading rules ----------


def load_rules(path: Path) -> list[AutoTagRule]:
    """Read rules from a TOML or JSON file.

    Accepted shapes: a bare array of rules, ``{"rules": [...]}``, or the same
    ``[autotag]`` table the config uses.
    """
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        data: Any = json.loads(text)
    else:
        import tomllib

        data = tomllib.loads(text)

    if isinstance(data, dict):
        nested = data.get("autotag")
        if isinstance(nested, dict) and "rules" in nested:
            data = nested["rules"]
        else:
            data = data.get("rules", [])

    return [AutoTagRule.model_validate(item) for item in data]


# ---------- the tagger ----------


class AutoTagger:
    """Turns rules plus a file's metadata into tag paths, and writes them."""

    def __init__(
        self,
        rules: Iterable[AutoTagRule],
        only_untagged: bool = False,
        root: str = "",
    ) -> None:
        self.rules = [rule for rule in rules if rule.enabled]
        self.only_untagged = only_untagged
        self.root = normalize_root(root)

    @classmethod
    def from_config(
        cls,
        rules_path: str | Path | None = None,
        only_untagged: bool | None = None,
        root: str | None = None,
    ) -> "AutoTagger":
        """Build a tagger from the ``[autotag]`` config section.

        An explicit ``rules_path`` wins over ``autotag.rules_file``. ``root``
        overrides ``autotag.root``, which is the parent every produced tag is filed
        under; an empty root files them at the top level. A rules file that is not
        there is a warning, not a crash: the config can point at a file that lives
        on a drive which is not mounted right now.
        """
        from meret.core.config import get_config

        section = get_config().config.autotag
        source = rules_path or section.rules_file

        rules: list[AutoTagRule] = []
        if source:
            path = Path(source).expanduser()
            if path.exists():
                rules = load_rules(path)
            else:
                logger.warning("auto-tagging rules file not found: %s", path)

        if only_untagged is None:
            only_untagged = section.only_untagged
        if root is None:
            root = section.root
        return cls(rules, only_untagged=only_untagged, root=root)

    def _with_root(self, tag: str) -> str:
        """File the tag under the configured parent, exactly once.

        A rule that already spells the root out — `metadata/camera/{exif.Make}` —
        is left alone, so both spellings of the same rule agree instead of
        producing `metadata/metadata/camera/Canon`.
        """
        if not self.root:
            return tag
        folded_tag = tag.casefold()
        folded_root = self.root.casefold()
        if folded_tag == folded_root or folded_tag.startswith(folded_root + "/"):
            return tag
        return f"{self.root}/{tag}"

    def plan(self, entry: FileEntry) -> list[tuple[str, str | None]]:
        """The tags this file should gain, as (tag path, colour) pairs.

        Tags the file already has are skipped, so a second run is a no-op. The
        comparison is on the final path, root included: `metadata/big` and `big`
        are two different tags, and only one of them is what this run produces.
        """
        facts = build_facts(entry)
        existing = {label.casefold() for label in entry.labels}
        planned: list[tuple[str, str | None]] = []
        seen: set[str] = set()

        for rule in self.rules:
            if not rule_matches(rule, facts):
                continue
            tag = render_tag(rule.tag, facts)
            if not tag:
                continue
            tag = self._with_root(tag)
            key = tag.casefold()
            if key in seen or key in existing:
                continue
            seen.add(key)
            planned.append((tag, rule.colour))

        return planned

    def tags_for(self, entry: FileEntry) -> list[str]:
        """Just the tag paths — handy for tests and dry runs."""
        return [tag for tag, _colour in self.plan(entry)]

    def apply(self, entry: FileEntry, repo: AbstractRepository) -> list[str]:
        """Write the planned tags for one entry. Returns the tag paths added."""
        if entry.id is None:
            return []
        if self.only_untagged and entry.labels:
            return []
        added: list[str] = []
        for tag, colour in self.plan(entry):
            repo.add_tag(entry.id, tag, colour)
            added.append(tag)
        return added

    def run(
        self,
        repo: AbstractRepository,
        *,
        limit: int | None = None,
        dry_run: bool = False,
        only_untagged: bool | None = None,
        paths: set[str] | None = None,
        progress: Callable[[int, int | None], None] | None = None,
    ) -> dict[str, int]:
        """Walk the library and tag it.

        ``paths`` narrows the walk to a set of file paths (a fresh scan).
        ``limit`` caps how many files are *read* from the database, before the
        path filter — so a limit plus a path filter can scan fewer files.
        The total is unknown while streaming, hence ``progress(done, None)``.
        """
        untagged_only = self.only_untagged if only_untagged is None else only_untagged
        stats = {"scanned": 0, "changed": 0, "tags": 0}

        for entry in repo.iter_files(limit=limit, untagged_only=untagged_only):
            if paths is not None and entry.filepath not in paths:
                continue

            stats["scanned"] += 1
            planned = self.plan(entry)
            if planned:
                stats["changed"] += 1
                stats["tags"] += len(planned)
                if not dry_run and entry.id is not None:
                    for tag, colour in planned:
                        repo.add_tag(entry.id, tag, colour)

            if progress is not None:
                progress(stats["scanned"], None)

        return stats
