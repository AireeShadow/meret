"""AI auto-tagging: a detector's answer becomes tags, through the same rule engine.

This is a second pass, not a mode of the first one. `meret autotag` reads metadata and
nothing else, and runs anywhere; `meret aitag` needs a model runtime, weights and time,
and most libraries never run it at all. Two commands, two config sections, two rule
files, two roots: `metadata/` for what the file says about itself, `ai/` for what a
model saw in it.

Nothing is shared with the metadata tagger except the rule engine, which is pure:
`AutoTagger` already knows how to match a rule against facts, render a tag template,
prefix the root and skip what a file already has. The AI pass composes it instead of
copying it, so the two can never drift apart.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from pathlib import Path

from meret.core.autotag import AutoTagRule, AutoTagger, load_rules
from meret.core.detectors.base import DETECT_KEY, Detector, facts
from meret.core.detectors.policy import load_policy
from meret.core.models import FileEntry, FileType
from meret.core.repository import AbstractRepository

logger = logging.getLogger(__name__)

#: The AI rules shipped with meret, used when nothing else is configured.
DEFAULT_AI_RULES_PATH = Path(__file__).parent / "rules" / "ai.toml"

#: Video and audio have no single frame a detector can be trusted with yet.
DETECTABLE = frozenset({FileType.IMAGE})


def _reads_detections(rule: AutoTagRule) -> bool:
    """True when a rule reads something a detector wrote."""
    return any(condition.field.startswith(f"{DETECT_KEY}.") for condition in rule.conditions)


class AiTagger:
    """Runs one detector and one rule set, and writes the result."""

    def __init__(
        self,
        detector: Detector,
        rules: Iterable[AutoTagRule],
        only_untagged: bool = False,
        root: str = "ai",
        force: bool = False,
    ) -> None:
        self.detector = detector
        self.rules = AutoTagger(rules, only_untagged=only_untagged, root=root)
        self.only_untagged = only_untagged
        self.force = force

    @classmethod
    def from_config(
        cls,
        rules_path: str | Path | None = None,
        only_untagged: bool | None = None,
        root: str | None = None,
        force: bool = False,
    ) -> "AiTagger":
        """Build the tagger from the `[aitag]` config section.

        An explicit `rules_path` wins over `aitag.rules_file`, and an external file
        wins over the shipped defaults — the same precedence the metadata tagger
        follows, so one explanation covers both. A configured file that is missing
        stays missing, with a warning: falling back to a default policy would tag a
        library nobody asked it to tag.

        A file without a `[detector]` section is an error, not an empty run: without a
        model there is nothing to tag, and a silent no-op would look like a broken
        command.
        """
        from meret.core.config import get_config

        section = get_config().config.aitag
        source = rules_path or section.rules_file

        if source:
            path = Path(source).expanduser()
            if not path.exists():
                logger.warning("AI tagging rules file not found: %s", path)
                raise ValueError(f"no such rules file: {path}")
        else:
            path = DEFAULT_AI_RULES_PATH
            logger.info("using the built-in AI tagging rules: %s", path)

        policy = load_policy(path)
        if policy is None:
            raise ValueError(f"no [detector] section in {path}")

        rules = load_rules(path)
        if rules and not any(_reads_detections(rule) for rule in rules):
            logger.warning(
                "none of the rules in %s reads a detect.* fact: they belong in the "
                "metadata rules file, not here",
                path,
            )

        if only_untagged is None:
            only_untagged = section.only_untagged
        if root is None:
            root = section.root

        return cls(
            policy.build(),
            rules,
            only_untagged=only_untagged,
            root=root,
            force=force,
        )

    def stale(self, entry: FileEntry) -> bool:
        """True when this file has to be asked about again.

        A file with no block, a block written by another model, and a file whose mtime
        moved all need a pass. Anything else is left alone: inference is the most
        expensive thing meret does, and re-running it over an unchanged library is
        minutes of GPU for the answer already on disk.
        """
        if self.force:
            return True
        metadata = entry.metadata or {}
        stored = metadata.get(DETECT_KEY)
        if not isinstance(stored, dict):
            return True
        if stored.get("model") != self.detector.name:
            return True
        return stored.get("mtime") != metadata.get("modified_at")

    def plan(self, entry: FileEntry) -> tuple[bool, list[tuple[str, str | None]]]:
        """Detect if needed, then plan the tags. Returns (was_detected, tags).

        The block is written into the entry before the rules run, because the rule
        engine reads facts out of `entry.metadata` and knows nothing about detectors.
        That single line is the whole coupling between the two halves of this feature.

        A detector that cannot answer leaves no block behind, so the next run asks
        again. Writing an empty block would make an unreadable file look answered.
        """
        detected = False
        if entry.type in DETECTABLE and self.stale(entry):
            answer = self.detector.detect(entry.filepath)
            if answer is None:
                logger.warning(
                    "detector %s gave no answer for %s", self.detector.name, entry.filepath
                )
            else:
                metadata = dict(entry.metadata or {})
                metadata[DETECT_KEY] = facts(
                    answer,
                    model=self.detector.name,
                    stamp=metadata.get("modified_at"),
                )
                entry.metadata = metadata
                detected = True

        return detected, self.rules.plan(entry)

    def run(
        self,
        repo: AbstractRepository,
        *,
        limit: int | None = None,
        dry_run: bool = False,
        only_untagged: bool | None = None,
        progress: Callable[[int, int], None] | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> dict[str, int]:
        """Walk the library, ask the model, write metadata and tags.

        The shape mirrors `AutoTagger.run` on purpose — a total counted up front, a
        `should_stop()` asked between files, a dry run that writes nothing — so the
        GUI's progress window and its Cancel button work for this pass unchanged.

        Metadata is saved before the tags are added: a repository that writes the whole
        file row would otherwise carry the just-added tags away with it.
        """
        untagged_only = self.only_untagged if only_untagged is None else only_untagged
        total = repo.count_files(untagged_only=untagged_only)
        if limit is not None:
            total = min(total, limit)

        stats = {"scanned": 0, "detected": 0, "changed": 0, "tags": 0, "interrupted": False}

        for entry in repo.iter_files(limit=limit, untagged_only=untagged_only):
            if should_stop is not None and should_stop():
                stats["interrupted"] = True
                logger.info("AI tagging cancelled after %d files", stats["scanned"])
                break

            stats["scanned"] += 1
            detected, planned = self.plan(entry)
            if detected:
                stats["detected"] += 1
            if planned:
                stats["changed"] += 1
                stats["tags"] += len(planned)

            if not dry_run and entry.id is not None:
                if detected:
                    repo.save_file(entry)
                if planned:
                    repo.add_tags(entry.id, planned)

            if progress is not None:
                progress(stats["scanned"], total)

        return stats
