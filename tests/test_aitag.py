"""Tests for AI auto-tagging: the detector, the facts and the pass.

The subject here is the seam, not the model. `FakeDetector` below has never seen a
photograph, and the pass has to work with it exactly as it works with YOLO — that is
what "abstract" has to mean when it is written down as a test.
"""

import pytest

from meret.core.aitag import DEFAULT_AI_RULES_PATH, DETECTABLE, AiTagger
from meret.core.autotag import AutoTagRule, build_facts, load_rules
from meret.core.detectors.base import (
    DETECT_KEY,
    Detection,
    create_detector,
    facts,
    known_backends,
)
from meret.core.detectors.policy import load_policy
from meret.core.models import FileEntry, FileType

PHOTO = "/media/IMG_0001.JPG"
STAMP = 1_700_000_000.0


class FakeDetector:
    """A detector with no model: canned answers, and a record of what it was asked.

    `answer` is what it finds; pass `answer=[]` for "looked and found nothing" and put
    a path in `blind` for "could not read this one". The three cases have to be
    distinguishable, because the pass treats them differently.
    """

    name = "fake"

    def __init__(self, answer=None, blind=()):
        self.answer = [Detection(label="cat", confidence=0.9)] if answer is None else answer
        self.blind = set(blind)
        self.asked = []

    def detect(self, path):
        self.asked.append(path)
        return None if path in self.blind else list(self.answer)


def make_entry(**overrides) -> FileEntry:
    """One indexed photo with an mtime, which is what the staleness check reads."""
    data = {
        "filename": "IMG_0001.JPG",
        "filepath": PHOTO,
        "type": FileType.IMAGE,
        "metadata": {"width": 4032, "height": 3024, "modified_at": STAMP},
    }
    data.update(overrides)
    return FileEntry(**data)


def make_rule(tag: str, field: str = "detect.cat", op: str = "gte", value=0.5) -> AutoTagRule:
    return AutoTagRule.model_validate(
        {"tag": tag, "conditions": [{"field": field, "op": op, "value": value}]}
    )


class StubRepo:
    """Only what AiTagger.run touches.

    Not an AbstractRepository subclass, so every method it uses has to be listed: the
    base class gives add_tags and count_files a working default, and a stub that does
    not inherit from it does not get one.
    """

    def __init__(self, entries):
        self.entries = entries
        self.added = []
        self.saved = []

    def iter_files(self, limit=None, untagged_only=False):
        items = [e for e in self.entries if not (untagged_only and e.labels)]
        return iter(items[:limit] if limit else items)

    def count_files(self, untagged_only=False):
        """What a walk would see, without walking: the progress bar's total."""
        return len([e for e in self.entries if not (untagged_only and e.labels)])

    def save_file(self, file):
        self.saved.append(file.filepath)
        return file.id or 0

    def add_tag(self, file_id, tag, colour=None):
        self.added.append((file_id, tag, colour))
        return 1

    def add_tags(self, file_id, tags):
        """One call per file, as the tagger uses it. Records the same triples."""
        for tag, colour in tags:
            self.added.append((file_id, tag, colour))
        return len(tags)


# ---------- facts ----------


def test_facts_keep_the_best_confidence_and_the_count():
    block = facts(
        [
            Detection(label="cat", confidence=0.4),
            Detection(label="cat", confidence=0.8),
            Detection(label="dog", confidence=0.6),
        ],
        model="fake",
        stamp=STAMP,
    )

    assert block["cat"] == 0.8
    assert block["cat.count"] == 2
    assert block["dog"] == 0.6
    assert block["dog.count"] == 1
    assert block["classes"] == "cat,dog"
    assert block["model"] == "fake"
    assert block["mtime"] == STAMP


def test_detections_become_ordinary_facts():
    """The whole coupling: a block in the metadata is a fact for the rule engine.

    Nothing in the rule engine knows about models, so this test is the contract: if it
    ever fails, the two halves have been wired together by hand somewhere.
    """
    entry = make_entry(
        metadata={
            "width": 4032,
            "height": 3024,
            "modified_at": STAMP,
            DETECT_KEY: facts([Detection(label="cat", confidence=0.62)], model="fake"),
        }
    )

    view = build_facts(entry)

    assert view["detect.cat"] == 0.62
    assert view["detect.cat.count"] == 1
    assert view["detect.classes"] == "cat"
    assert view["orientation"] == "landscape"  # the old facts are still there


# ---------- rules over detections ----------


def test_a_rule_over_a_detection_tags_the_file():
    tagger = AiTagger(FakeDetector(), [make_rule("animals/cat")], root="ai")

    detected, planned = tagger.plan(make_entry())

    assert detected
    assert planned == [("ai/animals/cat", None)]


def test_the_threshold_belongs_to_the_rule_not_to_the_model():
    """One 0.9 cat, two rules: the model's answer is the same, the decision is not."""
    entry = make_entry()
    strict = AiTagger(FakeDetector(), [make_rule("animals/cat", value=0.95)])
    lenient = AiTagger(FakeDetector(), [make_rule("animals/cat", value=0.5)])

    assert strict.plan(entry)[1] == []
    assert lenient.plan(make_entry())[1] == [("ai/animals/cat", None)]


def test_a_count_rule_tells_one_person_from_a_crowd():
    detector = FakeDetector([Detection(label="person", confidence=0.9)] * 6)
    tagger = AiTagger(
        detector, [make_rule("crowd", field="detect.person.count", value=5)], root="ai"
    )

    assert tagger.plan(make_entry())[1] == [("ai/crowd", None)]


def test_the_ai_root_keeps_model_tags_apart():
    rule = make_rule("animals/cat")

    prefixed = AiTagger(FakeDetector(), [rule], root="ai")
    bare = AiTagger(FakeDetector(), [rule], root="")

    assert prefixed.plan(make_entry())[1] == [("ai/animals/cat", None)]
    assert bare.plan(make_entry())[1] == [("animals/cat", None)]


def test_a_tag_the_file_already_has_is_not_added_twice():
    tagger = AiTagger(FakeDetector(), [make_rule("animals/cat")], root="ai")
    entry = make_entry(labels=["ai/animals/cat"])

    assert tagger.plan(entry)[1] == []


# ---------- the cache ----------


def test_stale_asks_again_only_when_the_answer_is_outdated():
    tagger = AiTagger(FakeDetector(), [])
    entry = make_entry()

    assert tagger.stale(entry)  # no block at all

    tagger.plan(entry)
    assert not tagger.stale(entry)  # same model, same mtime

    entry.metadata["modified_at"] = STAMP + 60
    assert tagger.stale(entry)  # the file changed on disk

    entry.metadata["modified_at"] = STAMP
    entry.metadata[DETECT_KEY]["model"] = "another-model"
    assert tagger.stale(entry)  # someone changed the checkpoint


def test_force_ignores_the_cache():
    tagger = AiTagger(FakeDetector(), [], force=True)
    entry = make_entry()
    tagger.plan(entry)

    assert tagger.stale(entry)


# ---------- the pass ----------


def test_run_detects_once_and_remembers_it():
    entry = make_entry(id=7)
    detector = FakeDetector()
    repo = StubRepo([entry])
    tagger = AiTagger(detector, [make_rule("animals/cat")], root="ai")

    stats = tagger.run(repo)

    assert stats == {"scanned": 1, "detected": 1, "changed": 1, "tags": 1, "interrupted": False}
    assert repo.added == [(7, "ai/animals/cat", None)]
    assert repo.saved == [PHOTO]

    second = StubRepo([make_entry(id=7, metadata=entry.metadata)])
    stats = tagger.run(second)

    assert stats["detected"] == 0
    assert detector.asked == [PHOTO]  # asked once, not twice


def test_run_writes_metadata_before_it_adds_tags():
    """A repository that rewrites the whole row must not carry the new tags away."""
    order = []

    class OrderedRepo(StubRepo):
        def save_file(self, file):
            order.append("save")
            return super().save_file(file)

        def add_tags(self, file_id, tags):
            order.append("tags")
            return super().add_tags(file_id, tags)

    repo = OrderedRepo([make_entry(id=7)])
    AiTagger(FakeDetector(), [make_rule("animals/cat")]).run(repo)

    assert order == ["save", "tags"]


def test_dry_run_asks_the_detector_and_writes_nothing():
    repo = StubRepo([make_entry(id=7)])
    tagger = AiTagger(FakeDetector(), [make_rule("animals/cat")])

    stats = tagger.run(repo, dry_run=True)

    assert stats["detected"] == 1
    assert stats["changed"] == 1
    assert repo.added == []
    assert repo.saved == []


def test_a_file_the_detector_cannot_read_is_tried_again():
    entry = make_entry(id=7)
    repo = StubRepo([entry])
    tagger = AiTagger(FakeDetector(blind={PHOTO}), [make_rule("animals/cat")])

    stats = tagger.run(repo)

    assert stats["detected"] == 0
    assert repo.saved == []
    assert DETECT_KEY not in entry.metadata
    assert tagger.stale(entry)  # unanswered, so the next run asks again


def test_nothing_found_is_an_answer_and_is_cached():
    """The other side of the same coin: an empty result must not be re-asked."""
    entry = make_entry(id=7)
    detector = FakeDetector(answer=[])
    tagger = AiTagger(detector, [make_rule("animals/cat")])

    tagger.run(StubRepo([entry]))

    assert DETECT_KEY in entry.metadata
    assert not tagger.stale(entry)


def test_video_and_audio_are_not_sent_to_the_detector():
    detector = FakeDetector()
    tagger = AiTagger(detector, [make_rule("animals/cat")])
    entry = make_entry(id=7, type=FileType.VIDEO)

    stats = tagger.run(StubRepo([entry]))

    assert detector.asked == []
    assert stats["detected"] == 0
    assert FileType.VIDEO not in DETECTABLE


def test_only_untagged_skips_files_that_already_have_tags():
    repo = StubRepo([make_entry(id=7, labels=["already/here"])])
    tagger = AiTagger(FakeDetector(), [make_rule("animals/cat")])

    stats = tagger.run(repo, only_untagged=True)

    assert stats["scanned"] == 0


def test_should_stop_ends_the_walk_on_a_file_boundary():
    entries = [make_entry(id=i, filepath=f"/m/{i}.jpg") for i in range(5)]
    calls = {"n": 0}

    def stop():
        calls["n"] += 1
        return calls["n"] > 2

    tagger = AiTagger(FakeDetector(), [make_rule("animals/cat")])
    stats = tagger.run(StubRepo(entries), should_stop=stop)

    assert stats["interrupted"]
    assert stats["scanned"] == 2  # two files finished, the third never started


def test_progress_reports_a_total_that_cannot_move():
    seen = []

    def record(done, total):
        seen.append((done, total))

    tagger = AiTagger(FakeDetector(), [make_rule("animals/cat")])
    tagger.run(StubRepo([make_entry(id=7)]), progress=record)

    assert seen == [(1, 1)]


# ---------- the policy file ----------


def write_policy(tmp_path, body: str):
    path = tmp_path / "ai.toml"
    path.write_text(body, encoding="utf-8")
    return path


def test_policy_reads_the_detector_section(tmp_path):
    path = write_policy(
        tmp_path,
        '[detector]\nbackend = "yolo"\nmodel = "yolo26n.pt"\nconfidence = 0.4\n\n'
        '[[rules]]\ntag = "animals/cat"\n\n'
        '[[rules.conditions]]\nfield = "detect.cat"\nop = "gte"\nvalue = 0.5\n',
    )

    policy = load_policy(path)
    assert policy is not None
    assert policy.backend == "yolo"
    assert policy.model == "yolo26n.pt"
    assert policy.confidence == 0.4
    assert [rule.tag for rule in load_rules(path)] == ["animals/cat"]


def test_a_rules_file_without_a_detector_section_has_no_policy(tmp_path):
    """Metadata rules must never make the AI pass run: no section, no detector."""
    path = write_policy(
        tmp_path,
        '[[rules]]\ntag = "big"\n\n'
        '[[rules.conditions]]\nfield = "width"\nop = "gte"\nvalue = 4000\n',
    )

    assert load_policy(path) is None


def test_a_bad_confidence_is_rejected_rather_than_clamped(tmp_path):
    path = write_policy(tmp_path, "[detector]\nconfidence = 1.5\n")

    with pytest.raises(ValueError):
        load_policy(path)


def test_an_unknown_backend_lists_the_real_ones():
    assert "yolo" in known_backends()  # registered before ultralytics is installed

    with pytest.raises(ValueError, match="yolo"):
        create_detector("not-a-backend")


def test_the_shipped_ai_rules_parse_and_fire():
    """A default policy is a policy, not a placeholder: it must parse and tag.

    The asserts are staged on purpose: "no tags" says nothing about whether the file
    failed to parse, the detector answered nothing, or the rule failed to match.
    """
    assert DEFAULT_AI_RULES_PATH.is_file(), "the default AI rules file did not ship"

    policy = load_policy(DEFAULT_AI_RULES_PATH)
    assert policy is not None, "the shipped file has no [detector] section"
    assert policy.backend in known_backends()

    rules = load_rules(DEFAULT_AI_RULES_PATH)
    assert rules, "the shipped AI rules file parsed to zero rules - check [[rules]]"

    detector = FakeDetector([Detection(label="person", confidence=0.91)])
    _detected, planned = AiTagger(detector, rules, root="ai").plan(make_entry())

    assert ("ai/people", None) in planned
