"""Tests for metadata-driven auto-tagging."""

import json

import pytest

from meret.core.autotag import (
    AutoTagRule,
    AutoTagger,
    build_facts,
    load_rules,
    render_tag,
    rule_matches,
)
from meret.core.models import FileEntry, FileType


def make_entry(**overrides) -> FileEntry:
    """A Canon photo: 4032x3024, taken in 2021, with padded EXIF strings."""
    data = {
        "filename": "IMG_0001.JPG",
        "filepath": "/media/IMG_0001.JPG",
        "type": FileType.IMAGE,
        "metadata": {
            "width": 4032,
            "height": 3024,
            "format": "JPEG",
            "size_bytes": 5_000_000,
            "exif": {"Make": "Canon ", "DateTimeOriginal": "2021:07:14 18:02:11"},
        },
    }
    data.update(overrides)
    return FileEntry(**data)


def make_rule(tag: str, conditions: list[dict], match: str = "all", colour=None) -> AutoTagRule:
    return AutoTagRule.model_validate(
        {"tag": tag, "conditions": conditions, "match": match, "colour": colour}
    )


def test_facts_flatten_exif_and_derive_orientation():
    facts = build_facts(make_entry())

    assert facts["exif.Make"] == "Canon"  # padded EXIF string is stripped
    assert facts["orientation"] == "landscape"
    assert facts["megapixels"] == 12.2
    assert facts["extension"] == ".jpg"
    assert facts["year"] == 2021


def test_orientation_is_portrait_for_a_tall_frame():
    facts = build_facts(make_entry(metadata={"width": 1080, "height": 1920}))
    assert facts["orientation"] == "portrait"


def test_numeric_condition():
    facts = build_facts(make_entry())
    rule = make_rule("big", [{"field": "width", "op": "gte", "value": 4000}])
    assert rule_matches(rule, facts)


def test_numeric_condition_ignores_a_non_numeric_fact():
    facts = build_facts(make_entry())
    rule = make_rule("x", [{"field": "format", "op": "gt", "value": 1}])
    assert not rule_matches(rule, facts)


def test_in_and_contains_are_case_insensitive():
    facts = build_facts(make_entry())

    in_condition = {"field": "exif.Make", "op": "in", "value": ["nikon", "CANON"]}
    assert rule_matches(make_rule("cam", [in_condition]), facts)

    contains_condition = {"field": "filename", "op": "contains", "value": "img_"}
    assert rule_matches(make_rule("cam", [contains_condition]), facts)


def test_regex_condition():
    facts = build_facts(make_entry())
    rule = make_rule("raw", [{"field": "filename", "op": "regex", "value": r"^IMG_\d{4}\.JPG$"}])
    assert rule_matches(rule, facts)


def test_bad_regex_is_false_not_an_exception():
    facts = build_facts(make_entry())
    rule = make_rule("bad", [{"field": "filename", "op": "regex", "value": "([unclosed"}])
    assert not rule_matches(rule, facts)


def test_missing_field_never_matches_except_for_the_missing_op():
    facts = build_facts(make_entry())
    eq_rule = make_rule("x", [{"field": "exif.Lens", "op": "eq", "value": "50mm"}])
    assert not rule_matches(eq_rule, facts)

    assert rule_matches(make_rule("x", [{"field": "exif.Lens", "op": "missing"}]), facts)
    assert not rule_matches(make_rule("x", [{"field": "exif.Lens", "op": "exists"}]), facts)


def test_match_any_needs_only_one_condition():
    facts = build_facts(make_entry())
    rule = make_rule(
        "x",
        [
            {"field": "orientation", "op": "eq", "value": "portrait"},
            {"field": "exif.Make", "op": "eq", "value": "Canon"},
        ],
        match="any",
    )
    assert rule_matches(rule, facts)


def test_condition_without_a_value_is_rejected():
    with pytest.raises(ValueError):
        make_rule("x", [{"field": "width", "op": "eq"}])


def test_render_tag_fills_placeholders_and_drops_unknown_ones():
    facts = build_facts(make_entry())
    assert render_tag("camera/{exif.Make}", facts) == "camera/Canon"
    assert render_tag("camera/{exif.Lens}", facts) is None
    assert render_tag("year/{year}", facts) == "year/2021"


def test_plan_skips_tags_the_file_already_has():
    entry = make_entry(labels=["camera/Canon"])
    tagger = AutoTagger([make_rule("camera/{exif.Make}", [{"field": "exif.Make", "op": "exists"}])])
    assert tagger.plan(entry) == []


def test_plan_keeps_the_rule_colour():
    rule = make_rule("big", [{"field": "width", "op": "gte", "value": 4000}], colour="red")
    assert AutoTagger([rule]).plan(make_entry()) == [("big", "red")]


def test_disabled_rules_are_ignored():
    rule = make_rule("big", [{"field": "width", "op": "gte", "value": 4000}])
    rule.enabled = False
    assert AutoTagger([rule]).plan(make_entry()) == []


class StubRepo:
    """Only what AutoTagger.run touches."""

    def __init__(self, entries):
        self.entries = entries
        self.added = []

    def iter_files(self, limit=None, untagged_only=False):
        items = [e for e in self.entries if not (untagged_only and e.labels)]
        return iter(items[:limit] if limit else items)

    def add_tag(self, file_id, tag, colour=None):
        self.added.append((file_id, tag, colour))
        return 1


def test_run_writes_tags_and_counts_them():
    entry = make_entry(id=7)
    repo = StubRepo([entry])
    tagger = AutoTagger([make_rule("big", [{"field": "width", "op": "gte", "value": 4000}])])

    stats = tagger.run(repo)

    assert stats == {"scanned": 1, "changed": 1, "tags": 1}
    assert repo.added == [(7, "big", None)]


def test_run_dry_run_writes_nothing():
    repo = StubRepo([make_entry(id=7)])
    tagger = AutoTagger([make_rule("big", [{"field": "width", "op": "gte", "value": 4000}])])

    stats = tagger.run(repo, dry_run=True)

    assert stats["changed"] == 1
    assert repo.added == []


def test_run_honours_the_path_filter():
    repo = StubRepo([make_entry(id=7)])
    tagger = AutoTagger([make_rule("big", [{"field": "width", "op": "gte", "value": 4000}])])

    stats = tagger.run(repo, paths={"/somewhere/else.jpg"})

    assert stats["scanned"] == 0
    assert repo.added == []


def test_only_untagged_skips_tagged_files():
    repo = StubRepo([make_entry(id=7, labels=["already/here"])])
    tagger = AutoTagger([make_rule("big", [{"field": "width", "op": "gte", "value": 4000}])])

    stats = tagger.run(repo, only_untagged=True)

    assert stats["scanned"] == 0


def test_load_rules_from_json(tmp_path):
    path = tmp_path / "rules.json"
    payload = [{"tag": "big", "conditions": [{"field": "width", "op": "gte", "value": 4000}]}]
    path.write_text(json.dumps(payload), encoding="utf-8")

    rules = load_rules(path)
    assert [rule.tag for rule in rules] == ["big"]


def test_load_rules_from_toml_with_autotag_table(tmp_path):
    path = tmp_path / "rules.toml"
    path.write_text(
        '[[autotag.rules]]\ntag = "big"\n\n'
        '[[autotag.rules.conditions]]\nfield = "width"\nop = "gte"\nvalue = 4000\n',
        encoding="utf-8",
    )
    rules = load_rules(path)
    assert [rule.tag for rule in rules] == ["big"]
