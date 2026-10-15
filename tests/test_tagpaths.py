import pytest

from meret.core import tagpaths
from meret.core.models import Tag


@pytest.fixture
def tags():
    return [
        Tag(id=1, name="genre"),
        Tag(id=2, name="mystery", parent_id=1),
        Tag(id=3, name="noir", parent_id=2),
        Tag(id=4, name="studio"),
    ]


def test_normalize():
    assert tagpaths.normalize(" /genre//mystery/ ") == "genre/mystery"
    assert tagpaths.normalize("/") == ""
    assert tagpaths.normalize("") == ""


def test_path_map(tags):
    assert set(tagpaths.path_map(tags)) == {
        "genre", "genre/mystery", "genre/mystery/noir", "studio"
    }


def test_resolve_case_insensitive(tags):
    assert tagpaths.resolve("/GENRE/Mystery", tags).name == "mystery"
    assert tagpaths.resolve("no/such", tags) is None


def test_missing_ancestors(tags):
    assert tagpaths.missing_ancestors("genre/mystery/noir", tags) == []
    assert tagpaths.missing_ancestors("genre/thriller", tags) == ["genre/thriller"]
    assert tagpaths.missing_ancestors("section/sub/leaf", tags) == [
        "section", "section/sub", "section/sub/leaf"
    ]


def test_subtree_names(tags):
    assert set(tagpaths.subtree_names("genre", tags)) == {"genre", "mystery", "noir"}
    assert tagpaths.subtree_names("studio", tags) == ["studio"]


def test_path_of_survives_cycle():
    broken = [Tag(id=1, name="a", parent_id=2), Tag(id=2, name="b", parent_id=1)]
    by_id = {t.id: t for t in broken}
    assert tagpaths.path_of(broken[0], by_id) == "b/a"     # no infinite loop

def test_resolve_falls_back_to_name(tags):
    # "noir" lives at genre/mystery/noir — found by name too
    assert tagpaths.resolve("noir", tags).name == "noir"
    assert tagpaths.resolve("GENRE/Mystery/Noir", tags).name == "noir"
    assert tagpaths.resolve("no/such", tags) is None


def test_name_map(tags):
    assert tagpaths.name_map(tags)["studio"].name == "studio"

def _tag(id: int, name: str, parent_id=None) -> Tag:
    return Tag(id=id, name=name, parent_id=parent_id)


def test_resolve_prefers_root_for_twins():
    tags = [_tag(1, "genre"), _tag(2, "mystery", 1), _tag(3, "mystery")]
    assert tagpaths.resolve("mystery", tags).id == 3
    assert tagpaths.resolve("genre/mystery", tags).id == 2


def test_resolve_all_returns_twins():
    tags = [_tag(1, "genre"), _tag(2, "mystery", 1), _tag(3, "mystery")]
    assert {t.id for t in tagpaths.resolve_all("mystery", tags)} == {2, 3}


def test_name_to_path_prefers_shallowest():
    tags = [_tag(1, "genre"), _tag(2, "mystery", 1), _tag(3, "mystery")]
    assert tagpaths.name_to_path(tags)["mystery"] == "mystery"
