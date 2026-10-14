import pytest

from meret.core import tagpaths
from meret.core.models import Tag


@pytest.fixture
def tags():
    return [
        Tag(id=1, name="жанр"),
        Tag(id=2, name="детектив", parent_id=1),
        Tag(id=3, name="нуар", parent_id=2),
        Tag(id=4, name="студия"),
    ]


def test_normalize():
    assert tagpaths.normalize(" /жанр//детектив/ ") == "жанр/детектив"
    assert tagpaths.normalize("/") == ""
    assert tagpaths.normalize("") == ""


def test_path_map(tags):
    assert set(tagpaths.path_map(tags)) == {
        "жанр", "жанр/детектив", "жанр/детектив/нуар", "студия"
    }


def test_resolve_case_insensitive(tags):
    assert tagpaths.resolve("/ЖАНР/Детектив", tags).name == "детектив"
    assert tagpaths.resolve("нет/такого", tags) is None


def test_missing_ancestors(tags):
    assert tagpaths.missing_ancestors("жанр/детектив/нуар", tags) == []
    assert tagpaths.missing_ancestors("жанр/триллер", tags) == ["жанр/триллер"]
    assert tagpaths.missing_ancestors("раздел/под/лист", tags) == [
        "раздел", "раздел/под", "раздел/под/лист"
    ]


def test_subtree_names(tags):
    assert set(tagpaths.subtree_names("жанр", tags)) == {"жанр", "детектив", "нуар"}
    assert tagpaths.subtree_names("студия", tags) == ["студия"]


def test_path_of_survives_cycle():
    broken = [Tag(id=1, name="a", parent_id=2), Tag(id=2, name="b", parent_id=1)]
    by_id = {t.id: t for t in broken}
    assert tagpaths.path_of(broken[0], by_id) == "b/a"     # не зацикливаемся