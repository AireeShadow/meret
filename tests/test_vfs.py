"""Тесты виртуальной ФС: теги как папки."""

import pytest

from meret.core.models import FileEntry, FileType
from meret.vfs import IsADirectory, NotFound, VirtualTree, sanitize


@pytest.fixture
def tree(mock_repo):
    return VirtualTree(mock_repo)


@pytest.fixture
def populated(mock_repo, tmp_path):
    """Дерево: путешествия/пляж + два одноимённых файла с тегом 'пляж'."""
    real = tmp_path / "beach.png"
    real.write_bytes(b"PNGDATA")

    f1 = mock_repo.save_file(
        FileEntry(
            filename="beach.png",
            filepath=str(real),
            type=FileType.IMAGE,
            metadata={"size_bytes": 7},
        )
    )
    f2 = mock_repo.save_file(
        FileEntry(
            filename="beach.png",
            filepath="/other/beach.png",
            type=FileType.IMAGE,
            metadata={"size_bytes": 7},
        )
    )
    mock_repo.add_tag(f1, "путешествия")
    mock_repo.add_tag(f1, "пляж")
    mock_repo.add_tag(f2, "пляж")
    mock_repo.set_tag_parent("пляж", "путешествия")
    return VirtualTree(mock_repo), real


# ---------- структура ----------

def test_root_lists_root_tags(tree, mock_repo):
    f = mock_repo.save_file(
        FileEntry(filename="a.png", filepath="/a.png", type=FileType.IMAGE)
    )
    mock_repo.add_tag(f, "путешествия")

    nodes = tree.listdir("/")
    assert [n.name for n in nodes] == ["путешествия"]
    assert nodes[0].is_dir


def test_nested_listing_shows_subtag(populated):
    vfs, _ = populated
    names = {n.name for n in vfs.listdir("/путешествия")}
    assert "пляж" in names


def test_files_under_leaf_tag(populated):
    vfs, _ = populated
    nodes = vfs.listdir("/путешествия/пляж")
    assert len(nodes) == 2
    assert all(not n.is_dir for n in nodes)


def test_collision_deduped(populated):
    vfs, _ = populated
    names = [n.name for n in vfs.listdir("/путешествия/пляж")]
    assert len(set(names)) == 2


def test_root_is_empty_without_tags(tree):
    assert tree.listdir("/") == []


# ---------- stat ----------

def test_stat_dir_and_file(populated):
    vfs, _ = populated
    assert vfs.stat("/путешествия").is_dir

    node = vfs.stat("/путешествия/пляж/beach.png")
    assert not node.is_dir
    assert node.size == 7


def test_root_stat(tree):
    assert tree.stat("/").is_dir


def test_isdir(tree, mock_repo):
    f = mock_repo.save_file(
        FileEntry(filename="a.png", filepath="/a.png", type=FileType.IMAGE)
    )
    mock_repo.add_tag(f, "тег")
    assert tree.isdir("/тег") is True
    assert tree.isdir("/нет") is False


# ---------- чтение ----------

def test_read_bytes(populated):
    vfs, _ = populated
    assert vfs.read("/путешествия/пляж/beach.png") == b"PNGDATA"


def test_real_path(populated):
    vfs, real = populated
    assert vfs.real_path("/путешествия/пляж/beach.png") == real


def test_read_directory_raises(populated):
    vfs, _ = populated
    with pytest.raises(IsADirectory):
        vfs.read("/путешествия")


# ---------- ошибки ----------

def test_not_found(tree):
    with pytest.raises(NotFound):
        tree.stat("/нет/такого")


def test_exists(tree, mock_repo):
    f = mock_repo.save_file(
        FileEntry(filename="a.png", filepath="/a.png", type=FileType.IMAGE)
    )
    mock_repo.add_tag(f, "тег")
    assert tree.exists("/тег") is True
    assert tree.exists("/нет") is False


def test_deep_missing_path(tree):
    with pytest.raises(NotFound):
        tree.listdir("/a/b/c")


# ---------- утилиты ----------

def test_walk_visits_everything(populated):
    vfs, _ = populated
    paths = [n.vpath for n in vfs.walk("/")]
    assert "/путешествия" in paths
    assert "/путешествия/пляж" in paths


def test_normalize_paths(tree):
    assert tree.normalize("путешествия//пляж/") == "/путешествия/пляж"
    assert tree.normalize("/") == "/"


def test_sanitize_forbidden_chars():
    assert sanitize("a/b") == "a∕b"
    assert sanitize("  ") == "_"

def test_tag_index_matches_tagpaths(tree, mock_repo):
    """Индекс VFS — ровно то, что строит tagpaths (а не своя копия логики)."""
    from meret.core import tagpaths

    mock_repo.create_tag("жанр")
    mock_repo.create_tag("детектив")
    mock_repo.set_tag_parent("детектив", "жанр")

    tags = mock_repo.get_all_tags()
    expected = tagpaths.index_by_parent(tags, key=lambda t: sanitize(t.name).casefold())
    assert tree._tag_index() == expected
    assert len(expected) == 2


def test_resolve_tag_uses_sanitized_name(tree, mock_repo):
    """Ключ индекса — sanitize(name), поэтому слэш в имени тега не ломает путь."""
    mock_repo.create_tag("a/b")
    assert tree.stat("/a∕b").is_dir