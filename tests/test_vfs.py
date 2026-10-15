"""Virtual FS tests: tags as folders."""

import pytest

from meret.core.models import FileEntry, FileType
from meret.vfs import IsADirectory, NotFound, VirtualTree, sanitize


@pytest.fixture
def tree(mock_repo):
    return VirtualTree(mock_repo)


@pytest.fixture
def populated(mock_repo, tmp_path):
    """Tree: travel/seaside + two same-named files tagged 'seaside'."""
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
    mock_repo.add_tag(f1, "travel")
    mock_repo.add_tag(f1, "seaside")
    mock_repo.add_tag(f2, "seaside")
    mock_repo.set_tag_parent("seaside", "travel")
    return VirtualTree(mock_repo), real


# ---------- structure ----------


def test_root_lists_root_tags(tree, mock_repo):
    f = mock_repo.save_file(FileEntry(filename="a.png", filepath="/a.png", type=FileType.IMAGE))
    mock_repo.add_tag(f, "travel")

    nodes = tree.listdir("/")
    assert [n.name for n in nodes] == ["travel"]
    assert nodes[0].is_dir


def test_nested_listing_shows_subtag(populated):
    vfs, _ = populated
    names = {n.name for n in vfs.listdir("/travel")}
    assert "seaside" in names


def test_files_under_leaf_tag(populated):
    vfs, _ = populated
    nodes = vfs.listdir("/travel/seaside")
    assert len(nodes) == 2
    assert all(not n.is_dir for n in nodes)


def test_collision_deduped(populated):
    vfs, _ = populated
    names = [n.name for n in vfs.listdir("/travel/seaside")]
    assert len(set(names)) == 2


def test_root_is_empty_without_tags(tree):
    assert tree.listdir("/") == []


# ---------- stat ----------


def test_stat_dir_and_file(populated):
    vfs, _ = populated
    assert vfs.stat("/travel").is_dir

    node = vfs.stat("/travel/seaside/beach.png")
    assert not node.is_dir
    assert node.size == 7


def test_root_stat(tree):
    assert tree.stat("/").is_dir


def test_isdir(tree, mock_repo):
    f = mock_repo.save_file(FileEntry(filename="a.png", filepath="/a.png", type=FileType.IMAGE))
    mock_repo.add_tag(f, "tag")
    assert tree.isdir("/tag") is True
    assert tree.isdir("/missing") is False


# ---------- reading ----------


def test_read_bytes(populated):
    vfs, _ = populated
    assert vfs.read("/travel/seaside/beach.png") == b"PNGDATA"


def test_real_path(populated):
    vfs, real = populated
    assert vfs.real_path("/travel/seaside/beach.png") == real


def test_read_directory_raises(populated):
    vfs, _ = populated
    with pytest.raises(IsADirectory):
        vfs.read("/travel")


# ---------- errors ----------


def test_not_found(tree):
    with pytest.raises(NotFound):
        tree.stat("/no/such")


def test_exists(tree, mock_repo):
    f = mock_repo.save_file(FileEntry(filename="a.png", filepath="/a.png", type=FileType.IMAGE))
    mock_repo.add_tag(f, "tag")
    assert tree.exists("/tag") is True
    assert tree.exists("/missing") is False


def test_deep_missing_path(tree):
    with pytest.raises(NotFound):
        tree.listdir("/a/b/c")


# ---------- utilities ----------


def test_walk_visits_everything(populated):
    vfs, _ = populated
    paths = [n.vpath for n in vfs.walk("/")]
    assert "/travel" in paths
    assert "/travel/seaside" in paths


def test_normalize_paths(tree):
    assert tree.normalize("travel//seaside/") == "/travel/seaside"
    assert tree.normalize("/") == "/"


def test_sanitize_forbidden_chars():
    assert sanitize("a/b") == "a∕b"
    assert sanitize("  ") == "_"


def test_tag_index_matches_tagpaths(tree, mock_repo):
    """The VFS index is exactly what tagpaths builds (not its own copy of the logic)."""
    from meret.core import tagpaths

    mock_repo.create_tag("genre")
    mock_repo.create_tag("mystery")
    mock_repo.set_tag_parent("mystery", "genre")

    tags = mock_repo.get_all_tags()
    expected = tagpaths.index_by_parent(tags, key=lambda t: sanitize(t.name).casefold())
    assert tree._tag_index() == expected
    assert len(expected) == 2


def test_resolve_tag_uses_sanitized_name(tree, mock_repo):
    """The index key is sanitize(name), so a ":" in the name doesn't break the path."""
    mock_repo.create_tag("a:b")
    assert tree.stat("/a∕b").is_dir
