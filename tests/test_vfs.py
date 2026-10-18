"""Virtual FS tests: tags as folders."""

import pytest

from meret.core.models import FileEntry, FileType
from meret.vfs import IsADirectory, NotFound, SourceFile, VirtualTree, sanitize
from tests.mocks import MockRepository


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


# ---------- cache ----------


def _names(nodes):
    return sorted(n.name for n in nodes)


def test_cache_sees_new_file_after_tagging(repo):
    """Both repositories: a write invalidates the snapshot via data_version()."""
    tree = VirtualTree(repo)
    a = repo.save_file(FileEntry(filename="a.png", filepath="/a.png", type=FileType.IMAGE))
    repo.add_tag(a, "trip")
    assert _names(tree.listdir("/trip")) == ["a.png"]

    b = repo.save_file(FileEntry(filename="b.png", filepath="/b.png", type=FileType.IMAGE))
    repo.add_tag(b, "trip")
    assert _names(tree.listdir("/trip")) == ["a.png", "b.png"]
    assert tree.stat("/trip/b.png").file_id == b


def test_cache_follows_tag_rename_and_untag(repo):
    tree = VirtualTree(repo)
    a = repo.save_file(FileEntry(filename="a.png", filepath="/a.png", type=FileType.IMAGE))
    repo.add_tag(a, "trip")
    assert tree.exists("/trip/a.png")

    repo.rename_tag("trip", "voyage")
    assert not tree.exists("/trip")
    assert tree.exists("/voyage/a.png")

    repo.remove_tag(a, "voyage")
    assert not tree.exists("/voyage/a.png")


def test_cache_sees_writes_from_another_connection(tmp_path):
    """A second process (`meret tag` while mounted) is just another connection."""
    from meret.core.sqlite_repo import SQLiteRepository

    db = str(tmp_path / "meret.db")
    tree = VirtualTree(SQLiteRepository(db))
    assert tree.listdir("/") == []

    other = SQLiteRepository(db)
    f = other.save_file(FileEntry(filename="a.png", filepath="/a.png", type=FileType.IMAGE))
    other.add_tag(f, "trip")
    assert _names(tree.listdir("/")) == ["trip"]


def _spy(monkeypatch, repo, *names):
    from collections import Counter

    calls = Counter()
    for name in names:
        original = getattr(repo, name)

        def spy(*args, _orig=original, _name=name, **kwargs):
            calls[_name] += 1
            return _orig(*args, **kwargs)

        monkeypatch.setattr(repo, name, spy)
    return calls


def test_stat_per_entry_does_not_requery(mock_repo, monkeypatch):
    """What Explorer / ls -l do: list a folder, then stat every entry."""
    mock_repo.create_tag("bulk/sub")
    for i in range(50):
        f = mock_repo.save_file(
            FileEntry(filename=f"f{i}.png", filepath=f"/f{i}.png", type=FileType.IMAGE)
        )
        mock_repo.add_tag(f, "bulk")

    tree = VirtualTree(mock_repo)
    calls = _spy(monkeypatch, mock_repo, "get_all_tags", "search_by_tag_ids")

    nodes = tree.listdir("/bulk")
    for node in nodes:
        assert tree.stat(node.vpath) == node
    tree.stat("/bulk")
    tree.listdir("/bulk")

    assert len(nodes) == 51
    assert calls == {"get_all_tags": 1, "search_by_tag_ids": 1}


class _NoVersionRepo(MockRepository):
    """A repository that can't report changes: the tree falls back to a TTL."""

    def data_version(self):
        return None


def test_without_data_version_ttl_zero_always_rereads():
    repo = _NoVersionRepo()
    tree = VirtualTree(repo, ttl=0)
    assert tree.listdir("/") == []
    repo.create_tag("trip")
    assert _names(tree.listdir("/")) == ["trip"]


def test_without_data_version_snapshot_held_until_invalidate():
    repo = _NoVersionRepo()
    tree = VirtualTree(repo, ttl=3600)
    assert tree.listdir("/") == []
    repo.create_tag("trip")
    assert tree.listdir("/") == []  # stale by design within the TTL

    tree.invalidate()
    assert _names(tree.listdir("/")) == ["trip"]


def test_file_mtime_from_metadata(mock_repo):
    f = mock_repo.save_file(
        FileEntry(
            filename="a.png",
            filepath="/a.png",
            type=FileType.IMAGE,
            metadata={"size_bytes": 3, "modified_at": 1_700_000_000.0},
        )
    )
    mock_repo.add_tag(f, "trip")
    tree = VirtualTree(mock_repo)
    assert tree.listdir("/trip")[0].mtime == 1_700_000_000.0
    assert tree.stat("/trip/a.png").mtime == 1_700_000_000.0


def test_file_size_and_mtime_from_disk_without_metadata(mock_repo, tmp_path):
    real = tmp_path / "a.png"
    real.write_bytes(b"12345")
    f = mock_repo.save_file(FileEntry(filename="a.png", filepath=str(real), type=FileType.IMAGE))
    mock_repo.add_tag(f, "trip")

    node = VirtualTree(mock_repo).stat("/trip/a.png")
    assert node.size == 5
    assert node.mtime == real.stat().st_mtime


# ---------- ranged reads ----------


@pytest.mark.parametrize(
    ("offset", "length", "expected"),
    [(0, 3, b"PNG"), (3, 100, b"DATA"), (6, 1, b"A"), (7, 10, b""), (100, 5, b""), (2, 0, b"")],
)
def test_read_range(populated, offset, length, expected):
    vfs, _ = populated
    assert vfs.read_range("/travel/seaside/beach.png", offset, length) == expected


def test_read_range_directory_raises(populated):
    vfs, _ = populated
    with pytest.raises(IsADirectory):
        vfs.read_range("/travel", 0, 10)


def test_open_source_missing_on_disk_is_not_found(mock_repo, tmp_path):
    f = mock_repo.save_file(
        FileEntry(filename="gone.png", filepath=str(tmp_path / "gone.png"), type=FileType.IMAGE)
    )
    mock_repo.add_tag(f, "trip")
    with pytest.raises(NotFound):
        VirtualTree(mock_repo).open_source("/trip/gone.png")


def test_source_file_parallel_reads(tmp_path):
    """One handle, many threads, different offsets — no shared file position."""
    from concurrent.futures import ThreadPoolExecutor

    data = bytes(range(256)) * 4096  # 1 MiB
    path = tmp_path / "big.bin"
    path.write_bytes(data)

    chunk = 4096
    offsets = list(range(0, len(data), chunk)) * 4
    with SourceFile(path) as src, ThreadPoolExecutor(max_workers=8) as pool:
        assert src.size == len(data)
        results = list(pool.map(lambda off: (off, src.pread(off, chunk)), offsets))

    for off, got in results:
        assert got == data[off : off + chunk]


def test_source_file_close_is_idempotent(tmp_path):
    path = tmp_path / "a.bin"
    path.write_bytes(b"x")
    src = SourceFile(path)
    src.close()
    src.close()

def test_node_vpath_format(tree, mock_repo):
    """Node vpath must be absolute path starting with /."""
    f = mock_repo.save_file(FileEntry(filename="a.png", filepath="/a.png", type=FileType.IMAGE))
    mock_repo.add_tag(f, "travel")
    
    nodes = tree.listdir("/")
    assert nodes[0].vpath == "/travel", f"Expected '/travel', got {nodes[0].vpath!r}"
    
    # Nested
    mock_repo.create_tag("beach")
    mock_repo.set_tag_parent("beach", "travel")
    
    travel_nodes = tree.listdir("/travel")
    assert travel_nodes[0].vpath == "/travel/beach", f"Expected '/travel/beach', got {travel_nodes[0].vpath!r}"
