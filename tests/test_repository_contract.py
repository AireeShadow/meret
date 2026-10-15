import pytest

from meret.core import tagpaths
from meret.core.models import FileEntry, FileType


def test_save_and_get_by_path(repo):
    repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    found = repo.get_file_by_path("/x/a.png")
    assert found is not None
    assert found.filename == "a.png"


def test_save_is_upsert_not_duplicate(repo):
    repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE, rating=5))
    assert repo.get_stats()["total_files"] == 1
    assert repo.get_file_by_path("/x/a.png").rating == 5


def test_get_missing_returns_none(repo):
    assert repo.get_file_by_path("/nope") is None
    assert repo.get_file_by_id(999) is None


def test_add_and_read_tags(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "vacation")
    repo.add_tag(fid, "beach")
    assert set(repo.get_file_by_id(fid).labels) == {"vacation", "beach"}


def test_add_tag_twice_is_idempotent(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "vacation")
    repo.add_tag(fid, "vacation")
    assert repo.get_file_by_id(fid).labels == ["vacation"]


def test_tags_case_insensitive(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "Vacation")
    repo.add_tag(fid, "vacation")
    assert len(repo.get_file_by_id(fid).labels) == 1


def test_remove_tag(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "vacation")
    assert repo.remove_tag(fid, "vacation") is True
    assert repo.get_file_by_id(fid).labels == []
    assert repo.remove_tag(fid, "no-such-tag") is False


def test_set_file_tags_replaces(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.set_file_tags(fid, ["a", "b", "c"])
    repo.set_file_tags(fid, ["c", "d"])
    assert set(repo.get_file_by_id(fid).labels) == {"c", "d"}


def test_rename_tag(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "vacation")
    assert repo.rename_tag("vacation", "holidays") is True
    assert repo.get_file_by_id(fid).labels == ["holidays"]
    assert repo.rename_tag("no-such-tag", "x") is False


def test_rename_tag_merges(repo):
    f1 = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    f2 = repo.save_file(FileEntry(filename="b.png", filepath="/x/b.png", type=FileType.IMAGE))
    repo.add_tag(f1, "A")
    repo.add_tag(f2, "B")
    repo.rename_tag("B", "A")
    assert repo.get_file_by_id(f2).labels == ["A"]
    assert {t.name for t in repo.get_all_tags()} == {"A"}


def test_delete_file_removes_links(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "vacation")
    assert repo.delete_file(fid) is True
    assert repo.get_file_by_id(fid) is None
    assert repo.get_tag_usage()[0].usage_count == 0


def test_prune_orphan_tags(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "wanted")
    repo.add_tag(fid, "unwanted")
    repo.remove_tag(fid, "unwanted")
    assert repo.prune_orphan_tags() == 1
    assert [t.name for t in repo.get_all_tags()] == ["wanted"]


def test_search_by_tags_any(repo):
    f1 = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    f2 = repo.save_file(FileEntry(filename="b.png", filepath="/x/b.png", type=FileType.IMAGE))
    repo.add_tag(f1, "vacation")
    repo.add_tag(f2, "work")
    assert {f.filename for f in repo.search_by_tags(["vacation"])} == {"a.png"}
    assert len(repo.search_by_tags(["vacation", "work"])) == 2


def test_search_by_tags_all(repo):
    f1 = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    f2 = repo.save_file(FileEntry(filename="b.png", filepath="/x/b.png", type=FileType.IMAGE))
    repo.set_file_tags(f1, ["vacation", "beach"])
    repo.set_file_tags(f2, ["vacation"])
    assert [f.filename for f in repo.search_by_tags(["vacation", "beach"], match_all=True)] == [
        "a.png"
    ]


def test_search_by_tags_empty_returns_all(repo):
    repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.save_file(FileEntry(filename="b.mp4", filepath="/x/b.mp4", type=FileType.VIDEO))
    assert len(repo.search_by_tags([])) == 2


def test_search_by_tags_file_type_filter(repo):
    f1 = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    f2 = repo.save_file(FileEntry(filename="b.mp4", filepath="/x/b.mp4", type=FileType.VIDEO))
    repo.add_tag(f1, "vacation")
    repo.add_tag(f2, "vacation")
    hits = repo.search_by_tags(["vacation"], file_type="image")
    assert [f.filename for f in hits] == ["a.png"]


def test_search_by_name(repo):
    repo.save_file(FileEntry(filename="sunset_beach.png", filepath="/x/s.png", type=FileType.IMAGE))
    repo.save_file(FileEntry(filename="cat.png", filepath="/x/c.png", type=FileType.IMAGE))
    assert [f.filename for f in repo.search_by_name("beach")] == ["sunset_beach.png"]


def test_tag_tree_hierarchy(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "travel")
    repo.add_tag(fid, "beach")
    assert repo.set_tag_parent("beach", "travel") is True
    tree = repo.get_tag_tree()
    assert [n.name for n in tree] == ["travel"]
    assert [c.name for c in tree[0].children] == ["beach"]
    assert repo.set_tag_parent("beach", "no-such-tag") is False


def test_tag_usage_sorted(repo):
    f1 = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    f2 = repo.save_file(FileEntry(filename="b.png", filepath="/x/b.png", type=FileType.IMAGE))
    repo.set_file_tags(f1, ["often"])
    repo.set_file_tags(f2, ["often", "rarely"])
    usage = repo.get_tag_usage()
    assert usage[0].name == "often"
    assert usage[0].usage_count == 2


def test_stats(repo):
    repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.save_file(FileEntry(filename="b.mp4", filepath="/x/b.mp4", type=FileType.VIDEO))
    stats = repo.get_stats()
    assert stats["total_files"] == 2
    assert stats["by_type"] == {"image": 1, "video": 1}


def test_create_tag(repo):
    assert repo.create_tag("genre") is True
    assert repo.create_tag("genre") is False  # already exists
    assert repo.create_tag("GENRE") is False  # and case doesn't matter
    assert [t.name for t in repo.get_all_tags()] == ["genre"]


def test_create_tag_case_insensitive_non_ascii(repo):
    # Non-ASCII on purpose: SQLite's COLLATE NOCASE only folds ASCII, so this
    # guards the name_key (casefold) logic. "жанр" is Russian for "genre".
    assert repo.create_tag("жанр") is True
    assert repo.create_tag("ЖАНР") is False
    assert [t.name for t in repo.get_all_tags()] == ["жанр"]


def test_set_tag_parent_cycle_is_rejected(repo):
    for name in ("a", "b", "c"):
        repo.create_tag(name)
    assert repo.set_tag_parent("b", "a") is True
    assert repo.set_tag_parent("c", "b") is True
    assert repo.set_tag_parent("a", "c") is False  # a under its own descendant
    assert repo.set_tag_parent("a", "a") is False  # a under itself
    assert [n.name for n in repo.get_tag_tree()] == ["a"]
    assert [n.name for n in repo.get_tag_tree()[0].children] == ["b"]


def test_set_tag_parent_to_root(repo):
    repo.create_tag("a")
    repo.create_tag("b")
    repo.set_tag_parent("b", "a")
    assert repo.set_tag_parent("b", None) is True
    assert sorted(n.name for n in repo.get_tag_tree()) == ["a", "b"]


def test_delete_tag_keeps_children(repo):
    repo.create_tag("genre")
    repo.create_tag("mystery")
    repo.set_tag_parent("mystery", "genre")
    assert repo.delete_tag("genre") is True
    assert [n.name for n in repo.get_tag_tree()] == ["mystery"]
    assert repo.delete_tag("no-such-tag") is False


def test_prune_keeps_parent_of_children(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.create_tag("genre")  # no files, but has a child
    repo.create_tag("mystery")
    repo.set_tag_parent("mystery", "genre")
    repo.add_tag(fid, "unwanted")
    repo.remove_tag(fid, "unwanted")

    # "unwanted" and the empty leaf "mystery" go away; "genre" survives as a parent
    assert repo.prune_orphan_tags() == 2
    assert [t.name for t in repo.get_all_tags()] == ["genre"]


def _paths(repo) -> set[str]:
    by_id = {t.id: t for t in repo.get_all_tags()}
    return {tagpaths.path_of(t, by_id) for t in repo.get_all_tags()}


def test_same_name_under_different_parents(repo):
    assert repo.create_tag("genre") is True
    assert repo.create_tag("studio") is True
    assert repo.create_tag("genre/mystery") is True
    assert repo.create_tag("studio/mystery") is True
    assert _paths(repo) == {"genre", "genre/mystery", "studio", "studio/mystery"}


def test_create_tag_is_strict_path(repo):
    repo.create_tag("genre/mystery")
    assert repo.create_tag("mystery") is True  # a root namesake lives elsewhere
    assert repo.create_tag("genre/mystery") is False
    assert _paths(repo) == {"genre", "genre/mystery", "mystery"}


def test_add_tag_reuses_twin_by_name(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.create_tag("genre/mystery")
    repo.add_tag(fid, "mystery")  # don't spawn a second namesake
    assert _paths(repo) == {"genre", "genre/mystery"}
    assert repo.get_file_by_id(fid).labels == ["mystery"]


def test_add_tag_by_path_creates_parents(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "genre/mystery")
    assert _paths(repo) == {"genre", "genre/mystery"}


def test_delete_tag_by_path_keeps_children(repo):
    repo.create_tag("genre/mystery")
    assert repo.delete_tag("genre/mystery") is True
    assert _paths(repo) == {"genre"}


def test_set_tag_parent_by_path(repo):
    repo.create_tag("genre")
    repo.create_tag("studio")
    repo.create_tag("genre/mystery")
    assert repo.set_tag_parent("genre/mystery", "studio") is True
    assert _paths(repo) == {"genre", "studio", "studio/mystery"}


def test_set_tag_parent_path_is_strict(repo):
    repo.create_tag("genre")
    assert repo.set_tag_parent("genre/missing", "genre") is False


def test_search_by_tag_ids_distinguishes_namesakes(repo):
    f1 = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    f2 = repo.save_file(FileEntry(filename="b.png", filepath="/x/b.png", type=FileType.IMAGE))
    repo.create_tag("genre/mystery")
    repo.create_tag("studio/mystery")
    repo.add_tag(f1, "genre/mystery")
    repo.add_tag(f2, "studio/mystery")

    by_id = {t.id: t for t in repo.get_all_tags()}
    genre = tagpaths.resolve("genre/mystery", repo.get_all_tags())

    assert [f.filename for f in repo.search_by_tag_ids([[genre.id]])] == ["a.png"]

def test_known_fingerprints_tracks_size_and_mtime(repo):
    assert repo.known_fingerprints() == {}

    repo.save_file(
        FileEntry(
            filename="a.png",
            filepath="/x/a.png",
            type=FileType.IMAGE,
            metadata={"size_bytes": 1234, "modified_at": 1700000000.0},
        )
    )
    assert repo.known_fingerprints() == {"/x/a.png": (1234, 1700000000.0)}
