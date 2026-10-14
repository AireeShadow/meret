import pytest

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
    repo.add_tag(fid, "отпуск")
    repo.add_tag(fid, "пляж")
    assert set(repo.get_file_by_id(fid).labels) == {"отпуск", "пляж"}


def test_add_tag_twice_is_idempotent(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "отпуск")
    repo.add_tag(fid, "отпуск")
    assert repo.get_file_by_id(fid).labels == ["отпуск"]


def test_tags_case_insensitive(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "Vacation")
    repo.add_tag(fid, "vacation")
    assert len(repo.get_file_by_id(fid).labels) == 1


def test_remove_tag(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "отпуск")
    assert repo.remove_tag(fid, "отпуск") is True
    assert repo.get_file_by_id(fid).labels == []
    assert repo.remove_tag(fid, "нет такого") is False


def test_set_file_tags_replaces(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.set_file_tags(fid, ["a", "b", "c"])
    repo.set_file_tags(fid, ["c", "d"])
    assert set(repo.get_file_by_id(fid).labels) == {"c", "d"}


def test_rename_tag(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "отпуск")
    assert repo.rename_tag("отпуск", "каникулы") is True
    assert repo.get_file_by_id(fid).labels == ["каникулы"]
    assert repo.rename_tag("нет такого", "x") is False


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
    repo.add_tag(fid, "отпуск")
    assert repo.delete_file(fid) is True
    assert repo.get_file_by_id(fid) is None
    assert repo.get_tag_usage()[0].usage_count == 0


def test_prune_orphan_tags(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "нужный")
    repo.add_tag(fid, "лишний")
    repo.remove_tag(fid, "лишний")
    assert repo.prune_orphan_tags() == 1
    assert [t.name for t in repo.get_all_tags()] == ["нужный"]


def test_search_by_tags_any(repo):
    f1 = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    f2 = repo.save_file(FileEntry(filename="b.png", filepath="/x/b.png", type=FileType.IMAGE))
    repo.add_tag(f1, "отпуск")
    repo.add_tag(f2, "работа")
    assert {f.filename for f in repo.search_by_tags(["отпуск"])} == {"a.png"}
    assert len(repo.search_by_tags(["отпуск", "работа"])) == 2


def test_search_by_tags_all(repo):
    f1 = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    f2 = repo.save_file(FileEntry(filename="b.png", filepath="/x/b.png", type=FileType.IMAGE))
    repo.set_file_tags(f1, ["отпуск", "пляж"])
    repo.set_file_tags(f2, ["отпуск"])
    assert [f.filename for f in repo.search_by_tags(["отпуск", "пляж"], match_all=True)] == ["a.png"]


def test_search_by_tags_empty_returns_all(repo):
    repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.save_file(FileEntry(filename="b.mp4", filepath="/x/b.mp4", type=FileType.VIDEO))
    assert len(repo.search_by_tags([])) == 2


def test_search_by_tags_file_type_filter(repo):
    f1 = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    f2 = repo.save_file(FileEntry(filename="b.mp4", filepath="/x/b.mp4", type=FileType.VIDEO))
    repo.add_tag(f1, "отпуск")
    repo.add_tag(f2, "отпуск")
    hits = repo.search_by_tags(["отпуск"], file_type="image")
    assert [f.filename for f in hits] == ["a.png"]


def test_search_by_name(repo):
    repo.save_file(FileEntry(filename="sunset_beach.png", filepath="/x/s.png", type=FileType.IMAGE))
    repo.save_file(FileEntry(filename="cat.png", filepath="/x/c.png", type=FileType.IMAGE))
    assert [f.filename for f in repo.search_by_name("beach")] == ["sunset_beach.png"]


def test_tag_tree_hierarchy(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.add_tag(fid, "путешествия")
    repo.add_tag(fid, "пляж")
    assert repo.set_tag_parent("пляж", "путешествия") is True
    tree = repo.get_tag_tree()
    assert [n.name for n in tree] == ["путешествия"]
    assert [c.name for c in tree[0].children] == ["пляж"]
    assert repo.set_tag_parent("пляж", "нет такого") is False


def test_tag_usage_sorted(repo):
    f1 = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    f2 = repo.save_file(FileEntry(filename="b.png", filepath="/x/b.png", type=FileType.IMAGE))
    repo.set_file_tags(f1, ["часто"])
    repo.set_file_tags(f2, ["часто", "реже"])
    usage = repo.get_tag_usage()
    assert usage[0].name == "часто"
    assert usage[0].usage_count == 2


def test_stats(repo):
    repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.save_file(FileEntry(filename="b.mp4", filepath="/x/b.mp4", type=FileType.VIDEO))
    stats = repo.get_stats()
    assert stats["total_files"] == 2
    assert stats["by_type"] == {"image": 1, "video": 1}

def test_create_tag(repo):
    assert repo.create_tag("жанр") is True
    assert repo.create_tag("жанр") is False        # уже есть
    assert repo.create_tag("ЖАНР") is False        # и регистр не важен
    assert [t.name for t in repo.get_all_tags()] == ["жанр"]


def test_set_tag_parent_cycle_is_rejected(repo):
    for name in ("a", "b", "c"):
        repo.create_tag(name)
    assert repo.set_tag_parent("b", "a") is True
    assert repo.set_tag_parent("c", "b") is True
    assert repo.set_tag_parent("a", "c") is False   # a под своим потомком
    assert repo.set_tag_parent("a", "a") is False   # сам под собой
    assert [n.name for n in repo.get_tag_tree()] == ["a"]
    assert [n.name for n in repo.get_tag_tree()[0].children] == ["b"]


def test_set_tag_parent_to_root(repo):
    repo.create_tag("a")
    repo.create_tag("b")
    repo.set_tag_parent("b", "a")
    assert repo.set_tag_parent("b", None) is True
    assert sorted(n.name for n in repo.get_tag_tree()) == ["a", "b"]


def test_delete_tag_keeps_children(repo):
    repo.create_tag("жанр")
    repo.create_tag("детектив")
    repo.set_tag_parent("детектив", "жанр")
    assert repo.delete_tag("жанр") is True
    assert [n.name for n in repo.get_tag_tree()] == ["детектив"]
    assert repo.delete_tag("нет такого") is False


def test_prune_keeps_parent_of_children(repo):
    fid = repo.save_file(FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE))
    repo.create_tag("жанр")            # без файлов, но с ребёнком
    repo.create_tag("детектив")
    repo.set_tag_parent("детектив", "жанр")
    repo.add_tag(fid, "лишний")
    repo.remove_tag(fid, "лишний")

    # уходят «лишний» и пустой лист «детектив»; «жанр» выживает как родитель
    assert repo.prune_orphan_tags() == 2
    assert [t.name for t in repo.get_all_tags()] == ["жанр"]