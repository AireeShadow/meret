from meret.core.models import FileEntry, FileType


def test_fts_finds_by_word(sqlite_repo):
    sqlite_repo.save_file(
        FileEntry(filename="sunset_beach.png", filepath="/x/s.png", type=FileType.IMAGE)
    )
    assert sqlite_repo.search_by_name("beach")


def test_fts_prefix(sqlite_repo):
    sqlite_repo.save_file(
        FileEntry(filename="vacation2023.png", filepath="/x/v.png", type=FileType.IMAGE)
    )
    assert sqlite_repo.search_by_name("vacation")


def test_foreign_keys_cascade(sqlite_repo):
    fid = sqlite_repo.save_file(
        FileEntry(filename="a.png", filepath="/x/a.png", type=FileType.IMAGE)
    )
    sqlite_repo.add_tag(fid, "тег")
    sqlite_repo.delete_file(fid)
    # связь должна уехать каскадом, тег остаться сиротой
    assert sqlite_repo.get_tag_usage()[0].usage_count == 0