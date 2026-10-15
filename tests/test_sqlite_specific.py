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
    sqlite_repo.add_tag(fid, "tag")
    sqlite_repo.delete_file(fid)
    # the link must be removed by cascade, leaving the tag orphaned
    assert sqlite_repo.get_tag_usage()[0].usage_count == 0


def _first_word(stmt: str) -> str:
    for line in stmt.splitlines():
        line = line.strip()
        if line and not line.startswith("--"):
            return line.split()[0].upper()
    return ""


def test_schema_splits_into_separate_statements():
    """execute() silently swallows concatenated statements — check them one by one."""
    from meret.core.sqlite_repo import SCHEMA_PATH, _iter_statements

    stmts = list(_iter_statements(SCHEMA_PATH.read_text(encoding="utf-8")))
    assert len(stmts) >= 8
    for stmt in stmts:
        assert _first_word(stmt) in {"PRAGMA", "CREATE", "INSERT", "DROP", "ALTER"}, stmt[:80]


def test_schema_creates_all_tables(sqlite_repo):
    conn = sqlite_repo._connect()
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert {"files", "tags", "file_tags"} <= names
