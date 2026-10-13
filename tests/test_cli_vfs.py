from pathlib import Path

import pytest
from typer.testing import CliRunner

import meret.cli.vfs_cmds as vfs_cmds
from meret.cli.app import app
from meret.core.models import FileEntry, FileType
from meret.core.sqlite_repo import SQLiteRepository

runner = CliRunner()


@pytest.fixture
def db(tmp_path):
    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    real = tmp_path / "beach.png"
    real.write_bytes(b"PNGDATA")

    f1 = repo.save_file(
        FileEntry(
            filename="beach.png",
            filepath=str(real),
            type=FileType.IMAGE,
            metadata={"size_bytes": 7},
        )
    )
    repo.add_tag(f1, "путешествия")
    repo.add_tag(f1, "пляж")
    repo.set_tag_parent("пляж", "путешествия")
    return tmp_path


def test_ls_root(db):
    r = runner.invoke(app, ["ls", "/", "--db", str(db)])
    assert r.exit_code == 0
    assert "путешествия" in r.output


def test_ls_nested_shows_subtag(db):
    r = runner.invoke(app, ["ls", "/путешествия", "--db", str(db)])
    assert r.exit_code == 0
    assert "пляж" in r.output


def test_ls_lists_files(db):
    r = runner.invoke(app, ["ls", "/путешествия/пляж", "--db", str(db)])
    assert r.exit_code == 0
    assert "beach.png" in r.output


def test_ls_long_shows_size(db):
    r = runner.invoke(app, ["ls", "/путешествия/пляж", "--long", "--db", str(db)])
    assert "7 B" in r.output


def test_ls_missing_path(db):
    r = runner.invoke(app, ["ls", "/нет", "--db", str(db)])
    assert r.exit_code == 1
    assert "Нет такого пути" in r.output


def test_ls_empty_tag(db):
    repo = SQLiteRepository(str(db / "meret.db"))
    f = repo.save_file(
        FileEntry(filename="x.png", filepath="/x.png", type=FileType.IMAGE)
    )
    repo.add_tag(f, "пустой")
    repo.remove_tag(f, "пустой")      # ← тег живёт, файлов нет

    r = runner.invoke(app, ["ls", "/пустой", "--db", str(db)])
    assert r.exit_code == 0
    assert "пусто" in r.output

def test_ls_tag_with_file(db):
    """А это — тег, у которого файл есть."""
    repo = SQLiteRepository(str(db / "meret.db"))
    f = repo.save_file(
        FileEntry(filename="x.png", filepath="/x.png", type=FileType.IMAGE)
    )
    repo.add_tag(f, "непустой")

    r = runner.invoke(app, ["ls", "/непустой", "--db", str(db)])
    assert "x.png" in r.output


def test_open_path_only(db):
    r = runner.invoke(
        app, ["open", "/путешествия/пляж/beach.png", "--path", "--db", str(db)]
    )
    assert r.exit_code == 0
    assert "beach.png" in r.output


def test_open_calls_external(db, monkeypatch):
    called = {}
    monkeypatch.setattr(vfs_cmds, "_open_external", lambda p: called.setdefault("p", p))

    r = runner.invoke(app, ["open", "/путешествия/пляж/beach.png", "--db", str(db)])
    assert r.exit_code == 0
    assert called["p"].name == "beach.png"


def test_open_directory_fails(db):
    r = runner.invoke(app, ["open", "/путешествия", "--db", str(db)])
    assert r.exit_code == 1
    assert "каталог" in r.output


def test_open_missing(db):
    r = runner.invoke(app, ["open", "/нет/файла", "--db", str(db)])
    assert r.exit_code == 1


def test_stat_file(db):
    r = runner.invoke(app, ["stat", "/путешествия/пляж/beach.png", "--db", str(db)])
    assert r.exit_code == 0
    assert "файл" in r.output
    assert "7 B" in r.output


def test_stat_directory(db):
    r = runner.invoke(app, ["stat", "/путешествия", "--db", str(db)])
    assert r.exit_code == 0
    assert "каталог" in r.output