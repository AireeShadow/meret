"""Путь к БД: без базы — ошибка, создаёт только init."""
import pytest
from typer.testing import CliRunner

from meret.cli.app import app
from meret.cli.deps import DB_NAME, DatabaseNotFound, get_repo, resolve_db_path

runner = CliRunner()


def test_resolve_is_pure(tmp_path):
    target = resolve_db_path(tmp_path)
    assert target == tmp_path / DB_NAME
    assert not target.exists()


def test_explicit_db_file(tmp_path):
    target = tmp_path / "custom.db"
    assert resolve_db_path(target) == target


def test_get_repo_raises_without_db(tmp_path):
    with pytest.raises(DatabaseNotFound):
        get_repo(tmp_path)


def test_get_repo_creates_when_asked(tmp_path):
    repo = get_repo(tmp_path, create=True)
    assert (tmp_path / DB_NAME).is_file()
    assert repo.get_stats()["total_files"] == 0


def test_command_without_db_errors(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    res = runner.invoke(app, ["tags"])
    assert res.exit_code == 1
    assert "не найдена" in res.output


def test_init_then_read(tmp_path):
    res = runner.invoke(app, ["init", str(tmp_path)])
    assert res.exit_code == 0, res.output

    res = runner.invoke(app, ["tags", "--db", str(tmp_path)])
    assert res.exit_code == 0, res.output


def test_where_shows_path(tmp_path):
    res = runner.invoke(app, ["where", "--db", str(tmp_path)])
    assert res.exit_code == 0, res.output
    assert "meret.db" in res.output