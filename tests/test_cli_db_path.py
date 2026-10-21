"""DB path: no database is an error; only init creates one."""

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
    assert "not found" in res.output


def test_init_then_read(tmp_path):
    res = runner.invoke(app, ["init", str(tmp_path)])
    assert res.exit_code == 0, res.output

    res = runner.invoke(app, ["tags", "--db", str(tmp_path)])
    assert res.exit_code == 0, res.output


def test_where_shows_path(tmp_path):
    res = runner.invoke(app, ["where", "--db", str(tmp_path)])
    assert res.exit_code == 0, res.output
    assert "meret.db" in res.output


# ---------- lookup order: argument > $MERET_DB > config > ./meret.db ----------


def test_explicit_path_beats_env(tmp_path, monkeypatch):
    monkeypatch.setenv("MERET_DB", str(tmp_path / "elsewhere.db"))
    assert resolve_db_path(tmp_path) == tmp_path / DB_NAME
    assert resolve_db_path(tmp_path / "custom.db") == tmp_path / "custom.db"


@pytest.mark.parametrize("as_dir", [True, False])
def test_env_used_without_a_path(tmp_path, monkeypatch, as_dir):
    target = tmp_path if as_dir else tmp_path / "lib.db"
    monkeypatch.setenv("MERET_DB", str(target))
    assert resolve_db_path() == (tmp_path / DB_NAME if as_dir else target)


def test_env_beats_config_and_config_beats_cwd(tmp_path, monkeypatch):
    from meret.core.config import get_config

    monkeypatch.chdir(tmp_path)
    assert resolve_db_path() == tmp_path / DB_NAME

    get_config().set("paths.default_db", str(tmp_path / "from-config.db"))
    assert resolve_db_path() == tmp_path / "from-config.db"

    monkeypatch.setenv("MERET_DB", str(tmp_path / "from-env.db"))
    assert resolve_db_path() == tmp_path / "from-env.db"


def test_tilde_directory_is_expanded_before_the_dir_check(tmp_path, monkeypatch):
    """`meret init ~/Media` in a shell that leaves ~ alone (PowerShell)."""
    (tmp_path / "Media").mkdir()
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    from pathlib import Path

    assert resolve_db_path(Path("~/Media")) == tmp_path / "Media" / DB_NAME


def test_init_and_commands_use_the_given_dir_despite_env(tmp_path, monkeypatch):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    lib = tmp_path / "lib"
    lib.mkdir()
    monkeypatch.setenv("MERET_DB", str(elsewhere))

    assert runner.invoke(app, ["init", str(lib)]).exit_code == 0
    assert (lib / DB_NAME).is_file() and not (elsewhere / DB_NAME).exists()

    res = runner.invoke(app, ["where", "--db", str(lib)])
    assert str(lib / DB_NAME) in res.output.replace("\n", "")


def test_commands_without_db_use_env(tmp_path, monkeypatch):
    monkeypatch.setenv("MERET_DB", str(tmp_path))
    assert runner.invoke(app, ["init"]).exit_code == 0  # init with no PATH honours it too
    assert (tmp_path / DB_NAME).is_file()
    assert runner.invoke(app, ["tags"]).exit_code == 0
