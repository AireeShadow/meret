from typer.testing import CliRunner

from meret.cli.app import app

runner = CliRunner()


def _init(db) -> None:
    """The database is no longer created silently — init is required."""
    res = runner.invoke(app, ["init", str(db)])
    assert res.exit_code == 0, res.output


def test_tag_new_builds_ancestors(tmp_path):
    db = tmp_path / "meret.db"
    _init(db)

    result = runner.invoke(app, ["tag-new", "genre/mystery", "--db", str(db)])
    assert result.exit_code == 0, result.output

    listed = runner.invoke(app, ["tags", "--db", str(db)])
    assert "genre/mystery" in listed.output


def test_parent_rejects_cycle(tmp_path):
    db = tmp_path / "meret.db"
    _init(db)

    for path in ("a", "b", "c"):
        res = runner.invoke(app, ["tag-new", path, "--db", str(db)])
        assert res.exit_code == 0, res.output

    res = runner.invoke(app, ["parent", "b", "a", "--db", str(db)])
    assert res.exit_code == 0, res.output

    res = runner.invoke(app, ["parent", "c", "b", "--db", str(db)])
    assert res.exit_code == 0, res.output

    res = runner.invoke(app, ["parent", "a", "c", "--db", str(db)])
    assert res.exit_code == 1, res.output
    assert "cycle" in res.output


def test_where_shows_db(tmp_path):
    res = runner.invoke(app, ["where", "--db", str(tmp_path)])
    assert res.exit_code == 0, res.output
    assert "meret.db" in res.output