from typer.testing import CliRunner

from meret.cli.app import app

runner = CliRunner()


def test_tag_new_builds_ancestors(tmp_path):
    db = tmp_path / "meret.db"
    result = runner.invoke(app, ["tag-new", "жанр/детектив", "--db", str(db)])
    assert result.exit_code == 0, result.output

    listed = runner.invoke(app, ["tags", "--db", str(db)])
    assert "жанр/детектив" in listed.output


def test_parent_rejects_cycle(tmp_path):
    db = tmp_path / "meret.db"

    for path in ("a", "b", "c"):
        res = runner.invoke(app, ["tag-new", path, "--db", str(db)])
        assert res.exit_code == 0, res.output

    res = runner.invoke(app, ["parent", "b", "a", "--db", str(db)])
    assert res.exit_code == 0, res.output

    res = runner.invoke(app, ["parent", "c", "b", "--db", str(db)])
    assert res.exit_code == 0, res.output

    res = runner.invoke(app, ["parent", "a", "c", "--db", str(db)])
    assert res.exit_code == 1, res.output
    assert "цикл" in res.output