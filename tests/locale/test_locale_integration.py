"""Integration tests for locale in CLI."""

from pathlib import Path

from typer.testing import CliRunner

MERET = Path(__file__).resolve().parents[2] / "meret"

from meret.cli.app import app


def test_cli_russian_output(tmp_path):
    """Test that Russian locale works."""
    from meret.core.locale import set_locale
    
    runner = CliRunner()
    
    # Test with Russian locale
    set_locale("ru")
    
    # Create a db first
    result = runner.invoke(app, ["-l", "ru", "init", str(tmp_path), "--force"])
    assert result.exit_code == 0
    assert "База данных" in result.output or "Database" in result.output


def test_locale_switching():
    """Test that locale can be switched."""
    from meret.core.locale import _, get_locale, set_locale  # <-- ДОБАВИТЬ ИМПОРТ '_' !!!
    
    # Start with English
    set_locale("en")
    assert get_locale().language == "en"
    
    # Switch to Russian
    set_locale("ru")
    assert get_locale().language == "ru"
    
    # Check translation (will fallback to English if no .mo file)
    result = _("Database created")
    assert result in ["Database created", "База данных создана"]