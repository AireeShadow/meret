"""Tests for config CLI commands."""
from typer.testing import CliRunner

from meret.cli.app import app

runner = CliRunner()


def test_config_path_shows_location(isolated_config):
    """config path shows file location."""
    result = runner.invoke(app, ["config", "path"])
    assert result.exit_code == 0
    assert "meret" in result.output.lower()
    assert ".toml" in result.output or ".json" in result.output


def test_config_get_set(isolated_config):
    """config get/set commands work."""
    # Set value
    result = runner.invoke(app, ["config", "set", "scan.workers", "4"])
    assert result.exit_code == 0
    assert "Set scan.workers = 4" in result.output
    
    # Get value
    result = runner.invoke(app, ["config", "get", "scan.workers"])
    assert result.exit_code == 0
    assert "4" in result.output


def test_config_get_missing_key(isolated_config):
    """config get for missing key shows error."""
    result = runner.invoke(app, ["config", "get", "nonexistent.key"])
    assert result.exit_code == 1
    assert "not found" in result.output.lower()


def test_config_show_outputs_valid_json(isolated_config):
    """config show outputs valid structure."""
    result = runner.invoke(app, ["config", "show"])
    assert result.exit_code == 0
    # Should contain config sections
    assert "paths" in result.output.lower() or "scan" in result.output.lower()


def test_config_reset_with_confirmation(isolated_config):
    """config reset asks for confirmation."""
    # First set a value
    runner.invoke(app, ["config", "set", "scan.workers", "99"])
    
    # Reset without force should prompt
    result = runner.invoke(app, ["config", "reset"], input="n\n")
    assert result.exit_code == 1  # Aborted
    
    # Reset with force
    result = runner.invoke(app, ["config", "reset", "--force"])
    assert result.exit_code == 0
    assert "reset" in result.output.lower()