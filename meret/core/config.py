"""
Configuration management for meret.

Supports TOML/JSON formats with Pydantic validation.
Config path: platformdirs.user_config_dir() / meret / config.toml
"""

import json
import logging
import os
from pathlib import Path
from typing import Any

import platformdirs
from pydantic import BaseModel, Field, field_validator

APP_NAME = "meret"
CONFIG_FILENAME = "config.toml"

logger = logging.getLogger(__name__)


class PathsConfig(BaseModel):
    """Path settings."""
    default_db: str | None = Field(
        default=None,
        description="Default database path (None = auto-detect)"
    )
    default_library: str | None = Field(
        default=None,
        description="Default library directory to scan"
    )


class ScanConfig(BaseModel):
    """Scanner settings."""
    workers: int = Field(default=8, ge=1)
    chunk_size: int = Field(default=1000, ge=1)
    skip_existing: bool = Field(default=True)
    
    @field_validator('workers')
    @classmethod
    def validate_workers(cls, v: int) -> int:
        """Cap workers at CPU count if too high."""
        import multiprocessing
        return min(v, multiprocessing.cpu_count() * 2)


class GUIConfig(BaseModel):
    """GUI settings (for future use)."""
    theme: str = Field(default="system", pattern="^(light|dark|system)$")
    thumbnail_size: int = Field(default=256, ge=64, le=1024)
    window_width: int = Field(default=1280, ge=800)
    window_height: int = Field(default=720, ge=600)


class LocaleConfig(BaseModel):
    """Localization settings."""
    # "auto" = the system language; `--lang` overrides either
    language: str = Field(default="auto", pattern="^(auto|[a-z]{2}(-[A-Z]{2})?)$")
    date_format: str = Field(default="%Y-%m-%d")


class MeretConfig(BaseModel):
    """Root configuration model."""
    paths: PathsConfig = Field(default_factory=PathsConfig)
    scan: ScanConfig = Field(default_factory=ScanConfig)
    gui: GUIConfig = Field(default_factory=GUIConfig)
    locale: LocaleConfig = Field(default_factory=LocaleConfig)
    
    # Allow extra fields for forward compatibility
    model_config = {"extra": "allow"}


class ConfigManager:
    """Manages meret configuration file."""
    
    def __init__(self, config_path: Path | str | None = None) -> None:
        """
        Initialize config manager.
        
        Args:
            config_path: Override config file path. If None, uses platform default.
        """
        if config_path:
            self.config_path = Path(config_path).expanduser().resolve()
        else:
            config_dir = Path(platformdirs.user_config_dir(APP_NAME))
            self.config_path = config_dir / CONFIG_FILENAME
        
        self._config: MeretConfig | None = None
        self._ensure_dir()
    
    def _ensure_dir(self) -> None:
        """Ensure config directory exists."""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
    
    @property
    def config(self) -> MeretConfig:
        """Get or load configuration."""
        if self._config is None:
            self._config = self.load()
        return self._config
    
    def load(self) -> MeretConfig:
        """
        Load configuration from file.
        Creates default config if file doesn't exist.
        """
        if not self.config_path.exists():
            return self._create_default()
        
        try:
            content = self.config_path.read_text(encoding="utf-8")
            
            # Try TOML first, fallback to JSON
            if self.config_path.suffix == ".toml":
                try:
                    import tomllib
                    data = tomllib.loads(content)
                except ImportError:
                    # Python < 3.11
                    try:
                        import tomli
                        data = tomli.loads(content)
                    except ImportError:
                        # Fallback to JSON if tomli not installed
                        data = json.loads(content)
            else:
                data = json.loads(content)
            
            config = MeretConfig.model_validate(data)
            logger.debug("config loaded from %s", self.config_path)
            return config
            
        except Exception as e:
            # Log warning and return defaults on error
            logger.warning(
                "Failed to load config from %s: %s. Using defaults.", self.config_path, e,
                exc_info=True,
            )
            return self._create_default()
    
    def save(self, config: MeretConfig | None = None) -> None:
        """Save configuration to file."""
        if config is None:
            config = self._config
        
        if config is None:
            config = MeretConfig()
        
        self._ensure_dir()
        
        # Exclude None values for TOML compatibility
        data = config.model_dump(exclude_none=True)
        
        # Use TOML if available, otherwise JSON
        if self.config_path.suffix == ".toml":
            try:
                import tomli_w
                with open(self.config_path, "wb") as f:
                    tomli_w.dump(data, f)
                logger.debug("config saved to %s", self.config_path)
                return
            except ImportError:
                # load() reads only the .toml — this save won't be seen again
                logger.warning("tomli-w is not installed: config saved as JSON, not %s",
                               self.config_path)
        
        # JSON fallback
        json_path = self.config_path.with_suffix(".json")
        json_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        logger.debug("config saved to %s", json_path)
        
    def _create_default(self) -> MeretConfig:
        """Create default configuration."""
        config = MeretConfig()
        self.save(config)
        return config
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get config value by dot-notation key.
        
        Examples:
            config.get("scan.workers")  # -> 8
            config.get("paths.default_db")  # -> None or path
        """
        parts = key.split(".")
        value: Any = self.config
        
        for part in parts:
            if isinstance(value, BaseModel):
                value = getattr(value, part, default)
            elif isinstance(value, dict):
                value = value.get(part, default)
            else:
                return default
            
            if value is None:
                return default
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """Set config value by dot-notation key and save."""
        parts = key.split(".")
        
        # Get current data
        data = self.config.model_dump()
        
        # Navigate and set
        target = data
        for part in parts[:-1]:
            if part not in target:
                raise AttributeError(f"Config has no attribute '{part}'")
            target = target[part]
        
        # Type conversion
        final_key = parts[-1]
        if isinstance(value, str):
            if value.lower() in ("true", "false"):
                value = value.lower() == "true"
            elif value.isdigit():
                value = int(value)
            elif "." in value and value.replace(".", "", 1).isdigit():
                value = float(value)
        
        target[final_key] = value
        
        # Re-validate entire model
        self._config = MeretConfig.model_validate(data)
        self.save()
    
    def reset(self) -> None:
        """Reset to defaults and save."""
        self._config = MeretConfig()
        self.save()
    
    def edit(self) -> None:
        """Open config file in default editor."""
        import subprocess
        import sys
        
        # Ensure file exists
        if not self.config_path.exists():
            self._create_default()
        
        if sys.platform == "win32":
            os.startfile(self.config_path)
        elif sys.platform == "darwin":
            subprocess.run(["open", str(self.config_path)])
        else:
            # Try $EDITOR, fallback to sensible editors
            editor = os.environ.get("EDITOR", "xdg-open")
            subprocess.run([editor, str(self.config_path)])


# Global instance for convenience
_config_instance: ConfigManager | None = None


def get_config(config_path: Path | str | None = None) -> ConfigManager:
    """
    Get global config manager instance.
    
    Thread-safe singleton pattern.
    """
    global _config_instance
    if _config_instance is None or config_path is not None:
        _config_instance = ConfigManager(config_path)
    return _config_instance


def reload_config() -> ConfigManager:
    """Force reload configuration from disk."""
    if _config_instance is not None:
        _config_instance._config = None  # Force reload
    return get_config()