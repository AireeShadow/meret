"""
Tests for localization support.
"""

import gettext
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from meret.core.locale import (
    LocaleConfig,
    LocaleManager,
    get_locale,
    set_locale,
    _,
    ngettext,
)


class TestLocaleConfig:
    """Tests for LocaleConfig dataclass."""
    
    def test_default_values(self):
        config = LocaleConfig(language="en")
        assert config.language == "en"
        assert config.encoding == "UTF-8"
        assert config.date_format is None
        assert config.decimal_separator is None
    
    def test_custom_values(self):
        config = LocaleConfig(
            language="ru",
            encoding="CP1251",
            date_format="%d.%m.%Y",
            decimal_separator=",",
        )
        assert config.language == "ru"
        assert config.encoding == "CP1251"
        assert config.date_format == "%d.%m.%Y"
        assert config.decimal_separator == ","


class TestLocaleManager:
    """Tests for LocaleManager class."""
    
    def test_init_with_language(self):
        loc = LocaleManager(language="ru")
        assert loc.language == "ru"
    
    def test_init_defaults_to_system_locale(self):
        with patch("meret.core.locale.system_language", return_value="ru"):
            assert LocaleManager().language == "ru"
            assert LocaleManager("auto").language == "ru"  # the config's default

    def test_init_fallback_on_locale_error(self, monkeypatch):
        for var in ("LANGUAGE", "LC_ALL", "LC_MESSAGES", "LANG"):
            monkeypatch.delenv(var, raising=False)
        with patch("meret.core.locale._language_from_windows", return_value=None), \
                patch("locale.getlocale", side_effect=AttributeError):
            loc = LocaleManager()
            assert loc.language == "en"
    
    def test_normalize_language_variants(self):
        assert LocaleManager._normalize_language("ru-RU") == "ru"
        assert LocaleManager._normalize_language("en_US") == "en"
        assert LocaleManager._normalize_language("DE-de") == "de"
        assert LocaleManager._normalize_language("FR") == "fr"
    
    def test_translate_returns_original_when_no_translation(self):
        loc = LocaleManager(language="xx")  # Non-existent locale
        assert loc.translate("Hello") == "Hello"
    
    def test_translate_caches_results(self):
        loc = LocaleManager(language="en")
        # First call
        result1 = loc.translate("Test")
        # Second call should use cache
        result2 = loc.translate("Test")
        assert result1 == result2
        assert (None, "Test") in loc._cache
    
    def test_translate_with_context(self):
        loc = LocaleManager(language="en")
        result = loc.translate("Open", context="menu")
        assert result == "Open"  # Fallback since no translation
    
    def test_translate_plural_singular(self):
        loc = LocaleManager(language="en")
        result = loc.translate_plural("file", "files", 1)
        assert result == "file"
    
    def test_translate_plural_plural(self):
        loc = LocaleManager(language="en")
        result = loc.translate_plural("file", "files", 5)
        assert result == "files"
    
    def test_translate_plural_with_placeholder(self):
        loc = LocaleManager(language="en")
        # Create a mock translation that uses {n}
        loc._translation = MagicMock()
        loc._translation.ngettext.return_value = "{n} files"
        
        result = loc.translate_plural("file", "files", 5)
        assert "5" in result
    
    def test_format_number_default(self):
        loc = LocaleManager(language="en")
        assert loc.format_number(1234.5) == "1234.50"
    
    def test_format_number_with_decimals(self):
        loc = LocaleManager(language="en")
        assert loc.format_number(1234.567, decimals=1) == "1234.6"
    
    def test_format_number_with_thousands_separator(self):
        loc = LocaleManager(language="en")
        loc._config = LocaleConfig(
            language="en",
            thousands_separator=",",
            decimal_separator=".",
        )
        assert loc.format_number(1234567.89) == "1,234,567.89"
    
    def test_format_number_custom_separator(self):
        loc = LocaleManager(language="en")
        result = loc.format_number(1.5, force_separator=",")
        assert result == "1,50"
    
    def test_format_file_size_bytes(self):
        loc = LocaleManager(language="en")
        result = loc.format_file_size(512)
        assert "512" in result
        assert "B" in result
    
    def test_format_file_size_kilobytes(self):
        loc = LocaleManager(language="en")
        result = loc.format_file_size(1536)
        assert "1.5" in result or "1,5" in result
        assert "KB" in result
    
    def test_format_file_size_megabytes(self):
        loc = LocaleManager(language="en")
        result = loc.format_file_size(1572864)  # 1.5 MB
        assert "MB" in result
    
    def test_format_file_size_gigabytes(self):
        loc = LocaleManager(language="en")
        result = loc.format_file_size(1610612736)  # 1.5 GB
        assert "GB" in result
    
    def test_get_locale_info(self):
        loc = LocaleManager(language="en")
        info = loc.get_locale_info()
        assert info["language"] == "en"
        assert info["encoding"] == "UTF-8"
        assert "has_translation" in info
        assert isinstance(info["has_translation"], bool)
    
    def test_load_translation_uses_fallback_on_invalid_file(self, tmp_path):
        # Create a minimal .mo file structure
        locale_dir = tmp_path / "locales"
        mo_dir = locale_dir / "ru" / "LC_MESSAGES"
        mo_dir.mkdir(parents=True)
        
        # Create an invalid .mo file (wrong magic number)
        mo_file = mo_dir / "messages.mo"
        mo_file.write_bytes(b"\x00" * 100)
        
        # Should not raise, uses NullTranslations fallback
        loc = LocaleManager(language="ru", locale_dir=locale_dir)
        assert loc.language == "ru"
        # Check that fallback was used (no real translation loaded)
        assert loc.translate("Hello") == "Hello"


class TestGlobalFunctions:
    """Tests for module-level convenience functions."""
    
    def test_get_locale_creates_instance(self):
        with patch("meret.core.locale._locale_manager", None):
            loc = get_locale()
            assert isinstance(loc, LocaleManager)
    
    def test_get_locale_returns_existing(self):
        loc1 = get_locale()
        loc2 = get_locale()
        assert loc1 is loc2
    
    def test_get_locale_with_language_creates_new(self):
        loc1 = get_locale("en")
        loc2 = get_locale("ru")
        assert loc1.language == "en"
        assert loc2.language == "ru"
    
    def test_set_locale_creates_new_manager(self):
        loc = set_locale("de")
        assert loc.language == "de"
        # Check global instance was updated
        assert get_locale().language == "de"
    
    def test_underscore_function(self):
        with patch("meret.core.locale._locale_manager", None):
            set_locale("en")
            result = _("Hello")
            assert result == "Hello"
    
    def test_ngettext_function(self):
        with patch("meret.core.locale._locale_manager", None):
            set_locale("en")
            result = ngettext("file", "files", 1)
            assert result == "file"


class TestLocaleIntegration:
    """Integration tests with real translation loading."""
    
    def test_translation_with_utf8_content(self):
        """Test handling of UTF-8 content in translations."""
        loc = LocaleManager(language="ru")
        
        # Since we don't have real .mo file, test that the manager
        # handles UTF-8 in original strings correctly
        result = loc.translate("Hello World")
        assert result == "Hello World"
        
        # Test with Unicode characters in input
        result = loc.translate("File: ")
        assert result == "File: "
