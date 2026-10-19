"""
Localization support for meret.

Handles translations, date/number formatting, and locale-aware sorting.
"""

import gettext
import locale
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any, Callable

# Default locale directory
DEFAULT_LOCALE_DIR = Path(__file__).parent.parent / "locales"
DEFAULT_DOMAIN = "messages"
DEFAULT_LANGUAGE = "en"


@dataclass(frozen=True)
class LocaleConfig:
    """Immutable locale configuration."""
    language: str
    encoding: str = "UTF-8"
    date_format: Optional[str] = None
    decimal_separator: Optional[str] = None
    thousands_separator: Optional[str] = None


class LocaleManager:
    """
    Manages translations and locale-specific formatting.
    
    Usage:
        loc = LocaleManager("ru")
        loc.translate("Hello")  # "Привет"
        loc.format_number(1234.5)  # "1 234,5" для ru
    """
    
    def __init__(
        self,
        language: Optional[str] = None,
        locale_dir: Optional[Path] = None,
        domain: str = DEFAULT_DOMAIN,
    ):
        self._locale_dir = locale_dir or DEFAULT_LOCALE_DIR
        self._domain = domain
        self._config = self._resolve_config(language)
        self._translation = self._load_translation()
        self._cache: Dict[str, str] = {}
    
    def _resolve_config(self, language: Optional[str]) -> LocaleConfig:
        """Resolve language from param, config, or system."""
        if language is None:
            # Try system locale using modern API
            try:
                system_locale = locale.getlocale()
                if system_locale and system_locale[0]:
                    language = system_locale[0].split("_")[0]
                else:
                    language = DEFAULT_LANGUAGE
            except (AttributeError, IndexError):
                language = DEFAULT_LANGUAGE
        
        # Validate language code
        language = self._normalize_language(language)
        
        return LocaleConfig(language=language)
    
    @staticmethod
    def _normalize_language(lang: str) -> str:
        """Normalize language code (ru-RU -> ru, en_US -> en)."""
        return lang.lower().split("-")[0].split("_")[0]
    
    def _load_translation(self) -> gettext.NullTranslations:
        """Load translation for current language."""
        try:
            translation = gettext.translation(
                self._domain,
                localedir=str(self._locale_dir),
                languages=[self._config.language],
                fallback=True,  # Returns NullTranslations if file not found
            )
        except (FileNotFoundError, OSError):
            # Fallback to English if translation file not found or invalid
            translation = gettext.NullTranslations()
        
        return translation
    
    @property
    def language(self) -> str:
        """Current language code."""
        return self._config.language
    
    def translate(self, message: str, context: Optional[str] = None) -> str:
        """
        Translate a message.
        
        Args:
            message: Text to translate
            context: Optional context for disambiguation
            
        Returns:
            Translated text or original if no translation found
        """
        cache_key = f"{context}:{message}" if context else message
        
        if cache_key not in self._cache:
            if context:
                # pgettext-style with context
                result = self._translation.pgettext(context, message)
            else:
                result = self._translation.gettext(message)
            self._cache[cache_key] = result
        
        return self._cache[cache_key]
    
    def translate_plural(
        self,
        singular: str,
        plural: str,
        n: int,
        context: Optional[str] = None,
    ) -> str:
        """
        Translate with plural forms support.
        
        Args:
            singular: Singular form message
            plural: Plural form message
            n: Quantity (determines which form to use)
            context: Optional context
            
        Returns:
            Translated text with correct plural form
        """
        if context:
            # npgettext-style
            template = self._translation.npgettext(context, singular, plural, n)
        else:
            template = self._translation.ngettext(singular, plural, n)
        
        # Replace {n} placeholder if present
        return template.format(n=n)
    
    def format_number(
        self,
        number: float,
        decimals: int = 2,
        force_separator: Optional[str] = None,
    ) -> str:
        """
        Format number according to locale.
        
        Args:
            number: Number to format
            decimals: Decimal places
            force_separator: Override decimal separator
            
        Returns:
            Formatted number string
        """
        decimal_sep = force_separator or self._config.decimal_separator or "."
        thousands_sep = self._config.thousands_separator or ""
        
        # Format with given decimals
        fmt = f"{number:.{decimals}f}"
        
        if thousands_sep:
            # Add thousands separators
            parts = fmt.split(".")
            integer_part = parts[0]
            decimal_part = parts[1] if len(parts) > 1 else ""
            
            # Insert thousands separators
            integer_part = re.sub(
                r"(?<!^)(?=(\d{3})+$)",
                thousands_sep,
                integer_part,
            )
            
            fmt = f"{integer_part}{decimal_sep}{decimal_part}" if decimal_part else integer_part
        else:
            fmt = fmt.replace(".", decimal_sep)
        
        return fmt
    
    def format_file_size(self, size_bytes: int) -> str:
        """
        Format file size in human-readable form.
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Localized size string (e.g., "1.5 MB" or "1,5 МБ")
        """
        units = [
            self.translate("B"),
            self.translate("KB"),
            self.translate("MB"),
            self.translate("GB"),
            self.translate("TB"),
        ]
        
        size = float(size_bytes)
        unit_idx = 0
        
        while size >= 1024 and unit_idx < len(units) - 1:
            size /= 1024
            unit_idx += 1
        
        formatted = self.format_number(size, decimals=1)
        return f"{formatted} {units[unit_idx]}"
    
    def get_locale_info(self) -> Dict[str, Any]:
        """Get current locale information."""
        return {
            "language": self._config.language,
            "encoding": self._config.encoding,
            "locale_dir": str(self._locale_dir),
            "domain": self._domain,
            "has_translation": not isinstance(
                self._translation, gettext.NullTranslations
            ),
        }


# Global instance (lazy-loaded)
_locale_manager: Optional[LocaleManager] = None


def get_locale(language: Optional[str] = None) -> LocaleManager:
    """Get or create global locale manager."""
    global _locale_manager
    if _locale_manager is None or language is not None:
        _locale_manager = LocaleManager(language)
    return _locale_manager


def set_locale(language: str) -> LocaleManager:
    """Set global locale language."""
    global _locale_manager
    _locale_manager = LocaleManager(language)
    return _locale_manager


# Convenience functions
def _(message: str, context: Optional[str] = None) -> str:
    """Shorthand for translate."""
    return get_locale().translate(message, context)


def ngettext(singular: str, plural: str, n: int, context: Optional[str] = None) -> str:
    """Shorthand for plural translation."""
    return get_locale().translate_plural(singular, plural, n, context)