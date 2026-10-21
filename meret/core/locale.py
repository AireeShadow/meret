"""
Localization support for meret.

Handles translations, date/number formatting, and locale-aware sorting.

Translations live in ``meret/locales/<lang>/LC_MESSAGES/messages.po`` and are read
directly — there is no compile step and no .mo file to keep in sync. (A .mo is
still used when a language has no .po, e.g. one installed from elsewhere.)

Language, first match wins:
    --lang  >  config locale.language (unless "auto")  >  system language  >  en

Writing translatable strings: keep markup and values out of the msgid —
``console.print(f"[green]✓ {_('Database created: {path}').format(path=p)}[/green]")``
so translators see one whole sentence with named placeholders.
"""

import ast
import gettext
import io
import locale
import logging
import os
import re
import struct
import sys
from array import array
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any

# Default locale directory
DEFAULT_LOCALE_DIR = Path(__file__).parent.parent / "locales"
DEFAULT_DOMAIN = "messages"
DEFAULT_LANGUAGE = "en"

#: (decimal separator, thousands separator) where they differ from "1234.5"
_NUMBER_SEPARATORS: Dict[str, tuple] = {
    "ru": (",", " "),  # 1 234,5 — with a no-break space
}

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LocaleConfig:
    """Immutable locale configuration."""
    language: str
    encoding: str = "UTF-8"
    date_format: Optional[str] = None
    decimal_separator: Optional[str] = None
    thousands_separator: Optional[str] = None


# ---------- system language ----------


def _language_from_env() -> Optional[str]:
    """POSIX convention: LANGUAGE (a list), LC_ALL, LC_MESSAGES, LANG."""
    for var in ("LANGUAGE", "LC_ALL", "LC_MESSAGES", "LANG"):
        value = os.environ.get(var, "").split(":")[0].strip()
        if value and value != "POSIX" and value.split(".")[0] != "C":
            return value
    return None


def _language_from_windows() -> Optional[str]:
    """The Windows display language, e.g. 'ru_RU'."""
    if sys.platform != "win32":
        return None
    try:
        import ctypes

        lcid = ctypes.windll.kernel32.GetUserDefaultUILanguage()
    except (AttributeError, OSError):
        return None
    return locale.windows_locale.get(lcid)


def _language_from_getlocale() -> Optional[str]:
    """locale.getlocale(): 'ru_RU' on POSIX, 'Russian_Russia' on Windows."""
    try:
        name = locale.getlocale()[0]
    except (AttributeError, ValueError, IndexError):
        return None
    if not name:
        return None
    # Windows-style names: locale_alias maps 'russian' -> 'ru_RU.ISO8859-5'
    return locale.locale_alias.get(name.split("_")[0].lower(), name)


def system_language() -> str:
    """The user's language as a short code ('ru', 'en'); 'en' if it can't be told."""
    for source in (_language_from_env, _language_from_windows, _language_from_getlocale):
        found = source()
        if found:
            return LocaleManager._normalize_language(found)
    return DEFAULT_LANGUAGE


# ---------- .po -> catalog ----------


def _parse_po(text: str) -> Dict[str, str]:
    """{key: translation} from .po text, keyed the way gettext expects in a .mo.

    Context: ``ctxt\\x04msgid``; plurals: ``msgid\\0msgid_plural`` -> forms joined
    by ``\\0``. Untranslated and fuzzy entries are skipped (as msgfmt does), the
    header (msgid "") is kept: it carries the charset and the plural rule.
    """
    catalog: Dict[str, str] = {}
    entry: Dict[str, Any] = {}
    field: Optional[str] = None
    fuzzy = False

    def flush() -> None:
        nonlocal entry, fuzzy
        if "msgid" in entry:
            key = entry["msgid"]
            if "msgid_plural" in entry:
                key += "\0" + entry["msgid_plural"]
                forms_map = entry.get("msgstr", {})
                highest = max(forms_map) if forms_map else 0
                forms = [forms_map.get(i, "") for i in range(highest + 1)]
                value = "\0".join(forms)
                translated = any(forms)
            else:
                value = entry.get("msgstr", {}).get(0, "")
                translated = bool(value)
            if "msgctxt" in entry:
                key = entry["msgctxt"] + "\x04" + key
            is_header = entry["msgid"] == "" and "msgctxt" not in entry
            if is_header or (translated and not fuzzy):
                catalog[key] = value
        entry, fuzzy = {}, False

    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            flush()
            field = None
            continue
        if line.startswith("#"):
            if line.startswith("#,") and "fuzzy" in line:
                if "msgid" in entry:
                    flush()
                fuzzy = True
            continue
        if line.startswith('"'):
            chunk = ast.literal_eval(line)
        else:
            keyword, _, rest = line.partition(" ")
            chunk = ast.literal_eval(rest.strip())
            if keyword in ("msgctxt", "msgid") and "msgid" in entry and (
                keyword == "msgctxt" or "msgstr" in entry
            ):
                flush()  # a new entry without a blank line in between
            if keyword.startswith("msgstr["):
                field = ("msgstr", int(keyword[7:-1]))
            elif keyword == "msgstr":
                field = ("msgstr", 0)
            elif keyword in ("msgctxt", "msgid", "msgid_plural"):
                field = keyword
            else:
                raise ValueError(f"line {number}: unknown keyword {keyword!r}")
            if isinstance(field, tuple):
                entry.setdefault("msgstr", {})[field[1]] = ""
            else:
                entry[field] = ""
        if field is None:
            raise ValueError(f"line {number}: text outside an entry")
        if isinstance(field, tuple):
            entry["msgstr"][field[1]] += chunk
        else:
            entry[field] += chunk
    flush()
    return catalog


def _compile_mo(catalog: Dict[str, str]) -> bytes:
    """A GNU .mo image of the catalog (the layout CPython's Tools/i18n/msgfmt.py writes)."""
    keys = sorted(catalog)
    ids = strs = b""
    offsets = []
    for key in keys:
        k, v = key.encode("utf-8"), catalog[key].encode("utf-8")
        offsets.append((len(ids), len(k), len(strs), len(v)))
        ids += k + b"\0"
        strs += v + b"\0"
    n = len(keys)
    keystart = 7 * 4 + 16 * n
    valuestart = keystart + len(ids)
    koffsets, voffsets = [], []
    for id_off, id_len, str_off, str_len in offsets:
        koffsets += [id_len, id_off + keystart]
        voffsets += [str_len, str_off + valuestart]
    header = struct.pack("Iiiiiii", 0x950412DE, 0, n, 7 * 4, 7 * 4 + n * 8, 0, 0)
    return header + array("i", koffsets + voffsets).tobytes() + ids + strs


def load_po(path: Path) -> gettext.GNUTranslations:
    """Read a .po file into a regular gettext translations object."""
    catalog = _parse_po(path.read_text(encoding="utf-8"))
    return gettext.GNUTranslations(io.BytesIO(_compile_mo(catalog)))


# ---------- manager ----------


class LocaleManager:
    """
    Manages translations and locale-specific formatting.

    Usage:
        loc = LocaleManager("ru")
        loc.translate("Hello")  # "Привет"
        loc.format_number(1234.5)  # "1 234,50" для ru
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
        self._cache: Dict[tuple, str] = {}

    def _resolve_config(self, language: Optional[str]) -> LocaleConfig:
        """Explicit language, or the system's (None or "auto")."""
        if language is None or language.strip().lower() == "auto":
            language = system_language()
        language = self._normalize_language(language)
        decimal, thousands = _NUMBER_SEPARATORS.get(language, (None, None))
        return LocaleConfig(
            language=language, decimal_separator=decimal, thousands_separator=thousands
        )

    @staticmethod
    def _normalize_language(lang: str) -> str:
        """Normalize language code (ru-RU -> ru, en_US.UTF-8 -> en)."""
        return lang.strip().lower().split(".")[0].split("@")[0].split("-")[0].split("_")[0]

    def _load_translation(self) -> gettext.NullTranslations:
        """The .po for the language if there is one, else a compiled .mo, else none."""
        language = self._config.language
        po = self._locale_dir / language / "LC_MESSAGES" / f"{self._domain}.po"
        try:
            if po.is_file():
                translation = load_po(po)
            else:
                translation = gettext.translation(
                    self._domain, localedir=str(self._locale_dir), languages=[language]
                )
        except FileNotFoundError:
            if language != DEFAULT_LANGUAGE:  # English is the source text, nothing to load
                logger.debug("no translation for %r in %s", language, self._locale_dir)
            return gettext.NullTranslations()
        except (OSError, ValueError, SyntaxError) as e:
            logger.warning("translation for %r is broken, using English: %s", language, e)
            return gettext.NullTranslations()
        logger.debug("loaded %r translation (%s)", language, po if po.is_file() else ".mo")
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
        cache_key = (context, message)
        cached = self._cache.get(cache_key)
        if cached is None:
            if context:
                # pgettext-style with context
                cached = self._translation.pgettext(context, message)
            else:
                cached = self._translation.gettext(message)
            self._cache[cache_key] = cached
        return cached

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

        if "{n}" not in template:
            return template
        try:
            return template.format(n=n)
        except (KeyError, IndexError):
            logger.debug("plural template %r has stray braces, leaving as is", template)
            return template

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

        # Format with given decimals; the sign stays out of the digit grouping
        fmt = f"{abs(number):.{decimals}f}"
        sign = "-" if number < 0 and float(fmt) != 0 else ""

        integer_part, _, decimal_part = fmt.partition(".")
        if thousands_sep:
            integer_part = re.sub(r"(?<!^)(?=(\d{3})+$)", thousands_sep, integer_part)

        if decimal_part:
            return f"{sign}{integer_part}{decimal_sep}{decimal_part}"
        return f"{sign}{integer_part}"

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
            # GNUTranslations subclasses NullTranslations: compare the exact type
            "has_translation": type(self._translation) is not gettext.NullTranslations,
        }


def _config_language() -> Optional[str]:
    """The locale.language from the config, or None when it is unset or "auto"."""
    try:
        from meret.core.config import get_config

        value = get_config().get("locale.language")
    except Exception as e:
        # A broken or absent config must never break translations
        logger.debug("config language unavailable: %s", e)
        return None
    if not value or value.strip().lower() == "auto":
        return None
    return value


# Global instance (lazy-loaded)
_locale_manager: Optional[LocaleManager] = None


def get_locale(language: Optional[str] = None) -> LocaleManager:
    """Get or create the global locale manager: explicit language, config, system."""
    global _locale_manager
    if _locale_manager is None or language is not None:
        _locale_manager = LocaleManager(language if language is not None else _config_language())
    return _locale_manager


def set_locale(language: Optional[str]) -> LocaleManager:
    """Set global locale language (None or "auto" = the system language)."""
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
