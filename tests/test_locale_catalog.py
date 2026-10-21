"""Translations: catalog loading, language detection, and the CLI in Russian."""

import re
import sys
import ast
import gettext
import string
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from meret.cli.app import app
from meret.core import locale as loc_module
from meret.core.locale import (
    DEFAULT_LOCALE_DIR,
    LocaleManager,
    _parse_po,
    load_po,
    system_language,
    get_locale
)

MERET = Path(__file__).resolve().parents[1] / "meret"
LANGUAGES = sorted(p.parent.parent.name for p in DEFAULT_LOCALE_DIR.glob("*/LC_MESSAGES/*.po"))
UNITS = {"B", "KB", "MB", "GB", "TB"}

runner = CliRunner()


# ---------- every msgid in the code has a translation ----------


def _msgids_in_code():
    """(key, where) for each translatable call with literal text.

    Covers _()/translate() and ngettext()/translate_plural(), so strings reached
    through self.translate() inside LocaleManager count too. Keys are shaped like
    the catalog: "ctxt\\x04msgid" for a context, "singular\\0plural" for plurals.
    """
    found = []
    for py in MERET.rglob("*.py"):
        tree = ast.parse(py.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = (
                node.func.id if isinstance(node.func, ast.Name)
                else getattr(node.func, "attr", "")
            )
            args = [a.value for a in node.args if isinstance(a, ast.Constant)]
            where = f"{py.relative_to(MERET.parent)}:{node.lineno}"
            context = next(
                (k.value.value for k in node.keywords
                 if k.arg == "context" and isinstance(k.value, ast.Constant)),
                None,
            )
            if name in ("_", "translate") and args and isinstance(args[0], str):
                found.append((f"{context}\x04{args[0]}" if context else args[0], where))
            elif name in ("ngettext", "translate_plural") and len(args) >= 2:
                found.append((f"{args[0]}\0{args[1]}", where))
    return found


def _fields(text):
    return sorted({name for _, name, _, _ in string.Formatter().parse(text) if name is not None})

def test_code_has_translatable_strings():
    assert len(_msgids_in_code()) > 30


@pytest.mark.parametrize("language", LANGUAGES)
def test_every_msgid_is_translated_with_the_same_placeholders(language):
    """The bug class of 0.6.1: msgids in code that the catalog doesn't have, or
    translations that drop/rename a {placeholder} (KeyError at .format time)."""
    po = DEFAULT_LOCALE_DIR / language / "LC_MESSAGES" / "messages.po"
    catalog = _parse_po(po.read_text(encoding="utf-8"))

    missing, broken = [], []
    for msgid, where in _msgids_in_code():
        if msgid not in catalog:
            missing.append(f"{where}  {msgid!r}")
            continue
        for form in catalog[msgid].split("\0"):
            if _fields(form) != _fields(msgid.split("\0")[0]):
                broken.append(f"{where}  {msgid!r} -> {form!r}")
    assert not missing, "not in the catalog:\n" + "\n".join(missing)
    assert not broken, "placeholders differ:\n" + "\n".join(broken)

@pytest.mark.parametrize("language", LANGUAGES)
def test_quoted_placeholders_stay_quoted(language):
    """`'{cmd}'` must keep both quotes: a stray apostrophe reads as a typo."""
    po = DEFAULT_LOCALE_DIR / language / "LC_MESSAGES" / "messages.po"
    catalog = _parse_po(po.read_text(encoding="utf-8"))
    bad = [f"{m!r} -> {t!r}" for m, t in catalog.items() if "'{cmd}'" in m and "'{cmd}'" not in t]
    assert not bad, "\n".join(bad)

def test_no_markup_in_msgids():
    """Markup belongs in the code: '[green]' in a msgid is a translator trap."""
    bad = [f"{w}  {m!r}" for m, w in _msgids_in_code() if "[/" in m or "[green" in m]
    assert not bad, "\n".join(bad)


@pytest.mark.parametrize("language", LANGUAGES)
def test_catalog_has_no_duplicate_msgids(language, caplog):
    """msgfmt rejects them, and here only the last copy counts — edits to an earlier
    copy would silently do nothing."""
    po = DEFAULT_LOCALE_DIR / language / "LC_MESSAGES" / "messages.po"
    _parse_po(po.read_text(encoding="utf-8"), source=str(po))
    dupes = [r.getMessage() for r in caplog.records if "duplicate msgid" in r.getMessage()]
    assert not dupes, "\n".join(dupes)

def test_an_untranslated_duplicate_does_not_shadow_the_translated_one(caplog):
    """`later wins` must not mean an empty copy wins over a real translation."""
    text = 'msgid "a"\nmsgstr "1"\n\nmsgid "a"\nmsgstr ""\n'
    assert _parse_po(text, source="t.po")["a"] == "1"
    assert "duplicate msgid" in caplog.text

#: format_file_size reaches these through self.translate(), not _()



@pytest.mark.parametrize("language", LANGUAGES)
def test_catalog_has_no_unused_msgids(language):
    """A msgid nothing asks for is a leftover — or a typo in the code."""
    po = DEFAULT_LOCALE_DIR / language / "LC_MESSAGES" / "messages.po"
    catalog = _parse_po(po.read_text(encoding="utf-8"))
    used = {m for m, _ in _msgids_in_code()} | UNITS | {""}
    unused = sorted(k for k in catalog if k not in used)
    assert not unused, "asked for by no _()/ngettext():\n" + "\n".join(map(repr, unused))


def test_duplicates_are_reported_and_the_later_wins(caplog):
    text = 'msgid "a"\nmsgstr "1"\n\nmsgid "b"\nmsgstr "x"\n\nmsgid "a"\nmsgstr "2"\n'
    assert _parse_po(text, source="t.po")["a"] == "2"
    (record,) = [r for r in caplog.records if "duplicate msgid" in r.getMessage()]
    assert "t.po:7" in record.getMessage() and "line 1" in record.getMessage()


def test_same_msgid_in_another_context_is_not_a_duplicate(caplog):
    text = (
        'msgid "file"\nmsgid_plural "files"\nmsgstr[0] "f"\nmsgstr[1] "fs"\n\n'
        'msgctxt "node type"\nmsgid "file"\nmsgstr "n"\n'
    )
    catalog = _parse_po(text)
    assert catalog["node type\x04file"] == "n"
    assert catalog["file\0files"] == "f\0fs"
    assert not [r for r in caplog.records if "duplicate msgid" in r.getMessage()]


# ---------- .po parsing and loading ----------


PO = r'''
# comment
msgid ""
msgstr ""
"Content-Type: text/plain; charset=UTF-8\n"
"Plural-Forms: nplurals=3; plural=(n%10==1 && n%100!=11 ? 0 : n%10>=2 && n%10<=4 && (n%100<10 || n%100>=20) ? 1 : 2);\n"

msgid "Hello"
msgstr "Привет"

msgctxt "menu"
msgid "Open"
msgstr "Открыть"

msgid "Long "
"line"
msgstr "Длинная "
"строка"

msgid "Quote \"x\"\n"
msgstr "Кавычки \"x\"\n"

msgid "file"
msgid_plural "files"
msgstr[0] "файл"
msgstr[1] "файла"
msgstr[2] "файлов"

#, fuzzy
msgid "Fuzzy"
msgstr "Нечётко"

msgid "Untranslated"
msgstr ""
'''


def _po_dir(tmp_path, text=PO, language="ru"):
    target = tmp_path / language / "LC_MESSAGES"
    target.mkdir(parents=True)
    (target / "messages.po").write_text(text, encoding="utf-8")
    return tmp_path


def test_po_is_read_without_any_mo(tmp_path):
    loc = LocaleManager("ru", locale_dir=_po_dir(tmp_path))
    assert loc.get_locale_info()["has_translation"] is True
    assert loc.translate("Hello") == "Привет"
    assert loc.translate("Open", context="menu") == "Открыть"
    assert loc.translate("Open") == "Open"  # without the context it's another message
    assert loc.translate("Long line") == "Длинная строка"
    assert loc.translate('Quote "x"\n') == 'Кавычки "x"\n'
    assert loc.translate("Fuzzy") == "Fuzzy"  # fuzzy entries are not trusted
    assert loc.translate("Untranslated") == "Untranslated"


@pytest.mark.parametrize(("n", "form"), [(1, "файл"), (2, "файла"), (5, "файлов"),
                                         (11, "файлов"), (21, "файл"), (22, "файла")])
def test_po_plural_rule_comes_from_the_header(tmp_path, n, form):
    loc = LocaleManager("ru", locale_dir=_po_dir(tmp_path))
    assert loc.translate_plural("file", "files", n) == form


def test_broken_po_falls_back_to_english(tmp_path, caplog):
    loc = LocaleManager("ru", locale_dir=_po_dir(tmp_path, 'msgid "a"\nmsgstr "b\n'))
    assert loc.translate("a") == "a"
    assert "broken" in caplog.text


def test_real_catalog_loads():
    translations = load_po(DEFAULT_LOCALE_DIR / "ru" / "LC_MESSAGES" / "messages.po")
    assert isinstance(translations, gettext.GNUTranslations)
    assert translations.gettext("Database created: {path}") == "База данных создана: {path}"


# ---------- language detection ----------


def _clear_env(monkeypatch):
    for var in ("LANGUAGE", "LC_ALL", "LC_MESSAGES", "LANG"):
        monkeypatch.delenv(var, raising=False)


@pytest.mark.parametrize(("var", "value", "expected"), [
    ("LANG", "ru_RU.UTF-8", "ru"),
    ("LANGUAGE", "de:en", "de"),
    ("LC_ALL", "pt_BR", "pt"),
])
def test_language_from_env(monkeypatch, var, value, expected):
    _clear_env(monkeypatch)
    monkeypatch.setenv(var, value)
    assert system_language() == expected


def test_c_locale_is_not_a_language(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("LANG", "C.UTF-8")
    with patch.object(loc_module, "_language_from_windows", return_value=None), \
            patch("locale.getlocale", return_value=(None, None)):
        assert system_language() == "en"


def test_windows_display_language(monkeypatch):
    _clear_env(monkeypatch)
    with patch.object(loc_module, "_language_from_windows", return_value="ru_RU"):
        assert system_language() == "ru"


def test_windows_style_getlocale_name(monkeypatch):
    """getlocale() on Windows says 'Russian_Russia' — that used to become 'russian'."""
    _clear_env(monkeypatch)
    with patch.object(loc_module, "_language_from_windows", return_value=None), \
            patch("locale.getlocale", return_value=("Russian_Russia", "1251")):
        assert system_language() == "ru"


def test_russian_number_format():
    ru = LocaleManager("ru")
    assert ru.format_number(1234567.891) == "1 234 567,89"
    assert ru.format_number(-123.4, decimals=1) == "-123,4"
    assert ru.format_file_size(1572864) == "1,5 МБ"


# ---------- CLI ----------


def test_cli_lang_ru(tmp_path):
    res = runner.invoke(app, ["--lang", "ru", "init", str(tmp_path)])
    assert res.exit_code == 0, res.output
    assert "База данных создана:" in res.output
    assert "{path}" not in res.output and str(tmp_path) in res.output.replace("\n", "")

    res = runner.invoke(app, ["--lang", "ru", "where", "--db", str(tmp_path)])
    assert "существует" in res.output


def test_cli_scan_summary_in_russian(tmp_path):
    from PIL import Image

    Image.new("RGB", (4, 4)).save(tmp_path / "a.png")
    runner.invoke(app, ["init", str(tmp_path)])
    res = runner.invoke(app, ["--lang", "ru", "scan", str(tmp_path), "--db", str(tmp_path)])
    assert res.exit_code == 0, res.output
    assert "Новых: 1, обновлено: 0, пропущено: 0, ошибок: 0" in res.output


def test_cli_language_from_config_and_lang_overrides_it(tmp_path):
    from meret.core.config import get_config

    get_config().set("locale.language", "ru")
    res = runner.invoke(app, ["where", "--db", str(tmp_path)])
    assert "отсутствует" in res.output

    res = runner.invoke(app, ["--lang", "en", "where", "--db", str(tmp_path)])
    assert "missing" in res.output


def test_cli_unknown_language_falls_back_to_english(tmp_path):
    res = runner.invoke(app, ["--lang", "xx", "where", "--db", str(tmp_path)])
    assert res.exit_code == 0
    assert "missing" in res.output


def test_config_accepts_auto():
    from meret.core.config import LocaleConfig

    assert LocaleConfig().language == "auto"
    assert LocaleConfig(language="ru").language == "ru"

# ---------- TUI-specific translations ----------

def test_tui_bindings_in_catalog():
    """Test that all TUI binding labels are in Russian catalog."""
    po = DEFAULT_LOCALE_DIR / "ru" / "LC_MESSAGES" / "messages.po"
    if not po.exists():
        pytest.skip("Russian .po not found")
    
    catalog = _parse_po(po.read_text(encoding="utf-8"))
    
    # All binding labels that should be translated
    bindings = [
        "Quit", "Config", "Init DB", "Open", "Scan", "Tag",
        "Refresh", "Delete tag", "Home", "Up", "Sort",
        "Mount", "Unmount", "Select Drive", "Search", "Help",
        "Cancel", "OK", "Yes", "No", "Close", "Select", "Command",
    ]
    
    missing = [b for b in bindings if b not in catalog]
    assert not missing, f"Missing TUI bindings in catalog: {missing}"


def test_tui_dialog_buttons_in_catalog():
    """Test dialog button labels are translated."""
    po = DEFAULT_LOCALE_DIR / "ru" / "LC_MESSAGES" / "messages.po"
    if not po.exists():
        pytest.skip("Russian .po not found")
    
    po_content = po.read_text(encoding="utf-8")
    
    buttons = {
        "Cancel": "Отмена",
        "OK": "ОК",
        "Yes": "Да",
        "No": "Нет",
    }
    
    for btn, expected in buttons.items():
        assert f'msgid "{btn}"' in po_content, f"Button '{btn}' not in .po"
        assert expected in po_content, f"Translation '{expected}' for '{btn}' not found"

# ---------- --lang is honoured before the commands are built ----------


def test_lang_flag_from_argv_is_applied(monkeypatch):
    import meret.cli.app as app_module

    monkeypatch.setattr(sys, "argv", ["meret", "--lang=ru", "where"])
    app_module._apply_language_from_argv()
    assert get_locale().language == "ru"


@pytest.mark.parametrize("flag", [["--lang", "ru"], ["-l", "ru"], ["--lang=ru"]])
def test_every_spelling_of_lang_is_understood(monkeypatch, flag):
    import meret.cli.app as app_module

    monkeypatch.setattr(sys, "argv", ["meret", *flag, "where"])
    app_module._apply_language_from_argv()
    assert get_locale().language == "ru"


def test_lang_flag_beats_the_config(monkeypatch):
    import meret.cli.app as app_module
    from meret.core.config import get_config

    get_config().set("locale.language", "en")
    monkeypatch.setattr(sys, "argv", ["meret", "--lang", "ru", "where"])
    app_module._apply_language_from_argv()
    assert get_locale().language == "ru"


def test_without_lang_the_config_decides(monkeypatch):
    import meret.cli.app as app_module
    from meret.core.config import get_config

    get_config().set("locale.language", "ru")
    monkeypatch.setattr(sys, "argv", ["meret", "where"])
    app_module._apply_language_from_argv()
    assert get_locale().language == "ru"


def test_without_lang_and_with_auto_the_system_decides(monkeypatch):
    """LANGUAGE=en comes from the autouse fixture, so this stays deterministic."""
    import meret.cli.app as app_module

    monkeypatch.setattr(sys, "argv", ["meret", "where"])
    app_module._apply_language_from_argv()
    assert get_locale().language == "en"


def test_dangling_lang_flag_is_not_a_crash(monkeypatch):
    """`meret where --lang` — typer complains about the value, we must not blow up."""
    import meret.cli.app as app_module

    monkeypatch.setattr(sys, "argv", ["meret", "where", "--lang"])
    app_module._apply_language_from_argv()
    assert get_locale().language == "en"


# ---------- an unknown command is translated ----------

def test_app_uses_the_translated_group():
    """A stray `app = typer.Typer(...)` below would silently drop cls=TranslatedGroup."""
    from meret.cli.app import TranslatedGroup, app

    assert app.info.cls is TranslatedGroup

def test_unknown_command_message_text():
    """The catalog wording, without the CLI in the way."""
    from meret.core.locale import set_locale, _

    set_locale("ru")
    msg = _("No such command '{cmd}'. Did you mean: {available}?").format(
        cmd="scna", available="scan"
    )
    # {cmd} is already substituted here — no braces left in the result
    assert msg == "Команда 'scna' не найдена. Возможно, вы имели в виду: scan?"

def _plain(text: str) -> str:
    """The error goes out in a Rich panel: drop the borders and squeeze whitespace."""
    return re.sub(r"\s+", " ", text.replace("│", " ").replace("─", " "))


def test_unknown_command_is_translated():
    """The group builds this message itself: typer's own gettext would say English."""
    res = runner.invoke(app, ["--lang", "ru", "scna"])
    assert res.exit_code == 2
    flat = _plain(res.output)
    assert "Команда 'scna' не найдена" in flat
    assert "scan" in flat  # the suggestion survives


def test_unknown_command_without_a_close_match():
    res = runner.invoke(app, ["--lang", "ru", "zzzzzz"])
    assert res.exit_code == 2
    assert "Доступные" in _plain(res.output)