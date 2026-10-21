"""Scratch check for the .po parser. Delete once it behaves."""
from meret.core.locale import DEFAULT_LOCALE_DIR, _parse_po

SAMPLE = 'msgid "a"\nmsgstr "1"\n\nmsgid "b"\nmsgstr "2"\n'

print("sample  :", _parse_po(SAMPLE))
print("plural  :", _parse_po('msgid "c"\nmsgid_plural "cs"\nmsgstr[0] "x"\nmsgstr[1] "y"\n'))
print("context :", _parse_po('msgctxt "n"\nmsgid "d"\nmsgstr "z"\n'))

try:
    print("broken  :", _parse_po('msgid "e"\nmsgstr "f\n'))
except Exception as exc:
    print("broken  : raises", type(exc).__name__)

po = DEFAULT_LOCALE_DIR / "ru" / "LC_MESSAGES" / "messages.po"
catalog = _parse_po(po.read_text(encoding="utf-8"), source=str(po))
print("catalog :", len(catalog), "entries; header:", "" in catalog)