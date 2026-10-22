"""Toolbar glyphs and the tiles that stand in for a missing preview.

Two jobs, one rule: never show a hole. The command glyphs are vendored from Lucide
(ISC, see icons/LICENSE) by scripts/fetch_icons.py and named after the command, so
an upstream rename never reaches this code; anything missing falls back to a tile
drawn by hand. Nothing is downloaded at runtime.

`icons` is a module sitting next to an `icons/` directory, and that only works while
the directory has no __init__.py: with one, the package shadows this module and
`from meret.gui import icons` resolves to something without command_icon().
"""

from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional, Tuple

from PySide6.QtCore import QRectF, Qt
from PySide6.QtGui import QColor, QIcon, QPainter, QPainterPath, QPalette, QPixmap
from PySide6.QtSvg import QSvgRenderer
from PySide6.QtWidgets import QApplication

from meret.core.models import FileType

#: The vendored SVGs: one <command><ICON_SUFFIX> per toolbar command.
ICON_DIR = Path(__file__).parent / "icons" / "commands"
ICON_SUFFIX = ".svg"

#: Kind for a file whose type is unknown or absent: it gets the neutral tile.
UNKNOWN = "unknown"

#: Tile geometry as a fraction of the requested size, plus the letter size.
_INSET = 0.06
_RADIUS = 0.18
_LETTER = 0.45

#: Neutral grey: a command whose glyph is missing must read as "no glyph", not as a
#: file type.
_NEUTRAL = "#7a7d82"

#: Placeholder tiles: a letter and a colour per kind. Keyed by the type's name, not by
#: the member, so a type added to FileType gets a tile without an edit here.
_TILES: Dict[str, Tuple[str, str]] = {
    "IMAGE": ("I", "#4f8fd0"),
    "VIDEO": ("V", "#c06a8a"),
    "AUDIO": ("A", "#5aa06a"),
    "DOCUMENT": ("D", "#b08a4a"),
    UNKNOWN: ("?", _NEUTRAL),
}


def _palette_colour() -> QColor:
    """The palette's text colour: the tint that keeps glyphs visible in dark mode."""
    app = QApplication.instance()
    if app is None:
        return QColor("#e6e6e6")
    return app.palette().color(QPalette.ColorRole.WindowText)


def _ratio() -> float:
    """Device pixel ratio, so a HiDPI screen gets a sharp glyph, not a blurry one."""
    app = QApplication.instance()
    return float(app.devicePixelRatio()) if app is not None else 1.0


@lru_cache(maxsize=512)
def _tinted(name: str, size: int, colour: str, ratio: float) -> Optional[QPixmap]:
    """Render one vendored SVG in `colour`. None when the file cannot be used.

    Cached on its arguments, so a repaint costs a dict lookup - and a palette switch
    is a different colour, hence a different key, never a stale pixmap.
    """
    path = ICON_DIR / f"{name}{ICON_SUFFIX}"
    if not path.is_file():
        return None
    renderer = QSvgRenderer(str(path))
    if not renderer.isValid():
        return None

    pixmap = QPixmap(int(size * ratio), int(size * ratio))
    pixmap.setDevicePixelRatio(ratio)
    pixmap.fill(Qt.GlobalColor.transparent)
    painter = QPainter(pixmap)
    renderer.render(painter)
    # SourceIn paints the glyph's own alpha with our colour: the SVGs are black
    painter.setCompositionMode(QPainter.CompositionMode.CompositionMode_SourceIn)
    painter.fillRect(pixmap.rect(), QColor(colour))
    painter.end()
    return pixmap


def _tile(letter: str, size: int, colour: QColor, background: str) -> QIcon:
    """A rounded tile with one letter on it: the fallback that always draws."""
    ratio = _ratio()
    pixmap = QPixmap(int(size * ratio), int(size * ratio))
    pixmap.setDevicePixelRatio(ratio)
    pixmap.fill(Qt.GlobalColor.transparent)

    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    body = QRectF(0, 0, size, size).adjusted(
        size * _INSET, size * _INSET, -size * _INSET, -size * _INSET
    )
    path = QPainterPath()
    path.addRoundedRect(body, size * _RADIUS, size * _RADIUS)
    painter.fillPath(path, QColor(background))

    font = painter.font()
    font.setPixelSize(max(8, int(size * _LETTER)))
    font.setBold(True)
    painter.setFont(font)
    painter.setPen(colour)
    painter.drawText(body, Qt.AlignmentFlag.AlignCenter, letter)
    painter.end()
    return QIcon(pixmap)


def command_icon(command: str, size: int) -> QIcon:
    """The glyph for a toolbar command: the vendored SVG, or a tile with its initial.

    A missing file is not an error: the button stays clickable and its tooltip still
    names the command, so a half-filled icon set is still usable.
    """
    colour = _palette_colour()
    pixmap = _tinted(command, size, colour.name(), _ratio())
    if pixmap is None:
        return _tile(command[:1].upper(), size, colour, _NEUTRAL)
    return QIcon(pixmap)


def placeholder(kind: str, size: int) -> QIcon:
    """The tile for a file kind. A kind without one gets the neutral tile."""
    letter, background = _TILES.get(kind, _TILES[UNKNOWN])
    return _tile(letter, size, _palette_colour(), background)


def placeholder_for(file_type: Optional[FileType], size: int) -> QIcon:
    """placeholder(), for a FileType or for nothing at all."""
    return placeholder(getattr(file_type, "name", UNKNOWN), size)


def clear_cache() -> None:
    """Drop the rendered glyphs. Tests need this after switching the palette."""
    _tinted.cache_clear()