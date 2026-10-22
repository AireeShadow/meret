"""Toolbar and placeholder icons.

The set is vendored under meret/gui/icons/ (see scripts/fetch_icons.py) and named
after the command, so the lookup is `icons/<command>.svg`. Everything falls back to
a QPainter-drawn tile when a file is missing, so the GUI opens with an empty icon
folder too and nothing needs a network at runtime.

Icons are rendered once per (key, size, colour) and cached: a toolbar repaints
constantly and a grid of five thousand rows must not re-render five thousand SVGs.

These are QPixmap, so build and use them on the GUI thread only.
"""

import logging
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

from PySide6.QtCore import QPointF, QRectF, Qt
from PySide6.QtGui import (
    QColor,
    QFont,
    QIcon,
    QPainter,
    QPalette,
    QPen,
    QPixmap,
    QPolygonF,
)
from PySide6.QtWidgets import QApplication

from meret.core.models import FileType

try:  # QtSvg ships with PySide6; the drawn tiles cover its absence
    from PySide6.QtSvg import QSvgRenderer
except ImportError:  # pragma: no cover - only without the QtSvg module
    QSvgRenderer = None

logger = logging.getLogger(__name__)

ICON_DIR = Path(__file__).parent / "icons"
ICON_SUFFIX = ".svg"

#: Render at twice the size and map it back: crisp on HiDPI, smooth on 1x.
SCALE = 2

IMAGE = "image"
VIDEO = "video"
AUDIO = "audio"
UNKNOWN = "unknown"

#: Wash per kind, for the drawn fallback. The vendored SVGs are tinted with the
#: palette instead, so a whole set looks like one family.
_COLOURS: Dict[str, str] = {
    IMAGE: "#4c9a8f",
    VIDEO: "#7a6bb5",
    AUDIO: "#c08a3e",
    UNKNOWN: "#6b7076",
}

_KIND_BY_TYPE = {
    FileType.IMAGE: IMAGE,
    FileType.VIDEO: VIDEO,
    FileType.AUDIO: AUDIO,
}

#: Two letters per command, used only when the SVG is missing.
_COMMAND_GLYPHS: Dict[str, Tuple[str, str]] = {
    "refresh": ("RF", "#4c9a8f"),
    "scan": ("SC", "#4c9a8f"),
    "search": ("SE", "#4c9a8f"),
    "open_file": ("OP", "#4c9a8f"),
    "tag": ("TG", "#7a6bb5"),
    "untag": ("UN", "#7a6bb5"),
    "delete": ("DL", "#7a6bb5"),
    "home": ("HM", "#5b7fb0"),
    "up": ("UP", "#5b7fb0"),
    "sort_files": ("ST", "#5b7fb0"),
    "select_drive": ("DR", "#5b7fb0"),
    "init_db": ("IN", "#c08a3e"),
    "config": ("CF", "#c08a3e"),
    "stats": ("SB", "#c08a3e"),
    "tree": ("TR", "#c08a3e"),
    "prune": ("PR", "#c08a3e"),
    "mount": ("MN", "#5aa46b"),
    "umount": ("UM", "#5aa46b"),
    "command_palette": (":", "#6b7076"),
    "help": ("?", "#6b7076"),
    "quit": ("QT", "#6b7076"),
}

_CACHE: Dict[Tuple[str, int], QIcon] = {}


def kind_for(file_type: Optional[FileType]) -> str:
    """Placeholder kind for a file type. Anything else gets the generic one."""
    return _KIND_BY_TYPE.get(file_type, UNKNOWN)


def _theme_colour() -> str:
    """Icon colour from the active palette, so light and dark both read.

    The vendored SVGs draw with `currentColor`, which Qt does not resolve: it
    renders black, and black on a dark theme is invisible.
    """
    app = QApplication.instance()
    if app is None:
        return _COLOURS[UNKNOWN]
    return app.palette().color(QPalette.ColorRole.WindowText).name()


# ---------- vendored SVGs ----------


def _render_svg(path: Path, side: int, colour: str) -> Optional[QPixmap]:
    """Render `path` at `side` and paint it in `colour`. None when unusable."""
    if QSvgRenderer is None or not path.is_file():
        return None

    renderer = QSvgRenderer(str(path))
    if not renderer.isValid():
        logger.warning("not a usable SVG: %s", path)
        return None

    pixmap = QPixmap(side * SCALE, side * SCALE)
    pixmap.fill(Qt.GlobalColor.transparent)
    painter = QPainter(pixmap)
    try:
        renderer.render(painter)
        # SourceIn replaces the colour and keeps the antialiased alpha
        painter.setCompositionMode(QPainter.CompositionMode.CompositionMode_SourceIn)
        painter.fillRect(pixmap.rect(), QColor(colour))
    finally:
        painter.end()
    pixmap.setDevicePixelRatio(SCALE)
    return pixmap


# ---------- drawn fallback ----------


def _frame(painter: QPainter, side: int, colour: QColor) -> None:
    """The rounded tile the drawn variants sit in."""
    painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    inset = max(1.0, side * 0.03)
    box = QRectF(inset, inset, side - 2 * inset, side - 2 * inset)

    wash = QColor(colour)
    wash.setAlpha(36)
    edge = QColor(colour)
    edge.setAlpha(150)
    painter.setPen(QPen(edge, max(1.0, side / 64.0)))
    painter.setBrush(wash)
    painter.drawRoundedRect(box, side * 0.12, side * 0.12)

    painter.setPen(Qt.PenStyle.NoPen)
    painter.setBrush(colour)


def _draw_text(painter: QPainter, text: str, side: float, ratio: float) -> None:
    """Centred text, sized to `ratio` of the tile."""
    font = QFont()
    font.setBold(True)
    font.setPixelSize(max(6, int(side * ratio)))
    painter.setFont(font)
    painter.drawText(QRectF(0, 0, side, side), Qt.AlignmentFlag.AlignCenter, text)


def _draw_glyph(painter: QPainter, kind: str, side: float, colour: QColor) -> None:
    """The figure inside a file tile: mountains, a play mark, a note, a question mark."""
    if kind == IMAGE:
        painter.drawEllipse(QPointF(0.32 * side, 0.32 * side), 0.075 * side, 0.075 * side)
        painter.drawPolygon(
            QPolygonF(
                [
                    QPointF(0.18 * side, 0.74 * side),
                    QPointF(0.40 * side, 0.42 * side),
                    QPointF(0.54 * side, 0.60 * side),
                    QPointF(0.64 * side, 0.50 * side),
                    QPointF(0.84 * side, 0.74 * side),
                ]
            )
        )
        return

    if kind == VIDEO:
        painter.drawPolygon(
            QPolygonF(
                [
                    QPointF(0.36 * side, 0.28 * side),
                    QPointF(0.36 * side, 0.72 * side),
                    QPointF(0.74 * side, 0.50 * side),
                ]
            )
        )
        return

    if kind == AUDIO:
        painter.drawEllipse(QPointF(0.44 * side, 0.68 * side), 0.10 * side, 0.085 * side)
        painter.setPen(QPen(colour, 0.055 * side))
        painter.drawLine(QPointF(0.53 * side, 0.68 * side), QPointF(0.53 * side, 0.28 * side))
        painter.setPen(Qt.PenStyle.NoPen)
        painter.drawPolygon(
            QPolygonF(
                [
                    QPointF(0.53 * side, 0.28 * side),
                    QPointF(0.74 * side, 0.35 * side),
                    QPointF(0.74 * side, 0.45 * side),
                    QPointF(0.53 * side, 0.38 * side),
                ]
            )
        )
        return

    _draw_text(painter, "?", side, 0.5)


def _render_kind(kind: str, side: int) -> QPixmap:
    """A file tile with the kind's figure in it."""
    colour = QColor(_COLOURS.get(kind, _COLOURS[UNKNOWN]))
    pixmap = QPixmap(side, side)
    pixmap.fill(Qt.GlobalColor.transparent)

    painter = QPainter(pixmap)
    try:
        _frame(painter, side, colour)
        _draw_glyph(painter, kind, float(side), colour)
    finally:
        painter.end()
    return pixmap


def _render_glyph(text: str, colour: str, side: int) -> QPixmap:
    """A toolbar tile with a short text glyph in it."""
    tint = QColor(colour)
    pixmap = QPixmap(side, side)
    pixmap.fill(Qt.GlobalColor.transparent)

    painter = QPainter(pixmap)
    try:
        _frame(painter, side, tint)
        _draw_text(painter, text, float(side), 0.40)
    finally:
        painter.end()
    return pixmap


# ---------- public ----------


def _cached(key: str, side: int, paint: Callable[[], QPixmap]) -> QIcon:
    """Build once per (key, size). The key carries the colour, so a theme change
    re-renders instead of serving a stale tint."""
    cache_key = (key, int(side))
    icon = _CACHE.get(cache_key)
    if icon is None:
        icon = QIcon(paint())
        _CACHE[cache_key] = icon
    return icon


def placeholder(kind: str, side: int) -> QIcon:
    """Icon for a file with no thumbnail: the set's kind icon, else a drawn tile."""
    return _cached(f"kind:{kind}", side, lambda: _kind_pixmap(kind, side))


def _kind_pixmap(kind: str, side: int) -> QPixmap:
    rendered = _render_svg(ICON_DIR / f"{kind}{ICON_SUFFIX}", side, _theme_colour())
    if rendered is not None:
        return rendered
    return _render_kind(kind, side)


def placeholder_for(file_type: Optional[FileType], side: int) -> QIcon:
    """Placeholder for a file type: what the grid shows before a decode lands."""
    return placeholder(kind_for(file_type), side)


def command_icon(command: str, side: int) -> QIcon:
    """Toolbar icon for a command: the vendored SVG, else a drawn two-letter tile."""
    return _cached(
        f"cmd:{command}:{_theme_colour()}", side, lambda: _command_pixmap(command, side)
    )


def _command_pixmap(command: str, side: int) -> QPixmap:
    rendered = _render_svg(
        ICON_DIR / f"{command}{ICON_SUFFIX}", side, _theme_colour()
    )
    if rendered is not None:
        return rendered
    glyph, colour = _COMMAND_GLYPHS.get(command, (command[:2].upper(), _COLOURS[UNKNOWN]))
    return _render_glyph(glyph, colour, side)