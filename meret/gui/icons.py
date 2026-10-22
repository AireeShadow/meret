"""Placeholder icons: for missing thumbnails and for the toolbar.

Drawn with QPainter, so the GUI ships no image files and the icons follow the
palette instead of fighting it. One pixmap per (key, side) is cached: a folder of
five thousand videos must not repaint five thousand icons, and a toolbar repaints
its buttons constantly.

These are QPixmap, so build and use them on the GUI thread only.
"""

import logging
from typing import Callable, Dict, Optional, Tuple

from PySide6.QtCore import QPointF, QRectF, Qt
from PySide6.QtGui import QColor, QFont, QIcon, QPainter, QPen, QPixmap, QPolygonF

from meret.core.models import FileType

logger = logging.getLogger(__name__)

IMAGE = "image"
VIDEO = "video"
AUDIO = "audio"
UNKNOWN = "unknown"

#: Wash and line art per file kind. Muted on purpose: a placeholder must not shout.
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

#: Two letters per command. Twenty-one buttons with words do not fit a 1280px
#: window, and Qt would hide the rest behind a ">>" button - which reads as
#: missing commands. The wash groups them: file work, tags, navigation, database.
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


def _frame(painter: QPainter, side: int, colour: QColor) -> None:
    """The rounded tile both variants sit in."""
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


def _draw_text(painter: QPainter, text: str, side: float, ratio: float) -> None:
    """Centred text, sized to `ratio` of the tile."""
    font = QFont()
    font.setBold(True)
    font.setPixelSize(max(6, int(side * ratio)))
    painter.setFont(font)
    painter.drawText(QRectF(0, 0, side, side), Qt.AlignmentFlag.AlignCenter, text)


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


def _cached(key: str, side: int, paint: Callable[[], QPixmap]) -> QIcon:
    """Build once per (key, size): the grid and the toolbar ask repeatedly."""
    cache_key = (key, int(side))
    icon = _CACHE.get(cache_key)
    if icon is None:
        icon = QIcon(paint())
        _CACHE[cache_key] = icon
    return icon


def placeholder(kind: str, side: int) -> QIcon:
    """Cached placeholder icon of `side` pixels."""
    return _cached(f"kind:{kind}", side, lambda: _render_kind(kind, side))


def placeholder_for(file_type: Optional[FileType], side: int) -> QIcon:
    """Placeholder for a file type: what the grid shows before a decode lands."""
    return placeholder(kind_for(file_type), side)


def command_icon(command: str, side: int) -> QIcon:
    """Cached toolbar glyph for a command. Unknown commands get their own letters."""
    glyph, colour = _COMMAND_GLYPHS.get(
        command, (command[:2].upper(), _COLOURS[UNKNOWN])
    )
    return _cached(
        f"cmd:{command}:{glyph}:{colour}", side, lambda: _render_glyph(glyph, colour, side)
    )