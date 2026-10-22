"""Placeholder icons for entries whose thumbnail is missing.

Drawn with QPainter, so the GUI ships no image files and the icons follow the
palette instead of fighting it. One pixmap per (kind, side) is cached: a folder
of five thousand videos must not repaint five thousand icons.

These are QPixmap, so build and use them on the GUI thread only.
"""

import logging
from typing import Dict, Optional, Tuple

from PySide6.QtCore import QPointF, QRectF, Qt
from PySide6.QtGui import QColor, QFont, QIcon, QPainter, QPen, QPixmap, QPolygonF

from meret.core.models import FileType

logger = logging.getLogger(__name__)

IMAGE = "image"
VIDEO = "video"
AUDIO = "audio"
UNKNOWN = "unknown"

#: Wash and line art per kind. Muted on purpose: a placeholder must not shout.
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

_CACHE: Dict[Tuple[str, int], QIcon] = {}


def kind_for(file_type: Optional[FileType]) -> str:
    """Placeholder kind for a file type. Anything else gets the generic one."""
    return _KIND_BY_TYPE.get(file_type, UNKNOWN)


def _draw_glyph(painter: QPainter, kind: str, side: float, colour: QColor) -> None:
    """The figure inside the tile: mountains, a play mark, a note, a question mark."""
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

    font = QFont()
    font.setBold(True)
    font.setPixelSize(int(side * 0.5))
    painter.setFont(font)
    painter.drawText(QRectF(0, 0, side, side), Qt.AlignmentFlag.AlignCenter, "?")


def _render(kind: str, side: int) -> QPixmap:
    """One tile: a soft rounded square with the kind's figure in it."""
    colour = QColor(_COLOURS.get(kind, _COLOURS[UNKNOWN]))
    pixmap = QPixmap(side, side)
    pixmap.fill(Qt.GlobalColor.transparent)

    painter = QPainter(pixmap)
    try:
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
        _draw_glyph(painter, kind, float(side), colour)
    finally:
        painter.end()
    return pixmap


def placeholder(kind: str, side: int) -> QIcon:
    """Cached placeholder icon of `side` pixels."""
    key = (kind, int(side))
    icon = _CACHE.get(key)
    if icon is None:
        icon = QIcon(_render(kind, side))
        _CACHE[key] = icon
    return icon


def placeholder_for(file_type: Optional[FileType], side: int) -> QIcon:
    """Placeholder for a file type: what the grid shows before a decode lands."""
    return placeholder(kind_for(file_type), side)