"""The palette: `system` keeps the platform look, `dark` sets a Fusion palette."""

from PySide6.QtCore import QCoreApplication
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication

_DARK_PALETTE = {
    "Window": "#1e1f22",
    "WindowText": "#e6e6e6",
    "Base": "#25262a",
    "AlternateBase": "#2b2d31",
    "Text": "#e6e6e6",
    "Button": "#2b2d31",
    "ButtonText": "#e6e6e6",
    "BrightText": "#ffffff",
    "Highlight": "#3d6ea5",
    "HighlightedText": "#ffffff",
    "ToolTipBase": "#2b2d31",
    "ToolTipText": "#e6e6e6",
    "PlaceholderText": "#9aa0a6",
    "Link": "#6ea8dc",
}

#: Without these, disabled buttons and dimmed text stay bright in dark mode.
_DISABLED_PALETTE = {
    "WindowText": "#7a7d82",
    "Text": "#7a7d82",
    "ButtonText": "#7a7d82",
    "HighlightedText": "#9aa0a6",
}


def apply_theme(app: QCoreApplication | None, theme: str) -> None:
    """`system` keeps the platform look; `dark` sets a Fusion palette.

    QCoreApplication, not QApplication, because instance() hands back the base type:
    a caller should not have to cast, and an app without a palette is simply left be.
    """
    if not isinstance(app, QApplication) or theme == "system":
        return
    app.setStyle("Fusion")
    if theme != "dark":
        app.setPalette(app.style().standardPalette())
        return
    palette = QPalette()
    for role, colour in _DARK_PALETTE.items():
        palette.setColor(getattr(QPalette.ColorRole, role), QColor(colour))
    for role, colour in _DISABLED_PALETTE.items():
        palette.setColor(
            QPalette.ColorGroup.Disabled,
            getattr(QPalette.ColorRole, role),
            QColor(colour),
        )
    app.setPalette(palette)
