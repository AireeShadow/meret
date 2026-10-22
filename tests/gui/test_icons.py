"""Icons: the vendored set is used, named after commands, and tinted with the theme."""

import pytest

pytest.importorskip("PySide6")


def test_every_command_has_a_usable_icon(qapp):
    """With the SVGs, or from the drawn fallback when the folder is empty."""
    from meret.gui import icons
    from meret.gui.commands import command_table

    for command, _description in command_table():
        icon = icons.command_icon(command, 28)
        assert not icon.isNull(), command
        assert not icon.pixmap(28, 28).isNull(), command


def test_icon_files_are_named_after_the_commands(qapp):
    """The loader looks up the command name, so an upstream rename never reaches
    the code. Run scripts/fetch_icons.py after adding a command."""
    from meret.gui import icons
    from meret.gui.commands import command_table

    missing = [
        command
        for command, _ in command_table()
        if not (icons.ICON_DIR / f"{command}{icons.ICON_SUFFIX}").is_file()
    ]
    assert missing == []


def test_icon_follows_the_palette_colour(qapp):
    """The SVGs draw with currentColor, which Qt renders black: without the tint a
    dark theme would show black icons on a dark toolbar."""
    from PySide6.QtGui import QColor, QPalette

    from meret.gui import icons

    original = QPalette(qapp.palette())
    palette = QPalette(original)
    palette.setColor(QPalette.ColorRole.WindowText, QColor("#ff0000"))
    qapp.setPalette(palette)
    try:
        image = icons.command_icon("search", 32).pixmap(32, 32).toImage()
        opaque = [
            image.pixelColor(x, y)
            for x in range(image.width())
            for y in range(image.height())
            if image.pixelColor(x, y).alpha() > 200
        ]
    finally:
        qapp.setPalette(original)

    assert opaque, "the icon rendered nothing"
    assert all(pixel.red() > 200 and pixel.green() < 60 for pixel in opaque)


def test_placeholder_icons_differ_per_kind(qapp):
    from meret.core.models import FileType
    from meret.gui import icons

    image = icons.placeholder_for(FileType.IMAGE, 64).pixmap(64, 64).toImage()
    video = icons.placeholder_for(FileType.VIDEO, 64).pixmap(64, 64).toImage()
    generic = icons.placeholder_for(None, 64).pixmap(64, 64).toImage()

    assert not image.isNull() and not video.isNull()
    assert image != video
    assert generic == icons.placeholder(icons.UNKNOWN, 64).pixmap(64, 64).toImage()