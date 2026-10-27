"""The settings dialog: a widget per config field, and Ok writes through the manager."""

import pytest

from tests.gui.conftest import pump

pytest.importorskip("PySide6")

from PySide6.QtWidgets import QCheckBox, QSpinBox


@pytest.fixture
def config(tmp_path):
    """A config manager on a throw-away file, put back afterwards."""
    from meret.core import config as config_module

    before = config_module._config_instance
    yield config_module.get_config(tmp_path / "config.toml")
    config_module._config_instance = before


@pytest.fixture
def dialog(config, qapp, qtbot):
    from meret.gui.settings import SettingsDialog

    widget = SettingsDialog()
    qtbot.addWidget(widget)
    return widget


def _answer(monkeypatch, action, code):
    """Answer the dialog without a real event loop: act on it, then finish.

    open() is what _edit_config calls, so patching it is what makes the whole flow
    run through - the dialog itself is never shown.
    """
    from meret.gui.settings import SettingsDialog

    def fake_open(self) -> None:
        action(self)
        self.finished.emit(code)

    monkeypatch.setattr(SettingsDialog, "open", fake_open)


def test_every_config_field_has_a_widget(dialog):
    """A field without a widget is a setting the dialog silently cannot change."""
    from meret.core.config import MeretConfig
    from meret.gui.settings import settings_table

    in_model = {
        f"{section}.{name}"
        for section, model in MeretConfig.model_fields.items()
        for name in model.annotation.model_fields
    }

    assert set(dialog._editors) == in_model
    assert {setting.key for setting in settings_table()} == in_model


def test_the_dialog_starts_from_the_current_values(config, qapp, qtbot):
    """The dialog reads the manager, so a value changed before it opens is shown."""
    from meret.gui.settings import SettingsDialog

    config.set("gui.theme", "dark")

    dialog = SettingsDialog()
    qtbot.addWidget(dialog)

    assert dialog.values()["gui.theme"] == "dark"
    assert dialog.values()["scan.workers"] == config.get("scan.workers")


def test_a_flag_is_a_checkbox_and_a_number_a_bounded_spinbox(dialog):
    """Bounds come from the model: 64..1024 for a thumbnail, 1.. for workers."""
    assert isinstance(dialog._editors["scan.skip_existing"], QCheckBox)

    thumbnails = dialog._editors["gui.thumbnail_size"]
    assert isinstance(thumbnails, QSpinBox)
    assert (thumbnails.minimum(), thumbnails.maximum()) == (64, 1024)

    assert dialog._editors["scan.workers"].minimum() == 1


def test_an_empty_path_becomes_none(config, dialog):
    """The paths are Optional: clearing one must mean "auto", not an empty string."""
    dialog._editors["paths.default_db"].setText("   ")

    assert dialog.values()["paths.default_db"] is None
    assert dialog.values()["locale.date_format"] == "%Y-%m-%d"  # not Optional


def test_the_dialog_is_visible_and_sized(config, qapp, qtbot):
    """Nothing else covers a real show(): open() is stubbed across the suite."""
    from meret.gui.settings import SettingsDialog

    dialog = SettingsDialog()
    qtbot.addWidget(dialog)
    dialog.show()
    pump(qapp)

    assert dialog.isVisible()
    assert dialog.width() >= 560
    assert dialog.height() > 100


def test_ok_saves_and_cancel_changes_nothing(tmp_path, config, qapp, qtbot, monkeypatch):
    from PySide6.QtWidgets import QDialog

    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    window = MainWindow(SQLiteRepository(str(tmp_path / "meret.db")), tmp_path / "meret.db")
    qtbot.addWidget(window)

    _answer(
        monkeypatch,
        lambda dialog: dialog._editors["scan.workers"].setValue(3),
        QDialog.DialogCode.Accepted,
    )
    window._edit_config()
    assert config.get("scan.workers") == 3

    _answer(
        monkeypatch,
        lambda dialog: dialog._editors["scan.workers"].setValue(9),
        QDialog.DialogCode.Rejected,
    )
    window._edit_config()
    assert config.get("scan.workers") == 3  # the rejected edit is gone


def test_saving_keeps_a_theme_change_in_effect(tmp_path, config, qapp, qtbot, monkeypatch):
    from PySide6.QtWidgets import QDialog

    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    window = MainWindow(SQLiteRepository(str(tmp_path / "meret.db")), tmp_path / "meret.db")
    qtbot.addWidget(window)

    def change(dialog) -> None:
        dialog._editors["gui.theme"].setCurrentText("dark")
        dialog._editors["gui.thumbnail_size"].setValue(128)

    _answer(monkeypatch, change, QDialog.DialogCode.Accepted)
    window._edit_config()
    pump(qapp)

    assert config.get("gui.theme") == "dark"
    assert window.grid.iconSize().width() == 128
    assert window._thumbs.size == 128
    assert all(not action.icon().isNull() for action in window._actions.values())


def test_the_poll_pauses_while_the_dialog_is_open(
    tmp_path, config, qapp, qtbot, monkeypatch
):
    """A modal window must not fight with a refresh underneath it."""
    from PySide6.QtWidgets import QDialog

    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    window = MainWindow(SQLiteRepository(str(tmp_path / "meret.db")), tmp_path / "meret.db")
    qtbot.addWidget(window)
    seen = []

    _answer(
        monkeypatch,
        lambda dialog: seen.append(window._poll.isActive()),
        QDialog.DialogCode.Accepted,
    )
    window._edit_config()

    assert seen == [False]  # stopped while the dialog was up
    assert window._poll.isActive()  # and ticking again once it closed

def test_the_dialog_shows_every_section_at_once(config, qapp, qtbot):
    """No tabs: switching them inside a dialog is where the app kept freezing."""
    from PySide6.QtWidgets import QGroupBox, QScrollArea

    from meret.gui.settings import SettingsDialog, sections

    dialog = SettingsDialog()
    qtbot.addWidget(dialog)
    dialog.show()
    pump(qapp)

    assert dialog.findChild(QScrollArea) is not None
    assert [box.title() for box in dialog.findChildren(QGroupBox)] == [
        title for _section, title in sections()
    ]


def test_the_dialog_settles_after_being_shown(config, qapp, qtbot):
    """A window that keeps resizing itself is a window in a loop: the size must hold."""
    from meret.gui.app import _apply_theme
    from meret.gui.settings import SettingsDialog

    _apply_theme(qapp, "dark")  # the app runs with a palette, the suite does not

    dialog = SettingsDialog()
    qtbot.addWidget(dialog)
    dialog.show()
    pump(qapp)
    size = dialog.size()

    for _round in range(3):
        pump(qapp)
        assert dialog.size() == size
        assert dialog.isVisible()


def test_every_editor_is_reachable_from_the_scroll_area(config, qapp, qtbot):
    """A field that ends up outside the scrolled body cannot be edited at all."""
    from meret.gui.settings import SettingsDialog, settings_table

    dialog = SettingsDialog()
    qtbot.addWidget(dialog)
    dialog.show()
    pump(qapp)

    for setting in settings_table():
        editor = dialog._editors[setting.key]
        assert editor.isVisible()

def test_a_theme_is_applied_only_when_it_changed(
    tmp_path, config, qapp, qtbot, monkeypatch
):
    """Re-polishing the app is a visible stall: a save with the same theme must skip it."""
    from PySide6.QtWidgets import QDialog

    import meret.gui.app as app_module
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    applied = []
    monkeypatch.setattr(app_module, "_apply_theme", lambda *a: applied.append(a[1]))

    window = MainWindow(SQLiteRepository(str(tmp_path / "meret.db")), tmp_path / "meret.db")
    qtbot.addWidget(window)
    applied.clear()  # __init__ applies the theme once, that is not the point here

    _answer(monkeypatch, lambda dialog: None, QDialog.DialogCode.Accepted)
    window._edit_config()
    assert applied == []  # nothing moved, nothing to re-polish

    def change(dialog) -> None:
        dialog._editors["gui.theme"].setCurrentText("dark")

    _answer(monkeypatch, change, QDialog.DialogCode.Accepted)
    window._edit_config()
    assert applied == ["dark"]

def test_every_tab_can_be_shown(config, qapp, qtbot):
    """The dialog must survive a show, with the app's own theme applied.

    The groups differ in size, and the app runs with a palette the suite never sets -
    the two together are what made the window vanish there.
    """
    from PySide6.QtWidgets import QGroupBox

    from meret.gui.app import _apply_theme
    from meret.gui.settings import SettingsDialog, sections

    _apply_theme(qapp, "dark")  # the app applies one before any window is built

    dialog = SettingsDialog()
    qtbot.addWidget(dialog)
    dialog.show()
    pump(qapp)
    width = dialog.width()

    boxes = dialog.findChildren(QGroupBox)
    assert len(boxes) == len(sections())
    assert all(box.isVisible() for box in boxes)
    assert dialog.isVisible()
    assert dialog.width() == width  # a fixed size: no resize churn

def test_switching_pages_back_and_forth_keeps_the_dialog_settled(config, qapp, qtbot):
    """The freeze came after a few scrolls: a layout that never settles.

    A spin still lets pump() return, so the tell is geometry: a dialog that changes
    size while the body scrolls is a dialog in a loop.
    """
    from PySide6.QtWidgets import QScrollArea

    from meret.gui.app import _apply_theme
    from meret.gui.settings import SettingsDialog

    _apply_theme(qapp, "dark")  # the app runs with a palette, the suite does not

    dialog = SettingsDialog()
    qtbot.addWidget(dialog)
    dialog.show()
    pump(qapp)
    size = dialog.size()

    area = dialog.findChild(QScrollArea)
    assert area is not None
    bar = area.verticalScrollBar()

    for _ in range(3):  # there and back, three times
        bar.setValue(bar.maximum())
        pump(qapp, ms=5)
        assert dialog.size() == size  # settled: no size churn while scrolling
        assert dialog.isVisible()
        bar.setValue(0)
        pump(qapp, ms=5)
        assert dialog.size() == size
        assert dialog.isVisible()

def test_the_dialog_does_not_flood_the_event_loop(config, qapp, qtbot, tally):
    """A storm is what makes processEvents() never settle - and the suite hang.

    The numbers are printed, not only asserted: when this fails, the busiest
    receiver in the report is the widget that has to be fixed.
    """
    from PySide6.QtWidgets import QScrollArea

    from meret.gui.settings import SettingsDialog

    dialog = SettingsDialog()
    qtbot.addWidget(dialog)
    dialog.show()
    pump(qapp, 50)

    area = dialog.findChild(QScrollArea)
    assert area is not None
    bar = area.verticalScrollBar()

    tally.reset()
    bar.setValue(bar.maximum())
    pump(qapp, 100)
    bar.setValue(0)
    pump(qapp, 100)
    tally.stop()

    print("\n" + tally.report())
    assert tally.rate() < 500  # a scroll is a handful of events, not thousands