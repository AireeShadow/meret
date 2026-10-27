"""The settings dialog: a widget per config field, and Ok writes through the manager."""

import pytest

from tests.gui.conftest import pump

pytest.importorskip("PySide6")

from PySide6.QtWidgets import QCheckBox, QSpinBox  # noqa: E402


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

def test_every_tab_can_be_shown(config, qapp, qtbot):
    """The dialog must survive a tab click, with the app's own theme applied.

    Pages differ in size, and the app runs with a palette the suite never sets - the
    two together are what made the window vanish there.
    """
    from PySide6.QtWidgets import QTabWidget

    from meret.gui.app import _apply_theme
    from meret.gui.settings import SettingsDialog, sections

    _apply_theme(qapp, "dark")  # the app applies one before any window is built

    dialog = SettingsDialog()
    qtbot.addWidget(dialog)
    dialog.show()
    pump(qapp)
    width = dialog.width()

    tabs = dialog.findChild(QTabWidget)
    assert tabs is not None
    assert tabs.count() == len(sections())

    for index in range(tabs.count()):
        tabs.setCurrentIndex(index)
        pump(qapp)
        assert tabs.currentIndex() == index
        assert dialog.isVisible()
        assert dialog.width() == width  # a fixed size: no resize churn

def test_switching_pages_back_and_forth_keeps_the_dialog_settled(config, qapp, qtbot):
    """The freeze came after a few switches: a layout that never settles.

    A spin still lets pump() return, so the tell is geometry: a dialog that changes
    size while pages change is a dialog in a loop.
    """
    from PySide6.QtWidgets import QTabWidget

    from meret.gui.app import _apply_theme
    from meret.gui.settings import SettingsDialog

    _apply_theme(qapp, "dark")  # the app runs with a palette, the suite does not

    dialog = SettingsDialog()
    qtbot.addWidget(dialog)
    dialog.show()
    pump(qapp)
    size = dialog.size()

    tabs = dialog.findChild(QTabWidget)
    order = list(range(tabs.count())) + list(reversed(range(tabs.count())))
    for index in order * 3:  # there and back, three times
        tabs.setCurrentIndex(index)
        pump(qapp, ms=5)
        assert dialog.size() == size  # settled: no size churn on a page change
        assert dialog.isVisible()

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