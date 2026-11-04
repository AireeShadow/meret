"""The viewer tab: a second page beside the browser, for images and video.

The regression worth keeping is the splitter one: the browser's panes collapsed to
zero width once the splitter became a tab page, because every stretch factor was
left at zero and all the extra width went to the left panel.
"""

import os
import pytest

from tests.gui.conftest import pump_until

pytest.importorskip("PySide6")


@pytest.fixture
def window(tmp_path, qapp, qtbot):
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    db = tmp_path / "meret.db"
    repo = SQLiteRepository(str(db))
    main = MainWindow(repo, db)
    qtbot.addWidget(main)
    # The start-up listing ends in _update_status(), and it would overwrite a
    # status line a test wrote before it lands
    assert pump_until(lambda: not main._tasks._inflight, timeout_ms=15000), (
        "the start-up listing never landed"
    )
    return main


@pytest.fixture
def image_file(tmp_path):
    """A real, decodable PNG: QPixmap must not see a stub."""
    from PIL import Image

    path = tmp_path / "picture.png"
    Image.new("RGB", (40, 30), (120, 90, 200)).save(path)
    return path


@pytest.fixture
def video_file(tmp_path):
    """A real one-second clip: an empty file makes ffmpeg complain about a moov atom."""
    import subprocess

    path = tmp_path / "clip.mp4"
    done = subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=c=black:s=32x32:d=1",
            "-pix_fmt",
            "yuv420p",
            str(path),
        ],
        capture_output=True,
    )
    if done.returncode != 0 or not path.exists():
        pytest.skip("ffmpeg is not on PATH, so no sample clip can be built")
    return path


def _item(window, name, source=None, vpath=None):
    """A grid row the way the window builds one."""
    return window._add_file_item(name, str(source) if source else None, 1, 0.0, None, vpath)


# ---------- the tabs themselves ----------


def test_the_window_opens_on_the_browser_tab(window):
    """Two pages, and the browser is the one that is shown."""
    assert window.tabs.count() == 2
    assert window.tabs.currentWidget() is not window.viewer
    assert window.tabs.tabText(0) and window.tabs.tabText(1)


def test_the_browser_panes_keep_their_width(window):
    """The regression: with every stretch factor at zero the right side collapses."""
    window.resize(1200, 800)
    window.show()
    pump_until(lambda: window.grid.width() > 0, timeout_ms=2000)

    assert window.grid.width() > 0, "the grid collapsed to zero width"
    assert window.preview.width() > 0, "the preview collapsed to zero width"


# ---------- opening files ----------


def test_double_click_shows_an_image_in_the_viewer(window, image_file):
    """A double-click lands on the viewer tab with the picture loaded."""
    window._open_in_viewer(_item(window, image_file.name, image_file))

    assert window.tabs.currentWidget() is window.viewer
    assert window.viewer._is_video is False
    assert not window.viewer._image.pixmap().isNull()


def test_a_video_extension_goes_to_the_player(window, video_file):
    """Video suffixes are played, not handed to QPixmap.

    isHidden(), not isVisible(): the window is never shown here, so every child is
    invisible regardless - what matters is that the viewer asked for the video to be
    shown and hid the image.
    """
    window.viewer.open_file(str(video_file))

    assert window.viewer._is_video is True
    assert not window.viewer._video.isHidden()
    assert not window.viewer._controls.isHidden()
    assert window.viewer._image.isHidden()


def test_a_file_that_cannot_be_decoded_says_so(window, tmp_path):
    """A broken image is a message, never a silently empty viewer."""
    broken = tmp_path / "broken.png"
    broken.write_bytes(b"not really a png")

    window.viewer.open_file(str(broken))

    assert "broken.png" in window.viewer._image.text()


def test_double_click_on_a_folder_descends(window):
    """A folder row has no source: it opens the virtual folder instead."""
    window._open_in_viewer(_item(window, "beach", vpath="/travel/beach"))

    assert window._vpath == "/travel/beach"
    assert window.tabs.currentWidget() is not window.viewer


# ---------- leaving the tab ----------


def test_leaving_the_viewer_stops_playback(window, video_file):
    """A hidden video must not keep playing sound behind the browser tab."""
    window.tabs.setCurrentWidget(window.viewer)
    window.viewer.open_file(str(video_file))

    window.tabs.setCurrentIndex(0)

    assert window.viewer._path is None
    assert window.viewer._is_video is False
    assert window.viewer._video.isHidden()


def test_stop_returns_to_the_placeholder(window, image_file):
    """stop() drops the picture and the path, so nothing stale is left behind."""
    window.viewer.open_file(str(image_file))
    window.viewer.stop()

    assert window.viewer._path is None
    assert window.viewer._image.pixmap().isNull()


def test_the_video_suffixes_are_a_fixed_set():
    """The set is the only thing deciding what is played, so pin its members."""
    from meret.gui.viewer import VIDEO_SUFFIXES

    assert ".mp4" in VIDEO_SUFFIXES
    assert ".mkv" in VIDEO_SUFFIXES
    assert ".png" not in VIDEO_SUFFIXES
    assert ".jpg" not in VIDEO_SUFFIXES


def test_the_controls_carry_four_buttons(window):
    """Previous, play/pause, stop, next - and nothing that seeks by itself.

    The scrub bar and the clock are widgets, and addWidget() wraps each in its own
    QWidgetAction, so only the actions with a label are buttons.
    """
    buttons = [a for a in window.viewer._controls.actions() if a.text()]

    assert len(buttons) == 4
    assert all(not a.icon().isNull() for a in buttons)


def test_scrubbing_is_not_overwritten_by_playback(window, monkeypatch):
    """The regression: positionChanged repainted the handle under the pointer."""
    viewer = window.viewer
    player = viewer._ensure_player()
    viewer._position.setRange(0, 1000)
    seeks = []
    monkeypatch.setattr(player, "setPosition", seeks.append)

    viewer._position.sliderPressed.emit()
    viewer._position.setValue(700)  # what Qt itself does while the handle is dragged
    player.positionChanged.emit(120)  # playback keeps reporting meanwhile

    assert viewer._position.value() == 700, "playback overwrote the dragged position"

    viewer._position.sliderMoved.emit(700)
    viewer._position.sliderReleased.emit()

    assert seeks == [700]


def test_next_asks_the_window_for_the_next_file(window, tmp_path):
    """The viewer has no folder of its own: it asks, the window selects."""
    from PIL import Image

    for name in ("a.png", "b.png"):
        path = tmp_path / name
        Image.new("RGB", (8, 8)).save(path)
        _item(window, name, path)
    window.grid.setCurrentRow(0)

    window.viewer.next_requested.emit()

    assert window.grid.currentRow() == 1


def test_previous_at_the_first_file_says_so(window, tmp_path):
    """No wrap: the first file reports the end instead of jumping to the last."""
    from PIL import Image

    path = tmp_path / "only.png"
    Image.new("RGB", (8, 8)).save(path)
    _item(window, "only.png", path)
    window.grid.setCurrentRow(0)

    window.viewer.previous_requested.emit()

    assert window.grid.currentRow() == 0
    assert window._status_label.text()


def test_every_control_glyph_is_vendored():
    """prune() deletes what GLYPHS does not name: a missing file is a silent tile."""
    from meret.gui import icons

    for name in ("previous", "play", "pause", "stop", "next"):
        path = icons.ICON_DIR / f"{name}{icons.ICON_SUFFIX}"
        assert path.is_file(), f"{path.name} is missing - run scripts/fetch_icons.py"


def test_the_media_backend_chatter_is_off_by_default(monkeypatch):
    """A window has no console: the backend's start-up lines are noise there."""
    from meret.gui.app import QUIET_MEDIA_RULES, quiet_media_logs

    monkeypatch.delenv("MERET_MEDIA_LOG", raising=False)
    monkeypatch.delenv("QT_LOGGING_RULES", raising=False)

    quiet_media_logs()

    assert QUIET_MEDIA_RULES in os.environ["QT_LOGGING_RULES"]


def test_the_quiet_rules_can_be_appended_to(monkeypatch):
    """A rule the user already set must survive, not be replaced."""
    from meret.gui.app import quiet_media_logs

    monkeypatch.delenv("MERET_MEDIA_LOG", raising=False)
    monkeypatch.setenv("QT_LOGGING_RULES", "meret.debug=true")

    quiet_media_logs()

    assert os.environ["QT_LOGGING_RULES"].startswith("meret.debug=true;")


def test_meret_media_log_turns_the_category_back_on(monkeypatch):
    """The escape hatch: one variable, and the full backend log is back."""
    from meret.gui.app import quiet_media_logs

    monkeypatch.setenv("MERET_MEDIA_LOG", "1")
    monkeypatch.delenv("QT_LOGGING_RULES", raising=False)

    quiet_media_logs()

    assert "qt.multimedia.*=true" in os.environ["QT_LOGGING_RULES"]
