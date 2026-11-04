"""The viewer tab: a second page beside the browser, for images and video.

The regression worth keeping is the splitter one: the browser's panes collapsed to
zero width once the splitter became a tab page, because every stretch factor was
left at zero and all the extra width went to the left panel.
"""

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


def test_a_video_extension_goes_to_the_player(window, tmp_path):
    """Video suffixes are played, not handed to QPixmap.

    isHidden(), not isVisible(): the window is never shown here, so every child is
    invisible regardless - what matters is that the viewer asked for the video to be
    shown and hid the image. An empty .mp4 is enough for that, ffmpeg's
    "moov atom not found" notwithstanding.
    """
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"")

    window.viewer.open_file(str(video))

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


def test_leaving_the_viewer_stops_playback(window, tmp_path):
    """A hidden video must not keep playing sound behind the browser tab."""
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"")
    window.tabs.setCurrentWidget(window.viewer)
    window.viewer.open_file(str(video))

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
