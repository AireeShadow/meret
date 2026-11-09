"""The viewer tab: one image or one video at a time, with playback controls.

Lives entirely on the GUI thread on purpose: QLabel and QMediaPlayer decode
asynchronously by themselves, so nothing here needs the TaskRunner pool.

The player is built on the first video, not in __init__: creating a
QMediaPlayer brings a media backend up, and a window that only ever shows
pictures should not pay for it at start-up.

QAction is imported from QtGui, never QtWidgets: Qt 6 moved it there and
PySide6 keeps no alias, so the import fails at start-up otherwise.

The control glyphs are vendored Lucide files named after the action, fetched by
scripts/fetch_icons.py and tinted with the palette like every other command.

The arrow keys mean "step": ten seconds inside a video, one file otherwise.
"""

from pathlib import Path

from PySide6.QtCore import Qt, QUrl, Signal
from PySide6.QtGui import QAction, QKeySequence, QPixmap, QResizeEvent
from PySide6.QtMultimedia import QAudioOutput, QMediaPlayer
from PySide6.QtMultimediaWidgets import QVideoWidget
from PySide6.QtWidgets import QLabel, QSlider, QToolBar, QVBoxLayout, QWidget

from meret.core.locale import _
from meret.gui import icons

#: Extensions played as video instead of being shown as an image.
VIDEO_SUFFIXES = frozenset({".mp4", ".avi", ".mkv", ".mov", ".webm", ".flv", ".wmv", ".m4v"})

#: Control glyph size, in pixels.
ICON = 18

#: How far the arrow keys move the playhead, in milliseconds.
JUMP_MS = 10_000


class ViewerView(QWidget):
    """Shows one media file: an image in a QLabel, a video in a QVideoWidget."""

    #: Stepping through the folder is the window's business, not the viewer's
    next_requested = Signal()
    previous_requested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._path: Path | None = None
        self._is_video = False
        #: True while the position slider is held: playback must not move it then
        self._scrubbing = False
        #: Built on the first video - see the module docstring
        self._player: QMediaPlayer | None = None

        self._image = QLabel(_("Nothing opened"), self)
        self._image.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._image.setWordWrap(True)

        self._video = QVideoWidget(self)

        self._controls = QToolBar(self)
        self._build_controls()
        self._build_shortcuts()

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._image, 1)
        layout.addWidget(self._video, 1)
        layout.addWidget(self._controls)

        # The shortcuts are WidgetWithChildrenShortcut, so the viewer itself has to
        # be able to take the focus or they never fire
        self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)

        self._video.hide()
        self._controls.hide()

    # ---------- public API ----------

    def open_file(self, path: str) -> None:
        """Show the file: as video when the extension says so, else as an image."""
        target = Path(path)
        if target.suffix.casefold() in VIDEO_SUFFIXES:
            self._show_video(target)
        else:
            self._show_image(target)

    def stop(self) -> None:
        """Stop playback and drop the picture. Called when the tab goes away."""
        self._release_media()
        self._path = None
        self._is_video = False
        self._scrubbing = False
        self._video.hide()
        self._controls.hide()
        self._image.setPixmap(QPixmap())
        self._image.setText(_("Nothing opened"))
        self._image.show()

    def retint(self) -> None:
        """Re-render every control glyph in the palette's current colour.

        Called after a theme change: the glyphs are tinted pixmaps, so the old ones
        keep the old colour until they are built again.
        """
        self._previous_action.setIcon(icons.command_icon("previous", ICON))
        self._play_action.setIcon(self._play_icon())
        self._stop_action.setIcon(icons.command_icon("stop", ICON))
        self._next_action.setIcon(icons.command_icon("next", ICON))

    # ---------- stepping ----------

    def step_forward(self) -> None:
        """Right arrow: ten seconds on in a video, the next file otherwise."""
        if not self._is_video:
            self.next_requested.emit()
            return
        self._seek_by(JUMP_MS)

    def step_back(self) -> None:
        """Left arrow: ten seconds back in a video, the previous file otherwise."""
        if not self._is_video:
            self.previous_requested.emit()
            return
        self._seek_by(-JUMP_MS)

    def _seek_by(self, delta: int) -> None:
        """Move the playhead, staying inside the clip."""
        player = self._player
        if player is None:
            return
        target = player.position() + delta
        duration = player.duration()
        if duration > 0:  # unknown duration: let the player clamp it
            target = min(target, duration)
        player.setPosition(max(0, target))

    # ---------- the player ----------

    def _ensure_player(self) -> QMediaPlayer:
        """Build the player once, and wire it to the controls and the video widget."""
        if self._player is not None:
            return self._player

        player = QMediaPlayer(self)
        audio = QAudioOutput(self)
        player.setAudioOutput(audio)
        player.setVideoOutput(self._video)
        player.playbackStateChanged.connect(self._on_state)
        player.positionChanged.connect(self._on_position)
        player.durationChanged.connect(self._on_duration)
        self._player = player
        return player

    def _release_media(self) -> None:
        """Stop and drop the source, leaving the player itself alive for the next file."""
        if self._player is not None:
            self._player.stop()
            self._player.setSource(QUrl())

    # ---------- controls ----------

    def _build_controls(self) -> None:
        """Previous, play/pause, stop, next, then the scrub bar and the clock."""
        self._previous_action = self._action(
            "previous", _("Previous file"), self.previous_requested.emit
        )
        self._play_action = self._action("play", _("Play"), self._toggle_play)
        self._stop_action = self._action("stop", _("Stop"), self._stop)
        self._next_action = self._action("next", _("Next file"), self.next_requested.emit)

        self._position = QSlider(Qt.Orientation.Horizontal, self)
        self._position.setRange(0, 0)
        self._position.setSingleStep(1000)
        self._position.setPageStep(JUMP_MS)
        # sliderPressed/Released bracket the drag. Without them the player's own
        # positionChanged repaints the handle under the pointer on every tick, so
        # the drag looks dead and the seek never lands.
        self._position.sliderPressed.connect(self._on_scrub_start)
        self._position.sliderMoved.connect(self._on_scrub_move)
        self._position.sliderReleased.connect(self._on_scrub_end)

        self._clock = QLabel(_("00:00 / 00:00"), self)
        self._clock.setMinimumWidth(96)
        self._clock.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        for action in (
            self._previous_action,
            self._play_action,
            self._stop_action,
            self._next_action,
        ):
            self._controls.addAction(action)
        self._controls.addWidget(self._position)
        self._controls.addWidget(self._clock)

    def _build_shortcuts(self) -> None:
        """Keys that work while the viewer or any of its widgets has the focus.

        Not keyPressEvent: a focused QSlider eats the arrows to move its handle, and
        a focused button eats Space and Enter to click itself. A shortcut is matched
        before the key reaches the focus widget, so one meaning wins everywhere.
        """
        self._back_action = self._shortcut(
            [QKeySequence(Qt.Key.Key_Left)],
            _("Back 10 seconds / previous file"),
            self.step_back,
        )
        self._forward_action = self._shortcut(
            [QKeySequence(Qt.Key.Key_Right)],
            _("Forward 10 seconds / next file"),
            self.step_forward,
        )
        self._play_pause_action = self._shortcut(
            [
                QKeySequence(Qt.Key.Key_Space),
                QKeySequence(Qt.Key.Key_Return),
                QKeySequence(Qt.Key.Key_Enter),
            ],
            _("Play or pause"),
            self._toggle_play,
        )

    def _shortcut(self, keys: list[QKeySequence], tip: str, slot) -> QAction:
        """One key binding on the viewer's own scope, with its tip."""
        action = QAction(self)
        action.setShortcuts(keys)
        action.setShortcutContext(Qt.ShortcutContext.WidgetWithChildrenShortcut)
        action.setToolTip(tip)
        action.triggered.connect(lambda _checked=False: slot())
        self.addAction(action)
        return action

    def _action(self, name: str, tip: str, slot) -> QAction:
        """One control: a vendored glyph, the tip, and the slot behind it."""
        action = QAction(icons.command_icon(name, ICON), tip, self)
        action.setToolTip(tip)
        action.triggered.connect(lambda _checked=False: slot())
        return action

    def _play_icon(self):
        """Play or pause, whichever the player is doing right now."""
        playing = (
            self._player is not None
            and self._player.playbackState() == QMediaPlayer.PlaybackState.PlayingState
        )
        return icons.command_icon("pause" if playing else "play", ICON)

    def _toggle_play(self) -> None:
        player = self._player
        if player is None:
            return
        if player.playbackState() == QMediaPlayer.PlaybackState.PlayingState:
            player.pause()
        else:
            player.play()

    def _stop(self) -> None:
        """Stop and rewind: the next Play starts from the beginning."""
        if self._player is None:
            return
        self._player.stop()
        self._player.setPosition(0)
        self._position.setValue(0)

    def _on_state(self, state: QMediaPlayer.PlaybackState) -> None:
        """The play button follows the player, so it never lies about the state."""
        playing = state == QMediaPlayer.PlaybackState.PlayingState
        tip = _("Pause") if playing else _("Play")
        self._play_action.setIcon(icons.command_icon("pause" if playing else "play", ICON))
        self._play_action.setToolTip(tip)

    # ---------- the scrub bar ----------

    def _on_scrub_start(self) -> None:
        self._scrubbing = True

    def _on_scrub_move(self, value: int) -> None:
        """While held, the bar shows the drag and the clock previews the target."""
        duration = self._player.duration() if self._player is not None else 0
        self._clock.setText(f"{_clock(value)} / {_clock(duration)}")

    def _on_scrub_end(self) -> None:
        """Let go: seek once, and hand the bar back to the player."""
        self._scrubbing = False
        if self._player is not None:
            self._player.setPosition(self._position.value())

    def _on_position(self, position: int) -> None:
        if not self._scrubbing:
            self._position.setValue(position)
        duration = self._player.duration() if self._player is not None else 0
        self._clock.setText(f"{_clock(position)} / {_clock(duration)}")

    def _on_duration(self, duration: int) -> None:
        self._position.setRange(0, max(0, duration))
        position = self._player.position() if self._player is not None else 0
        self._clock.setText(f"{_clock(position)} / {_clock(duration)}")

    # ---------- media ----------

    def _show_image(self, path: Path) -> None:
        """Load an image; a file QPixmap cannot read says so instead of going blank."""
        self._release_media()
        self._is_video = False
        self._scrubbing = False
        self._video.hide()
        self._controls.hide()

        pixmap = QPixmap(str(path))
        if pixmap.isNull():
            self._path = None
            self._image.setPixmap(QPixmap())
            self._image.setText(_("Cannot open {path}").format(path=path))
            return
        self._path = path
        self._image.setText("")
        self._paint(pixmap)

    def _show_video(self, path: Path) -> None:
        """Load a video and start it."""
        self._path = path
        self._is_video = True
        self._image.hide()
        self._video.show()
        self._controls.show()
        player = self._ensure_player()
        player.setSource(QUrl.fromLocalFile(str(path)))
        player.play()

    def _paint(self, pixmap: QPixmap) -> None:
        """Fit the image into the label, keeping its aspect ratio."""
        self._image.setPixmap(
            pixmap.scaled(
                self._image.size(),
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
        )

    def resizeEvent(self, event: QResizeEvent) -> None:
        """Re-fit the image when the tab is resized, otherwise it stays at the old size."""
        super().resizeEvent(event)
        if self._is_video or self._path is None:
            return
        pixmap = QPixmap(str(self._path))
        if not pixmap.isNull():
            self._paint(pixmap)


def _clock(ms: int) -> str:
    """m:ss for a millisecond position; a negative or missing one reads 0:00."""
    seconds = max(0, int(ms // 1000))
    return f"{seconds // 60}:{seconds % 60:02d}"
