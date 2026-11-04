"""The viewer tab: one image or one video at a time, with playback controls.

Lives entirely on the GUI thread on purpose: QLabel and QMediaPlayer decode
asynchronously by themselves, so nothing here needs the TaskRunner pool.

QAction is imported from QtGui, never QtWidgets: Qt 6 moved it there and
PySide6 keeps no alias, so the import fails at start-up otherwise.
"""

from pathlib import Path

from PySide6.QtCore import Qt, QUrl
from PySide6.QtGui import QAction, QPixmap, QResizeEvent
from PySide6.QtMultimedia import QAudioOutput, QMediaPlayer
from PySide6.QtMultimediaWidgets import QVideoWidget
from PySide6.QtWidgets import QLabel, QSlider, QToolBar, QVBoxLayout, QWidget

from meret.core.locale import _

#: Extensions played as video instead of being shown as an image.
VIDEO_SUFFIXES = frozenset({".mp4", ".avi", ".mkv", ".mov", ".webm", ".flv", ".wmv", ".m4v"})


class ViewerView(QWidget):
    """Shows one media file: an image in a QLabel, a video in a QVideoWidget."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._path: Path | None = None
        self._is_video = False

        self._image = QLabel(_("Nothing opened"), self)
        self._image.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._image.setWordWrap(True)

        self._video = QVideoWidget(self)

        self._player = QMediaPlayer(self)
        self._audio = QAudioOutput(self)
        self._player.setAudioOutput(self._audio)
        self._player.setVideoOutput(self._video)

        # Playback controls, kept as one widget: shown for video, hidden for images
        self._controls = QToolBar(self)
        self._play_action = QAction(_("Play"), self)
        self._play_action.triggered.connect(self._toggle_play)
        self._controls.addAction(self._play_action)
        self._position = QSlider(Qt.Orientation.Horizontal, self)
        self._position.setRange(0, 0)
        self._position.sliderMoved.connect(self._player.setPosition)
        self._controls.addWidget(self._position)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._image, 1)
        layout.addWidget(self._video, 1)
        layout.addWidget(self._controls)

        self._player.playbackStateChanged.connect(self._on_state)
        self._player.positionChanged.connect(self._position.setValue)
        self._player.durationChanged.connect(self._position.setRange)

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
        self._player.stop()
        self._player.setSource(QUrl())
        self._path = None
        self._is_video = False
        self._video.hide()
        self._controls.hide()
        self._image.setPixmap(QPixmap())
        self._image.setText(_("Nothing opened"))
        self._image.show()

    # ---------- internals ----------

    def _show_image(self, path: Path) -> None:
        """Load an image; a file QPixmap cannot read says so instead of going blank."""
        self._player.stop()
        self._player.setSource(QUrl())
        self._is_video = False
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
        self._player.setSource(QUrl.fromLocalFile(str(path)))
        self._player.play()

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

    def _toggle_play(self) -> None:
        if self._player.playbackState() == QMediaPlayer.PlaybackState.PlayingState:
            self._player.pause()
        else:
            self._player.play()

    def _on_state(self, state: QMediaPlayer.PlaybackState) -> None:
        playing = state == QMediaPlayer.PlaybackState.PlayingState
        self._play_action.setText(_("Pause") if playing else _("Play"))
