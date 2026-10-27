"""Thumbnail decoding and its on-disk cache.

Workers produce QImage, never QPixmap: QPixmap belongs to the GUI thread, QImage
is safe to build in a pool thread. The GUI thread does the conversion on arrival.

Images go through Pillow. A video container has no Pillow plugin, so one frame is
pulled out of the file with ffmpeg (the toolchain ffprobe already comes from) and
decoded from a pipe. Audio only ever has an embedded cover, which Pillow reads on
its own. A missing thumbnail is never just "nothing": it carries a reason, so the
grid can say why and the tooltip is honest.
"""

import hashlib
import io
import logging
import shutil
import subprocess
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import platformdirs
from PIL import Image, ImageOps
from PySide6.QtGui import QImage

from meret.core.config import get_config

logger = logging.getLogger(__name__)

CACHE_SUBDIR = "thumbs"
MAX_SIZE = 1024

#: Seek points for the frame grab: a clip shorter than a second has nothing at 1s.
VIDEO_SEEKS = ("1", "0")
#: Seconds a single ffmpeg call may take before it is abandoned.
VIDEO_TIMEOUT = 20

#: Containers Pillow cannot open: the frame has to come from ffmpeg.
VIDEO_SUFFIXES = frozenset(
    {".mp4", ".m4v", ".mkv", ".mov", ".avi", ".wmv", ".webm", ".flv", ".mpg", ".mpeg"}
)


class Reason:
    """Why a thumbnail is missing. The caller turns this into a tooltip."""

    NO_TOOL = "no-tool"        # ffmpeg is not on PATH, so a video has no frame
    NO_FRAME = "no-frame"      # ffmpeg ran and gave nothing usable
    NO_DECODER = "no-decoder"  # this kind of file has no preview at all
    UNREADABLE = "unreadable"  # the file is gone or cannot be read


@dataclass(frozen=True)
class Thumbnail:
    """A decoded thumbnail, or the reason there is none."""

    image: QImage | None = None
    reason: str = ""

    @property
    def ok(self) -> bool:
        """True when there is a usable image."""
        return self.image is not None and not self.image.isNull()


@lru_cache(maxsize=1)
def ffmpeg_path() -> str | None:
    """ffmpeg from PATH, probed once. None when only ffprobe is installed."""
    return shutil.which("ffmpeg")


def cache_dir() -> Path:
    """Where thumbnails live: the user's cache dir, created on demand."""
    path = Path(platformdirs.user_cache_dir("meret")) / CACHE_SUBDIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def _video_frame(source: Path) -> tuple[Image.Image | None, str]:
    """One frame of a video, decoded from ffmpeg's stdout.

    `-ss` before `-i` is a fast keyframe seek, and the scale keeps a 4K frame from
    being decoded at full size only to be shrunk again right after. A clip shorter
    than the seek point yields nothing, so the very first frame is the fallback.
    """
    binary = ffmpeg_path()
    if binary is None:
        logger.debug("no ffmpeg on PATH, no thumbnail for %s", source)
        return None, Reason.NO_TOOL

    for seek in VIDEO_SEEKS:
        command = [
            binary,
            "-nostdin",
            "-loglevel", "error",
            "-ss", seek,
            "-i", str(source),
            "-frames:v", "1",
            "-vf", f"scale='min({MAX_SIZE},iw)':-2",
            "-f", "image2pipe",
            "-vcodec", "png",
            "-",
        ]
        try:
            done = subprocess.run(command, capture_output=True, timeout=VIDEO_TIMEOUT)
        except (OSError, subprocess.SubprocessError) as e:
            logger.debug("ffmpeg failed for %s: %s", source, e)
            return None, Reason.NO_FRAME
        if not done.stdout:
            continue
        try:
            with Image.open(io.BytesIO(done.stdout)) as frame:
                return frame.convert("RGB"), ""
        except (OSError, ValueError) as e:
            logger.debug("ffmpeg returned no usable frame for %s: %s", source, e)
    return None, Reason.NO_FRAME


def _decode(source: Path) -> tuple[Image.Image | None, str]:
    """The source as a detached PIL image, plus why it is missing, if it is."""
    if source.suffix.casefold() in VIDEO_SUFFIXES:
        return _video_frame(source)
    try:
        with Image.open(source) as img:
            # copy(): the handle closes with the block, the pixels must outlive it
            return ImageOps.exif_transpose(img).copy(), ""
    except FileNotFoundError:
        return None, Reason.UNREADABLE
    except (OSError, ValueError) as e:
        logger.debug("cannot decode %s: %s", source, e)
        return None, Reason.NO_DECODER


class ThumbnailCache:
    """Disk-backed thumbnails. Safe to call from any thread."""

    def __init__(self, size: int | None = None, scope: str = "") -> None:
        self.size = min(int(size or get_config().get("gui.thumbnail_size", 256)), MAX_SIZE)
        self._dir = cache_dir()
        # The cache dir is shared by every database, and a file id is only unique
        # inside one of them: the scope belongs in the key
        self._scope = hashlib.sha1(str(scope).encode("utf-8")).hexdigest()[:12]

    def _target(self, file_id: int, mtime: float, side: int) -> Path:
        """Key on the database, the file's identity and its fingerprint.

        Editing a file moves its mtime, so a stale thumbnail cannot survive.
        """
        raw = f"{self._scope}:{file_id}:{side}:{mtime:.6f}"
        return self._dir / f"{hashlib.sha1(raw.encode('utf-8')).hexdigest()}.png"

    def load_result(
        self,
        source: Path,
        *,
        file_id: int,
        mtime: float = 0.0,
        size: int | None = None,
    ) -> Thumbnail:
        """A thumbnail of `source`, from the cache or freshly decoded."""
        side = min(int(size or self.size), MAX_SIZE)
        target = self._target(file_id, mtime, side)

        if target.is_file():
            image = QImage(str(target))
            if not image.isNull():
                return Thumbnail(image)
            # A crash between save() and replace() leaves a truncated PNG, and it
            # would stay unreadable forever: drop it and decode the file again
            target.unlink(missing_ok=True)

        reason = self._render(source, target, side)
        if reason:
            return Thumbnail(None, reason)

        image = QImage(str(target))
        if image.isNull():
            return Thumbnail(None, Reason.UNREADABLE)
        return Thumbnail(image)

    def load(
        self,
        source: Path,
        *,
        file_id: int,
        mtime: float = 0.0,
        size: int | None = None,
    ) -> QImage | None:
        """Just the image, for callers that do not care why it is missing."""
        return self.load_result(source, file_id=file_id, mtime=mtime, size=size).image

    def _render(self, source: Path, target: Path, side: int) -> str:
        """Decode, rotate by EXIF, fit into side x side, write atomically.

        Returns "" on success, otherwise the reason there is no thumbnail.
        """
        image, reason = _decode(source)
        if image is None:
            return reason or Reason.NO_DECODER
        try:
            image.thumbnail((side, side), Image.Resampling.LANCZOS)
            if image.mode not in ("RGB", "RGBA"):
                image = image.convert("RGB")
            partial = target.with_name(target.name + ".part")
            image.save(partial, "PNG")
            # replace() is atomic: a reader never sees a half-written PNG
            partial.replace(target)
            return ""
        except (OSError, ValueError) as e:
            logger.debug("thumbnail failed for %s: %s", source, e)
            return Reason.UNREADABLE