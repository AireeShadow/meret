"""Thumbnail decoding and its on-disk cache.

Workers produce QImage, never QPixmap: QPixmap belongs to the GUI thread, QImage
is safe to build in a pool thread. The GUI thread does the conversion on arrival.
"""

import hashlib
import logging
from pathlib import Path
from typing import Optional

import platformdirs
from PIL import Image, ImageOps
from PySide6.QtGui import QImage

from meret.core.config import get_config

logger = logging.getLogger(__name__)

CACHE_SUBDIR = "thumbs"
MAX_SIZE = 1024


def cache_dir() -> Path:
    """Where thumbnails live: the user's cache dir, created on demand."""
    path = Path(platformdirs.user_cache_dir("meret")) / CACHE_SUBDIR
    path.mkdir(parents=True, exist_ok=True)
    return path


class ThumbnailCache:
    """Disk-backed thumbnails. Safe to call from any thread."""

    def __init__(self, size: Optional[int] = None, scope: str = "") -> None:
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

    def load(
        self,
        source: Path,
        *,
        file_id: int,
        mtime: float = 0.0,
        size: Optional[int] = None,
    ) -> Optional[QImage]:
        """A thumbnail of `source`, from the cache or freshly decoded."""
        side = min(int(size or self.size), MAX_SIZE)
        target = self._target(file_id, mtime, side)

        if not target.is_file() and not self._render(source, target, side):
            return None

        image = QImage(str(target))
        return None if image.isNull() else image

    def _render(self, source: Path, target: Path, side: int) -> bool:
        """Decode, rotate by EXIF, fit into side x side, write atomically."""
        try:
            with Image.open(source) as img:
                img = ImageOps.exif_transpose(img)
                img.thumbnail((side, side), Image.Resampling.LANCZOS)
                if img.mode not in ("RGB", "RGBA"):
                    img = img.convert("RGB")
                partial = target.with_name(target.name + ".part")
                img.save(partial, "PNG")
                # replace() is atomic: a reader never sees a half-written PNG
                partial.replace(target)
            return True
        except (OSError, ValueError) as e:
            logger.debug("thumbnail failed for %s: %s", source, e)
            return False