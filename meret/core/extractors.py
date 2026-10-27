"""
Metadata extractors for different media types.

Each extractor can:
1. Tell whether it handles a given file (can_extract)
2. Extract metadata (extract)

Adding new formats is easy: subclass MetadataExtractor
and register it in ExtractorRegistry.
"""

import json
import logging
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from meret.core.models import FileType

logger = logging.getLogger(__name__)

_ffprobe_missing_reported = False


def _ffprobe(file_path: Path) -> dict[str, Any]:
    """Runs ffprobe and returns its JSON.

    `-v error` (not quiet) so a failure carries ffprobe's own reason in stderr.
    """
    global _ffprobe_missing_reported
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-print_format",
                "json",
                "-show_format",
                "-show_streams",
                str(file_path),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
            timeout=30,
        )
    except FileNotFoundError:
        if not _ffprobe_missing_reported:  # once, not once per file
            _ffprobe_missing_reported = True
            logger.error("ffprobe not found on PATH: video/audio metadata is unavailable")
        raise
    except subprocess.CalledProcessError as e:
        raise ExtractionError(
            f"ffprobe exited with {e.returncode}: {(e.stderr or '').strip() or 'no details'}"
        ) from e
    return json.loads(result.stdout)


class ExtractionError(Exception):
    """Metadata extraction error"""



class MetadataExtractor(ABC):
    """
    Base class for all metadata extractors.

    Subclasses must implement:
    - supported_extensions: a set of extensions (e.g. {'.jpg', '.png'})
    - file_type: the file type from the FileType enum
    - extract(): the extraction logic
    """

    supported_extensions: set[str] = set()
    file_type: FileType = FileType.UNKNOWN

    def can_extract(self, file_path: Path) -> bool:
        """Checks whether the extractor can handle this file"""
        return file_path.suffix.lower() in self.supported_extensions

    @abstractmethod
    def extract(self, file_path: Path) -> dict[str, Any]:
        """
        Extracts metadata from a file.

        Returns:
            A dict of metadata. On error, a dict with an 'error' key.
        """
        ...

    def _base_metadata(self, file_path: Path) -> dict[str, Any]:
        """Basic file info shared by all types"""
        try:
            stat = file_path.stat()
            return {
                "size_bytes": stat.st_size,
                "modified_at": stat.st_mtime,
                "extension": file_path.suffix.lower(),
            }
        except OSError as e:
            logger.warning("Cannot stat %s: %s", file_path, e)
            return {}


class ImageExtractor(MetadataExtractor):
    """Extracts image metadata: dimensions, format, EXIF"""

    supported_extensions = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp", ".heic"}
    file_type = FileType.IMAGE

    def extract(self, file_path: Path) -> dict[str, Any]:
        from PIL import ExifTags, Image

        metadata = self._base_metadata(file_path)

        try:
            with Image.open(file_path) as img:
                metadata.update(
                    {
                        "width": img.width,
                        "height": img.height,
                        "mode": img.mode,
                        "format": img.format,
                    }
                )

                # EXIF — only if present
                exif = img.getexif()
                if exif:
                    metadata["exif"] = self._parse_exif(exif, ExifTags)

        except Exception as e:
            logger.warning("Image extraction failed for %s: %s", file_path, e,
                           exc_info=logger.isEnabledFor(logging.DEBUG))
            metadata["error"] = str(e)

        return metadata

    @staticmethod
    def _parse_exif(exif_data, exif_tags) -> dict[str, Any]:
        """Parses EXIF into a readable form"""
        parsed = {}
        for tag_id, value in exif_data.items():
            tag_name = exif_tags.TAGS.get(tag_id, str(tag_id))
            # bytes aren't JSON-serializable — convert them
            if isinstance(value, bytes):
                value = value.decode("utf-8", errors="replace")
            else:
                value = str(value)
            parsed[tag_name] = value
        return parsed


class VideoExtractor(MetadataExtractor):
    """Extracts video metadata: duration, resolution, codecs"""

    supported_extensions = {".mp4", ".avi", ".mkv", ".mov", ".webm", ".flv", ".wmv", ".m4v"}
    file_type = FileType.VIDEO

    def extract(self, file_path: Path) -> dict[str, Any]:
        metadata = self._base_metadata(file_path)

        try:
            probe_data = self._probe(file_path)
            fmt = probe_data.get("format", {})

            metadata.update(
                {
                    "duration": float(fmt.get("duration", 0) or 0),
                    "bitrate": int(fmt.get("bit_rate", 0) or 0),
                    "container": fmt.get("format_name"),
                    "streams": self._parse_streams(probe_data.get("streams", [])),
                }
            )

            # Take the resolution from the first video stream
            for stream in metadata["streams"]:
                if stream.get("codec_type") == "video":
                    metadata["width"] = stream.get("width")
                    metadata["height"] = stream.get("height")
                    break

        except Exception as e:
            logger.warning("Video extraction failed for %s: %s", file_path, e,
                           exc_info=logger.isEnabledFor(logging.DEBUG))
            metadata["error"] = str(e)

        return metadata

    @staticmethod
    def _probe(file_path: Path) -> dict[str, Any]:
        """Runs ffprobe and returns JSON"""
        return _ffprobe(file_path)

    @staticmethod
    def _parse_streams(streams: list[dict]) -> list[dict[str, Any]]:
        """Keeps only the useful stream fields"""
        keep = {
            "codec_type",
            "codec_name",
            "width",
            "height",
            "duration",
            "sample_rate",
            "channels",
            "bit_rate",
        }
        return [{k: v for k, v in s.items() if k in keep} for s in streams]


class AudioExtractor(MetadataExtractor):
    """Extracts audio metadata: duration, bitrate, tags"""

    supported_extensions = {".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a", ".wma", ".opus"}
    file_type = FileType.AUDIO

    def extract(self, file_path: Path) -> dict[str, Any]:
        metadata = self._base_metadata(file_path)

        try:
            probe_data = self._probe(file_path)
            fmt = probe_data.get("format", {})

            metadata.update(
                {
                    "duration": float(fmt.get("duration", 0) or 0),
                    "bitrate": int(fmt.get("bit_rate", 0) or 0),
                    "codec": fmt.get("format_name"),
                }
            )

            # ffprobe puts ID3 tags into format.tags
            tags = fmt.get("tags", {})
            if tags:
                metadata["tags"] = tags

            # Audio specifics from the first stream
            for stream in probe_data.get("streams", []):
                if stream.get("codec_type") == "audio":
                    metadata.update(
                        {
                            "sample_rate": stream.get("sample_rate"),
                            "channels": stream.get("channels"),
                        }
                    )
                    break

        except Exception as e:
            logger.warning("Audio extraction failed for %s: %s", file_path, e,
                           exc_info=logger.isEnabledFor(logging.DEBUG))
            metadata["error"] = str(e)

        return metadata

    @staticmethod
    def _probe(file_path: Path) -> dict[str, Any]:
        return _ffprobe(file_path)


class ExtractorRegistry:
    """
    Extractor registry. Picks the right one by extension.

    Usage:
        registry = ExtractorRegistry()
        metadata = registry.extract(Path("video.mp4"))
    """

    def __init__(self, extractors: list[MetadataExtractor] | None = None):
        self._extractors: list[MetadataExtractor] = extractors or [
            ImageExtractor(),
            VideoExtractor(),
            AudioExtractor(),
        ]

    def register(self, extractor: MetadataExtractor) -> None:
        """Add a new extractor (e.g. for PDFs or archives)"""
        self._extractors.append(extractor)
        logger.info("Registered extractor: %s", type(extractor).__name__)

    def get_extractor(self, file_path: Path) -> MetadataExtractor | None:
        """Returns a matching extractor or None"""
        for extractor in self._extractors:
            if extractor.can_extract(file_path):
                return extractor
        return None

    def get_file_type(self, file_path: Path) -> FileType:
        """Determines the file type via the registry"""
        extractor = self.get_extractor(file_path)
        return extractor.file_type if extractor else FileType.UNKNOWN

    def extract(self, file_path: Path) -> dict[str, Any] | None:
        """
        Extracts metadata, automatically picking an extractor.
        Returns None if there is no matching extractor.
        """
        extractor = self.get_extractor(file_path)
        if not extractor:
            logger.debug("No extractor for %s", file_path)
            return None

        return extractor.extract(file_path)

    @property
    def supported_types(self) -> set[str]:
        """All supported extensions (for CLI help)"""
        exts = set()
        for extractor in self._extractors:
            exts.update(extractor.supported_extensions)
        return exts
