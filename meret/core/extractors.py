"""
Metadata extractors for different media types.

Каждый экстрактор умеет:
1. Определить, подходит ли он для файла (can_extract)
2. Извлечь метаданные (extract)

Новые форматы добавляются просто: наследуйся от MetadataExtractor
и зарегистрируй его в ExtractorRegistry.
"""

import json
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, List

from meret.core.models import FileType
from meret.core.logger import Logger


class ExtractionError(Exception):
    """Ошибка извлечения метаданных"""
    pass


class MetadataExtractor(ABC):
    """
    Базовый класс для всех экстракторов метаданных.
    
    Наследники должны реализовать:
    - supported_extensions: набор расширений (например, {'.jpg', '.png'})
    - file_type: тип файла из enum FileType
    - extract(): логику извлечения
    """
    
    supported_extensions: set[str] = set()
    file_type: FileType = FileType.UNKNOWN
    
    def __init__(self):
        self.logger = Logger().logger
    
    def can_extract(self, file_path: Path) -> bool:
        """Проверяет, умеет ли экстрактор работать с этим файлом"""
        return file_path.suffix.lower() in self.supported_extensions
    
    @abstractmethod
    def extract(self, file_path: Path) -> Dict[str, Any]:
        """
        Извлекает метаданные из файла.
        
        Returns:
            Словарь с метаданными. При ошибке — dict с ключом 'error'.
        """
        ...

    def _base_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Базовая инфа о файле, общая для всех типов"""
        try:
            stat = file_path.stat()
            return {
                "size_bytes": stat.st_size,
                "modified_at": stat.st_mtime,
                "extension": file_path.suffix.lower(),
            }
        except OSError as e:
            self.logger.warning(f"Cannot stat {file_path}: {e}")
            return {}


class ImageExtractor(MetadataExtractor):
    """Извлекает метаданные изображений: размеры, формат, EXIF"""
    
    supported_extensions = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp", ".heic"}
    file_type = FileType.IMAGE
    
    def extract(self, file_path: Path) -> Dict[str, Any]:
        from PIL import Image, ExifTags
        
        metadata = self._base_metadata(file_path)
        
        try:
            with Image.open(file_path) as img:
                metadata.update({
                    "width": img.width,
                    "height": img.height,
                    "mode": img.mode,
                    "format": img.format,
                })
                
                # EXIF — только если он есть
                exif = img.getexif()
                if exif:
                    metadata["exif"] = self._parse_exif(exif, ExifTags)
                
        except Exception as e:
            self.logger.warning(f"Image extraction failed for {file_path}: {e}")
            metadata["error"] = str(e)
        
        return metadata
    
    @staticmethod
    def _parse_exif(exif_data, exif_tags) -> Dict[str, Any]:
        """Парсит EXIF в читаемый вид"""
        parsed = {}
        for tag_id, value in exif_data.items():
            tag_name = exif_tags.TAGS.get(tag_id, str(tag_id))
            # bytes не сериализуются в JSON — конвертируем
            if isinstance(value, bytes):
                value = value.decode("utf-8", errors="replace")
            else:
                value = str(value)
            parsed[tag_name] = value
        return parsed


class VideoExtractor(MetadataExtractor):
    """Извлекает метаданные видео: длительность, разрешение, кодеки"""
    
    supported_extensions = {".mp4", ".avi", ".mkv", ".mov", ".webm", ".flv", ".wmv", ".m4v"}
    file_type = FileType.VIDEO
    
    def extract(self, file_path: Path) -> Dict[str, Any]:
        metadata = self._base_metadata(file_path)
        
        try:
            probe_data = self._probe(file_path)
            fmt = probe_data.get("format", {})
            
            metadata.update({
                "duration": float(fmt.get("duration", 0) or 0),
                "bitrate": int(fmt.get("bit_rate", 0) or 0),
                "container": fmt.get("format_name"),
                "streams": self._parse_streams(probe_data.get("streams", [])),
            })
            
            # Берём разрешение из первого видеопотока
            for stream in metadata["streams"]:
                if stream.get("codec_type") == "video":
                    metadata["width"] = stream.get("width")
                    metadata["height"] = stream.get("height")
                    break
                
        except Exception as e:
            self.logger.warning(f"Video extraction failed for {file_path}: {e}")
            metadata["error"] = str(e)
        
        return metadata
    
    @staticmethod
    def _probe(file_path: Path) -> Dict[str, Any]:
        """Запускает ffprobe и возвращает JSON"""
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_format", "-show_streams", str(file_path)],
            capture_output=True, text=True, check=True, timeout=30
        )
        return json.loads(result.stdout)
    
    @staticmethod
    def _parse_streams(streams: List[dict]) -> List[Dict[str, Any]]:
        """Оставляет только полезные поля стримов"""
        keep = {"codec_type", "codec_name", "width", "height",
                "duration", "sample_rate", "channels", "bit_rate"}
        return [
            {k: v for k, v in s.items() if k in keep}
            for s in streams
        ]


class AudioExtractor(MetadataExtractor):
    """Извлекает метаданные аудио: длительность, битрейт, теги"""
    
    supported_extensions = {".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a", ".wma", ".opus"}
    file_type = FileType.AUDIO
    
    def extract(self, file_path: Path) -> Dict[str, Any]:
        metadata = self._base_metadata(file_path)
        
        try:
            probe_data = self._probe(file_path)
            fmt = probe_data.get("format", {})
            
            metadata.update({
                "duration": float(fmt.get("duration", 0) or 0),
                "bitrate": int(fmt.get("bit_rate", 0) or 0),
                "codec": fmt.get("format_name"),
            })
            
            # ID3-теги ffprobe кладёт в format.tags
            tags = fmt.get("tags", {})
            if tags:
                metadata["tags"] = tags
            
            # Аудио-специфика из первого стрима
            for stream in probe_data.get("streams", []):
                if stream.get("codec_type") == "audio":
                    metadata.update({
                        "sample_rate": stream.get("sample_rate"),
                        "channels": stream.get("channels"),
                    })
                    break
                
        except Exception as e:
            self.logger.warning(f"Audio extraction failed for {file_path}: {e}")
            metadata["error"] = str(e)
        
        return metadata
    
    @staticmethod
    def _probe(file_path: Path) -> Dict[str, Any]:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_format", "-show_streams", str(file_path)],
            capture_output=True, text=True, check=True, timeout=30
        )
        return json.loads(result.stdout)


class ExtractorRegistry:
    """
    Реестр экстракторов. Сам подбирает нужный по расширению.
    
    Использование:
        registry = ExtractorRegistry()
        metadata = registry.extract(Path("video.mp4"))
    """
    
    def __init__(self, extractors: Optional[List[MetadataExtractor]] = None):
        self._extractors: List[MetadataExtractor] = extractors or [
            ImageExtractor(),
            VideoExtractor(),
            AudioExtractor(),
        ]
        self.logger = Logger().logger
    
    def register(self, extractor: MetadataExtractor) -> None:
        """Добавить новый экстрактор (например, для PDF или архивов)"""
        self._extractors.append(extractor)
        self.logger.info(f"Registered extractor: {type(extractor).__name__}")
    
    def get_extractor(self, file_path: Path) -> Optional[MetadataExtractor]:
        """Возвращает подходящий экстрактор или None"""
        for extractor in self._extractors:
            if extractor.can_extract(file_path):
                return extractor
        return None
    
    def get_file_type(self, file_path: Path) -> FileType:
        """Определяет тип файла через реестр"""
        extractor = self.get_extractor(file_path)
        return extractor.file_type if extractor else FileType.UNKNOWN
    
    def extract(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """
        Извлекает метаданные, автоматически подобрав экстрактор.
        Возвращает None, если подходящего экстрактора нет.
        """
        extractor = self.get_extractor(file_path)
        if not extractor:
            self.logger.debug(f"No extractor for {file_path}")
            return None
        
        return extractor.extract(file_path)
    
    @property
    def supported_types(self) -> set[str]:
        """Все поддерживаемые расширения (для CLI-справки)"""
        exts = set()
        for extractor in self._extractors:
            exts.update(extractor.supported_extensions)
        return exts