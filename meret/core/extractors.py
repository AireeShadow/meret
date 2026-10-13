# meret/core/extractors.py
from abc import ABC, abstractmethod

class MetadataExtractor(ABC):
    @abstractmethod
    def can_extract(self, file_path: Path) -> bool: ...
    
    @abstractmethod
    def extract(self, file_path: Path) -> Dict[str, Any]: ...

class ImageExtractor(MetadataExtractor):
    def can_extract(self, file_path: Path) -> bool:
        return file_path.suffix.lower() in {'.jpg', '.jpeg', '.png', '.webp'}
    
    def extract(self, file_path: Path) -> Dict[str, Any]:
        from PIL import Image
        with Image.open(file_path) as img:
            return {
                "width": img.width,
                "height": img.height,
                "exif": self._get_exif(img),
                "mode": img.mode
            }

class VideoExtractor(MetadataExtractor):
    def can_extract(self, file_path: Path) -> bool:
        return file_path.suffix.lower() in {'.mp4', '.avi', '.mkv'}
    
    def extract(self, file_path: Path) -> Dict[str, Any]:
        from ffmpeg import probe
        result = probe(str(file_path))
        return {
            "duration": float(result['format'].get('duration', 0)),
            "bitrate": int(result['format'].get('bit_rate', 0)),
            "streams": result.get('streams', [])
        }

# Использование
class Indexer:
    def __init__(self):
        self.extractors: List[MetadataExtractor] = [
            ImageExtractor(),
            VideoExtractor(),
            AudioExtractor(),
        ]
    
    def _extract_metadata(self, file_path: Path) -> Optional[Dict]:
        for extractor in self.extractors:
            if extractor.can_extract(file_path):
                return extractor.extract(file_path)
        return None