from pathlib import Path
from typing import List, Optional, Generator
from PIL import Image, ExifTags
from ffmpeg import probe

from meret.core.models import FileEntry, FileType
from meret.core.repository import AbstractRepository


class Indexer:
    def __init__(
        self, 
        repository: AbstractRepository,
        library_path: Path
    ):
        self.repo = repository
        self.library = Path(library_path)
        
        self._extractors = {
            FileType.IMAGE: self._extract_image_meta,
            FileType.VIDEO: self._extract_video_meta,
            FileType.AUDIO: self._extract_audio_meta,
        }
    
    def scan(self, single_file: Optional[Path] = None) -> List[FileEntry]:
        """Сканирует файлы и возвращает список FileEntry"""
        files_to_scan = [single_file] if single_file else self._iter_files()
        
        entries = []
        for file_path in files_to_scan:
            if not file_path or not file_path.is_file():
                continue
            
            entry = self._process_file(file_path)
            if entry:
                entries.append(entry)
        
        return entries
    
    def index(self, entries: List[FileEntry]) -> dict:
        """Сохраняет записи в БД"""
        indexed = 0
        updated = 0
        
        for entry in entries:
            existing = self.repo.get_file_by_path(entry.filepath)
            if existing:
                updated += 1
            else:
                indexed += 1
            
            self.repo.save_file(entry)
        
        return {"indexed": indexed, "updated": updated}
    
    def _iter_files(self) -> Generator[Path, None, None]:
        """Генератор всех файлов"""
        for path in self.library.rglob("*"):
            if path.is_file():
                yield path
    
    def _process_file(self, file_path: Path) -> Optional[FileEntry]:
        """Определяет тип и извлекает метаданные"""
        file_type = self._detect_type(file_path)
        
        if file_type == FileType.UNKNOWN:
            return None
        
        metadata = None
        if file_type in self._extractors:
            metadata = self._extractors[file_type](file_path)
        
        return FileEntry(
            filename=file_path.name,
            filepath=str(file_path),
            type=file_type,
            metadata=metadata
        )
    
    def _detect_type(self, path: Path) -> FileType:
        suffix = path.suffix.lower()
        
        if suffix in {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.raw'}:
            return FileType.IMAGE
        elif suffix in {'.mp4', '.avi', '.mkv', '.mov', '.webm', '.flv', '.wmv', '.m4v'}:
            return FileType.VIDEO
        elif suffix in {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a', '.wma'}:
            return FileType.AUDIO
        
        # Fallback на mimetypes
        import mimetypes
        mime, _ = mimetypes.guess_type(str(path))
        if mime:
            if mime.startswith('image'):
                return FileType.IMAGE
            elif mime.startswith('video'):
                return FileType.VIDEO
            elif mime.startswith('audio'):
                return FileType.AUDIO
        
        return FileType.UNKNOWN
    
    def _extract_image_meta(self, path: Path) -> dict:
        try:
            with Image.open(path) as img:
                meta = {
                    "width": img.width,
                    "height": img.height,
                    "mode": img.mode,
                    "format": img.format,
                }
                
                # EXIF
                exif = img.getexif()
                if exif:
                    meta["exif"] = {
                        ExifTags.TAGS.get(k, k): str(v)
                        for k, v in exif.items()
                    }
                
                return meta
        except Exception as e:
            return {"error": str(e), "width": 0, "height": 0}
    
    def _extract_video_meta(self, path: Path) -> dict:
        try:
            result = probe(str(path))
            return {
                "duration": float(result['format'].get('duration', 0)),
                "bitrate": int(result['format'].get('bit_rate', 0)),
                "size": int(result['format'].get('size', 0)),
                "streams": [
                    {
                        "codec_type": s.get('codec_type'),
                        "width": s.get('width'),
                        "height": s.get('height'),
                        "duration": s.get('duration')
                    }
                    for s in result.get('streams', [])
                ]
            }
        except Exception as e:
            return {"error": str(e)}
    
    def _extract_audio_meta(self, path: Path) -> dict:
        try:
            result = probe(str(path))
            return {
                "duration": float(result['format'].get('duration', 0)),
                "bitrate": int(result['format'].get('bit_rate', 0)),
                "sample_rate": result['streams'][0].get('sample_rate') if result.get('streams') else None
            }
        except Exception as e:
            return {"error": str(e)}