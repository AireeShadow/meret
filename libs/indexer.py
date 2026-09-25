import json
import threading
from pathlib import Path
from typing import Dict, Any, Optional, Generator
from PIL import Image, ExifTags
from ffmpeg import probe
from mimetypes import guess_type

from libs.database import CommonDbOperations
from libs.logger import Logger


class Indexer(Logger):
    def __init__(self, library_location: str, db_location: str) -> None:
        super().__init__()
        self.library_location = Path(library_location)
        self.db_location = db_location
        self.db = CommonDbOperations(db_location)
        self._supported_image = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'}
        self._supported_video = {'.mp4', '.avi', '.mkv', '.mov', '.webm', '.flv', '.wmv'}
        self._supported_audio = {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a'}

    def _get_files(self) -> Generator[Path, None, None]:
        """Generator for all files recursively"""
        if not self.library_location.exists():
            self.logger.error(f"Library path does not exist: {self.library_location}")
            return
        yield from self.library_location.rglob("*")

    def _get_exif(self, filename: Path) -> Optional[Dict]:
        """Extract EXIF data from image"""
        try:
            with Image.open(filename) as img:
                exif_data = img.getexif()
                if not exif_data:
                    return None
                return {
                    ExifTags.TAGS.get(k, k): str(v)
                    for k, v in exif_data.items()
                    if k in ExifTags.TAGS
                }
        except Exception as ex:
            self.logger.warning(f"Failed to extract EXIF from {filename}: {ex}")
            return None

    def _get_media_metadata(self, filename: Path) -> Optional[Dict]:
        """Extract metadata from video/audio using ffmpeg"""
        try:
            result = probe(str(filename))
            return {
                'format': result.get('format', {}),
                'streams': result.get('streams', [])
            }
        except Exception as ex:
            self.logger.debug(f"No ffmpeg metadata for {filename}: {ex}")
            return None

    def _get_file_type(self, filename: Path) -> Optional[str]:
        """Determine file type"""
        file_type, _ = guess_type(str(filename))
        return file_type

    def _categorize_file(self, filename: Path) -> tuple[str, Optional[Dict]]:
        """
        Categorize file and extract metadata.
        Returns: (category, metadata)
        """
        suffix = filename.suffix.lower()
        
        if suffix in self._supported_image:
            return "image", self._get_exif(filename)
        elif suffix in self._supported_video:
            return "video", self._get_media_metadata(filename)
        elif suffix in self._supported_audio:
            return "audio", self._get_media_metadata(filename)
        else:
            mime = self._get_file_type(filename)
            if mime:
                if mime.startswith("image"):
                    return "image", self._get_exif(filename)
                elif mime.startswith("video"):
                    return "video", self._get_media_metadata(filename)
                elif mime.startswith("audio"):
                    return "audio", self._get_media_metadata(filename)
            return "unknown", None

    def _index_file(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Index single file"""
        if not file_path.is_file():
            return None
            
        category, metadata = self._categorize_file(file_path)
        
        if category == "unknown":
            self.logger.debug(f"Skipping unknown file: {file_path}")
            return None

        return {
            "filename": file_path.name,
            "filepath": str(file_path),
            "labels": [],
            "rating": 0,
            "type": category,
            "label_group": "",
            "label_subgroup": None,
            "metadata": metadata
        }

    def run_index(self, filename: Optional[str] = None, batch_size: int = 100) -> Dict[str, int]:
        """
        Index files into database.
        
        Args:
            filename: Optional single file to index
            batch_size: Number of files to process before commit
            
        Returns:
            Dict with stats: {'indexed': int, 'skipped': int, 'errors': int}
        """
        stats = {'indexed': 0, 'skipped': 0, 'errors': 0}
        
        if filename:
            files_to_process = [Path(filename)]
        else:
            files_to_process = self._get_files()
        
        batch = []
        
        for file_path in files_to_process:
            try:
                data = self._index_file(file_path)
                if data:
                    batch.append(data)
                    if len(batch) >= batch_size:
                        self._insert_batch(batch)
                        stats['indexed'] += len(batch)
                        batch = []
                else:
                    stats['skipped'] += 1
            except Exception as ex:
                self.logger.error(f"Error indexing {file_path}: {ex}")
                stats['errors'] += 1
        
        # Insert remaining
        if batch:
            self._insert_batch(batch)
            stats['indexed'] += len(batch)
        
        self.logger.info(f"Indexing complete: {stats}")
        return stats

    def _insert_batch(self, batch: list):
        """Insert batch of files with upsert"""
        for data in batch:
            try:
                self.db.insert_or_update(
                    table="Files",
                    data=data,
                    unique_key="filepath"
                )
            except Exception as ex:
                self.logger.error(f"Failed to insert {data['filepath']}: {ex}")

    def close(self):
        """Cleanup"""
        self.db.close()