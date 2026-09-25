from pathlib import Path
from PIL import Image, ExifTags
from ffmpeg import probe
from mimetypes import guess_type

from database import CommonDbOperations
from logger import Logger


class Indexer(Logger):
    def __init__(self, library_location: str, db_location: str) -> None:
        super().__init__()
        self.library_location = Path(library_location)
        self.db_location = db_location

    def _get_files(self):
        return self.library_location.rglob("*")

    def _get_exif(self, filename: str) -> dict | None:
        try:
            with Image.open(filename) as img:
                exif_data = img.getexif()
                return {
                    ExifTags.TAGS.get(k, k): v
                    for k, v in exif_data.items()
                    if k in ExifTags.TAGS
                }
        except Exception as ex:
            self.logger.warning(f"Failed to extract EXIF from {filename}: {ex}")
            return None

    def _get_metadata(self, filename: str) -> dict | None:
        try:
            return probe(filename)["streams"]
        except Exception as ex:
            self.logger.error(f"Exception fetching metadata for {filename}: {type(ex).__name__}: {ex}")
            return None

    def _get_file_type(self, filename: str) -> str | None:
        file_type, _ = guess_type(filename)
        return file_type

    def _index(self, filename: str) -> dict | None:
        file_type = self._get_file_type(filename)
        if file_type is None:
            self.logger.info(f"Unknown file type for {filename}")
            return None

        is_media = False
        metadata = None

        if file_type.startswith("audio") or file_type.startswith("video"):
            metadata = self._get_metadata(filename)
            is_media = True
        elif file_type.startswith("image"):
            metadata = self._get_exif(filename)
            is_media = True
        else:
            self.logger.info(f"{filename} is not media: {file_type}")

        if is_media and metadata is not None:
            return {
                "filename": str(filename),
                "labels": None,
                "rating": 0,
                "type": file_type,
                "label_group": "",
                "label_subgroup": None,
                "metadata": metadata
            }

        return None

    def run_index(self, filename: str = None):
        db = CommonDbOperations(db_location=self.db_location)

        files_to_process = [Path(filename)] if filename else self._get_files()

        for file_path in files_to_process:
            data = self._index(str(file_path))
            if data:
                db.insert(table="Files", data=data)
            else:
                self.logger.debug(f"Data is None, not writing {file_path} to db.")
