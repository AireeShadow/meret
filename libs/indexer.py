from pathlib import Path
from PIL import Image, ExifTags
from ffmpeg import probe
from mimetypes import guess_type

from database import CommonDbOperations
from logger import Logger

class Indexer(Logger):
    def __init__(self, library_location: str, db_location: str) -> None:
        super().__init__()
        self.library_location = library_location
        self.db_location = db_location

    def _get_files(self, path: str='*'):
        path = Path(self.library_location)
        for f in path.rglob(path):
            yield f

    @staticmethod
    def _get_exif(filename: str) -> dict:
        with Image.open(filename) as img:
            try:
                img.verify()
            except Exception:
                return None
            else:
                img_exif = img.getexif()
                exif_dict = {ExifTags.TAGS[k]: v for k, v in img_exif.items() if k in ExifTags.TAGS}
                return exif_dict

    @staticmethod
    def _get_metadata(filename: str) -> dict:
        return probe(filename)["streams"]
    
    @staticmethod
    def _get_file_type(filename: str) -> str:
        file_type = guess_type(filename)
        return file_type[0]
    
    def index(self, filename: str=None, not_media: bool=False) -> bool:
        db = CommonDbOperations(db_location=self.db_location)
        if not filename:
            for f in self._get_files():
                file_type = self._get_file_type(filename=f)
                match file_type:
                    case 'audio' | 'video':
                        metadata = Indexer._get_metadata(filename=filename)
                    case 'image':
                        metadata = Indexer._get_exif(filename=filename)
                    case _:
                        metadata = None
                        self.logger.info(f'{f} is not audio, video or image: {file_type}, not getting additional information')
                if not not_media:
                    db.insert(
                        table='Files',
                        data={
                            'filename': filename,
                            'labels': None,
                            'rating': 0,
                            'type': file_type,
                            'label_group': '',
                            'label_subgroup': None,
                            'EXIF': metadata
                        }
                    )
                else:
                    self.logger.debug(f'Not writing {filename} to db!')
                    
                   
    

    

    