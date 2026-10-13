from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Generator, List, Optional

from meret.core.extractors import ExtractorRegistry
from meret.core.logger import Logger
from meret.core.models import FileEntry, FileType
from meret.core.repository import AbstractRepository


class Indexer(Logger):
    def __init__(
        self,
        repository: AbstractRepository,
        library_path: str | Path,
        workers: int = 4,
    ) -> None:
        super().__init__()
        self.repo = repository
        self.library = Path(library_path)
        self.workers = max(1, workers)
        self.registry = ExtractorRegistry()

    def _iter_files(self, single_file: Optional[Path] = None) -> Generator[Path, None, None]:
        if single_file is not None:
            if single_file.is_file():
                yield single_file
            return
        for path in self.library.rglob("*"):
            if path.is_file():
                yield path

    def _process_file(self, file_path: Path) -> Optional[FileEntry]:
        """Только извлечение — БД не трогаем (важно для потоков)."""
        file_type = self.registry.get_file_type(file_path)
        if file_type is FileType.UNKNOWN:
            return None
        return FileEntry(
            filename=file_path.name,
            filepath=str(file_path),
            type=file_type,
            metadata=self.registry.extract(file_path),
        )

    def scan(self, single_file: Optional[Path] = None, progress_callback=None) -> List[FileEntry]:
        files = list(self._iter_files(single_file))
        entries: List[FileEntry] = []
        if not files:
            self.logger.warning("No files to scan")
            return entries

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = {pool.submit(self._process_file, f): f for f in files}
            for done, future in enumerate(as_completed(futures), start=1):
                try:
                    entry = future.result()
                    if entry:
                        entries.append(entry)
                except Exception as e:
                    self.logger.error(f"Failed: {futures[future]}: {e}")
                if progress_callback:
                    progress_callback(done, len(files))
        return entries

    def index(self, entries: List[FileEntry]) -> Dict[str, int]:
        stats = {"indexed": 0, "updated": 0, "errors": 0}
        for entry in entries:
            try:
                exists = self.repo.get_file_by_path(entry.filepath) is not None
                self.repo.save_file(entry)
                stats["updated" if exists else "indexed"] += 1
            except Exception as e:
                stats["errors"] += 1
                self.logger.error(f"Failed to index {entry.filepath}: {e}")
        return stats

    def run(self, single_file=None, dry_run: bool = False, progress_callback=None):
        entries = self.scan(single_file=single_file, progress_callback=progress_callback)
        if dry_run:
            return {"indexed": 0, "updated": 0, "errors": 0, "found": len(entries)}
        return self.index(entries)