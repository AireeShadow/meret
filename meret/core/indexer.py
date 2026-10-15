"""File indexer — the scan orchestrator.

It knows nothing about formats: extractors handle metadata and the
repository handles storage. Only traversal and coordination logic lives here.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Generator, List, Optional

from meret.core.extractors import ExtractorRegistry
from meret.core.logger import Logger
from meret.core.models import FileEntry, FileType
from meret.core.repository import AbstractRepository

PHASE_COLLECT = "collect"
PHASE_EXTRACT = "extract"
PHASE_INDEX = "index"


class Indexer(Logger):
    """Walks a directory, extracts metadata and writes it to the repository."""

    def __init__(
        self,
        repository: AbstractRepository,
        library_path: str | Path,
        workers: int = 8,
        chunk_size: int = 1000,
        skip_existing: bool = False,
    ) -> None:
        super().__init__()
        self.repo = repository
        self.library = Path(library_path)
        self.workers = max(1, workers)
        self.chunk_size = max(1, chunk_size)
        self.skip_existing = skip_existing
        self.registry = ExtractorRegistry()
        self.interrupted = False

    # ---------- traversal ----------

    def _iter_files(self, single_file: Optional[Path] = None) -> Generator[Path, None, None]:
        if single_file is not None:
            if single_file.is_file():
                yield single_file
            return
        for path in self.library.rglob("*"):
            if path.is_file():
                yield path

    def collect(
        self,
        single_file: Optional[Path] = None,
        progress_callback=None,
    ) -> List[Path]:
        """STAGE 1: collect the list of supported files. Fast, no metadata."""
        found: List[Path] = []

        if single_file is not None:
            candidates = [single_file] if single_file.is_file() else []
            for done, path in enumerate(candidates, start=1):
                if self.registry.get_file_type(path) is not FileType.UNKNOWN:
                    found.append(path)
                if progress_callback:
                    progress_callback(PHASE_COLLECT, done, len(candidates))
            return found

        # rglob is lazy — collect everything first so we know the total
        all_files = list(self._iter_files())
        total = len(all_files)

        for done, path in enumerate(all_files, start=1):
            if self.registry.get_file_type(path) is not FileType.UNKNOWN:
                found.append(path)
            if progress_callback:
                progress_callback(PHASE_COLLECT, done, total)

        self.logger.info(f"Collected {len(found)}/{total} supported files")
        return found

    # ---------- processing ----------

    def _process_file(self, file_path: Path) -> Optional[FileEntry]:
        """Extraction only — no DB access (important for threads)."""
        file_type = self.registry.get_file_type(file_path)
        if file_type is FileType.UNKNOWN:
            return None
        return FileEntry(
            filename=file_path.name,
            filepath=str(file_path),
            type=file_type,
            metadata=self.registry.extract(file_path),
        )

    def extract(
        self,
        files: List[Path],
        progress_callback=None,
    ) -> List[FileEntry]:
        """STAGE 2: extract metadata in parallel."""
        entries: List[FileEntry] = []
        if not files:
            return entries

        pool = ThreadPoolExecutor(max_workers=self.workers)
        futures = {pool.submit(self._process_file, f): f for f in files}

        try:
            for done, future in enumerate(as_completed(futures), start=1):
                try:
                    entry = future.result()
                    if entry:
                        entries.append(entry)
                except Exception as e:
                    self.logger.error(f"Failed: {futures[future]}: {e}")
                if progress_callback:
                    progress_callback(PHASE_EXTRACT, done, len(files))
        except KeyboardInterrupt:
            self.interrupted = True
            self.logger.warning("Extraction interrupted by user")
            for future in futures:
                future.cancel()
            pool.shutdown(wait=False, cancel_futures=True)
        else:
            pool.shutdown(wait=True)

        return entries

    # ---------- writing ----------

    def index(self, entries: List[FileEntry], progress_callback=None) -> Dict[str, int]:
        """STAGE 3: write to the DB in batches. One transaction per chunk_size files."""
        if not entries:
            return {"indexed": 0, "updated": 0, "errors": 0}

        def on_chunk(done: int, total: int) -> None:
            if progress_callback:
                progress_callback(PHASE_INDEX, done, total)

        return self.repo.save_files(entries, chunk_size=self.chunk_size, on_progress=on_chunk)

    # ---------- orchestration ----------

    def run(
        self,
        single_file: Optional[Path] = None,
        dry_run: bool = False,
        progress_callback=None,
    ) -> Dict[str, int]:
        self.interrupted = False

        files = self.collect(single_file=single_file, progress_callback=progress_callback)

        skipped = 0
        if self.skip_existing and files:
            known = self.repo.known_paths()
            before = len(files)
            files = [f for f in files if str(f) not in known]
            skipped = before - len(files)
            self.logger.info(f"Skipped {skipped}/{before} already indexed files")

        entries = self.extract(files, progress_callback=progress_callback)

        if dry_run:
            return {
                "indexed": 0,
                "updated": 0,
                "errors": 0,
                "found": len(entries),
                "skipped": skipped,
                "interrupted": self.interrupted,
            }

        stats = self.index(entries, progress_callback=progress_callback)
        stats["skipped"] = skipped
        stats["interrupted"] = self.interrupted
        return stats
