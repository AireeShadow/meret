"""File indexer — the scan orchestrator.

It knows nothing about formats: extractors handle metadata and the
repository handles storage. Only traversal and coordination logic lives here.
"""

import logging
import time
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from meret.core.extractors import ExtractorRegistry
from meret.core.models import FileEntry, FileType
from meret.core.repository import AbstractRepository

PHASE_COLLECT = "collect"
PHASE_EXTRACT = "extract"
PHASE_INDEX = "index"

logger = logging.getLogger(__name__)


class Indexer:
    """Walks a directory, extracts metadata and writes it to the repository."""

    def __init__(
        self,
        repository: AbstractRepository,
        library_path: str | Path,
        workers: int = 8,
        chunk_size: int = 1000,
        skip_existing: bool = False,
    ) -> None:
        self.repo = repository
        self.library = Path(library_path)
        self.workers = max(1, workers)
        self.chunk_size = max(1, chunk_size)
        self.skip_existing = skip_existing
        self.registry = ExtractorRegistry()
        self.interrupted = False

    # ---------- traversal ----------

    def _iter_files(self, single_file: Path | None = None) -> Generator[Path, None, None]:
        if single_file is not None:
            if single_file.is_file():
                yield single_file
            return
        for path in self.library.rglob("*"):
            if path.is_file():
                yield path

    def collect(
        self,
        single_file: Path | None = None,
        progress_callback=None,
    ) -> list[Path]:
        """STAGE 1: collect the list of supported files. Fast, no metadata."""
        found: list[Path] = []

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

        logger.info("Collected %d/%d supported files under %s", len(found), total, self.library)
        return found

    # ---------- processing ----------
    @staticmethod
    def _fingerprint(path: Path) -> tuple[int, float] | None:
        try:
            st = path.stat()
        except OSError:
            return None
        return int(st.st_size), float(st.st_mtime)
    
    def _process_file(self, file_path: Path) -> FileEntry | None:
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
        files: list[Path],
        progress_callback=None,
    ) -> list[FileEntry]:
        """STAGE 2: extract metadata in parallel."""
        entries: list[FileEntry] = []
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
                    logger.error("Failed: %s: %s", futures[future], e, exc_info=True)
                if progress_callback:
                    progress_callback(PHASE_EXTRACT, done, len(files))
        except KeyboardInterrupt:
            self.interrupted = True
            logger.warning("Extraction interrupted by user after %d/%d files", len(entries), len(files))
            for future in futures:
                future.cancel()
            pool.shutdown(wait=False, cancel_futures=True)
        else:
            pool.shutdown(wait=True)

        return entries

    # ---------- writing ----------

    def index(self, entries: list[FileEntry], progress_callback=None) -> dict[str, int]:
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
        single_file: Path | None = None,
        dry_run: bool = False,
        progress_callback=None,
    ) -> dict[str, int]:
        self.interrupted = False
        logger.info(
            "Scan %s: workers=%d chunk_size=%d skip_existing=%s dry_run=%s",
            single_file or self.library, self.workers, self.chunk_size, self.skip_existing, dry_run,
        )
        started = time.perf_counter()

        files = self.collect(single_file=single_file, progress_callback=progress_callback)
        logger.debug("collect took %.1f s", time.perf_counter() - started)

        skipped = 0
        if self.skip_existing and files:
            known = self.repo.known_fingerprints()
            before = len(files)
            files = [f for f in files if not self._unchanged(f, known)]
            skipped = before - len(files)
            logger.info("Skipped %d/%d unchanged files", skipped, before)

        t0 = time.perf_counter()
        entries = self.extract(files, progress_callback=progress_callback)
        logger.info("Extracted metadata for %d/%d files in %.1f s",
                    len(entries), len(files), time.perf_counter() - t0)

        if dry_run:
            return {
                "indexed": 0,
                "updated": 0,
                "errors": 0,
                "found": len(entries),
                "skipped": skipped,
                "interrupted": self.interrupted,
            }

        t0 = time.perf_counter()
        stats = self.index(entries, progress_callback=progress_callback)
        stats["skipped"] = skipped
        stats["interrupted"] = self.interrupted
        logger.info("Indexed in %.1f s (total %.1f s): %s",
                    time.perf_counter() - t0, time.perf_counter() - started, stats)
        return stats

    def _unchanged(self, path: Path, known: dict[str, tuple[int, float]]) -> bool:
        """True only when both sides are known and equal.

        A file that cannot be stat'ed gives None on one side, and comparing that to
        the None of a path that is not in the database read as "unchanged" - so a
        file that vanished between collect and fingerprint was counted as skipped
        and never indexed.
        """
        recorded = known.get(str(path))
        current = self._fingerprint(path)
        return recorded is not None and current is not None and recorded == current
