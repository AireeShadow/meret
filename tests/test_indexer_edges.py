"""Indexer branches the main suite does not reach: stat failures, a broken file."""

import logging

from PIL import Image

from meret.core.indexer import Indexer


def test_fingerprint_is_size_and_mtime(tmp_path):
    path = tmp_path / "a.png"
    path.write_bytes(b"1234")

    size, mtime = Indexer._fingerprint(path)

    assert size == 4
    assert mtime > 0


def test_fingerprint_of_a_missing_file_is_none(tmp_path):
    assert Indexer._fingerprint(tmp_path / "gone.png") is None


def test_an_unstattable_file_is_not_read_as_unchanged(tmp_path, make_png, mock_repo, monkeypatch):
    """(None, None) compared equal, so a file that could not be stat'ed was counted
    as skipped and never indexed."""
    make_png("a")
    monkeypatch.setattr(Indexer, "_fingerprint", staticmethod(lambda path: None))

    stats = Indexer(mock_repo, tmp_path, skip_existing=True).run()

    assert stats["skipped"] == 0
    assert stats["indexed"] == 1


def test_workers_and_chunk_size_are_clamped(tmp_path, mock_repo):
    indexer = Indexer(mock_repo, tmp_path, workers=0, chunk_size=0)

    assert indexer.workers == 1
    assert indexer.chunk_size == 1


def test_collect_walks_subdirectories(tmp_path, mock_repo):
    nested = tmp_path / "deep" / "deeper"
    nested.mkdir(parents=True)
    Image.new("RGB", (4, 4)).save(nested / "a.png")

    files = Indexer(mock_repo, tmp_path).collect()

    assert [f.name for f in files] == ["a.png"]


def test_extract_survives_one_broken_file(tmp_path, make_png, mock_repo, monkeypatch, caplog):
    """One unreadable file must not take the whole scan down."""
    caplog.set_level(logging.ERROR, logger="meret")
    make_png("good")
    make_png("bad")

    # _process_file is a regular method, unlike the static _fingerprint
    original = Indexer._process_file

    def broken(self, path):
        # make_png appends a counter, so match on the prefix, never the full name
        if path.name.startswith("bad"):
            raise OSError("cannot read")
        return original(self, path)

    monkeypatch.setattr(Indexer, "_process_file", broken)

    indexer = Indexer(mock_repo, tmp_path)
    entries = indexer.extract(indexer.collect())

    assert len(entries) == 1
    assert entries[0].filename.startswith("good")
    assert "cannot read" in caplog.text


def test_index_with_nothing_writes_nothing(tmp_path, mock_repo):
    assert Indexer(mock_repo, tmp_path).index([]) == {
        "indexed": 0,
        "updated": 0,
        "errors": 0,
    }


def test_chunk_size_reaches_the_repository(tmp_path, make_png, mock_repo, monkeypatch):
    make_png("a")
    seen = {}
    original = mock_repo.save_files

    def spy(entries, chunk_size=None, on_progress=None):
        seen["chunk_size"] = chunk_size
        return original(entries, chunk_size=chunk_size, on_progress=on_progress)

    monkeypatch.setattr(mock_repo, "save_files", spy)

    Indexer(mock_repo, tmp_path, chunk_size=7).run()

    assert seen["chunk_size"] == 7


def test_dry_run_reports_the_same_keys(tmp_path, make_png, mock_repo):
    """The CLI reads stats['interrupted'] and stats['skipped'] in both branches."""
    make_png("a")

    stats = Indexer(mock_repo, tmp_path).run(dry_run=True)

    assert set(stats) == {"indexed", "updated", "errors", "found", "skipped", "interrupted"}