from pathlib import Path

from PIL import Image

from meret.core.extractors import VideoExtractor
from meret.core.indexer import Indexer


def test_scan_only_supported(tmp_path, make_png, mock_repo):
    make_png("a")
    make_png("b")
    (tmp_path / "note.txt").write_text("hello")

    indexer = Indexer(mock_repo, tmp_path)
    files = indexer.collect()
    assert len(files) == 2

    entries = indexer.extract(files)
    assert all(e.type.value == "image" for e in entries)


def test_scan_extracts_metadata(tmp_path, make_png, mock_repo):
    make_png("a", size=(32, 24))
    entries = Indexer(mock_repo, tmp_path).scan()
    assert entries[0].metadata["width"] == 32
    assert entries[0].metadata["height"] == 24


def test_scan_single_file(tmp_path, make_png, mock_repo):
    target = make_png("only")
    make_png("other")
    entries = Indexer(mock_repo, tmp_path).scan(single_file=target)
    assert [e.filename for e in entries] == [target.name]


def test_video_via_registry(tmp_path, mock_repo, monkeypatch):
    monkeypatch.setattr(VideoExtractor, "extract", lambda self, p: {"duration": 12.0})
    (tmp_path / "clip.mp4").write_bytes(b"\x00" * 16)

    entries = Indexer(mock_repo, tmp_path).scan()
    assert entries[0].type.value == "video"
    assert entries[0].metadata == {"duration": 12.0}


def test_dry_run_writes_nothing(tmp_path, make_png, mock_repo):
    make_png("a")
    stats = Indexer(mock_repo, tmp_path).run(dry_run=True)
    assert stats["found"] == 1
    assert mock_repo.get_stats()["total_files"] == 0


def test_run_indexes(tmp_path, make_png, mock_repo):
    make_png("a")
    make_png("b")
    stats = Indexer(mock_repo, tmp_path).run()
    assert stats["indexed"] == 2
    assert mock_repo.get_stats()["total_files"] == 2


def test_progress_callback(tmp_path, make_png, mock_repo):
    for _ in range(3):
        make_png("a")

    seen = []
    indexer = Indexer(mock_repo, tmp_path)
    indexer.run(progress_callback=lambda phase, d, t: seen.append((phase, d, t)))

    phases = {p for p, _, _ in seen}
    assert phases == {"collect", "extract"}
    assert seen[-1][1:] == (3, 3)


def test_missing_library(mock_repo, tmp_path):
    assert Indexer(mock_repo, tmp_path / "nope").scan() == []