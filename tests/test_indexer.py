"""Indexer tests: two phases — collect (listing) and extract (metadata)."""

import pytest

from meret.core.extractors import VideoExtractor
from meret.core.indexer import PHASE_COLLECT, PHASE_EXTRACT, Indexer


# ---------- collect phase ----------


def test_collect_only_supported(tmp_path, make_png, mock_repo):
    make_png("a")
    make_png("b")
    (tmp_path / "note.txt").write_text("hello")

    files = Indexer(mock_repo, tmp_path).collect()
    assert len(files) == 2
    assert all(f.suffix == ".png" for f in files)


def test_collect_single_file(tmp_path, make_png, mock_repo):
    target = make_png("only")
    make_png("other")

    files = Indexer(mock_repo, tmp_path).collect(single_file=target)
    assert [f.name for f in files] == [target.name]


def test_collect_single_unsupported(tmp_path, mock_repo):
    note = tmp_path / "note.txt"
    note.write_text("hi")
    assert Indexer(mock_repo, tmp_path).collect(single_file=note) == []


def test_collect_missing_library(mock_repo, tmp_path):
    assert Indexer(mock_repo, tmp_path / "nope").collect() == []


# ---------- extract phase ----------


def test_extract_metadata(tmp_path, make_png, mock_repo):
    make_png("a", size=(32, 24))

    indexer = Indexer(mock_repo, tmp_path)
    entries = indexer.extract(indexer.collect())
    assert entries[0].metadata["width"] == 32
    assert entries[0].metadata["height"] == 24


def test_extract_empty_list(mock_repo, tmp_path):
    assert Indexer(mock_repo, tmp_path).extract([]) == []


def test_video_via_registry(tmp_path, mock_repo, monkeypatch):
    monkeypatch.setattr(VideoExtractor, "extract", lambda self, p: {"duration": 12.0})
    (tmp_path / "clip.mp4").write_bytes(b"\x00" * 16)

    indexer = Indexer(mock_repo, tmp_path)
    entries = indexer.extract(indexer.collect())
    assert entries[0].type.value == "video"
    assert entries[0].metadata == {"duration": 12.0}


# ---------- orchestration ----------


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
    assert stats["interrupted"] is False
    assert mock_repo.get_stats()["total_files"] == 2


def test_run_skips_unsupported(tmp_path, make_png, mock_repo):
    make_png("a")
    (tmp_path / "note.txt").write_text("hi")
    stats = Indexer(mock_repo, tmp_path).run()
    assert stats["indexed"] == 1


def test_run_single_file(tmp_path, make_png, mock_repo):
    target = make_png("only")
    make_png("other")
    stats = Indexer(mock_repo, tmp_path).run(single_file=target)
    assert stats["indexed"] == 1
    assert mock_repo.get_stats()["total_files"] == 1


# ---------- progress ----------


def test_progress_callback_reports_both_phases(tmp_path, make_png, mock_repo):
    for _ in range(3):
        make_png("a")

    seen = []
    Indexer(mock_repo, tmp_path).run(
        progress_callback=lambda phase, done, total: seen.append((phase, done, total))
    )

    phases = [p for p, _, _ in seen]
    assert PHASE_COLLECT in phases
    assert PHASE_EXTRACT in phases
    assert seen[-1][1:] == (3, 3)  # both phases run to completion


def test_progress_callback_single_file(tmp_path, make_png, mock_repo):
    target = make_png("only")
    seen = []
    Indexer(mock_repo, tmp_path).collect(
        single_file=target,
        progress_callback=lambda phase, done, total: seen.append((phase, done, total)),
    )
    assert seen == [(PHASE_COLLECT, 1, 1)]


# ---------- misc ----------


def test_interrupted_defaults_false(mock_repo, tmp_path):
    assert Indexer(mock_repo, tmp_path).interrupted is False


def test_skip_existing_skips_indexed(tmp_path, make_png, mock_repo):
    make_png("a")
    make_png("b")
    Indexer(mock_repo, tmp_path).run()

    seen = []
    stats = Indexer(mock_repo, tmp_path, skip_existing=True).run(
        progress_callback=lambda phase, done, total: seen.append(phase)
    )

    assert stats["skipped"] == 2
    assert stats["indexed"] == 0
    assert mock_repo.get_stats()["total_files"] == 2
    assert PHASE_EXTRACT not in seen  # nothing to extract


def test_skip_existing_indexes_only_new(tmp_path, make_png, mock_repo):
    make_png("a")
    Indexer(mock_repo, tmp_path).run()

    make_png("b")
    stats = Indexer(mock_repo, tmp_path, skip_existing=True).run()

    assert stats["skipped"] == 1
    assert stats["indexed"] == 1
    assert mock_repo.get_stats()["total_files"] == 2


def test_skip_existing_off_by_default(tmp_path, make_png, mock_repo):
    make_png("a")
    Indexer(mock_repo, tmp_path).run()
    stats = Indexer(mock_repo, tmp_path).run()

    assert stats["skipped"] == 0
    assert stats["updated"] == 1
