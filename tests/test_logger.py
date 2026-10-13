import logging

from meret.core.logger import Logger


def test_handlers_not_duplicated(clean_logger, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for _ in range(5):
        Logger()
    assert len(clean_logger.handlers) == 1


def test_log_file_written_once(clean_logger, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Logger().logger.error("boom")
    for handler in clean_logger.handlers:
        handler.flush()
    assert (tmp_path / "meret.log").read_text().count("boom") == 1