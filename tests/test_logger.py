import logging
import os
import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from meret.core.logger import (
    ENV_FILE,
    ENV_LEVEL,
    _MARK,
    disable_console_logging,
    log_file_path,
    resolve_level,
    setup_logging,
    traced,
)


def _ours():
    return [h for h in logging.getLogger().handlers if getattr(h, _MARK, False)]


def _flush():
    for handler in _ours():
        handler.flush()


def _log_text() -> str:
    _flush()
    return Path(os.environ[ENV_FILE]).read_text(encoding="utf-8")


# ---------- setup ----------


def test_writes_to_env_file_with_module_name():
    path = setup_logging()
    assert path == Path(os.environ[ENV_FILE])

    logging.getLogger("meret.core.something").info("hello from a module")
    text = _log_text()
    assert "hello from a module" in text
    assert "meret.core.something" in text


def test_default_location_is_user_log_dir(monkeypatch):
    monkeypatch.delenv(ENV_FILE)
    path = log_file_path()
    assert path.name == "meret.log"
    assert "meret" in str(path.parent).lower()


def test_level_precedence(monkeypatch):
    assert resolve_level() == logging.INFO
    monkeypatch.setenv(ENV_LEVEL, "debug")
    assert resolve_level() == logging.DEBUG
    assert resolve_level("WARNING") == logging.WARNING  # explicit beats the env
    with pytest.raises(ValueError):
        resolve_level("chatty")


def test_info_default_hides_debug():
    setup_logging()
    log = logging.getLogger("meret.x")
    log.debug("quiet-debug")
    log.info("loud-info")
    text = _log_text()
    assert "loud-info" in text and "quiet-debug" not in text


def test_third_party_only_from_warning():
    setup_logging("DEBUG")
    logging.getLogger("PIL.PngImagePlugin").debug("png-chatter")
    logging.getLogger("winfspy.operations").error("unhandled-in-winfspy")
    text = _log_text()
    assert "png-chatter" not in text
    assert "unhandled-in-winfspy" in text


def test_setup_twice_does_not_duplicate():
    setup_logging()
    setup_logging(console=True)
    setup_logging()
    assert len(_ours()) == 1
    logging.getLogger("meret").warning("once")
    assert _log_text().count("once") == 1


def test_disable_console_keeps_file():
    setup_logging(console=True)
    assert len(_ours()) == 2
    disable_console_logging()
    assert [type(h).__name__ for h in _ours()] == ["_SharedRotatingFileHandler"]


def test_rollover_blocked_by_another_process_keeps_logging(monkeypatch):
    """On Windows renaming an open log fails — records must still be written."""
    setup_logging()
    handler = _ours()[0]
    handler.maxBytes = 200

    def locked(source, dest):
        raise PermissionError(32, "The process cannot access the file")

    monkeypatch.setattr(handler, "rotate", locked)
    log = logging.getLogger("meret.x")
    for i in range(20):
        log.warning("record %02d %s", i, "x" * 40)

    text = _log_text()
    assert "record 00" in text and "record 19" in text


# ---------- traced ----------


class _Thing:
    log = logging.getLogger("meret.test.thing")

    @traced(log)
    def work(self, n, label="x"):
        return list(range(n))

    @traced(log)
    def boom(self):
        raise RuntimeError("kaput")


def test_traced_records_args_result_and_location(caplog):
    caplog.set_level(logging.DEBUG, logger="meret")
    assert _Thing().work(3, label="y") == [0, 1, 2]

    (record,) = caplog.records
    assert record.name == "meret.test.thing"
    assert record.funcName == "_Thing.work"  # the method, not the wrapper
    assert record.pathname == __file__
    msg = record.getMessage()
    assert "work(3, label='y') -> list[3] in" in msg and msg.endswith(" ms")


def test_traced_records_exceptions_and_reraises(caplog):
    caplog.set_level(logging.DEBUG, logger="meret")
    with pytest.raises(RuntimeError):
        _Thing().boom()
    assert "boom() raised RuntimeError: kaput" in caplog.records[0].getMessage()


def test_traced_silent_below_level(caplog):
    caplog.set_level(logging.INFO, logger="meret")
    _Thing().work(2)
    assert caplog.records == []


def test_repository_calls_are_traced(sqlite_repo, caplog):
    caplog.set_level(logging.DEBUG, logger="meret")
    fid = sqlite_repo.save_file(
        __import__("meret.core.models", fromlist=["FileEntry"]).FileEntry(
            filename="a.png", filepath="/a.png"
        )
    )
    sqlite_repo.add_tag(fid, "trip")
    names = [r.funcName for r in caplog.records if r.name == "meret.core.sqlite_repo"]
    assert "SQLiteRepository.save_file" in names
    assert "SQLiteRepository.add_tag" in names


# ---------- CLI ----------


def test_cli_logs_invocation_and_debug_with_verbose(tmp_path):
    from meret.cli.app import app

    runner = CliRunner()
    res = runner.invoke(app, ["-v", "where", "--db", str(tmp_path)])
    assert res.exit_code == 0, res.output

    text = _log_text()
    assert "meret " in text and ": where" in text
    assert "db path from argument" in text  # a DEBUG line from deps


def test_cli_rejects_unknown_level(tmp_path):
    from meret.cli.app import app

    res = CliRunner().invoke(app, ["--log-level", "chatty", "where", "--db", str(tmp_path)])
    assert res.exit_code == 2
    assert "unknown log level" in res.output


def test_cli_logs_command_shows_path_and_tail(tmp_path):
    from meret.cli.app import app

    runner = CliRunner()
    runner.invoke(app, ["where", "--db", str(tmp_path)])
    res = runner.invoke(app, ["logs", "-n", "5"])
    assert res.exit_code == 0
    assert os.environ[ENV_FILE] in res.output.replace("\n", "")
    assert ": where" in res.output


# ---------- extractors ----------


def test_missing_ffprobe_reported_once(tmp_path, monkeypatch, caplog):
    import meret.core.extractors as ex

    monkeypatch.setattr(ex, "_ffprobe_missing_reported", False)

    def no_ffprobe(*a, **kw):
        raise FileNotFoundError("ffprobe")

    monkeypatch.setattr(ex.subprocess, "run", no_ffprobe)
    caplog.set_level(logging.WARNING, logger="meret")
    for name in ("a.mp4", "b.mkv"):
        (tmp_path / name).write_bytes(b"x")
        meta = ex.VideoExtractor().extract(tmp_path / name)
        assert "error" in meta

    errors = [r for r in caplog.records if "ffprobe not found" in r.getMessage()]
    assert len(errors) == 1


def test_ffprobe_failure_carries_its_reason(tmp_path, monkeypatch):
    import meret.core.extractors as ex

    def failing(cmd, **kw):
        raise subprocess.CalledProcessError(1, cmd, output="", stderr="moov atom not found\n")

    monkeypatch.setattr(ex.subprocess, "run", failing)
    (tmp_path / "broken.mp4").write_bytes(b"x")
    meta = ex.VideoExtractor().extract(tmp_path / "broken.mp4")
    assert "moov atom not found" in meta["error"]


def test_uncaught_exceptions_reach_the_log(monkeypatch):
    import sys
    import threading

    from meret.core.logger import log_uncaught_exceptions

    seen = []
    monkeypatch.setattr(sys, "excepthook", lambda *a: seen.append(a[0]))
    monkeypatch.setattr(threading, "excepthook", lambda args: seen.append(args.exc_type))
    setup_logging()
    log_uncaught_exceptions()
    log_uncaught_exceptions()  # idempotent: no double logging

    try:
        raise ValueError("main-thread crash")
    except ValueError:
        sys.excepthook(*sys.exc_info())

    worker = threading.Thread(target=lambda: 1 / 0, name="worker-x")
    worker.start()
    worker.join()

    text = _log_text()
    assert text.count("uncaught exception\n") == 1 and "main-thread crash" in text
    assert "uncaught exception in thread worker-x" in text and "ZeroDivisionError" in text
    assert seen == [ValueError, ZeroDivisionError]  # previous hooks still run
