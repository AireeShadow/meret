"""Logging for meret: the one place that decides where logs go and how verbose they are.

Modules just do ``logger = logging.getLogger(__name__)`` (all names live under
"meret."). Entry points — the CLI callback and the TUI — call setup_logging()
once; importing meret as a library stays silent.

    level   -v / --log-level  >  $MERET_LOG_LEVEL  >  INFO
    file    $MERET_LOG_FILE   >  <user log dir>/meret.log   (`meret logs` shows it)

Third-party libraries (winfspy, PIL, asyncio, ...) are recorded from WARNING up,
so e.g. winfspy's "Unhandled exception" tracebacks from a mount land in the file.
"""

import logging
import os
import reprlib
import sys
import threading
import time
from functools import wraps
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar

import platformdirs

APP_NAME = "meret"
ENV_LEVEL = "MERET_LOG_LEVEL"
ENV_FILE = "MERET_LOG_FILE"
DEFAULT_LEVEL = "INFO"

FILE_FORMAT = (
    "%(asctime)s %(levelname)-7s [%(process)d %(threadName)s] "
    "%(name)s:%(funcName)s:%(lineno)d  %(message)s"
)
CONSOLE_FORMAT = "%(levelname)-7s %(name)s: %(message)s"

_MARK = "_meret_handler"  # our handlers on the root logger, so setup can replace them


def log_file_path() -> Path:
    env = os.environ.get(ENV_FILE)
    if env:
        return Path(env).expanduser()
    return Path(platformdirs.user_log_dir(APP_NAME, appauthor=False)) / "meret.log"


def resolve_level(level: Optional[str] = None) -> int:
    """Explicit level, else $MERET_LOG_LEVEL, else INFO. ValueError if unknown."""
    name = (level or os.environ.get(ENV_LEVEL) or DEFAULT_LEVEL).strip().upper()
    value = logging.getLevelName(name)
    if not isinstance(value, int):
        raise ValueError(f"unknown log level: {name!r} (use DEBUG, INFO, WARNING or ERROR)")
    return value


class _SharedRotatingFileHandler(RotatingFileHandler):
    """Rotation that survives other meret processes holding the file.

    The TUI runs `meret scan` in a subprocess and a mount may run alongside; on
    Windows renaming a file someone has open fails. Then keep appending and try
    again on a later record — FileHandler.emit reopens the stream by itself.
    """

    def doRollover(self) -> None:
        try:
            super().doRollover()
        except OSError:
            pass


class _StderrHandler(logging.StreamHandler):
    """Writes to whatever sys.stderr is *now* — test runners and friends swap it."""

    @property
    def stream(self):
        return sys.stderr

    @stream.setter
    def stream(self, value) -> None:
        pass


def setup_logging(
    level: Optional[str] = None, *, console: bool = False, log_file: Optional[Path] = None
) -> Path:
    """Configure logging for an entry point. Calling it again replaces its handlers.

    console=True also echoes records to stderr (never in the TUI: it owns the screen).
    Returns the log file path.
    """
    meret_level = resolve_level(level)
    root = logging.getLogger()
    _remove_handlers(root, lambda h: True)

    path = Path(log_file) if log_file else log_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = _SharedRotatingFileHandler(
        path, maxBytes=5_000_000, backupCount=5, encoding="utf-8", delay=True
    )
    file_handler.setFormatter(logging.Formatter(FILE_FORMAT))
    file_handler.setLevel(logging.DEBUG)  # Handle all messages from meret
    handlers: list[logging.Handler] = [file_handler]

    if console:
        stream = _StderrHandler()  # stderr: stdout is the command's output
        stream.setFormatter(logging.Formatter(CONSOLE_FORMAT))
        stream.setLevel(logging.DEBUG)  # Echo all meret messages to console
        handlers.append(stream)

    for handler in handlers:
        setattr(handler, _MARK, True)
        root.addHandler(handler)

    root.setLevel(logging.WARNING)  # third-party noise stays out
    logging.getLogger(APP_NAME).setLevel(meret_level)  # meret controls its own level
    return path

def log_uncaught_exceptions() -> None:
    """Crashes go to the log file too, not just to a terminal that may be long gone.

    Wraps whatever hook is installed (Typer's pretty traceback keeps working).
    Covers the main thread and plain threads; safe to call more than once.
    """
    log = logging.getLogger(APP_NAME)

    previous = sys.excepthook
    if not getattr(previous, _MARK, False):

        def hook(exc_type, exc, tb):
            if not issubclass(exc_type, KeyboardInterrupt):
                log.critical("uncaught exception", exc_info=(exc_type, exc, tb))
            previous(exc_type, exc, tb)

        setattr(hook, _MARK, True)
        sys.excepthook = hook

    previous_thread = threading.excepthook
    if not getattr(previous_thread, _MARK, False):

        def thread_hook(args):
            if not issubclass(args.exc_type, SystemExit):
                log.critical(
                    "uncaught exception in thread %s",
                    args.thread.name if args.thread else "?",
                    exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
                )
            previous_thread(args)

        setattr(thread_hook, _MARK, True)
        threading.excepthook = thread_hook


def disable_console_logging() -> None:
    """Drop the stderr echo — for the TUI, where it would scribble over the screen."""
    _remove_handlers(logging.getLogger(), lambda h: not isinstance(h, logging.FileHandler))


def _remove_handlers(root: logging.Logger, which: Callable[[logging.Handler], bool]) -> None:
    for handler in [h for h in root.handlers if getattr(h, _MARK, False) and which(h)]:
        root.removeHandler(handler)
        handler.close()


# ---------- call tracing ----------

_repr = reprlib.Repr()
_repr.maxstring = 80
_repr.maxother = 80
_repr.maxlist = _repr.maxtuple = _repr.maxset = _repr.maxdict = 3

F = TypeVar("F", bound=Callable[..., Any])


def _summary(value: Any) -> str:
    if isinstance(value, (list, tuple, set, dict)):
        return f"{type(value).__name__}[{len(value)}]"
    return _repr.repr(value)


def traced(logger: logging.Logger, level: int = logging.DEBUG) -> Callable[[F], F]:
    """Log a method's arguments, result and duration — only when `level` is enabled.

    The record points at the method itself (its file, name and `def` line), not at
    this wrapper. Exceptions are logged with the same line and re-raised.
    """

    def decorate(fn: F) -> F:
        code = fn.__code__

        def emit(msg: str, *args: Any) -> None:
            record = logger.makeRecord(
                logger.name, level, code.co_filename, code.co_firstlineno, msg, args, None,
                func=fn.__qualname__,
            )
            logger.handle(record)

        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not logger.isEnabledFor(level):
                return fn(*args, **kwargs)
            shown = ", ".join(
                [_repr.repr(a) for a in args[1:]]  # args[0] is self
                + [f"{k}={_repr.repr(v)}" for k, v in kwargs.items()]
            )
            start = time.perf_counter()
            try:
                result = fn(*args, **kwargs)
            except Exception as exc:
                ms = (time.perf_counter() - start) * 1000
                emit("%s(%s) raised %s: %s after %.1f ms",
                     fn.__name__, shown, type(exc).__name__, exc, ms)
                raise
            ms = (time.perf_counter() - start) * 1000
            emit("%s(%s) -> %s in %.1f ms", fn.__name__, shown, _summary(result), ms)
            return result

        return wrapper  # type: ignore[return-value]

    return decorate
