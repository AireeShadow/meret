"""Running repository calls off the Qt event loop.

Every SQLiteRepository call opens its own connection, so a call is safe from any
thread — but it is not fast, and the event loop must never wait on it. TaskRunner
runs a callable in a thread pool and hands the result back on the GUI thread.

Each task carries a generation. A panel that re-asks (another tag, another
folder) calls bump(); results from an older generation are dropped instead of
overwriting fresher rows.
"""

import logging
from typing import Any, Callable, Optional

from PySide6.QtCore import QObject, QRunnable, QThreadPool, Signal, Slot

logger = logging.getLogger(__name__)


class _TaskSignals(QObject):
    """Signals need a QObject; QRunnable is not one."""

    done = Signal(object)
    failed = Signal(object)


class Task(QRunnable):
    """One callable, executed in the pool."""

    def __init__(
        self,
        fn: Callable[..., Any],
        args: tuple = (),
        kwargs: Optional[dict] = None,
    ) -> None:
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs or {}
        self.generation = 0
        self.on_done: Optional[Callable[[Any], None]] = None
        self.on_failed: Optional[Callable[[Exception], None]] = None
        self.signals = _TaskSignals()
        self.setAutoDelete(True)

    @Slot()
    def run(self) -> None:
        try:
            result = self.fn(*self.args, **self.kwargs)
        except Exception as e:
            # A worker must never take the app down: report it and carry on
            logger.debug("task %s failed: %s", getattr(self.fn, "__name__", self.fn), e)
            self.signals.failed.emit(e)
        else:
            self.signals.done.emit(result)


class TaskRunner(QObject):
    """Pool plus generation bookkeeping. Create one per window, on the GUI thread."""

    _done = Signal(object, object)    # task, result
    _failed = Signal(object, object)  # task, exception

    def __init__(self, parent: Optional[QObject] = None, max_threads: int = 0) -> None:
        super().__init__(parent)
        self._pool = QThreadPool(self)
        if max_threads > 0:
            self._pool.setMaxThreadCount(max_threads)
        self._generation = 0
        # The runner lives on the GUI thread, so both handlers are queued into it
        self._done.connect(self._deliver_done)
        self._failed.connect(self._deliver_failed)

    @property
    def generation(self) -> int:
        return self._generation

    def bump(self) -> int:
        """Invalidate everything in flight. Returns the new generation."""
        self._generation += 1
        return self._generation

    def run(
        self,
        fn: Callable[..., Any],
        on_done: Optional[Callable[[Any], None]] = None,
        on_failed: Optional[Callable[[Exception], None]] = None,
        *,
        args: tuple = (),
        kwargs: Optional[dict] = None,
        generation: Optional[int] = None,
    ) -> Task:
        """Queue `fn`. Callbacks run on the GUI thread; stale ones are skipped."""
        task = Task(fn, args, kwargs)
        task.generation = self._generation if generation is None else generation
        task.on_done = on_done
        task.on_failed = on_failed
        # Bound methods of a QObject: the receiver thread is the runner's thread
        task.signals.done.connect(self._done.emit)
        task.signals.failed.connect(self._failed.emit)
        self._pool.start(task)
        return task

    @Slot(object, object)
    def _deliver_done(self, task: Task, result: Any) -> None:
        if task.generation != self._generation or task.on_done is None:
            return
        task.on_done(result)

    @Slot(object, object)
    def _deliver_failed(self, task: Task, error: Exception) -> None:
        if task.generation != self._generation or task.on_failed is None:
            return
        task.on_failed(error)

    def clear(self) -> None:
        """Drop queued work. Running tasks finish on their own."""
        self._pool.clear()

    def wait(self, msecs: int = -1) -> bool:
        """Block until the pool is empty — for tests and shutdown."""
        return self._pool.waitForDone(msecs)

    def shutdown(self, msecs: int = 5000) -> None:
        """Bump, clear and wait, so no task outlives the window it paints into."""
        self.bump()
        self._pool.clear()
        self._pool.waitForDone(msecs)