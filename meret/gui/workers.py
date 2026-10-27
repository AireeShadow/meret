"""Running repository calls off the Qt event loop.

Every SQLiteRepository call opens its own connection, so a call is safe from any
thread - but it is not fast, and the event loop must never wait on it. TaskRunner
runs a callable in a thread pool and hands the result back on the GUI thread.

Each task carries a generation. A panel that re-asks (another tag, another
folder) calls bump(); results from an older generation are dropped instead of
overwriting fresher rows. A task marked sticky ignores that: a version probe or a
scan must still report even though the user kept browsing while it ran.

Lifetime rules, learned the hard way:
- the pool must not delete the runnable, and Python must not collect it while a
  pool thread is inside run(). Module-level _INFLIGHT is the safety net: it keeps
  every task alive from start() until its result has been delivered, even if the
  TaskRunner that queued it is already garbage.
- a task keeps its runner alive, so a queued delivery still finds its receiver.
- delivery is explicitly queued, so callbacks never run on a pool thread.
- a runnable that is dropped from the queue never emits, so clear() has to release
  it itself - otherwise the safety net would hold it (and its closures) for the
  whole run.
"""

import logging
from collections.abc import Callable
from typing import Any

from PySide6.QtCore import QObject, QRunnable, Qt, QThreadPool, Signal, Slot

logger = logging.getLogger(__name__)

#: Tasks the pool is working on right now. A task may outlive its runner, and the
#: pool thread emits into the task's signals object, so Python must not collect it
#: mid-flight. Entries are removed when the result is delivered.
_INFLIGHT: set["Task"] = set()


class _TaskSignals(QObject):
    """Signals need a QObject; QRunnable is not one.

    Both carry the task, so the receiver can check the generation and drop a
    result that a newer request has already superseded.
    """

    done = Signal(object, object)    # task, result
    failed = Signal(object, object)  # task, exception


class Task(QRunnable):
    """One callable, executed in the pool."""

    def __init__(
        self,
        fn: Callable[..., Any],
        args: tuple = (),
        kwargs: dict | None = None,
    ) -> None:
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs or {}
        self.generation = 0
        self.sticky = False
        self.on_done: Callable[[Any], None] | None = None
        self.on_failed: Callable[[Exception], None] | None = None
        self.runner: TaskRunner | None = None
        self.signals = _TaskSignals()
        # Python keeps ownership: _INFLIGHT references the task until delivery
        self.setAutoDelete(False)

    @Slot()
    def run(self) -> None:
        try:
            result = self.fn(*self.args, **self.kwargs)
        except Exception as e:
            # A worker must never take the app down: report it and carry on
            logger.debug("task %s failed: %s", getattr(self.fn, "__name__", self.fn), e)
            self.signals.failed.emit(self, e)
        else:
            self.signals.done.emit(self, result)


class TaskRunner(QObject):
    """Pool plus generation bookkeeping. Create one per window, on the GUI thread."""

    def __init__(self, parent: QObject | None = None, max_threads: int = 0) -> None:
        super().__init__(parent)
        self._pool = QThreadPool(self)
        if max_threads > 0:
            self._pool.setMaxThreadCount(max_threads)
        self._generation = 0
        # Runner-scoped view of the same tasks, for clear() and shutdown()
        self._inflight: set[Task] = set()

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
        on_done: Callable[[Any], None] | None = None,
        on_failed: Callable[[Exception], None] | None = None,
        *,
        args: tuple = (),
        kwargs: dict | None = None,
        generation: int | None = None,
        sticky: bool = False,
    ) -> Task:
        """Queue `fn`. Callbacks run on the GUI thread; stale ones are skipped.

        sticky=True for work that must not be cancelled by a bump() - a probe of
        the database, or a scan whose ending the user's browsing must not eat.
        """
        task = Task(fn, args, kwargs)
        task.generation = self._generation if generation is None else generation
        task.sticky = sticky
        task.on_done = on_done
        task.on_failed = on_failed
        task.runner = self
        # Explicitly queued: results must land on the runner's thread (the GUI
        # thread), never on the pool thread that produced them
        task.signals.done.connect(
            self._deliver_done, Qt.ConnectionType.QueuedConnection
        )
        task.signals.failed.connect(
            self._deliver_failed, Qt.ConnectionType.QueuedConnection
        )
        self._inflight.add(task)
        _INFLIGHT.add(task)
        self._pool.start(task)
        return task

    @Slot(object, object)
    def _deliver_done(self, task: Task, result: Any) -> None:
        try:
            if not task.sticky and task.generation != self._generation:
                logger.debug("dropping result from generation %s", task.generation)
            elif task.on_done is not None:
                task.on_done(result)
        finally:
            self._release(task)

    @Slot(object, object)
    def _deliver_failed(self, task: Task, error: Exception) -> None:
        try:
            if not task.sticky and task.generation != self._generation:
                return
            if task.on_failed is not None:
                task.on_failed(error)
            else:  # never let a failure disappear just because nobody listened
                logger.warning(
                    "task %s failed: %s", getattr(task.fn, "__name__", task.fn), error
                )
        finally:
            self._release(task)

    def _release(self, task: Task) -> None:
        """The result is delivered: the task may be collected now."""
        self._inflight.discard(task)
        _INFLIGHT.discard(task)
        task.runner = None

    def clear(self) -> None:
        """Drop queued work: nothing still registered will deliver any more.

        QThreadPool.clear() drops runnables that never started, and those emit no
        signal at all, so they are released here instead of leaking into the
        module-level safety net along with the closures they carry. A task that is
        already running finishes on its own and releases itself.
        """
        self._pool.clear()
        for task in list(self._inflight):
            task.on_done = None
            task.on_failed = None
            self._release(task)

    def wait(self, msecs: int = -1) -> bool:
        """Block until the pool is empty - for tests and shutdown."""
        return self._pool.waitForDone(msecs)

    def shutdown(self, msecs: int = 5000) -> None:
        """Bump, clear and wait, so no task outlives the window it paints into."""
        self.bump()
        self.clear()
        self._pool.waitForDone(msecs)