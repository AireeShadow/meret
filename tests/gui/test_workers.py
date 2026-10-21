"""The runner delivers on the GUI thread, drops stale work and survives GC."""
import gc

import pytest

from meret.gui.workers import TaskRunner

pytest.importorskip("PySide6")


@pytest.fixture
def runner(qapp):
    """A runner that is shut down before it is dropped."""
    made = TaskRunner(qapp)
    yield made
    made.shutdown()


def test_result_is_delivered(runner, qtbot):
    seen: list = []
    runner.run(lambda: 42, on_done=seen.append)
    qtbot.waitUntil(lambda: bool(seen), timeout=2000)
    assert seen == [42]


def test_failure_reaches_on_failed(runner, qtbot):
    errors: list = []

    def boom():
        raise ValueError("nope")

    runner.run(boom, on_failed=errors.append)
    qtbot.waitUntil(lambda: bool(errors), timeout=2000)
    assert isinstance(errors[0], ValueError)


def test_delivery_happens_on_the_gui_thread(runner, qapp, qtbot):
    from PySide6.QtCore import QThread

    threads: list = []
    # currentThread() must be read inside on_done: that is the delivery thread,
    # while the lambda itself runs on a pool thread
    runner.run(lambda: None, on_done=lambda _result: threads.append(QThread.currentThread()))
    qtbot.waitUntil(lambda: bool(threads), timeout=2000)
    assert threads[0] is qapp.thread()


def test_stale_result_is_dropped(runner, qtbot):
    seen: list = []
    generation = runner.bump()
    runner.run(lambda: "old", on_done=seen.append, generation=generation)
    assert runner.wait(2000)  # the pool is done, but nothing was delivered yet
    runner.bump()             # ...and now the result is stale
    qtbot.wait(100)
    assert seen == []


def test_in_flight_task_survives_collection(runner, qtbot):
    seen: list = []
    runner.run(lambda: 1, on_done=seen.append)
    gc.collect()  # nothing may be collected while the pool is inside run()
    qtbot.waitUntil(lambda: bool(seen), timeout=2000)
    assert seen == [1]


def test_runner_may_be_dropped_mid_flight(qapp, qtbot):
    seen: list = []

    def make_and_forget():
        # No parent, no shutdown: the runner becomes garbage right away
        TaskRunner().run(lambda: "x", on_done=seen.append)

    make_and_forget()
    gc.collect()  # this used to kill the pool thread
    qtbot.waitUntil(lambda: seen == ["x"], timeout=2000)