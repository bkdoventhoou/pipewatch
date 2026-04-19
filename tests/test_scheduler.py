"""Tests for PipelineScheduler."""

import time
import threading
import pytest
from unittest.mock import MagicMock, patch

from pipewatch.scheduler import PipelineScheduler


def test_task_called_immediately_on_start():
    calls = []
    stop = threading.Event()

    def task():
        calls.append(1)
        stop.set()

    scheduler = PipelineScheduler(interval_seconds=10, task=task)
    scheduler.start()
    stop.wait(timeout=2)
    scheduler.stop()
    assert len(calls) >= 1


def test_task_called_multiple_times()
    def task():
        calls.append(1)

    scheduler = PipelineScheduler(interval_seconds=0.05, task=task)
    scheduler.start()
    time.sleep(0.2)
    scheduler.stop()
    assert len(calls) >= 3


def test_stop_halts_scheduler():
    task = MagicMock()
    scheduler = PipelineScheduler(interval_seconds=0.05, task=task)
    scheduler.start()
    time.sleep(0.15)
    scheduler.stop()
    count_after_stop = task.call_count
    time.sleep(0.15)
    assert task.call_count == count_after_stop


def test_is_running_false_before_start():
    scheduler = PipelineScheduler(interval_seconds=1, task=lambda: None)
    assert not scheduler.is_running()


def test_is_running_true_after_start():
    scheduler = PipelineScheduler(interval_seconds=10, task=lambda: None)
    scheduler.start()
    assert scheduler.is_running()
    scheduler.stop()


def test_double_start_raises():
    scheduler = PipelineScheduler(interval_seconds=10, task=lambda: None)
    scheduler.start()
    with pytest.raises(RuntimeError, match="already running"):
        scheduler.start()
    scheduler.stop()


def test_task_exception_does_not_crash_scheduler():
    call_count = [0]
    stop = threading.Event()

    def bad_task():
        call_count[0] += 1
        if call_count[0] >= 3:
            stop.set()
        raise ValueError("boom")

    scheduler = PipelineScheduler(interval_seconds=0.05, task=bad_task)
    scheduler.start()
    stop.wait(timeout=2)
    scheduler.stop()
    assert call_count[0] >= 3
