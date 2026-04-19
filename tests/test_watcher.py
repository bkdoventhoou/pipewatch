"""Tests for PipelineWatcher."""

import pytest
from unittest.mock import MagicMock, patch

from pipewatch.metrics import PipelineMetric, MetricStatus, ThresholdConfig
from pipewatch.collector import MetricCollector
from pipewatch.alerts import AlertDispatcher
from pipewatch.watcher import PipelineWatcher


def make_collector(value: float, warn: float = 80.0, crit: float = 90.0) -> MetricCollector:
    col = MetricCollector()
    col.add_threshold("latency", ThresholdConfig(warning=warn, critical=crit))
    col.record("latency", value, unit="ms")
    return col


def test_run_once_ok_no_alert():
    col = make_collector(50.0)
    dispatcher = MagicMock(spec=AlertDispatcher)
    watcher = PipelineWatcher(col, dispatcher)
    report = watcher.run_once()
    dispatcher.dispatch.assert_not_called()
    assert report.entries[0].status == MetricStatus.OK


def test_run_once_warning_dispatches():
    col = make_collector(85.0)
    dispatcher = MagicMock(spec=AlertDispatcher)
    watcher = PipelineWatcher(col, dispatcher)
    report = watcher.run_once()
    dispatcher.dispatch.assert_called_once()
    assert report.entries[0].status == MetricStatus.WARNING


def test_run_once_critical_dispatches():
    col = make_collector(95.0)
    dispatcher = MagicMock(spec=AlertDispatcher)
    watcher = PipelineWatcher(col, dispatcher)
    report = watcher.run_once()
    dispatcher.dispatch.assert_called_once()
    assert report.entries[0].status == MetricStatus.CRITICAL


def test_on_report_callback_called():
    col = make_collector(50.0)
    dispatcher = MagicMock(spec=AlertDispatcher)
    callback = MagicMock()
    watcher = PipelineWatcher(col, dispatcher, on_report=callback)
    watcher.run_once()
    callback.assert_called_once()


def test_max_ticks_stops_loop():
    col = make_collector(50.0)
    dispatcher = MagicMock(spec=AlertDispatcher)
    watcher = PipelineWatcher(col, dispatcher, interval=0.0)
    with patch("time.sleep"):
        watcher.start(max_ticks=3)
    assert not watcher._running


def test_stop_sets_flag():
    col = make_collector(50.0)
    dispatcher = MagicMock(spec=AlertDispatcher)
    watcher = PipelineWatcher(col, dispatcher)
    watcher._running = True
    watcher.stop()
    assert not watcher._running
