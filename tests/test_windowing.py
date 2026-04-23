"""Tests for pipewatch.windowing."""

from datetime import datetime, timedelta

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.windowing import MetricWindow, build_windows, slice_window

NOW = datetime(2024, 6, 1, 12, 0, 0)


def make_metric(pipeline: str, name: str, value: float, age_seconds: float) -> PipelineMetric:
    ts = NOW - timedelta(seconds=age_seconds)
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=ts,
    )


# ---------------------------------------------------------------------------
# slice_window
# ---------------------------------------------------------------------------

def test_slice_window_keeps_recent():
    metrics = [
        make_metric("p", "m", 1.0, age_seconds=30),
        make_metric("p", "m", 2.0, age_seconds=90),
    ]
    result = slice_window(metrics, seconds=60, label="last_1m", now=NOW)
    assert len(result) == 1
    assert result[0].value == 1.0


def test_slice_window_empty_when_all_old():
    metrics = [make_metric("p", "m", 5.0, age_seconds=400)]
    result = slice_window(metrics, seconds=300, label="last_5m", now=NOW)
    assert result == []


def test_slice_window_keeps_all_when_all_fresh():
    metrics = [
        make_metric("p", "m", 1.0, age_seconds=10),
        make_metric("p", "m", 2.0, age_seconds=20),
        make_metric("p", "m", 3.0, age_seconds=30),
    ]
    result = slice_window(metrics, seconds=60, label="last_1m", now=NOW)
    assert len(result) == 3


# ---------------------------------------------------------------------------
# MetricWindow
# ---------------------------------------------------------------------------

def test_metric_window_average():
    metrics = [
        make_metric("p", "m", 10.0, age_seconds=5),
        make_metric("p", "m", 20.0, age_seconds=10),
    ]
    win = MetricWindow(
        pipeline="p", metric_name="m", label="last_1m",
        start=NOW - timedelta(seconds=60), end=NOW, metrics=metrics,
    )
    assert win.average == pytest.approx(15.0)
    assert win.count == 2


def test_metric_window_average_none_when_empty():
    win = MetricWindow(
        pipeline="p", metric_name="m", label="last_1m",
        start=NOW - timedelta(seconds=60), end=NOW,
    )
    assert win.average is None


def test_metric_window_to_dict_keys():
    win = MetricWindow(
        pipeline="etl", metric_name="rows", label="last_5m",
        start=NOW - timedelta(seconds=300), end=NOW,
    )
    d = win.to_dict()
    assert set(d.keys()) == {"pipeline", "metric_name", "label", "start", "end", "count", "average"}


# ---------------------------------------------------------------------------
# build_windows
# ---------------------------------------------------------------------------

def test_build_windows_creates_entry_per_key_and_config():
    history = {
        "etl:rows": [make_metric("etl", "rows", 100.0, age_seconds=10)],
        "etl:errors": [make_metric("etl", "errors", 0.0, age_seconds=10)],
    }
    configs = [{"label": "last_5m", "seconds": 300}, {"label": "last_1h", "seconds": 3600}]
    windows = build_windows(history, configs, now=NOW)
    assert len(windows) == 4  # 2 keys × 2 windows


def test_build_windows_respects_time_boundary():
    history = {
        "etl:rows": [
            make_metric("etl", "rows", 1.0, age_seconds=10),
            make_metric("etl", "rows", 2.0, age_seconds=400),
        ]
    }
    configs = [{"label": "last_5m", "seconds": 300}]
    windows = build_windows(history, configs, now=NOW)
    assert len(windows) == 1
    assert windows[0].count == 1


def test_build_windows_skips_keys_without_colon():
    history = {"badkey": [make_metric("p", "m", 1.0, age_seconds=5)]}
    windows = build_windows(history, [{"label": "last_1m", "seconds": 60}], now=NOW)
    assert windows == []
