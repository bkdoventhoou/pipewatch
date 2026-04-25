"""Tests for pipewatch.watermark."""
from __future__ import annotations

import time
from typing import List

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.watermark import (
    WatermarkEntry,
    WatermarkTracker,
    track_watermarks,
)


def make_metric(
    pipeline: str = "pipe",
    name: str = "latency",
    value: float = 1.0,
    ts: float = 1000.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=ts,
    )


def test_first_update_sets_high_and_low():
    tracker = WatermarkTracker()
    m = make_metric(value=5.0, ts=100.0)
    entry = tracker.update(m)
    assert entry.high == 5.0
    assert entry.low == 5.0
    assert entry.high_ts == 100.0
    assert entry.low_ts == 100.0


def test_higher_value_updates_high():
    tracker = WatermarkTracker()
    tracker.update(make_metric(value=3.0, ts=100.0))
    entry = tracker.update(make_metric(value=9.0, ts=200.0))
    assert entry.high == 9.0
    assert entry.high_ts == 200.0
    assert entry.low == 3.0


def test_lower_value_updates_low():
    tracker = WatermarkTracker()
    tracker.update(make_metric(value=5.0, ts=100.0))
    entry = tracker.update(make_metric(value=1.0, ts=200.0))
    assert entry.low == 1.0
    assert entry.low_ts == 200.0
    assert entry.high == 5.0


def test_middle_value_changes_nothing():
    tracker = WatermarkTracker()
    tracker.update(make_metric(value=1.0, ts=100.0))
    tracker.update(make_metric(value=9.0, ts=200.0))
    entry = tracker.update(make_metric(value=5.0, ts=300.0))
    assert entry.high == 9.0
    assert entry.low == 1.0


def test_separate_pipelines_tracked_independently():
    tracker = WatermarkTracker()
    tracker.update(make_metric(pipeline="a", value=10.0))
    tracker.update(make_metric(pipeline="b", value=2.0))
    assert tracker.get("a", "latency").high == 10.0
    assert tracker.get("b", "latency").high == 2.0


def test_get_unknown_returns_none():
    tracker = WatermarkTracker()
    assert tracker.get("missing", "x") is None


def test_reset_removes_entry():
    tracker = WatermarkTracker()
    tracker.update(make_metric(value=5.0))
    tracker.reset("pipe", "latency")
    assert tracker.get("pipe", "latency") is None


def test_all_entries_returns_all():
    tracker = WatermarkTracker()
    tracker.update(make_metric(pipeline="a", name="m1", value=1.0))
    tracker.update(make_metric(pipeline="b", name="m2", value=2.0))
    assert len(tracker.all_entries()) == 2


def test_to_dict_has_expected_keys():
    tracker = WatermarkTracker()
    entry = tracker.update(make_metric(value=4.0, ts=500.0))
    d = entry.to_dict()
    assert set(d.keys()) == {"pipeline", "metric_name", "high", "low", "high_ts", "low_ts"}


def test_track_watermarks_convenience():
    metrics = [
        make_metric(value=3.0, ts=100.0),
        make_metric(value=7.0, ts=200.0),
        make_metric(value=1.0, ts=300.0),
    ]
    tracker = track_watermarks(metrics)
    entry = tracker.get("pipe", "latency")
    assert entry is not None
    assert entry.high == 7.0
    assert entry.low == 1.0
