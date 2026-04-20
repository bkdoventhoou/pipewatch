"""Tests for pipewatch.throttle.AlertThrottle."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.throttle import AlertThrottle, ThrottleEntry


def make_metric(status: MetricStatus, pipeline: str = "etl", name: str = "row_count", value: float = 10.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=pipeline,
        metric_name=name,
        value=value,
        status=status,
        timestamp=time.time(),
    )


def test_ok_metric_never_fires():
    throttle = AlertThrottle(cooldown_seconds=60.0)
    metric = make_metric(MetricStatus.OK)
    assert throttle.should_fire(metric) is False


def test_first_warning_fires():
    throttle = AlertThrottle(cooldown_seconds=60.0)
    metric = make_metric(MetricStatus.WARNING)
    assert throttle.should_fire(metric) is True


def test_second_warning_suppressed_within_cooldown():
    throttle = AlertThrottle(cooldown_seconds=60.0)
    metric = make_metric(MetricStatus.WARNING)
    throttle.record(metric)
    assert throttle.should_fire(metric) is False


def test_warning_fires_after_cooldown_expires():
    throttle = AlertThrottle(cooldown_seconds=10.0)
    metric = make_metric(MetricStatus.WARNING)

    with patch("pipewatch.throttle.time.monotonic", return_value=1000.0):
        throttle.record(metric)

    with patch("pipewatch.throttle.time.monotonic", return_value=1011.0):
        assert throttle.should_fire(metric) is True


def test_record_increments_fire_count():
    throttle = AlertThrottle(cooldown_seconds=60.0)
    metric = make_metric(MetricStatus.CRITICAL)
    throttle.record(metric)
    throttle._registry[throttle._key(metric)].last_fired -= 999  # force expire
    throttle.record(metric)
    entry = throttle.get_entry(metric)
    assert entry is not None
    assert entry.fire_count == 2


def test_reset_clears_entry():
    throttle = AlertThrottle(cooldown_seconds=60.0)
    metric = make_metric(MetricStatus.WARNING)
    throttle.record(metric)
    throttle.reset(metric)
    assert throttle.get_entry(metric) is None


def test_stats_returns_all_entries():
    throttle = AlertThrottle(cooldown_seconds=60.0)
    m1 = make_metric(MetricStatus.WARNING, pipeline="p1")
    m2 = make_metric(MetricStatus.CRITICAL, pipeline="p2")
    throttle.record(m1)
    throttle.record(m2)
    stats = throttle.stats()
    assert len(stats) == 2
    for v in stats.values():
        assert "last_fired" in v
        assert "fire_count" in v


def test_different_statuses_tracked_separately():
    throttle = AlertThrottle(cooldown_seconds=60.0)
    warn = make_metric(MetricStatus.WARNING)
    crit = make_metric(MetricStatus.CRITICAL)
    throttle.record(warn)
    assert throttle.should_fire(crit) is True


def test_throttle_entry_to_dict():
    entry = ThrottleEntry(last_fired=500.0, fire_count=3)
    d = entry.to_dict()
    assert d["last_fired"] == 500.0
    assert d["fire_count"] == 3
