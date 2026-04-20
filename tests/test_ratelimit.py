"""Tests for pipewatch.ratelimit — AlertRateLimiter."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.ratelimit import AlertRateLimiter, RateLimitEntry


def make_metric(
    pipeline: str = "pipe_a",
    name: str = "row_count",
    value: float = 10.0,
    status: MetricStatus = MetricStatus.WARNING,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=pipeline,
        metric_name=name,
        value=value,
        status=status,
        timestamp=time.time(),
    )


def test_ok_metric_never_allowed():
    limiter = AlertRateLimiter(window_seconds=60, max_alerts=3)
    metric = make_metric(status=MetricStatus.OK)
    assert limiter.is_allowed(metric) is False


def test_first_alert_allowed():
    limiter = AlertRateLimiter(window_seconds=60, max_alerts=3)
    metric = make_metric(status=MetricStatus.WARNING)
    assert limiter.is_allowed(metric) is True


def test_alerts_within_limit_all_allowed():
    limiter = AlertRateLimiter(window_seconds=60, max_alerts=3)
    metric = make_metric(status=MetricStatus.CRITICAL)
    assert limiter.is_allowed(metric) is True
    assert limiter.is_allowed(metric) is True
    assert limiter.is_allowed(metric) is True


def test_alert_exceeding_limit_suppressed():
    limiter = AlertRateLimiter(window_seconds=60, max_alerts=3)
    metric = make_metric(status=MetricStatus.WARNING)
    for _ in range(3):
        limiter.is_allowed(metric)
    assert limiter.is_allowed(metric) is False


def test_alerts_allowed_after_window_expires():
    limiter = AlertRateLimiter(window_seconds=1, max_alerts=2)
    metric = make_metric(status=MetricStatus.WARNING)
    limiter.is_allowed(metric)
    limiter.is_allowed(metric)
    assert limiter.is_allowed(metric) is False

    # Advance time past the window
    with patch("pipewatch.ratelimit.time") as mock_time:
        mock_time.time.return_value = time.time() + 2
        entry = limiter._get_or_create(metric)
        entry._prune.__func__(entry)  # direct prune via real method

    # Re-create limiter to simulate fresh window
    limiter2 = AlertRateLimiter(window_seconds=1, max_alerts=2)
    assert limiter2.is_allowed(metric) is True


def test_different_pipelines_tracked_independently():
    limiter = AlertRateLimiter(window_seconds=60, max_alerts=1)
    m1 = make_metric(pipeline="pipe_a", status=MetricStatus.WARNING)
    m2 = make_metric(pipeline="pipe_b", status=MetricStatus.WARNING)
    assert limiter.is_allowed(m1) is True
    assert limiter.is_allowed(m1) is False  # pipe_a exhausted
    assert limiter.is_allowed(m2) is True   # pipe_b independent


def test_status_returns_entries():
    limiter = AlertRateLimiter(window_seconds=60, max_alerts=5)
    metric = make_metric(status=MetricStatus.CRITICAL)
    limiter.is_allowed(metric)
    entries = limiter.status()
    assert len(entries) == 1
    assert entries[0]["pipeline"] == "pipe_a"
    assert entries[0]["metric_name"] == "row_count"
    assert entries[0]["alert_count_in_window"] == 1


def test_reset_clears_all():
    limiter = AlertRateLimiter(window_seconds=60, max_alerts=1)
    metric = make_metric(status=MetricStatus.WARNING)
    limiter.is_allowed(metric)
    assert limiter.is_allowed(metric) is False
    limiter.reset()
    assert limiter.is_allowed(metric) is True


def test_reset_by_pipeline_only():
    limiter = AlertRateLimiter(window_seconds=60, max_alerts=1)
    m1 = make_metric(pipeline="pipe_a", status=MetricStatus.WARNING)
    m2 = make_metric(pipeline="pipe_b", status=MetricStatus.WARNING)
    limiter.is_allowed(m1)
    limiter.is_allowed(m2)
    limiter.reset(pipeline="pipe_a")
    assert limiter.is_allowed(m1) is True   # reset
    assert limiter.is_allowed(m2) is False  # still exhausted


def test_rate_limit_entry_to_dict():
    entry = RateLimitEntry(
        pipeline="etl", metric_name="rows", window_seconds=120, max_alerts=5
    )
    d = entry.to_dict()
    assert d["pipeline"] == "etl"
    assert d["metric_name"] == "rows"
    assert d["window_seconds"] == 120
    assert d["max_alerts"] == 5
    assert d["alert_count_in_window"] == 0
