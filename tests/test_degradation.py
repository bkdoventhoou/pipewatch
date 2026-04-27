"""Tests for pipewatch.degradation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.degradation import (
    DegradationResult,
    _slope,
    detect_degradation,
    detect_all_degradations,
)


def make_metric(
    status: MetricStatus,
    pipeline: str = "pipe",
    name: str = "rows",
    value: float = 1.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=datetime.now(timezone.utc),
    )


# --- _slope ---

def test_slope_flat():
    assert _slope([0.0, 0.0, 0.0, 0.0]) == pytest.approx(0.0)


def test_slope_rising():
    s = _slope([0.0, 1.0, 2.0, 3.0])
    assert s > 0


def test_slope_falling():
    s = _slope([3.0, 2.0, 1.0, 0.0])
    assert s < 0


def test_slope_single_returns_zero():
    assert _slope([5.0]) == 0.0


# --- detect_degradation ---

def test_returns_none_on_too_few():
    series = [make_metric(MetricStatus.WARNING)] * 3
    assert detect_degradation(series, min_samples=4) is None


def test_stable_series_not_degrading():
    series = [make_metric(MetricStatus.OK)] * 6
    r = detect_degradation(series)
    assert r is not None
    assert r.degrading is False
    assert r.score_slope == pytest.approx(0.0)


def test_worsening_series_is_degrading():
    series = [
        make_metric(MetricStatus.OK),
        make_metric(MetricStatus.OK),
        make_metric(MetricStatus.WARNING),
        make_metric(MetricStatus.CRITICAL),
    ]
    r = detect_degradation(series, min_samples=4, slope_threshold=0.1)
    assert r is not None
    assert r.degrading is True
    assert r.score_slope > 0
    assert r.latest_status == MetricStatus.CRITICAL


def test_improving_series_not_degrading():
    series = [
        make_metric(MetricStatus.CRITICAL),
        make_metric(MetricStatus.WARNING),
        make_metric(MetricStatus.OK),
        make_metric(MetricStatus.OK),
    ]
    r = detect_degradation(series)
    assert r is not None
    assert r.degrading is False
    assert r.score_slope < 0


def test_to_dict_keys():
    series = [make_metric(MetricStatus.OK)] * 5
    r = detect_degradation(series)
    d = r.to_dict()
    for key in ("pipeline", "metric_name", "score_slope", "degrading", "sample_count", "latest_status"):
        assert key in d


# --- detect_all_degradations ---

def test_detect_all_returns_results_for_each_key():
    history = {
        "pipe/rows": [make_metric(MetricStatus.OK)] * 5,
        "pipe/errors": [
            make_metric(MetricStatus.OK),
            make_metric(MetricStatus.WARNING),
            make_metric(MetricStatus.CRITICAL),
            make_metric(MetricStatus.CRITICAL),
        ],
    }
    results = detect_all_degradations(history)
    assert len(results) == 2


def test_detect_all_skips_short_series():
    history = {
        "pipe/rows": [make_metric(MetricStatus.WARNING)] * 2,
    }
    results = detect_all_degradations(history, min_samples=4)
    assert results == []
