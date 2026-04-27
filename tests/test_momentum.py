"""Tests for pipewatch.momentum."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.momentum import (
    MomentumResult,
    _slope,
    analyze_all_momentum,
    analyze_momentum,
)


def make_metric(
    value: float,
    ts: float,
    pipeline: str = "pipe",
    name: str = "rows",
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=ts,
    )


# ---------------------------------------------------------------------------
# _slope helper
# ---------------------------------------------------------------------------

def test_slope_flat():
    assert _slope([1, 2, 3], [5, 5, 5]) == pytest.approx(0.0)


def test_slope_rising():
    s = _slope([0, 1, 2], [0, 1, 2])
    assert s == pytest.approx(1.0)


def test_slope_single_point_returns_zero():
    assert _slope([1], [1]) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# analyze_momentum
# ---------------------------------------------------------------------------

def test_returns_none_on_too_few_samples():
    metrics = [make_metric(1.0, float(i)) for i in range(2)]
    assert analyze_momentum(metrics) is None


def test_returns_result_with_three_samples():
    metrics = [make_metric(float(i), float(i)) for i in range(3)]
    result = analyze_momentum(metrics)
    assert isinstance(result, MomentumResult)
    assert result.samples == 3


def test_stable_series_not_accelerating():
    # constant value -> both derivatives ≈ 0
    metrics = [make_metric(10.0, float(i)) for i in range(5)]
    result = analyze_momentum(metrics, accel_threshold=0.01)
    assert result is not None
    assert not result.accelerating
    assert result.second_derivative == pytest.approx(0.0, abs=1e-9)


def test_linearly_increasing_not_accelerating():
    # v(t) = t => first_d = 1, second_d ≈ 0
    metrics = [make_metric(float(i), float(i)) for i in range(6)]
    result = analyze_momentum(metrics, accel_threshold=0.01)
    assert result is not None
    assert result.first_derivative == pytest.approx(1.0, rel=1e-3)
    assert not result.accelerating


def test_quadratic_series_accelerating():
    # v(t) = t^2 -> velocity grows linearly -> second_d > 0
    metrics = [make_metric(float(i ** 2), float(i)) for i in range(6)]
    result = analyze_momentum(metrics, accel_threshold=0.01)
    assert result is not None
    assert result.accelerating


def test_to_dict_keys():
    metrics = [make_metric(float(i ** 2), float(i)) for i in range(5)]
    d = analyze_momentum(metrics).to_dict()
    for key in ("pipeline", "metric_name", "first_derivative", "second_derivative", "accelerating", "samples"):
        assert key in d


# ---------------------------------------------------------------------------
# analyze_all_momentum
# ---------------------------------------------------------------------------

def test_analyze_all_skips_short_series():
    history = {
        "pipe/rows": [make_metric(1.0, float(i)) for i in range(2)],
        "pipe/lag": [make_metric(float(i ** 2), float(i)) for i in range(5)],
    }
    results = analyze_all_momentum(history)
    assert len(results) == 1
    assert results[0].metric_name == "lag"


def test_analyze_all_returns_all_valid():
    history = {
        f"pipe/m{j}": [make_metric(float(i), float(i)) for i in range(4)]
        for j in range(3)
    }
    results = analyze_all_momentum(history)
    assert len(results) == 3
