"""Tests for pipewatch.density."""

import time
from typing import List

import pytest

from pipewatch.density import (
    DensityResult,
    analyze_all_densities,
    analyze_density,
)
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(
    pipeline: str = "pipe",
    name: str = "rows",
    value: float = 1.0,
    status: MetricStatus = MetricStatus.OK,
    timestamp: float = 0.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=timestamp,
    )


def make_series(count: int, spacing_seconds: float = 60.0) -> List[PipelineMetric]:
    """Build a series of evenly spaced metrics."""
    base = 1_000_000.0
    return [
        make_metric(timestamp=base + i * spacing_seconds)
        for i in range(count)
    ]


# --- analyze_density ---

def test_analyze_density_returns_none_on_empty():
    assert analyze_density([]) is None


def test_analyze_density_returns_none_on_single_metric():
    assert analyze_density([make_metric(timestamp=0.0)]) is None


def test_analyze_density_returns_none_on_zero_window():
    metrics = [make_metric(timestamp=5.0), make_metric(timestamp=5.0)]
    assert analyze_density(metrics) is None


def test_analyze_density_one_per_minute():
    # 2 points, 60 s apart => 1 point/min over a 60 s window
    series = make_series(count=2, spacing_seconds=60.0)
    result = analyze_density(series, sparse_threshold=1.0)
    assert result is not None
    assert result.total_points == 2
    assert abs(result.points_per_minute - 2.0) < 0.01  # 2 pts / 1 min window
    assert result.window_seconds == pytest.approx(60.0)


def test_analyze_density_sparse_flag_set_when_below_threshold():
    # 2 points, 300 s apart => 2 pts / 5 min = 0.4 pts/min
    series = make_series(count=2, spacing_seconds=300.0)
    result = analyze_density(series, sparse_threshold=1.0)
    assert result is not None
    assert result.is_sparse is True


def test_analyze_density_not_sparse_when_above_threshold():
    # 6 points, 10 s apart => 6 pts / (50 s / 60) = ~7.2 pts/min
    series = make_series(count=6, spacing_seconds=10.0)
    result = analyze_density(series, sparse_threshold=1.0)
    assert result is not None
    assert result.is_sparse is False


def test_analyze_density_to_dict_keys():
    series = make_series(count=3, spacing_seconds=30.0)
    result = analyze_density(series)
    assert result is not None
    d = result.to_dict()
    expected_keys = {
        "pipeline", "metric_name", "total_points",
        "window_seconds", "points_per_minute", "is_sparse", "sparse_threshold",
    }
    assert expected_keys == set(d.keys())


# --- analyze_all_densities ---

def test_analyze_all_densities_empty_history():
    assert analyze_all_densities({}) == []


def test_analyze_all_densities_skips_single_point_series():
    history = {"pipe:rows": [make_metric(timestamp=0.0)]}
    assert analyze_all_densities(history) == []


def test_analyze_all_densities_returns_result_per_series():
    history = {
        "pipe:rows": make_series(count=3, spacing_seconds=60.0),
        "pipe:errors": make_series(count=4, spacing_seconds=30.0),
    }
    results = analyze_all_densities(history)
    assert len(results) == 2
