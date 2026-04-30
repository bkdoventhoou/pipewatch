"""Tests for pipewatch.dispersion."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.dispersion import (
    DispersionResult,
    analyze_all_dispersions,
    analyze_dispersion,
)
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(
    value: float,
    pipeline: str = "pipe_a",
    name: str = "row_count",
    status: MetricStatus = MetricStatus.OK,
    ts: float = 0.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=ts or datetime.now(timezone.utc).timestamp(),
    )


def test_analyze_dispersion_returns_none_on_empty():
    assert analyze_dispersion([]) is None


def test_analyze_dispersion_returns_none_on_single_metric():
    assert analyze_dispersion([make_metric(10.0)]) is None


def test_analyze_dispersion_stable_series_low_cv():
    metrics = [make_metric(float(v)) for v in [100, 101, 99, 100, 100]]
    result = analyze_dispersion(metrics, cv_threshold=0.5)
    assert result is not None
    assert result.is_high is False
    assert result.cv is not None
    assert result.cv < 0.5


def test_analyze_dispersion_volatile_series_high_cv():
    # values vary wildly relative to mean
    metrics = [make_metric(float(v)) for v in [1, 100, 1, 100, 1]]
    result = analyze_dispersion(metrics, cv_threshold=0.5)
    assert result is not None
    assert result.is_high is True
    assert result.cv is not None and result.cv > 0.5


def test_analyze_dispersion_zero_mean_cv_is_none():
    metrics = [make_metric(0.0), make_metric(0.0), make_metric(0.0)]
    result = analyze_dispersion(metrics)
    assert result is not None
    assert result.cv is None
    assert result.is_high is False


def test_analyze_dispersion_range_correct():
    metrics = [make_metric(float(v)) for v in [2, 5, 8]]
    result = analyze_dispersion(metrics)
    assert result is not None
    assert abs(result.range_ - 6.0) < 1e-9


def test_analyze_dispersion_count_matches():
    metrics = [make_metric(float(v)) for v in range(7)]
    result = analyze_dispersion(metrics)
    assert result is not None
    assert result.count == 7


def test_to_dict_keys():
    metrics = [make_metric(float(v)) for v in [10, 20, 30]]
    result = analyze_dispersion(metrics)
    assert result is not None
    d = result.to_dict()
    for key in ("pipeline", "metric_name", "count", "mean", "variance",
                "std_dev", "range", "cv", "is_high", "cv_threshold"):
        assert key in d, f"Missing key: {key}"


def test_analyze_all_dispersions_groups_by_key():
    history = {
        "pipe_a:row_count": [make_metric(float(v), "pipe_a", "row_count") for v in [10, 20]],
        "pipe_b:error_rate": [make_metric(float(v), "pipe_b", "error_rate") for v in [1, 2]],
    }
    results = analyze_all_dispersions(history)
    assert len(results) == 2
    pipelines = {r.pipeline for r in results}
    assert pipelines == {"pipe_a", "pipe_b"}


def test_analyze_all_dispersions_skips_single_entry_series():
    history = {
        "pipe_a:row_count": [make_metric(10.0)],
        "pipe_b:error_rate": [make_metric(1.0), make_metric(2.0)],
    }
    results = analyze_all_dispersions(history)
    assert len(results) == 1
    assert results[0].pipeline == "pipe_b"
