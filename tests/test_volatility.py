"""Tests for pipewatch.volatility."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.volatility import (
    VolatilityResult,
    analyze_all_volatility,
    analyze_volatility,
)
from pipewatch.collector import MetricCollector


def make_metric(pipeline: str, name: str, value: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime.now(tz=timezone.utc),
    )


def make_series(pipeline: str, name: str, values: List[float]) -> List[PipelineMetric]:
    return [make_metric(pipeline, name, v) for v in values]


def test_returns_none_on_too_few_samples():
    series = make_series("p", "m", [1.0, 2.0, 3.0])
    result = analyze_volatility(series, min_samples=4)
    assert result is None


def test_returns_none_on_zero_mean():
    series = make_series("p", "m", [0.0, 0.0, 0.0, 0.0])
    result = analyze_volatility(series, min_samples=4)
    assert result is None


def test_stable_series_not_volatile():
    # All same value → std=0, cv=0
    series = make_series("p", "latency", [10.0, 10.0, 10.0, 10.0, 10.0])
    result = analyze_volatility(series, threshold_cv=0.5, min_samples=4)
    assert result is not None
    assert result.is_volatile is False
    assert result.coefficient_of_variation == pytest.approx(0.0)
    assert result.mean == pytest.approx(10.0)


def test_erratic_series_is_volatile():
    # High spread relative to mean
    series = make_series("p", "latency", [1.0, 100.0, 1.0, 100.0, 1.0, 100.0])
    result = analyze_volatility(series, threshold_cv=0.5, min_samples=4)
    assert result is not None
    assert result.is_volatile is True
    assert result.coefficient_of_variation > 0.5


def test_result_fields_are_correct():
    series = make_series("etl", "row_count", [4.0, 6.0, 4.0, 6.0])
    result = analyze_volatility(series, threshold_cv=0.5, min_samples=4)
    assert result is not None
    assert result.pipeline == "etl"
    assert result.metric_name == "row_count"
    assert result.sample_count == 4
    assert result.mean == pytest.approx(5.0)
    assert result.threshold_cv == 0.5


def test_to_dict_keys():
    series = make_series("etl", "row_count", [4.0, 6.0, 4.0, 6.0])
    result = analyze_volatility(series, min_samples=4)
    assert result is not None
    d = result.to_dict()
    for key in ("pipeline", "metric_name", "sample_count", "mean", "std_dev",
                "coefficient_of_variation", "is_volatile", "threshold_cv"):
        assert key in d


def test_analyze_all_volatility_skips_insufficient():
    collector = MetricCollector()
    collector.record(make_metric("p", "m", 1.0))
    collector.record(make_metric("p", "m", 2.0))  # only 2 samples
    results = analyze_all_volatility(collector, min_samples=4)
    assert results == {}


def test_analyze_all_volatility_returns_result():
    collector = MetricCollector()
    for v in [10.0, 10.0, 10.0, 10.0, 10.0]:
        collector.record(make_metric("pipe", "metric", v))
    results = analyze_all_volatility(collector, min_samples=4)
    assert len(results) == 1
    key = next(iter(results))
    assert results[key].pipeline == "pipe"
