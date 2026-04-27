"""Tests for pipewatch.normalization."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.normalization import (
    normalize_minmax,
    normalize_zscore,
    normalize_metrics,
    NormalizedMetric,
)


def make_metric(pipeline: str, name: str, value: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


# --- minmax ---

def test_minmax_empty_returns_empty():
    assert normalize_minmax([]) == []


def test_minmax_single_value_is_zero():
    m = make_metric("p", "rows", 42.0)
    result = normalize_minmax([m])
    assert len(result) == 1
    assert result[0].normalized_value == 0.0
    assert result[0].strategy == "minmax"


def test_minmax_two_values_span():
    metrics = [
        make_metric("p", "rows", 0.0),
        make_metric("p", "rows", 100.0),
    ]
    result = normalize_minmax(metrics)
    assert result[0].normalized_value == pytest.approx(0.0)
    assert result[1].normalized_value == pytest.approx(1.0)


def test_minmax_preserves_pipeline_and_name():
    m = make_metric("etl", "latency", 5.0)
    result = normalize_minmax([m])
    assert result[0].pipeline == "etl"
    assert result[0].metric_name == "latency"
    assert result[0].original_value == 5.0


def test_minmax_three_values_midpoint():
    metrics = [
        make_metric("p", "x", 0.0),
        make_metric("p", "x", 50.0),
        make_metric("p", "x", 100.0),
    ]
    result = normalize_minmax(metrics)
    assert result[1].normalized_value == pytest.approx(0.5)


# --- zscore ---

def test_zscore_empty_returns_empty():
    assert normalize_zscore([]) == []


def test_zscore_all_same_values_gives_zero():
    metrics = [make_metric("p", "x", 7.0) for _ in range(4)]
    result = normalize_zscore(metrics)
    assert all(r.normalized_value == pytest.approx(0.0) for r in result)


def test_zscore_known_values():
    # values: 2, 4, 4, 4, 5, 5, 7, 9  mean=5, std=2
    vals = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
    metrics = [make_metric("p", "x", v) for v in vals]
    result = normalize_zscore(metrics)
    assert result[0].strategy == "zscore"
    # z for 2 = (2-5)/2 = -1.5
    assert result[0].normalized_value == pytest.approx(-1.5, abs=1e-4)
    # z for 9 = (9-5)/2 = 2.0
    assert result[-1].normalized_value == pytest.approx(2.0, abs=1e-4)


# --- dispatch ---

def test_normalize_metrics_defaults_to_minmax():
    metrics = [make_metric("p", "x", 10.0), make_metric("p", "x", 20.0)]
    result = normalize_metrics(metrics)
    assert result[0].strategy == "minmax"


def test_normalize_metrics_zscore_strategy():
    metrics = [make_metric("p", "x", 1.0), make_metric("p", "x", 3.0)]
    result = normalize_metrics(metrics, strategy="zscore")
    assert result[0].strategy == "zscore"


def test_normalize_metrics_unknown_strategy_raises():
    with pytest.raises(ValueError, match="Unknown normalization strategy"):
        normalize_metrics([], strategy="l2")


def test_to_dict_has_expected_keys():
    m = make_metric("pipe", "count", 5.0)
    result = normalize_minmax([m])
    d = result[0].to_dict()
    assert set(d.keys()) == {"pipeline", "metric_name", "original_value", "normalized_value", "strategy"}
