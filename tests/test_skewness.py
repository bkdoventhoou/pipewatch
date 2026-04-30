"""Tests for pipewatch.skewness."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.skewness import (
    SkewnessResult,
    _skewness,
    analyze_all_skewness,
    analyze_skewness,
)


def make_metric(pipeline: str, name: str, value: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime.now(tz=timezone.utc),
    )


# ---------------------------------------------------------------------------
# _skewness helper
# ---------------------------------------------------------------------------

def test_skewness_returns_none_on_too_few():
    assert _skewness([]) is None
    assert _skewness([1.0]) is None
    assert _skewness([1.0, 2.0]) is None


def test_skewness_returns_none_on_zero_std():
    assert _skewness([5.0, 5.0, 5.0, 5.0]) is None


def test_skewness_symmetric_distribution():
    # Perfectly symmetric: [-1, 0, 1]
    result = _skewness([-1.0, 0.0, 1.0])
    assert result is not None
    assert abs(result) < 1e-9


def test_skewness_right_skewed():
    # Long tail to the right
    values = [1.0, 1.0, 1.0, 1.0, 10.0]
    result = _skewness(values)
    assert result is not None
    assert result > 0


def test_skewness_left_skewed():
    values = [-10.0, 1.0, 1.0, 1.0, 1.0]
    result = _skewness(values)
    assert result is not None
    assert result < 0


# ---------------------------------------------------------------------------
# analyze_skewness
# ---------------------------------------------------------------------------

def test_analyze_skewness_returns_none_on_empty():
    assert analyze_skewness([]) is None


def test_analyze_skewness_returns_none_on_too_few():
    metrics = [make_metric("p", "m", v) for v in [1.0, 2.0]]
    assert analyze_skewness(metrics) is None


def test_analyze_skewness_symmetric():
    metrics = [make_metric("pipe", "rows", v) for v in [-1.0, 0.0, 1.0]]
    result = analyze_skewness(metrics)
    assert result is not None
    assert result.interpretation == "symmetric"
    assert result.pipeline == "pipe"
    assert result.metric_name == "rows"
    assert result.sample_count == 3
    assert abs(result.skewness) < 1e-9


def test_analyze_skewness_right():
    metrics = [make_metric("p", "m", v) for v in [1.0, 1.0, 1.0, 1.0, 10.0]]
    result = analyze_skewness(metrics, threshold=0.5)
    assert result is not None
    assert result.interpretation == "right"


def test_analyze_skewness_left():
    metrics = [make_metric("p", "m", v) for v in [-10.0, 1.0, 1.0, 1.0, 1.0]]
    result = analyze_skewness(metrics, threshold=0.5)
    assert result is not None
    assert result.interpretation == "left"


def test_analyze_skewness_to_dict_keys():
    metrics = [make_metric("p", "m", v) for v in [1.0, 2.0, 3.0]]
    result = analyze_skewness(metrics)
    assert result is not None
    d = result.to_dict()
    for key in ("pipeline", "metric_name", "sample_count", "mean", "skewness", "interpretation"):
        assert key in d


# ---------------------------------------------------------------------------
# analyze_all_skewness
# ---------------------------------------------------------------------------

def test_analyze_all_skewness_empty_history():
    assert analyze_all_skewness({}) == []


def test_analyze_all_skewness_skips_insufficient_series():
    history = {
        "p:m": [make_metric("p", "m", v) for v in [1.0, 2.0]],  # too few
    }
    assert analyze_all_skewness(history) == []


def test_analyze_all_skewness_returns_results_for_valid_series():
    history = {
        "p:rows": [make_metric("p", "rows", v) for v in [1.0, 2.0, 3.0]],
        "p:errors": [make_metric("p", "errors", v) for v in [1.0, 1.0, 1.0, 10.0]],
    }
    results = analyze_all_skewness(history)
    assert len(results) == 2
    pipelines = {r.pipeline for r in results}
    assert pipelines == {"p"}
