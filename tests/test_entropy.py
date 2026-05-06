"""Tests for pipewatch.entropy."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.entropy import (
    EntropyResult,
    _shannon_entropy,
    analyze_entropy,
    analyze_all_entropies,
)
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(pipeline: str, name: str, value: float, ts: float = 0.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        metric_name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime.fromtimestamp(ts, tz=timezone.utc),
    )


def make_series(values: List[float], pipeline="p", name="m") -> List[PipelineMetric]:
    return [make_metric(pipeline, name, v, float(i)) for i, v in enumerate(values)]


def test_shannon_entropy_empty_returns_none():
    assert _shannon_entropy([]) is None


def test_shannon_entropy_all_same_returns_zero():
    assert _shannon_entropy([5.0, 5.0, 5.0]) == 0.0


def test_shannon_entropy_uniform_is_max():
    # 10 distinct values each in its own bin => max entropy = log2(10)
    values = list(range(10))
    entropy = _shannon_entropy(values, bins=10)
    assert entropy == pytest.approx(math.log2(10), abs=0.01)


def test_analyze_entropy_returns_none_on_single_metric():
    series = make_series([1.0])
    assert analyze_entropy(series) is None


def test_analyze_entropy_returns_none_on_empty():
    assert analyze_entropy([]) is None


def test_analyze_entropy_constant_series_not_high_entropy():
    series = make_series([3.0] * 20)
    result = analyze_entropy(series, threshold=0.75)
    assert result is not None
    assert result.entropy == 0.0
    assert result.normalised == 0.0
    assert result.high_entropy is False


def test_analyze_entropy_uniform_is_high_entropy():
    # spread values evenly across 10 bins
    values = [float(i) for i in range(100)]
    series = make_series(values)
    result = analyze_entropy(series, threshold=0.75, bins=10)
    assert result is not None
    assert result.high_entropy is True
    assert result.normalised > 0.9


def test_analyze_entropy_result_fields():
    series = make_series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = analyze_entropy(series, bins=5)
    assert result.pipeline == "p"
    assert result.metric_name == "m"
    assert result.sample_count == 5
    assert result.max_entropy == pytest.approx(math.log2(5), abs=0.01)


def test_analyze_entropy_to_dict_has_required_keys():
    series = make_series([1.0, 2.0, 3.0])
    result = analyze_entropy(series)
    d = result.to_dict()
    for key in ("pipeline", "metric_name", "entropy", "max_entropy", "normalised", "sample_count", "high_entropy"):
        assert key in d


def test_analyze_all_entropies_groups_by_key():
    history = {
        "p|m": make_series([1.0, 2.0, 3.0], pipeline="p", name="m"),
        "q|n": make_series([10.0] * 10, pipeline="q", name="n"),
    }
    results = analyze_all_entropies(history)
    assert len(results) == 2
    pipelines = {r.pipeline for r in results}
    assert pipelines == {"p", "q"}


def test_analyze_all_entropies_skips_single_metric():
    history = {
        "p|m": make_series([1.0]),
        "q|n": make_series([1.0, 2.0, 3.0], pipeline="q", name="n"),
    }
    results = analyze_all_entropies(history)
    assert len(results) == 1
    assert results[0].pipeline == "q"
