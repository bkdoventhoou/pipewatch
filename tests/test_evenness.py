"""Tests for pipewatch.evenness."""
from __future__ import annotations

import math
from unittest.mock import MagicMock

import pytest

from pipewatch.evenness import (
    EvennessResult,
    _bin_values,
    _normalised_entropy,
    analyze_all_evenness,
    analyze_evenness,
)
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(pipeline: str, name: str, value: float, ts: float = 0.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=ts,
    )


# ---------------------------------------------------------------------------
# _bin_values
# ---------------------------------------------------------------------------

def test_bin_values_all_same():
    counts = _bin_values([5.0, 5.0, 5.0], bins=5)
    assert counts[0] == 3
    assert sum(counts) == 3


def test_bin_values_spread():
    values = [float(i) for i in range(10)]
    counts = _bin_values(values, bins=10)
    assert sum(counts) == 10


# ---------------------------------------------------------------------------
# _normalised_entropy
# ---------------------------------------------------------------------------

def test_normalised_entropy_uniform():
    counts = [10, 10, 10, 10]  # perfectly uniform
    entropy = _normalised_entropy(counts)
    assert abs(entropy - 1.0) < 1e-9


def test_normalised_entropy_concentrated():
    counts = [100, 0, 0, 0]  # all in one bin
    entropy = _normalised_entropy(counts)
    assert entropy == 0.0


def test_normalised_entropy_empty():
    assert _normalised_entropy([0, 0, 0]) == 0.0


# ---------------------------------------------------------------------------
# analyze_evenness
# ---------------------------------------------------------------------------

def test_analyze_evenness_returns_none_on_too_few():
    metrics = [make_metric("p", "m", float(i)) for i in range(3)]
    result = analyze_evenness(metrics, min_samples=5)
    assert result is None


def test_analyze_evenness_uniform_series():
    # values spread evenly across range → high entropy → not uneven
    metrics = [make_metric("pipe", "rows", float(i), ts=float(i)) for i in range(20)]
    result = analyze_evenness(metrics, entropy_threshold=0.5, bins=5, min_samples=5)
    assert result is not None
    assert result.pipeline == "pipe"
    assert result.metric_name == "rows"
    assert result.sample_count == 20
    assert result.entropy > 0.5
    assert result.is_uneven is False


def test_analyze_evenness_concentrated_series():
    # all values identical → entropy = 0 → uneven
    metrics = [make_metric("pipe", "rows", 42.0, ts=float(i)) for i in range(10)]
    result = analyze_evenness(metrics, entropy_threshold=0.5, bins=5, min_samples=5)
    assert result is not None
    assert result.entropy == 0.0
    assert result.is_uneven is True


def test_analyze_evenness_to_dict_keys():
    metrics = [make_metric("p", "m", float(i)) for i in range(10)]
    result = analyze_evenness(metrics, min_samples=5)
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline", "metric_name", "sample_count",
        "entropy", "is_uneven", "entropy_threshold",
    }


# ---------------------------------------------------------------------------
# analyze_all_evenness
# ---------------------------------------------------------------------------

def test_analyze_all_evenness_skips_short_series():
    collector = MagicMock()
    collector.get_history.return_value = {
        "pipe/rows": [make_metric("pipe", "rows", float(i)) for i in range(2)],
    }
    results = analyze_all_evenness(collector, min_samples=5)
    assert results == {}


def test_analyze_all_evenness_returns_results():
    collector = MagicMock()
    collector.get_history.return_value = {
        "pipe/rows": [make_metric("pipe", "rows", float(i)) for i in range(10)],
        "pipe/lag": [make_metric("pipe", "lag", 1.0) for _ in range(10)],
    }
    results = analyze_all_evenness(collector, min_samples=5)
    assert "pipe/rows" in results
    assert "pipe/lag" in results
    assert results["pipe/lag"].is_uneven is True
