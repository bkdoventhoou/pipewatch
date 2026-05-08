"""Tests for pipewatch.clipping."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from pipewatch.clipping import ClippingResult, detect_clipping, detect_all_clippings
from pipewatch.metrics import PipelineMetric, MetricStatus


def make_metric(pipeline: str, name: str, value: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=0.0,
    )


def test_detect_clipping_returns_none_on_empty():
    assert detect_clipping([], low_bound=0.0, high_bound=100.0) is None


def test_detect_clipping_invalid_bounds_raises():
    metrics = [make_metric("p", "m", 50.0)]
    with pytest.raises(ValueError):
        detect_clipping(metrics, low_bound=100.0, high_bound=0.0)


def test_detect_clipping_no_clips():
    metrics = [make_metric("p", "m", v) for v in [10.0, 50.0, 90.0]]
    result = detect_clipping(metrics, low_bound=0.0, high_bound=100.0)
    assert result is not None
    assert result.clipped_low == 0
    assert result.clipped_high == 0
    assert result.total == 3


def test_detect_clipping_counts_low_clips():
    metrics = [make_metric("p", "m", v) for v in [0.0, 0.0, 50.0]]
    result = detect_clipping(metrics, low_bound=0.0, high_bound=100.0)
    assert result.clipped_low == 2
    assert result.clipped_high == 0


def test_detect_clipping_counts_high_clips():
    metrics = [make_metric("p", "m", v) for v in [50.0, 100.0, 100.0]]
    result = detect_clipping(metrics, low_bound=0.0, high_bound=100.0)
    assert result.clipped_low == 0
    assert result.clipped_high == 2


def test_detect_clipping_clip_rate():
    metrics = [make_metric("p", "m", v) for v in [0.0, 50.0, 100.0, 75.0]]
    result = detect_clipping(metrics, low_bound=0.0, high_bound=100.0)
    assert result.to_dict()["clip_rate"] == 0.5


def test_detect_clipping_to_dict_keys():
    metrics = [make_metric("pipe", "metric", 42.0)]
    result = detect_clipping(metrics, low_bound=0.0, high_bound=100.0)
    d = result.to_dict()
    for key in ("pipeline", "metric_name", "clipped_low", "clipped_high", "total", "low_bound", "high_bound", "clip_rate"):
        assert key in d


def test_detect_all_clippings_uses_collector():
    m1 = make_metric("p1", "rows", 0.0)
    m2 = make_metric("p2", "latency", 50.0)
    collector = MagicMock()
    collector.get_history.return_value = {
        "p1:rows": [m1],
        "p2:latency": [m2],
    }
    results = detect_all_clippings(collector, low_bound=0.0, high_bound=100.0)
    assert "p1:rows" in results
    assert "p2:latency" in results
    assert results["p1:rows"].clipped_low == 1
    assert results["p2:latency"].clipped_low == 0
