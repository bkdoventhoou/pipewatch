"""Tests for pipewatch.smoothing."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.smoothing import smooth_series, smooth_all, SmoothedSeries


def make_metric(
    value: float,
    pipeline: str = "pipe_a",
    name: str = "row_count",
    ts: float = 0.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=ts,
    )


def test_smooth_series_returns_none_on_empty():
    assert smooth_series([]) is None


def test_smooth_series_single_value():
    metrics = [make_metric(10.0)]
    result = smooth_series(metrics, alpha=0.5)
    assert isinstance(result, SmoothedSeries)
    assert result.smoothed_values == [10.0]
    assert result.raw_values == [10.0]


def test_smooth_series_alpha_one_equals_raw():
    """With alpha=1.0 the EMA equals the raw series."""
    raw = [1.0, 5.0, 3.0, 7.0]
    metrics = [make_metric(v, ts=float(i)) for i, v in enumerate(raw)]
    result = smooth_series(metrics, alpha=1.0)
    assert result is not None
    assert result.smoothed_values == pytest.approx(raw)


def test_smooth_series_smoothing_reduces_variance():
    raw = [1.0, 100.0, 1.0, 100.0]
    metrics = [make_metric(v, ts=float(i)) for i, v in enumerate(raw)]
    result = smooth_series(metrics, alpha=0.2)
    assert result is not None
    smoothed_range = max(result.smoothed_values) - min(result.smoothed_values)
    raw_range = max(raw) - min(raw)
    assert smoothed_range < raw_range


def test_smooth_series_invalid_alpha_raises():
    metrics = [make_metric(1.0)]
    with pytest.raises(ValueError, match="alpha"):
        smooth_series(metrics, alpha=0.0)
    with pytest.raises(ValueError, match="alpha"):
        smooth_series(metrics, alpha=1.5)


def test_smooth_series_preserves_metadata():
    metrics = [make_metric(5.0, pipeline="etl", name="latency")]
    result = smooth_series(metrics, alpha=0.5)
    assert result is not None
    assert result.pipeline == "etl"
    assert result.metric_name == "latency"
    assert result.alpha == 0.5


def test_smooth_series_to_dict_keys():
    metrics = [make_metric(3.0), make_metric(6.0, ts=1.0)]
    result = smooth_series(metrics, alpha=0.3)
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline", "metric_name", "alpha",
        "raw_values", "smoothed_values", "latest_smoothed",
    }
    assert d["latest_smoothed"] == pytest.approx(result.smoothed_values[-1])


def test_smooth_all_returns_one_per_series():
    history = {
        "pipe_a:row_count": [make_metric(1.0), make_metric(2.0, ts=1.0)],
        "pipe_b:errors": [make_metric(0.0, pipeline="pipe_b", name="errors")],
        "pipe_c:empty": [],
    }
    results = smooth_all(history, alpha=0.5)
    assert len(results) == 2
    pipelines = {r.pipeline for r in results}
    assert "pipe_a" in pipelines
    assert "pipe_b" in pipelines
