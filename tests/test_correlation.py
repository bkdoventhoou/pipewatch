"""Tests for pipewatch.correlation."""
import pytest
from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.correlation import correlate_metrics, correlate_all, _pearson
from datetime import datetime


def make_metric(pipeline, name, value):
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime.utcnow(),
    )


def test_pearson_perfect_positive():
    assert abs(_pearson([1, 2, 3], [2, 4, 6]) - 1.0) < 1e-9


def test_pearson_perfect_negative():
    assert abs(_pearson([1, 2, 3], [6, 4, 2]) + 1.0) < 1e-9


def test_pearson_zero_std_returns_none():
    assert _pearson([1, 1, 1], [1, 2, 3]) is None


def test_pearson_too_few_returns_none():
    assert _pearson([1], [1]) is None


def test_correlate_metrics_returns_result():
    metrics = [
        make_metric("pipe", "a", 1),
        make_metric("pipe", "a", 2),
        make_metric("pipe", "a", 3),
        make_metric("pipe", "b", 2),
        make_metric("pipe", "b", 4),
        make_metric("pipe", "b", 6),
    ]
    r = correlate_metrics(metrics, "pipe", "a", "b")
    assert r is not None
    assert abs(r.coefficient - 1.0) < 1e-9
    assert r.sample_size == 3


def test_correlate_metrics_insufficient_data():
    metrics = [make_metric("pipe", "a", 1)]
    r = correlate_metrics(metrics, "pipe", "a", "b")
    assert r is None


def test_correlate_metrics_wrong_pipeline():
    metrics = [
        make_metric("other", "a", 1),
        make_metric("other", "b", 2),
    ]
    r = correlate_metrics(metrics, "pipe", "a", "b")
    assert r is None


def test_correlate_all_returns_pairs():
    metrics = []
    for v in [1, 2, 3]:
        metrics.append(make_metric("pipe", "x", v))
        metrics.append(make_metric("pipe", "y", v * 2))
        metrics.append(make_metric("pipe", "z", v * 3))
    results = correlate_all(metrics, "pipe")
    assert len(results) == 3
    pairs = {(r.metric_a, r.metric_b) for r in results}
    assert len(pairs) == 3


def test_correlate_all_empty():
    assert correlate_all([], "pipe") == []
