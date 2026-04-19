"""Tests for pipewatch.trend module."""
import pytest
from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.trend import analyze_trend, analyze_all_trends, TrendResult
from datetime import datetime


def make_metric(pipeline, name, value, status=MetricStatus.OK):
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=datetime.utcnow(),
    )


def test_analyze_trend_returns_none_on_empty():
    assert analyze_trend([]) is None


def test_analyze_trend_single_value_stable():
    metrics = [make_metric("pipe1", "latency", 5.0)]
    result = analyze_trend(metrics)
    assert result.direction == "stable"
    assert result.slope == 0.0


def test_analyze_trend_increasing():
    metrics = [make_metric("pipe1", "latency", float(i)) for i in range(10)]
    result = analyze_trend(metrics, threshold=0.01)
    assert result.direction == "up"
    assert result.slope > 0


def test_analyze_trend_decreasing():
    metrics = [make_metric("pipe1", "latency", float(10 - i)) for i in range(10)]
    result = analyze_trend(metrics, threshold=0.01)
    assert result.direction == "down"
    assert result.slope < 0


def test_analyze_trend_stable():
    metrics = [make_metric("pipe1", "latency", 3.0) for _ in range(5)]
    result = analyze_trend(metrics)
    assert result.direction == "stable"
    assert result.avg == 3.0
    assert result.min == 3.0
    assert result.max == 3.0


def test_trend_result_to_dict():
    metrics = [make_metric("pipe1", "rows", float(i)) for i in range(5)]
    result = analyze_trend(metrics)
    d = result.to_dict()
    assert d["pipeline"] == "pipe1"
    assert d["metric_name"] == "rows"
    assert "direction" in d
    assert "slope" in d
    assert d["sample_count"] == 5


def test_analyze_all_trends_groups_correctly():
    m1 = [make_metric("pipe1", "latency", float(i)) for i in range(5)]
    m2 = [make_metric("pipe2", "rows", float(10 - i)) for i in range(5)]
    history = {
        ("pipe1", "latency"): m1,
        ("pipe2", "rows"): m2,
    }
    results = analyze_all_trends(history)
    assert len(results) == 2
    pipelines = {r.pipeline for r in results}
    assert "pipe1" in pipelines
    assert "pipe2" in pipelines


def test_analyze_all_trends_empty():
    results = analyze_all_trends({})
    assert results == []
