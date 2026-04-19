"""Tests for pipewatch.aggregator."""

from __future__ import annotations

import pytest
from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.aggregator import aggregate_metrics, AggregatedMetric


def make_metric(pipeline="pipe1", name="row_count", value=100.0, status=MetricStatus.OK):
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status)


def test_aggregate_single_metric():
    metrics = [make_metric(value=50.0)]
    result = aggregate_metrics(metrics)
    assert len(result) == 1
    a = result[0]
    assert a.pipeline == "pipe1"
    assert a.metric_name == "row_count"
    assert a.count == 1
    assert a.min_value == 50.0
    assert a.max_value == 50.0
    assert a.mean_value == 50.0
    assert a.median_value == 50.0


def test_aggregate_multiple_values():
    metrics = [
        make_metric(value=10.0),
        make_metric(value=20.0),
        make_metric(value=30.0),
    ]
    result = aggregate_metrics(metrics)
    assert len(result) == 1
    a = result[0]
    assert a.count == 3
    assert a.min_value == 10.0
    assert a.max_value == 30.0
    assert a.mean_value == 20.0
    assert a.median_value == 20.0


def test_aggregate_groups_by_pipeline_and_name():
    metrics = [
        make_metric(pipeline="pipe1", name="rows", value=100.0),
        make_metric(pipeline="pipe2", name="rows", value=200.0),
        make_metric(pipeline="pipe1", name="latency", value=5.0),
    ]
    result = aggregate_metrics(metrics)
    assert len(result) == 3
    pipelines = {(a.pipeline, a.metric_name) for a in result}
    assert ("pipe1", "rows") in pipelines
    assert ("pipe2", "rows") in pipelines
    assert ("pipe1", "latency") in pipelines


def test_aggregate_status_counts():
    metrics = [
        make_metric(value=10.0, status=MetricStatus.OK),
        make_metric(value=20.0, status=MetricStatus.WARNING),
        make_metric(value=30.0, status=MetricStatus.WARNING),
        make_metric(value=40.0, status=MetricStatus.CRITICAL),
    ]
    result = aggregate_metrics(metrics)
    assert len(result) == 1
    statuses = result[0].statuses
    assert statuses.get("ok") == 1
    assert statuses.get("warning") == 2
    assert statuses.get("critical") == 1


def test_aggregate_empty_returns_empty():
    result = aggregate_metrics([])
    assert result == []


def test_to_dict_keys():
    metrics = [make_metric(value=42.0)]
    a = aggregate_metrics(metrics)[0]
    d = a.to_dict()
    for key in ("pipeline", "metric_name", "count", "min", "max", "mean", "median", "statuses"):
        assert key in d


def test_aggregate_even_count_median():
    """Median of an even number of values should be the average of the two middle values."""
    metrics = [
        make_metric(value=10.0),
        make_metric(value=20.0),
        make_metric(value=30.0),
        make_metric(value=40.0),
    ]
    result = aggregate_metrics(metrics)
    assert len(result) == 1
    assert result[0].median_value == 25.0
