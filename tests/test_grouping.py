"""Tests for pipewatch.grouping."""

import pytest
from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.grouping import (
    MetricGroup,
    group_by,
    group_by_pipeline,
    group_by_metric_name,
)


def make_metric(pipeline: str, name: str, value: float, status: MetricStatus) -> PipelineMetric:
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status)


def test_group_by_pipeline_creates_correct_keys():
    metrics = [
        make_metric("pipe_a", "rows", 100.0, MetricStatus.OK),
        make_metric("pipe_b", "rows", 50.0, MetricStatus.WARNING),
        make_metric("pipe_a", "latency", 0.5, MetricStatus.OK),
    ]
    groups = group_by_pipeline(metrics)
    assert set(groups.keys()) == {"pipe_a", "pipe_b"}


def test_group_by_pipeline_counts_correctly():
    metrics = [
        make_metric("pipe_a", "rows", 100.0, MetricStatus.OK),
        make_metric("pipe_a", "latency", 0.5, MetricStatus.CRITICAL),
        make_metric("pipe_b", "rows", 50.0, MetricStatus.WARNING),
    ]
    groups = group_by_pipeline(metrics)
    assert groups["pipe_a"].count == 2
    assert groups["pipe_b"].count == 1


def test_group_health_critical_takes_priority():
    metrics = [
        make_metric("pipe_a", "rows", 10.0, MetricStatus.OK),
        make_metric("pipe_a", "latency", 99.0, MetricStatus.CRITICAL),
    ]
    groups = group_by_pipeline(metrics)
    assert groups["pipe_a"].health == "critical"


def test_group_health_warning_when_no_critical():
    metrics = [
        make_metric("pipe_a", "rows", 10.0, MetricStatus.OK),
        make_metric("pipe_a", "latency", 5.0, MetricStatus.WARNING),
    ]
    groups = group_by_pipeline(metrics)
    assert groups["pipe_a"].health == "warning"


def test_group_health_ok_when_all_ok():
    metrics = [
        make_metric("pipe_a", "rows", 10.0, MetricStatus.OK),
        make_metric("pipe_a", "latency", 5.0, MetricStatus.OK),
    ]
    groups = group_by_pipeline(metrics)
    assert groups["pipe_a"].health == "ok"


def test_group_avg_value():
    metrics = [
        make_metric("pipe_a", "rows", 100.0, MetricStatus.OK),
        make_metric("pipe_a", "latency", 200.0, MetricStatus.OK),
    ]
    groups = group_by_pipeline(metrics)
    assert groups["pipe_a"].avg_value == pytest.approx(150.0)


def test_group_by_metric_name():
    metrics = [
        make_metric("pipe_a", "rows", 100.0, MetricStatus.OK),
        make_metric("pipe_b", "rows", 80.0, MetricStatus.WARNING),
        make_metric("pipe_a", "latency", 0.5, MetricStatus.OK),
    ]
    groups = group_by_metric_name(metrics)
    assert set(groups.keys()) == {"rows", "latency"}
    assert groups["rows"].count == 2


def test_to_dict_has_expected_keys():
    metrics = [make_metric("pipe_a", "rows", 42.0, MetricStatus.OK)]
    groups = group_by_pipeline(metrics)
    d = groups["pipe_a"].to_dict()
    for key in ("key", "count", "ok", "warning", "critical", "health", "avg_value"):
        assert key in d


def test_group_by_unknown_field_uses_unknown():
    metrics = [make_metric("pipe_a", "rows", 10.0, MetricStatus.OK)]
    groups = group_by(metrics, "nonexistent_field")
    assert "unknown" in groups
