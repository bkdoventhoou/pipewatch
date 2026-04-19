"""Tests for pipewatch.filter module."""

import pytest
from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.filter import (
    filter_by_status,
    filter_by_pipeline,
    filter_by_metric_name,
    apply_filters,
)


def make_metric(pipeline="pipe_a", name="row_count", value=100.0, status=MetricStatus.OK):
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status)


METRICS = [
    make_metric(pipeline="pipe_a", name="row_count", value=100, status=MetricStatus.OK),
    make_metric(pipeline="pipe_a", name="error_rate", value=0.5, status=MetricStatus.WARNING),
    make_metric(pipeline="pipe_b", name="row_count", value=0, status=MetricStatus.CRITICAL),
    make_metric(pipeline="pipe_b", name="latency", value=30, status=MetricStatus.OK),
]


def test_filter_by_status_ok():
    result = filter_by_status(METRICS, [MetricStatus.OK])
    assert all(m.status == MetricStatus.OK for m in result)
    assert len(result) == 2


def test_filter_by_status_multiple():
    result = filter_by_status(METRICS, [MetricStatus.WARNING, MetricStatus.CRITICAL])
    assert len(result) == 2


def test_filter_by_pipeline():
    result = filter_by_pipeline(METRICS, "pipe_a")
    assert len(result) == 2
    assert all(m.pipeline == "pipe_a" for m in result)


def test_filter_by_pipeline_no_match():
    result = filter_by_pipeline(METRICS, "pipe_z")
    assert result == []


def test_filter_by_metric_name():
    result = filter_by_metric_name(METRICS, "row_count")
    assert len(result) == 2
    assert all(m.name == "row_count" for m in result)


def test_apply_filters_pipeline_and_status():
    result = apply_filters(METRICS, pipeline="pipe_a", statuses=[MetricStatus.WARNING])
    assert len(result) == 1
    assert result[0].name == "error_rate"


def test_apply_filters_name_only():
    result = apply_filters(METRICS, name="latency")
    assert len(result) == 1
    assert result[0].pipeline == "pipe_b"


def test_apply_filters_no_filters_returns_all():
    result = apply_filters(METRICS)
    assert len(result) == len(METRICS)


def test_apply_filters_all_three():
    result = apply_filters(
        METRICS,
        pipeline="pipe_b",
        name="row_count",
        statuses=[MetricStatus.CRITICAL],
    )
    assert len(result) == 1
    assert result[0].value == 0
