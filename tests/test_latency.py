"""Tests for pipewatch.latency — latency gap analysis."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.latency import LatencyResult, analyze_latency, analyze_all_latencies
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(
    pipeline: str = "pipe_a",
    name: str = "row_count",
    value: float = 100.0,
    status: MetricStatus = MetricStatus.OK,
    timestamp: float = 0.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=timestamp,
    )


def test_analyze_latency_returns_none_on_single_metric():
    metrics = [make_metric(timestamp=1000.0)]
    result = analyze_latency("pipe_a", "row_count", metrics)
    assert result is None


def test_analyze_latency_returns_none_on_empty():
    result = analyze_latency("pipe_a", "row_count", [])
    assert result is None


def test_analyze_latency_two_metrics_gap():
    metrics = [
        make_metric(timestamp=1000.0),
        make_metric(timestamp=1060.0),
    ]
    result = analyze_latency("pipe_a", "row_count", metrics)
    assert result is not None
    assert result.min_gap == pytest.approx(60.0)
    assert result.max_gap == pytest.approx(60.0)
    assert result.avg_gap == pytest.approx(60.0)
    assert result.sample_count == 1


def test_analyze_latency_multiple_gaps():
    metrics = [
        make_metric(timestamp=0.0),
        make_metric(timestamp=10.0),
        make_metric(timestamp=40.0),
    ]
    result = analyze_latency("pipe_a", "row_count", metrics)
    assert result is not None
    assert result.min_gap == pytest.approx(10.0)
    assert result.max_gap == pytest.approx(30.0)
    assert result.avg_gap == pytest.approx(20.0)
    assert result.sample_count == 2


def test_analyze_latency_filters_by_pipeline_and_name():
    metrics = [
        make_metric(pipeline="pipe_a", name="row_count", timestamp=0.0),
        make_metric(pipeline="pipe_b", name="row_count", timestamp=5.0),
        make_metric(pipeline="pipe_a", name="row_count", timestamp=20.0),
    ]
    result = analyze_latency("pipe_a", "row_count", metrics)
    assert result is not None
    assert result.sample_count == 1
    assert result.avg_gap == pytest.approx(20.0)


def test_to_dict_keys():
    result = LatencyResult(
        pipeline="pipe_a",
        metric_name="row_count",
        min_gap=5.0,
        max_gap=15.0,
        avg_gap=10.0,
        sample_count=2,
    )
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline", "metric_name",
        "min_gap_seconds", "max_gap_seconds",
        "avg_gap_seconds", "sample_count",
    }


def test_analyze_all_latencies_groups_by_pipeline_and_name():
    history = {
        "pipe_a:row_count": [
            make_metric(pipeline="pipe_a", name="row_count", timestamp=0.0),
            make_metric(pipeline="pipe_a", name="row_count", timestamp=30.0),
        ],
        "pipe_b:error_rate": [
            make_metric(pipeline="pipe_b", name="error_rate", timestamp=0.0),
            make_metric(pipeline="pipe_b", name="error_rate", timestamp=60.0),
        ],
    }
    results = analyze_all_latencies(history)
    assert len(results) == 2
    pipelines = {r.pipeline for r in results}
    assert pipelines == {"pipe_a", "pipe_b"}


def test_analyze_all_latencies_skips_single_entry_series():
    history = {
        "pipe_a:row_count": [
            make_metric(pipeline="pipe_a", name="row_count", timestamp=0.0),
        ],
    }
    results = analyze_all_latencies(history)
    assert results == []
