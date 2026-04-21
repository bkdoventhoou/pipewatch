"""Tests for pipewatch.rollup."""

import time
import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.rollup import RollupBucket, rollup_metrics


def make_metric(
    pipeline: str = "etl",
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


def test_rollup_empty_returns_empty():
    assert rollup_metrics([]) == []


def test_rollup_single_metric():
    m = make_metric(value=42.0, timestamp=1000.0)
    buckets = rollup_metrics([m], window_seconds=60.0)
    assert len(buckets) == 1
    b = buckets[0]
    assert b.pipeline == "etl"
    assert b.metric_name == "row_count"
    assert b.count == 1
    assert b.average == 42.0
    assert b.min_value == 42.0
    assert b.max_value == 42.0
    assert b.ok_count == 1


def test_rollup_two_metrics_same_window():
    m1 = make_metric(value=10.0, timestamp=1000.0)
    m2 = make_metric(value=30.0, timestamp=1050.0)
    buckets = rollup_metrics([m1, m2], window_seconds=60.0)
    assert len(buckets) == 1
    b = buckets[0]
    assert b.count == 2
    assert b.average == 20.0
    assert b.min_value == 10.0
    assert b.max_value == 30.0


def test_rollup_metrics_in_different_windows():
    m1 = make_metric(value=5.0, timestamp=0.0)
    m2 = make_metric(value=15.0, timestamp=60.0)
    buckets = rollup_metrics([m1, m2], window_seconds=60.0)
    assert len(buckets) == 2
    assert buckets[0].count == 1
    assert buckets[1].count == 1


def test_rollup_status_counts():
    metrics = [
        make_metric(value=1.0, status=MetricStatus.OK, timestamp=0.0),
        make_metric(value=2.0, status=MetricStatus.WARNING, timestamp=10.0),
        make_metric(value=3.0, status=MetricStatus.CRITICAL, timestamp=20.0),
    ]
    buckets = rollup_metrics(metrics, window_seconds=60.0)
    assert len(buckets) == 1
    b = buckets[0]
    assert b.ok_count == 1
    assert b.warning_count == 1
    assert b.critical_count == 1


def test_rollup_groups_by_pipeline_and_name():
    m1 = make_metric(pipeline="pipe_a", name="rows", value=1.0, timestamp=0.0)
    m2 = make_metric(pipeline="pipe_b", name="rows", value=2.0, timestamp=0.0)
    m3 = make_metric(pipeline="pipe_a", name="errors", value=3.0, timestamp=0.0)
    buckets = rollup_metrics([m1, m2, m3], window_seconds=60.0)
    assert len(buckets) == 3
    keys = {(b.pipeline, b.metric_name) for b in buckets}
    assert ("pipe_a", "rows") in keys
    assert ("pipe_b", "rows") in keys
    assert ("pipe_a", "errors") in keys


def test_rollup_to_dict_keys():
    m = make_metric(value=7.0, timestamp=120.0)
    b = rollup_metrics([m], window_seconds=60.0)[0]
    d = b.to_dict()
    for key in ("pipeline", "metric_name", "window_start", "window_end",
                "count", "average", "min", "max",
                "ok_count", "warning_count", "critical_count"):
        assert key in d


def test_rollup_average_none_on_empty_bucket():
    b = RollupBucket(
        pipeline="p", metric_name="m",
        window_start=0.0, window_end=60.0
    )
    assert b.average is None
    assert b.to_dict()["average"] is None
    assert b.to_dict()["min"] is None
    assert b.to_dict()["max"] is None
