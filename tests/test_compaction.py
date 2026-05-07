"""Tests for pipewatch.compaction."""

import time
import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.compaction import compact_series, compact_all, CompactedRun


def make_metric(pipeline, name, value, status, ts=None):
    m = PipelineMetric(pipeline=pipeline, name=name, value=value)
    m.status = status
    m.timestamp = ts if ts is not None else time.time()
    return m


def test_compact_series_empty_returns_empty():
    assert compact_series([]) == []


def test_compact_series_single_metric():
    m = make_metric("p", "rows", 10.0, MetricStatus.OK, ts=100.0)
    runs = compact_series([m])
    assert len(runs) == 1
    assert runs[0].count == 1
    assert runs[0].status == MetricStatus.OK
    assert runs[0].avg_value == 10.0


def test_compact_series_same_status_merged():
    metrics = [
        make_metric("p", "rows", 10.0, MetricStatus.OK, ts=1.0),
        make_metric("p", "rows", 20.0, MetricStatus.OK, ts=2.0),
        make_metric("p", "rows", 30.0, MetricStatus.OK, ts=3.0),
    ]
    runs = compact_series(metrics)
    assert len(runs) == 1
    assert runs[0].count == 3
    assert runs[0].avg_value == pytest.approx(20.0)
    assert runs[0].start_ts == 1.0
    assert runs[0].end_ts == 3.0


def test_compact_series_status_change_creates_new_run():
    metrics = [
        make_metric("p", "rows", 10.0, MetricStatus.OK, ts=1.0),
        make_metric("p", "rows", 50.0, MetricStatus.WARNING, ts=2.0),
        make_metric("p", "rows", 90.0, MetricStatus.CRITICAL, ts=3.0),
    ]
    runs = compact_series(metrics)
    assert len(runs) == 3
    assert [r.status for r in runs] == [
        MetricStatus.OK, MetricStatus.WARNING, MetricStatus.CRITICAL
    ]


def test_compact_series_alternating_creates_many_runs():
    statuses = [MetricStatus.OK, MetricStatus.WARNING] * 4
    metrics = [
        make_metric("p", "x", float(i), s, ts=float(i))
        for i, s in enumerate(statuses)
    ]
    runs = compact_series(metrics)
    assert len(runs) == 8


def test_compact_series_to_dict_keys():
    m = make_metric("pipe", "lag", 5.0, MetricStatus.OK, ts=42.0)
    run = compact_series([m])[0]
    d = run.to_dict()
    for key in ("pipeline", "metric_name", "status", "start_ts", "end_ts", "count", "avg_value"):
        assert key in d


def test_compact_all_groups_by_key():
    history = {
        ("pipe_a", "rows"): [
            make_metric("pipe_a", "rows", 1.0, MetricStatus.OK, ts=1.0),
            make_metric("pipe_a", "rows", 2.0, MetricStatus.OK, ts=2.0),
        ],
        ("pipe_b", "lag"): [
            make_metric("pipe_b", "lag", 9.0, MetricStatus.CRITICAL, ts=1.0),
        ],
    }
    result = compact_all(history)
    assert ("pipe_a", "rows") in result
    assert ("pipe_b", "lag") in result
    assert result[("pipe_a", "rows")][0].count == 2
    assert result[("pipe_b", "lag")][0].status == MetricStatus.CRITICAL


def test_compact_all_empty_history():
    assert compact_all({}) == {}
