"""Tests for pipewatch.profiling module."""

from __future__ import annotations

from typing import List

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.profiling import MetricProfile, profile_all, profile_metric


def make_metric(pipeline: str, name: str, value: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=0.0,
    )


def test_profile_metric_returns_none_on_empty():
    assert profile_metric([]) is None


def test_profile_metric_single_value():
    metrics = [make_metric("etl", "row_count", 100.0)]
    result = profile_metric(metrics)
    assert result is not None
    assert result.count == 1
    assert result.mean == 100.0
    assert result.std == 0.0
    assert result.min_val == 100.0
    assert result.max_val == 100.0
    assert result.p50 == 100.0
    assert result.p95 == 100.0


def test_profile_metric_multiple_values():
    vals = [10.0, 20.0, 30.0, 40.0, 50.0]
    metrics = [make_metric("etl", "latency", v) for v in vals]
    result = profile_metric(metrics)
    assert result is not None
    assert result.count == 5
    assert result.mean == pytest.approx(30.0)
    assert result.min_val == 10.0
    assert result.max_val == 50.0
    assert result.p50 == pytest.approx(30.0)
    assert result.p95 == pytest.approx(48.0, abs=0.5)


def test_profile_metric_std_nonzero():
    metrics = [make_metric("p", "m", v) for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]]
    result = profile_metric(metrics)
    assert result is not None
    assert result.std > 0.0


def test_profile_metric_to_dict_keys():
    metrics = [make_metric("pipe", "errors", 5.0)]
    d = profile_metric(metrics).to_dict()
    for key in ("pipeline", "metric_name", "count", "mean", "std", "min", "max", "p50", "p95"):
        assert key in d


def test_profile_all_groups_by_key():
    history = {
        "pipe1::row_count": [make_metric("pipe1", "row_count", v) for v in [1.0, 2.0, 3.0]],
        "pipe2::latency": [make_metric("pipe2", "latency", v) for v in [10.0, 20.0]],
    }
    profiles = profile_all(history)
    assert len(profiles) == 2
    pipelines = {p.pipeline for p in profiles}
    assert "pipe1" in pipelines
    assert "pipe2" in pipelines


def test_profile_all_empty_history():
    assert profile_all({}) == []


def test_profile_all_skips_empty_lists():
    history = {
        "pipe1::m": [],
        "pipe2::m": [make_metric("pipe2", "m", 1.0)],
    }
    profiles = profile_all(history)
    assert len(profiles) == 1
    assert profiles[0].pipeline == "pipe2"
