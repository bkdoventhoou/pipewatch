"""Tests for pipewatch.sampling."""

from __future__ import annotations

import time
from typing import List

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.sampling import (
    SampledSeries,
    sample_all,
    sample_by_count,
    sample_every_nth,
    sample_series,
)


def make_metric(pipeline: str, name: str, value: float, ts: float = 0.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=ts,
    )


def make_series(n: int, pipeline="pipe", name="m") -> List[PipelineMetric]:
    return [make_metric(pipeline, name, float(i), ts=float(i)) for i in range(n)]


def test_sample_every_nth_step1():
    metrics = make_series(6)
    result = sample_every_nth(metrics, 1)
    assert result == metrics


def test_sample_every_nth_step2():
    metrics = make_series(6)
    result = sample_every_nth(metrics, 2)
    assert len(result) == 3
    assert [m.value for m in result] == [0.0, 2.0, 4.0]


def test_sample_every_nth_invalid_raises():
    with pytest.raises(ValueError):
        sample_every_nth([], 0)


def test_sample_by_count_exact():
    metrics = make_series(5)
    result = sample_by_count(metrics, 5)
    assert len(result) == 5


def test_sample_by_count_downsamples():
    metrics = make_series(10)
    result = sample_by_count(metrics, 5)
    assert len(result) == 5


def test_sample_by_count_fewer_than_max():
    metrics = make_series(3)
    result = sample_by_count(metrics, 10)
    assert len(result) == 3


def test_sample_by_count_invalid_raises():
    with pytest.raises(ValueError):
        sample_by_count([], 0)


def test_sample_series_empty():
    series = sample_series([], max_count=5)
    assert isinstance(series, SampledSeries)
    assert series.samples == []


def test_sample_series_uses_every_nth():
    metrics = make_series(8)
    series = sample_series(metrics, every_nth=4)
    assert len(series.samples) == 2
    assert series.pipeline == "pipe"
    assert series.metric_name == "m"


def test_sample_series_uses_max_count():
    metrics = make_series(20)
    series = sample_series(metrics, max_count=4)
    assert len(series.samples) == 4


def test_sample_series_no_strategy_returns_all():
    metrics = make_series(5)
    series = sample_series(metrics)
    assert len(series.samples) == 5


def test_sample_series_to_dict_keys():
    metrics = make_series(3)
    series = sample_series(metrics, max_count=2)
    d = series.to_dict()
    assert "pipeline" in d
    assert "metric_name" in d
    assert "sample_count" in d
    assert "samples" in d
    assert d["sample_count"] == 2


def test_sample_all_groups_by_key():
    history = {
        ("pipe_a", "rows"): make_series(10, pipeline="pipe_a", name="rows"),
        ("pipe_b", "lag"): make_series(10, pipeline="pipe_b", name="lag"),
    }
    results = sample_all(history, max_count=3)
    assert len(results) == 2
    assert all(len(s.samples) == 3 for s in results)


def test_sample_all_skips_empty_series():
    history = {
        ("pipe_a", "rows"): [],
        ("pipe_b", "lag"): make_series(5, pipeline="pipe_b", name="lag"),
    }
    results = sample_all(history, max_count=3)
    assert len(results) == 1
    assert results[0].pipeline == "pipe_b"
