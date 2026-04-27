"""Tests for pipewatch.stickiness."""

from __future__ import annotations

import time
from typing import List

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.stickiness import analyze_stickiness, analyze_all_stickiness


def make_metric(
    pipeline: str = "pipe",
    name: str = "rows",
    value: float = 1.0,
    status: MetricStatus = MetricStatus.OK,
    timestamp: float = None,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=timestamp if timestamp is not None else time.time(),
    )


def make_series(statuses: List[MetricStatus], base_ts: float = 1000.0) -> List[PipelineMetric]:
    return [
        make_metric(status=s, timestamp=base_ts + i * 10)
        for i, s in enumerate(statuses)
    ]


def test_returns_none_on_empty():
    assert analyze_stickiness([]) is None


def test_returns_none_when_latest_is_ok():
    series = make_series([MetricStatus.WARNING, MetricStatus.OK])
    assert analyze_stickiness(series) is None


def test_streak_of_one_warning():
    series = make_series([MetricStatus.OK, MetricStatus.WARNING])
    result = analyze_stickiness(series, streak_threshold=3)
    assert result is not None
    assert result.streak == 1
    assert result.is_stuck is False
    assert result.status == MetricStatus.WARNING


def test_streak_meets_threshold():
    series = make_series(
        [MetricStatus.OK] + [MetricStatus.WARNING] * 3
    )
    result = analyze_stickiness(series, streak_threshold=3)
    assert result is not None
    assert result.streak == 3
    assert result.is_stuck is True


def test_streak_counts_only_current_status():
    series = make_series(
        [MetricStatus.CRITICAL, MetricStatus.CRITICAL, MetricStatus.WARNING, MetricStatus.WARNING]
    )
    result = analyze_stickiness(series, streak_threshold=3)
    assert result.streak == 2
    assert result.status == MetricStatus.WARNING
    assert result.is_stuck is False


def test_duration_is_computed_correctly():
    base = 1000.0
    series = make_series([MetricStatus.WARNING] * 4, base_ts=base)
    result = analyze_stickiness(series, streak_threshold=3)
    # 4 metrics spaced 10s apart → first of streak is index 0 → duration = 30s
    assert result.duration_seconds == pytest.approx(30.0)


def test_to_dict_keys():
    series = make_series([MetricStatus.CRITICAL] * 2)
    result = analyze_stickiness(series, streak_threshold=3)
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline", "metric_name", "status", "streak", "duration_seconds", "is_stuck"
    }


def test_analyze_all_stickiness_filters_ok_pipelines():
    history = {
        "pipe/rows": make_series([MetricStatus.OK, MetricStatus.OK]),
        "pipe/lag": make_series([MetricStatus.WARNING, MetricStatus.WARNING, MetricStatus.WARNING]),
    }
    results = analyze_all_stickiness(history, streak_threshold=3)
    assert len(results) == 1
    assert results[0].metric_name == "lag"


def test_analyze_all_stickiness_empty_history():
    assert analyze_all_stickiness({}) == []
