"""Tests for pipewatch.cadence."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.cadence import (
    CadenceResult,
    analyze_cadence,
    analyze_all_cadences,
)
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(
    pipeline: str = "etl",
    name: str = "row_count",
    value: float = 1.0,
    timestamp: float = 0.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=timestamp,
    )


def test_analyze_cadence_returns_none_on_single_metric():
    metrics = [make_metric(timestamp=0.0)]
    assert analyze_cadence(metrics, expected_interval=60.0) is None


def test_analyze_cadence_returns_none_on_empty():
    assert analyze_cadence([], expected_interval=60.0) is None


def test_analyze_cadence_returns_none_on_zero_interval():
    metrics = [make_metric(timestamp=0.0), make_metric(timestamp=60.0)]
    assert analyze_cadence(metrics, expected_interval=0.0) is None


def test_analyze_cadence_regular_series():
    metrics = [make_metric(timestamp=float(i * 60)) for i in range(5)]
    result = analyze_cadence(metrics, expected_interval=60.0)
    assert result is not None
    assert result.mean_interval == pytest.approx(60.0)
    assert result.max_gap == pytest.approx(60.0)
    assert result.missed_count == 0
    assert result.irregular is False


def test_analyze_cadence_detects_missed_emission():
    # Gap of 120s where 60s is expected — should count as missed
    timestamps = [0.0, 60.0, 180.0, 240.0]  # 120s gap between index 1 and 2
    metrics = [make_metric(timestamp=t) for t in timestamps]
    result = analyze_cadence(metrics, expected_interval=60.0)
    assert result is not None
    assert result.missed_count == 1


def test_analyze_cadence_irregular_flag():
    # Mean interval is 120s but expected 60s → deviation = 100% > 25%
    metrics = [make_metric(timestamp=float(i * 120)) for i in range(4)]
    result = analyze_cadence(metrics, expected_interval=60.0)
    assert result is not None
    assert result.irregular is True


def test_analyze_cadence_not_irregular_within_ratio():
    # Mean interval is 65s, expected 60s → deviation ≈ 8% < 25%
    metrics = [make_metric(timestamp=float(i * 65)) for i in range(5)]
    result = analyze_cadence(metrics, expected_interval=60.0)
    assert result is not None
    assert result.irregular is False


def test_analyze_cadence_to_dict_keys():
    metrics = [make_metric(timestamp=float(i * 60)) for i in range(3)]
    result = analyze_cadence(metrics, expected_interval=60.0)
    d = result.to_dict()
    assert "pipeline" in d
    assert "metric_name" in d
    assert "expected_interval" in d
    assert "mean_interval" in d
    assert "max_gap" in d
    assert "missed_count" in d
    assert "irregular" in d


def test_analyze_all_cadences_groups_by_key():
    history = {
        "etl:row_count": [make_metric("etl", "row_count", timestamp=float(i * 60)) for i in range(4)],
        "etl:error_rate": [make_metric("etl", "error_rate", timestamp=float(i * 30)) for i in range(4)],
    }
    results = analyze_all_cadences(history, expected_interval=60.0)
    assert len(results) == 2


def test_analyze_all_cadences_skips_single_entry_series():
    history = {
        "etl:row_count": [make_metric(timestamp=0.0)],
    }
    results = analyze_all_cadences(history, expected_interval=60.0)
    assert results == []
