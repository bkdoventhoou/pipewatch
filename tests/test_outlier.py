"""Tests for pipewatch.outlier module."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.outlier import OutlierResult, _quartiles, detect_all_outliers, detect_outlier


def make_metric(
    value: float,
    pipeline: str = "pipe",
    name: str = "rows",
    offset_s: int = 0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=offset_s),
    )


def make_series(values: List[float]) -> List[PipelineMetric]:
    return [make_metric(v, offset_s=i * 60) for i, v in enumerate(values)]


def test_detect_outlier_returns_none_on_too_few():
    metrics = make_series([1.0, 2.0, 3.0])
    assert detect_outlier(metrics) is None


def test_detect_outlier_returns_none_on_zero_iqr():
    metrics = make_series([5.0, 5.0, 5.0, 5.0, 5.0])
    assert detect_outlier(metrics) is None


def test_detect_outlier_not_an_outlier():
    metrics = make_series([10.0, 11.0, 12.0, 13.0, 12.0])
    result = detect_outlier(metrics)
    assert result is not None
    assert result.is_outlier is False


def test_detect_outlier_detects_spike():
    metrics = make_series([10.0, 11.0, 10.5, 11.5, 999.0])
    result = detect_outlier(metrics)
    assert result is not None
    assert result.is_outlier is True
    assert result.value == 999.0


def test_detect_outlier_detects_low_spike():
    metrics = make_series([100.0, 101.0, 99.0, 100.5, -500.0])
    result = detect_outlier(metrics)
    assert result is not None
    assert result.is_outlier is True


def test_detect_outlier_returns_correct_pipeline_and_name():
    metrics = make_series([10.0, 11.0, 10.5, 11.5, 12.0])
    result = detect_outlier(metrics)
    assert result is not None
    assert result.pipeline == "pipe"
    assert result.metric_name == "rows"


def test_detect_outlier_to_dict_keys():
    metrics = make_series([10.0, 11.0, 10.5, 11.5, 12.0])
    result = detect_outlier(metrics)
    assert result is not None
    d = result.to_dict()
    for key in ("pipeline", "metric_name", "value", "q1", "q3", "iqr",
                "lower_fence", "upper_fence", "is_outlier"):
        assert key in d


def test_detect_all_outliers_groups_by_key():
    history = {
        ("pipe_a", "rows"): make_series([10.0, 11.0, 10.5, 11.5, 12.0]),
        ("pipe_b", "lag"): make_series([1.0, 1.1, 1.0, 1.2, 999.0]),
    }
    results = detect_all_outliers(history)
    assert len(results) == 2
    outlier_results = [r for r in results if r.is_outlier]
    assert len(outlier_results) == 1
    assert outlier_results[0].pipeline == "pipe_b"


def test_quartiles_even_length():
    q1, q3 = _quartiles([1.0, 2.0, 3.0, 4.0])
    assert q1 <= q3


def test_quartiles_odd_length():
    q1, q3 = _quartiles([1.0, 2.0, 3.0, 4.0, 5.0])
    assert q1 <= q3
