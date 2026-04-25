"""Tests for pipewatch.jitter."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.jitter import JitterResult, detect_jitter, detect_all_jitter, _count_flips
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(value: float, name: str = "rows", pipeline: str = "etl") -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime.now(tz=timezone.utc),
    )


def make_series(values: List[float], **kwargs) -> List[PipelineMetric]:
    return [make_metric(v, **kwargs) for v in values]


# --- _count_flips ---

def test_count_flips_monotone_increasing():
    assert _count_flips([1, 2, 3, 4]) == 0


def test_count_flips_alternating():
    # 1 -> 3 -> 1 -> 3  => two flips
    assert _count_flips([1, 3, 1, 3]) == 2


def test_count_flips_too_short():
    assert _count_flips([1, 2]) == 0


# --- detect_jitter ---

def test_detect_jitter_returns_none_on_too_few_points():
    series = make_series([1.0, 2.0, 3.0])  # only 3, min_points=4
    assert detect_jitter(series) is None


def test_detect_jitter_stable_series_not_jittery():
    series = make_series([10.0, 10.5, 11.0, 11.5, 12.0])
    result = detect_jitter(series)
    assert result is not None
    assert result.jittery is False
    assert result.flip_count == 0


def test_detect_jitter_alternating_is_jittery():
    series = make_series([1.0, 5.0, 1.0, 5.0, 1.0, 5.0])
    result = detect_jitter(series)
    assert result is not None
    assert result.jittery is True
    assert result.flip_count >= 3


def test_detect_jitter_max_swing_computed():
    series = make_series([0.0, 10.0, 0.0, 10.0, 0.0])
    result = detect_jitter(series)
    assert result is not None
    assert result.max_swing == pytest.approx(10.0)


def test_detect_jitter_flip_rate_in_result():
    series = make_series([1.0, 5.0, 1.0, 5.0, 1.0, 5.0])
    result = detect_jitter(series)
    assert result is not None
    assert 0.0 <= result.flip_rate <= 1.0


def test_detect_jitter_to_dict_keys():
    series = make_series([1.0, 5.0, 1.0, 5.0, 1.0])
    result = detect_jitter(series)
    assert result is not None
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline", "metric_name", "flip_count", "flip_rate", "max_swing", "jittery"}


# --- detect_all_jitter ---

def test_detect_all_jitter_skips_short_series():
    history = {
        "etl:rows": make_series([1.0, 2.0]),  # too short
        "etl:errors": make_series([1.0, 5.0, 1.0, 5.0, 1.0]),
    }
    results = detect_all_jitter(history)
    assert len(results) == 1
    assert results[0].metric_name == "rows" or results[0].pipeline == "etl"


def test_detect_all_jitter_returns_all_valid():
    history = {
        "etl:rows": make_series([1.0, 5.0, 1.0, 5.0, 1.0]),
        "etl:lag": make_series([2.0, 2.1, 2.2, 2.3, 2.4]),
    }
    results = detect_all_jitter(history)
    assert len(results) == 2
