"""Tests for pipewatch.recurrence."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.recurrence import (
    RecurrenceResult,
    detect_all_recurrences,
    detect_recurrence,
)


def make_metric(
    pipeline: str = "pipe",
    name: str = "rows",
    value: float = 1.0,
    status: MetricStatus = MetricStatus.OK,
    ts: float = 0.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        metric_name=name,
        value=value,
        status=status,
        timestamp=ts or datetime.now(timezone.utc).timestamp(),
    )


def test_detect_recurrence_returns_none_on_too_few():
    metrics = [make_metric(status=MetricStatus.WARNING)] * 2
    assert detect_recurrence(metrics, min_count=3) is None


def test_detect_recurrence_no_breaches_not_recurring():
    metrics = [make_metric(status=MetricStatus.OK)] * 5
    result = detect_recurrence(metrics)
    assert result is not None
    assert result.breach_count == 0
    assert result.is_recurring is False


def test_detect_recurrence_all_breaches_is_recurring():
    metrics = [make_metric(status=MetricStatus.WARNING)] * 5
    result = detect_recurrence(metrics, threshold=0.3)
    assert result is not None
    assert result.breach_count == 5
    assert result.recurrence_rate == 1.0
    assert result.is_recurring is True


def test_detect_recurrence_partial_breach_above_threshold():
    metrics = [
        make_metric(status=MetricStatus.WARNING),
        make_metric(status=MetricStatus.WARNING),
        make_metric(status=MetricStatus.OK),
        make_metric(status=MetricStatus.OK),
        make_metric(status=MetricStatus.OK),
    ]
    result = detect_recurrence(metrics, threshold=0.3)
    assert result is not None
    assert result.breach_count == 2
    assert pytest.approx(result.recurrence_rate, 0.001) == 0.4
    assert result.is_recurring is True


def test_detect_recurrence_partial_breach_below_threshold():
    metrics = [
        make_metric(status=MetricStatus.WARNING),
        make_metric(status=MetricStatus.OK),
        make_metric(status=MetricStatus.OK),
        make_metric(status=MetricStatus.OK),
        make_metric(status=MetricStatus.OK),
    ]
    result = detect_recurrence(metrics, threshold=0.3)
    assert result is not None
    assert result.is_recurring is False


def test_detect_recurrence_critical_counts_as_breach():
    metrics = [make_metric(status=MetricStatus.CRITICAL)] * 4
    result = detect_recurrence(metrics, min_count=3)
    assert result is not None
    assert result.breach_count == 4
    assert result.is_recurring is True


def test_detect_recurrence_to_dict_keys():
    metrics = [make_metric(status=MetricStatus.WARNING)] * 4
    result = detect_recurrence(metrics)
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline", "metric_name", "breach_count",
        "total_count", "recurrence_rate", "is_recurring",
    }


def test_detect_all_recurrences_groups_by_key():
    history = {
        "pipe:rows": [make_metric(pipeline="pipe", name="rows", status=MetricStatus.WARNING)] * 5,
        "pipe:lag": [make_metric(pipeline="pipe", name="lag", status=MetricStatus.OK)] * 5,
    }
    results = detect_all_recurrences(history, threshold=0.3, min_count=3)
    assert len(results) == 2
    recurring = [r for r in results if r.is_recurring]
    assert len(recurring) == 1
    assert recurring[0].metric_name == "rows"


def test_detect_all_recurrences_skips_empty():
    history = {"pipe:rows": []}
    results = detect_all_recurrences(history)
    assert results == []
