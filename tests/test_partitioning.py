"""Tests for pipewatch.partitioning."""

from datetime import datetime, timezone

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.partitioning import MetricPartition, partition_metrics


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_metric(pipeline: str, name: str, value: float, ts: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=ts,
    )


def _ts(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> float:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp()


# ---------------------------------------------------------------------------
# partition_metrics – hourly
# ---------------------------------------------------------------------------

def test_partition_empty_returns_empty():
    assert partition_metrics([]) == {}


def test_partition_single_metric_hourly():
    m = make_metric("pipe", "rows", 10.0, _ts(2024, 6, 1, 14, 30))
    result = partition_metrics([m], granularity="hourly")
    assert "2024-06-01T14" in result
    assert result["2024-06-01T14"].count == 1


def test_partition_two_metrics_same_hour():
    m1 = make_metric("pipe", "rows", 10.0, _ts(2024, 6, 1, 14, 10))
    m2 = make_metric("pipe", "rows", 20.0, _ts(2024, 6, 1, 14, 50))
    result = partition_metrics([m1, m2], granularity="hourly")
    assert len(result) == 1
    assert result["2024-06-01T14"].count == 2


def test_partition_two_metrics_different_hours():
    m1 = make_metric("pipe", "rows", 10.0, _ts(2024, 6, 1, 13, 0))
    m2 = make_metric("pipe", "rows", 20.0, _ts(2024, 6, 1, 14, 0))
    result = partition_metrics([m1, m2], granularity="hourly")
    assert len(result) == 2


# ---------------------------------------------------------------------------
# partition_metrics – daily
# ---------------------------------------------------------------------------

def test_partition_daily_groups_same_day():
    m1 = make_metric("pipe", "rows", 5.0, _ts(2024, 6, 1, 8))
    m2 = make_metric("pipe", "rows", 15.0, _ts(2024, 6, 1, 20))
    result = partition_metrics([m1, m2], granularity="daily")
    assert "2024-06-01" in result
    assert result["2024-06-01"].count == 2


def test_partition_daily_splits_across_days():
    m1 = make_metric("pipe", "rows", 5.0, _ts(2024, 6, 1, 8))
    m2 = make_metric("pipe", "rows", 15.0, _ts(2024, 6, 2, 8))
    result = partition_metrics([m1, m2], granularity="daily")
    assert len(result) == 2


# ---------------------------------------------------------------------------
# MetricPartition.average
# ---------------------------------------------------------------------------

def test_partition_average_is_correct():
    m1 = make_metric("p", "n", 10.0, _ts(2024, 6, 1, 9))
    m2 = make_metric("p", "n", 30.0, _ts(2024, 6, 1, 9, 30))
    result = partition_metrics([m1, m2], granularity="hourly")
    bucket = list(result.values())[0]
    assert bucket.average == pytest.approx(20.0)


def test_partition_average_none_when_empty():
    p = MetricPartition(
        label="2024-06-01T09",
        start=datetime(2024, 6, 1, 9, tzinfo=timezone.utc),
        end=datetime(2024, 6, 1, 10, tzinfo=timezone.utc),
    )
    assert p.average is None


# ---------------------------------------------------------------------------
# to_dict
# ---------------------------------------------------------------------------

def test_partition_to_dict_keys():
    m = make_metric("pipe", "rows", 10.0, _ts(2024, 6, 1, 14, 0))
    result = partition_metrics([m], granularity="daily")
    d = list(result.values())[0].to_dict()
    assert set(d.keys()) == {"label", "start", "end", "count", "average"}


# ---------------------------------------------------------------------------
# Invalid granularity
# ---------------------------------------------------------------------------

def test_invalid_granularity_raises():
    m = make_metric("pipe", "rows", 1.0, _ts(2024, 6, 1))
    with pytest.raises(ValueError, match="Unknown granularity"):
        partition_metrics([m], granularity="minutely")
