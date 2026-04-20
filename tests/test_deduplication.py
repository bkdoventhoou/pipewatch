"""Tests for pipewatch.deduplication."""

import time
import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.deduplication import AlertDeduplicator, _make_key


def make_metric(
    pipeline: str = "pipe",
    name: str = "row_count",
    value: float = 10.0,
    status: MetricStatus = MetricStatus.WARNING,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=time.time(),
    )


def test_ok_metric_never_duplicate():
    dedup = AlertDeduplicator(window_seconds=60)
    m = make_metric(status=MetricStatus.OK)
    assert dedup.is_duplicate(m) is False
    assert dedup.is_duplicate(m) is False


def test_first_warning_not_duplicate():
    dedup = AlertDeduplicator(window_seconds=60)
    m = make_metric(status=MetricStatus.WARNING)
    assert dedup.is_duplicate(m) is False


def test_second_warning_is_duplicate_within_window():
    dedup = AlertDeduplicator(window_seconds=60)
    m = make_metric(status=MetricStatus.WARNING)
    dedup.is_duplicate(m)
    assert dedup.is_duplicate(m) is True


def test_duplicate_increments_count():
    dedup = AlertDeduplicator(window_seconds=60)
    m = make_metric(status=MetricStatus.CRITICAL)
    dedup.is_duplicate(m)
    dedup.is_duplicate(m)
    dedup.is_duplicate(m)
    entry = dedup.get_entry(m)
    assert entry is not None
    assert entry.count == 3


def test_different_pipelines_not_duplicate():
    dedup = AlertDeduplicator(window_seconds=60)
    m1 = make_metric(pipeline="pipe_a", status=MetricStatus.WARNING)
    m2 = make_metric(pipeline="pipe_b", status=MetricStatus.WARNING)
    dedup.is_duplicate(m1)
    assert dedup.is_duplicate(m2) is False


def test_different_statuses_not_duplicate():
    dedup = AlertDeduplicator(window_seconds=60)
    m_warn = make_metric(status=MetricStatus.WARNING)
    m_crit = make_metric(status=MetricStatus.CRITICAL)
    dedup.is_duplicate(m_warn)
    assert dedup.is_duplicate(m_crit) is False


def test_fires_again_after_window_expires():
    dedup = AlertDeduplicator(window_seconds=0.05)
    m = make_metric(status=MetricStatus.WARNING)
    dedup.is_duplicate(m)
    time.sleep(0.1)
    assert dedup.is_duplicate(m) is False


def test_evict_expired_removes_old_entries():
    dedup = AlertDeduplicator(window_seconds=0.05)
    m = make_metric(status=MetricStatus.WARNING)
    dedup.is_duplicate(m)
    time.sleep(0.1)
    removed = dedup.evict_expired()
    assert removed == 1
    assert dedup.stats()["tracked"] == 0


def test_clear_removes_all():
    dedup = AlertDeduplicator(window_seconds=60)
    dedup.is_duplicate(make_metric(pipeline="a", status=MetricStatus.WARNING))
    dedup.is_duplicate(make_metric(pipeline="b", status=MetricStatus.CRITICAL))
    dedup.clear()
    assert dedup.stats()["tracked"] == 0


def test_make_key_is_deterministic():
    m = make_metric()
    assert _make_key(m) == _make_key(m)
