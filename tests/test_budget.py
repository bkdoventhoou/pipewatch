"""Tests for pipewatch.budget."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.budget import AlertBudget, BudgetEntry
from pipewatch.metrics import PipelineMetric, MetricStatus


def make_metric(pipeline="pipe_a", name="rows", status=MetricStatus.WARNING, value=5.0):
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status)


def _now():
    return datetime(2024, 1, 1, 12, 0, 0)


def test_ok_metric_never_consumes_budget():
    budget = AlertBudget(max_alerts=5, window_seconds=60)
    m = make_metric(status=MetricStatus.OK)
    assert budget.allow(m, now=_now()) is False
    assert budget.used("pipe_a", now=_now()) == 0


def test_first_warning_consumes_budget():
    budget = AlertBudget(max_alerts=5, window_seconds=60)
    m = make_metric(status=MetricStatus.WARNING)
    assert budget.allow(m, now=_now()) is True
    assert budget.used("pipe_a", now=_now()) == 1


def test_budget_exhausted_blocks_alert():
    budget = AlertBudget(max_alerts=2, window_seconds=3600)
    m = make_metric(status=MetricStatus.CRITICAL)
    now = _now()
    assert budget.allow(m, now=now) is True
    assert budget.allow(m, now=now) is True
    assert budget.allow(m, now=now) is False  # budget exhausted


def test_remaining_decrements_correctly():
    budget = AlertBudget(max_alerts=3, window_seconds=3600)
    m = make_metric(status=MetricStatus.WARNING)
    now = _now()
    assert budget.remaining("pipe_a", now=now) == 3
    budget.allow(m, now=now)
    assert budget.remaining("pipe_a", now=now) == 2


def test_old_entries_pruned_after_window():
    budget = AlertBudget(max_alerts=2, window_seconds=60)
    m = make_metric(status=MetricStatus.WARNING)
    old_time = _now()
    budget.allow(m, now=old_time)
    budget.allow(m, now=old_time)
    # Both slots used; advance past window
    future = old_time + timedelta(seconds=61)
    assert budget.allow(m, now=future) is True  # window reset


def test_pipelines_are_isolated():
    budget = AlertBudget(max_alerts=1, window_seconds=3600)
    now = _now()
    m_a = make_metric(pipeline="pipe_a", status=MetricStatus.WARNING)
    m_b = make_metric(pipeline="pipe_b", status=MetricStatus.WARNING)
    assert budget.allow(m_a, now=now) is True
    assert budget.allow(m_b, now=now) is True  # different pipeline
    assert budget.allow(m_a, now=now) is False  # pipe_a exhausted


def test_summary_returns_per_pipeline_info():
    budget = AlertBudget(max_alerts=5, window_seconds=3600)
    now = _now()
    budget.allow(make_metric(pipeline="pipe_a", status=MetricStatus.WARNING), now=now)
    budget.allow(make_metric(pipeline="pipe_a", status=MetricStatus.CRITICAL), now=now)
    budget.allow(make_metric(pipeline="pipe_b", status=MetricStatus.WARNING), now=now)
    summary = budget.summary(now=now)
    assert summary["pipe_a"]["used"] == 2
    assert summary["pipe_a"]["remaining"] == 3
    assert summary["pipe_b"]["used"] == 1


def test_budget_entry_to_dict():
    entry = BudgetEntry(pipeline="p", metric_name="m", fired_at=_now())
    d = entry.to_dict()
    assert d["pipeline"] == "p"
    assert d["metric_name"] == "m"
    assert "fired_at" in d
