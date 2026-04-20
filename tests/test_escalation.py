"""Tests for pipewatch.escalation."""

import pytest
from pipewatch.escalation import AlertEscalator, EscalationEntry
from pipewatch.metrics import PipelineMetric, MetricStatus
from datetime import datetime


def make_metric(status: MetricStatus, pipeline="pipe_a", name="row_count", value=10.0):
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


def test_ok_metric_returns_none():
    esc = AlertEscalator(threshold=3)
    metric = make_metric(MetricStatus.OK)
    assert esc.evaluate(metric) is None


def test_first_warning_returns_warning():
    esc = AlertEscalator(threshold=3)
    metric = make_metric(MetricStatus.WARNING)
    assert esc.evaluate(metric) == MetricStatus.WARNING


def test_warning_escalates_after_threshold():
    esc = AlertEscalator(threshold=3)
    metric = make_metric(MetricStatus.WARNING)
    esc.evaluate(metric)
    esc.evaluate(metric)
    result = esc.evaluate(metric)
    assert result == MetricStatus.CRITICAL


def test_warning_not_escalated_before_threshold():
    esc = AlertEscalator(threshold=3)
    metric = make_metric(MetricStatus.WARNING)
    esc.evaluate(metric)
    result = esc.evaluate(metric)
    assert result == MetricStatus.WARNING


def test_critical_stays_critical():
    esc = AlertEscalator(threshold=2)
    metric = make_metric(MetricStatus.CRITICAL)
    for _ in range(5):
        result = esc.evaluate(metric)
    assert result == MetricStatus.CRITICAL
    entry = esc.get_entry(metric)
    assert entry is not None
    assert not entry.escalated  # CRITICAL is not "escalated", it's already critical


def test_ok_resets_counter():
    esc = AlertEscalator(threshold=3)
    warn = make_metric(MetricStatus.WARNING)
    ok = make_metric(MetricStatus.OK)
    esc.evaluate(warn)
    esc.evaluate(warn)
    esc.evaluate(ok)  # reset
    result = esc.evaluate(warn)  # back to 1
    assert result == MetricStatus.WARNING
    entry = esc.get_entry(warn)
    assert entry.count == 1


def test_entry_escalated_flag_set():
    esc = AlertEscalator(threshold=2)
    metric = make_metric(MetricStatus.WARNING)
    esc.evaluate(metric)
    esc.evaluate(metric)
    entry = esc.get_entry(metric)
    assert entry.escalated is True


def test_all_entries_returns_list():
    esc = AlertEscalator(threshold=3)
    m1 = make_metric(MetricStatus.WARNING, pipeline="p1", name="m1")
    m2 = make_metric(MetricStatus.CRITICAL, pipeline="p2", name="m2")
    esc.evaluate(m1)
    esc.evaluate(m2)
    entries = esc.all_entries()
    assert len(entries) == 2
    pipelines = {e["pipeline"] for e in entries}
    assert pipelines == {"p1", "p2"}


def test_entry_to_dict_keys():
    entry = EscalationEntry(
        pipeline="pipe", metric_name="rows", status=MetricStatus.WARNING, count=2
    )
    d = entry.to_dict()
    assert set(d.keys()) == {"pipeline", "metric_name", "status", "count", "escalated"}
    assert d["status"] == "warning"
