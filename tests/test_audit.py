"""Tests for pipewatch.audit."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.audit import AuditEvent, AuditLog
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(pipeline: str, name: str, value: float, status: MetricStatus) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=datetime.now(timezone.utc),
    )


def test_no_event_on_first_same_repeated_status():
    log = AuditLog()
    m = make_metric("pipe", "rows", 10.0, MetricStatus.OK)
    event = log.record(m)
    # First record: previous is None, current is OK — transition from None->OK captured
    assert event is not None
    assert event.previous_status is None
    assert event.current_status == MetricStatus.OK


def test_no_event_when_status_unchanged():
    log = AuditLog()
    m = make_metric("pipe", "rows", 10.0, MetricStatus.OK)
    log.record(m)
    event = log.record(make_metric("pipe", "rows", 11.0, MetricStatus.OK))
    assert event is None


def test_event_emitted_on_transition():
    log = AuditLog()
    log.record(make_metric("pipe", "rows", 10.0, MetricStatus.OK))
    event = log.record(make_metric("pipe", "rows", 5.0, MetricStatus.WARNING))
    assert event is not None
    assert event.previous_status == MetricStatus.OK
    assert event.current_status == MetricStatus.WARNING
    assert event.value == 5.0


def test_multiple_transitions_recorded():
    log = AuditLog()
    log.record(make_metric("pipe", "rows", 10.0, MetricStatus.OK))
    log.record(make_metric("pipe", "rows", 5.0, MetricStatus.WARNING))
    log.record(make_metric("pipe", "rows", 1.0, MetricStatus.CRITICAL))
    events = log.get_events()
    assert len(events) == 3


def test_filter_by_pipeline():
    log = AuditLog()
    log.record(make_metric("pipe_a", "rows", 10.0, MetricStatus.OK))
    log.record(make_metric("pipe_b", "rows", 5.0, MetricStatus.WARNING))
    events = log.get_events(pipeline="pipe_a")
    assert len(events) == 1
    assert events[0].pipeline == "pipe_a"


def test_filter_by_metric_name():
    log = AuditLog()
    log.record(make_metric("pipe", "rows", 10.0, MetricStatus.OK))
    log.record(make_metric("pipe", "latency", 5.0, MetricStatus.WARNING))
    events = log.get_events(metric_name="latency")
    assert len(events) == 1
    assert events[0].metric_name == "latency"


def test_to_dict_keys():
    log = AuditLog()
    event = log.record(make_metric("pipe", "rows", 10.0, MetricStatus.OK))
    d = event.to_dict()
    assert set(d.keys()) == {
        "pipeline", "metric_name", "previous_status",
        "current_status", "value", "timestamp"
    }
    assert d["previous_status"] is None
    assert d["current_status"] == "ok"


def test_clear_resets_state():
    log = AuditLog()
    log.record(make_metric("pipe", "rows", 10.0, MetricStatus.OK))
    log.clear()
    assert log.get_events() == []
    # After clear, next record should again emit a transition-from-None event
    event = log.record(make_metric("pipe", "rows", 10.0, MetricStatus.OK))
    assert event is not None
    assert event.previous_status is None
