"""Tests for SLATracker breach recording and retrieval."""

from __future__ import annotations

import pytest

from pipewatch.sla import SLAResult
from pipewatch.sla_tracker import SLABreachEvent, SLATracker


def make_result(
    pipeline="pipe_a",
    metric_name="row_count",
    met=True,
    target_pct=99.0,
    actual_pct=99.5,
) -> SLAResult:
    return SLAResult(
        pipeline=pipeline,
        metric_name=metric_name,
        met=met,
        target_pct=target_pct,
        actual_pct=actual_pct,
    )


def test_record_met_sla_returns_none():
    tracker = SLATracker()
    result = make_result(met=True)
    assert tracker.record(result) is None


def test_record_breached_sla_returns_event():
    tracker = SLATracker()
    result = make_result(met=False, target_pct=99.0, actual_pct=95.0)
    event = tracker.record(result)
    assert isinstance(event, SLABreachEvent)
    assert event.pipeline == "pipe_a"
    assert event.metric_name == "row_count"
    assert event.sla_target_pct == 99.0
    assert event.actual_pct == 95.0
    assert abs(event.delta_pct - (-4.0)) < 0.001


def test_breach_count_increments():
    tracker = SLATracker()
    result = make_result(met=False, target_pct=99.0, actual_pct=90.0)
    tracker.record(result)
    tracker.record(result)
    assert tracker.breach_count("pipe_a", "row_count") == 2


def test_get_breaches_returns_empty_for_unknown():
    tracker = SLATracker()
    assert tracker.get_breaches("ghost", "metric") == []


def test_all_breaches_aggregates_across_pipelines():
    tracker = SLATracker()
    tracker.record(make_result(pipeline="a", met=False, actual_pct=80.0))
    tracker.record(make_result(pipeline="b", metric_name="latency", met=False, actual_pct=70.0))
    assert len(tracker.all_breaches()) == 2


def test_met_sla_does_not_add_to_history():
    tracker = SLATracker()
    tracker.record(make_result(met=True))
    assert tracker.breach_count("pipe_a", "row_count") == 0


def test_clear_removes_breach_history():
    tracker = SLATracker()
    tracker.record(make_result(met=False, actual_pct=80.0))
    tracker.clear("pipe_a", "row_count")
    assert tracker.breach_count("pipe_a", "row_count") == 0


def test_breach_event_to_dict_has_expected_keys():
    tracker = SLATracker()
    event = tracker.record(make_result(met=False, target_pct=99.0, actual_pct=95.0))
    d = event.to_dict()
    for key in ("pipeline", "metric_name", "breached_at", "sla_target_pct", "actual_pct", "delta_pct"):
        assert key in d
