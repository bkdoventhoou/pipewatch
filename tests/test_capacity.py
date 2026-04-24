"""Tests for pipewatch.capacity module."""

from __future__ import annotations

from datetime import datetime
from typing import List

import pytest

from pipewatch.capacity import CapacityEntry, CapacityReport, evaluate_capacity
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(pipeline: str, name: str, value: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime.utcnow(),
    )


def test_evaluate_capacity_empty_metrics():
    report = evaluate_capacity([], {"row_count": 1000.0})
    assert report.entries == []


def test_evaluate_capacity_no_matching_limits():
    m = make_metric("etl", "latency", 50.0)
    report = evaluate_capacity([m], {"row_count": 1000.0})
    assert report.entries == []


def test_evaluate_capacity_below_limit():
    m = make_metric("etl", "row_count", 400.0)
    report = evaluate_capacity([m], {"row_count": 1000.0})
    assert len(report.entries) == 1
    entry = report.entries[0]
    assert entry.current == 400.0
    assert entry.limit == 1000.0
    assert pytest.approx(entry.utilization, rel=1e-4) == 0.4
    assert entry.breached is False


def test_evaluate_capacity_at_limit_is_breached():
    m = make_metric("etl", "row_count", 1000.0)
    report = evaluate_capacity([m], {"row_count": 1000.0})
    assert report.entries[0].breached is True


def test_evaluate_capacity_above_limit_is_breached():
    m = make_metric("etl", "row_count", 1200.0)
    report = evaluate_capacity([m], {"row_count": 1000.0})
    entry = report.entries[0]
    assert entry.breached is True
    assert entry.utilization > 1.0


def test_evaluate_capacity_custom_threshold():
    m = make_metric("etl", "row_count", 850.0)
    report = evaluate_capacity([m], {"row_count": 1000.0}, breach_threshold=0.8)
    assert report.entries[0].breached is True


def test_evaluate_capacity_zero_limit_skipped():
    m = make_metric("etl", "row_count", 100.0)
    report = evaluate_capacity([m], {"row_count": 0.0})
    assert report.entries == []


def test_capacity_report_breached_entries():
    m1 = make_metric("etl", "row_count", 1100.0)
    m2 = make_metric("etl", "latency", 30.0)
    report = evaluate_capacity([m1, m2], {"row_count": 1000.0, "latency": 100.0})
    assert len(report.breached_entries()) == 1
    assert report.breached_entries()[0].metric_name == "row_count"


def test_capacity_report_to_dict_keys():
    m = make_metric("etl", "row_count", 500.0)
    report = evaluate_capacity([m], {"row_count": 1000.0})
    d = report.to_dict()
    assert "entries" in d
    assert "total" in d
    assert "breached" in d
    assert d["total"] == 1
    assert d["breached"] == 0


def test_capacity_entry_to_dict():
    entry = CapacityEntry(
        pipeline="pipe",
        metric_name="rows",
        current=750.0,
        limit=1000.0,
        utilization=0.75,
        breached=False,
    )
    d = entry.to_dict()
    assert d["pipeline"] == "pipe"
    assert d["metric_name"] == "rows"
    assert d["utilization"] == 0.75
    assert d["breached"] is False
