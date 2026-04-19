"""Tests for reporter and formatters."""
import json
from datetime import datetime

import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.collector import MetricCollector
from pipewatch.reporter import build_report, ReportEntry
from pipewatch.formatters import format_text, format_json, get_formatter


def make_collector(*records):
    """records: list of (name, value, warning, critical)"""
    col = MetricCollector()
    for name, value, warn, crit in records:
        col.add_threshold(name, warning=warn, critical=crit)
        col.record(PipelineMetric(name=name, value=value, timestamp=datetime.utcnow()))
    return col


def test_build_report_ok():
    col = make_collector(("row_count", 500, 100, 50))
    report = build_report(col)
    assert len(report.entries) == 1
    assert report.entries[0].status == MetricStatus.OK


def test_build_report_warning():
    col = make_collector(("latency", 80, 100, 50))
    report = build_report(col)
    assert report.entries[0].status == MetricStatus.WARNING


def test_build_report_critical():
    col = make_collector(("error_rate", 30, 100, 50))
    report = build_report(col)
    assert report.entries[0].status == MetricStatus.CRITICAL


def test_summary_counts():
    col = make_collector(
        ("a", 500, 100, 50),
        ("b", 80, 100, 50),
        ("c", 30, 100, 50),
    )
    report = build_report(col)
    summary = report.summary()
    assert summary["ok"] == 1
    assert summary["warning"] == 1
    assert summary["critical"] == 1


def test_empty_collector():
    col = MetricCollector()
    col.add_threshold("x", warning=10, critical=5)
    report = build_report(col)
    assert report.entries == []


def test_format_json_valid():
    col = make_collector(("row_count", 500, 100, 50))
    report = build_report(col)
    output = format_json(report)
    data = json.loads(output)
    assert "entries" in data
    assert data["entries"][0]["metric"] == "row_count"


def test_format_text_contains_metric():
    col = make_collector(("row_count", 500, 100, 50))
    report = build_report(col)
    output = format_text(report, color=False)
    assert "row_count" in output
    assert "OK" in output


def test_get_formatter_invalid():
    with pytest.raises(ValueError):
        get_formatter("xml")
