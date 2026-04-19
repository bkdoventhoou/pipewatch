"""Tests for pipewatch.exporter module."""

import json
import csv
import io
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from pipewatch.metrics import MetricStatus
from pipewatch.reporter import PipelineReport, ReportEntry
from pipewatch.exporter import export_json, export_csv, export_report


def make_report():
    report = PipelineReport()
    for name, value, status in [
        ("row_count", 1000, MetricStatus.OK),
        ("error_rate", 0.08, MetricStatus.WARNING),
        ("latency", 95.0, MetricStatus.CRITICAL),
    ]:
        entry = ReportEntry(
            pipeline="etl_main",
            metric={"name": name, "value": value, "timestamp": "2024-01-01T00:00:00"},
            status=status,
        )
        report.add(entry)
    return report


def test_export_json_structure():
    report = make_report()
    result = export_json(report)
    data = json.loads(result)
    assert "summary" in data
    assert "entries" in data
    assert len(data["entries"]) == 3


def test_export_json_summary_counts():
    report = make_report()
    data = json.loads(export_json(report))
    summary = data["summary"]
    assert summary["ok"] == 1
    assert summary["warning"] == 1
    assert summary["critical"] == 1


def test_export_csv_has_header_and_rows():
    report = make_report()
    result = export_csv(report)
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)
    assert len(rows) == 3
    assert "pipeline" in reader.fieldnames
    assert "metric" in reader.fieldnames
    assert "status" in reader.fieldnames


def test_export_csv_pipeline_field():
    report = make_report()
    result = export_csv(report)
    reader = csv.DictReader(io.StringIO(result))
    for row in reader:
        assert row["pipeline"] == "etl_main"


def test_export_report_json():
    report = make_report()
    result = export_report(report, "json")
    data = json.loads(result)
    assert "entries" in data


def test_export_report_csv():
    report = make_report()
    result = export_report(report, "csv")
    assert "pipeline" in result
    assert "etl_main" in result


def test_export_report_invalid_format():
    report = make_report()
    with pytest.raises(ValueError, match="Unsupported export format"):
        export_report(report, "xml")
