"""Export pipeline reports to various output formats (JSON, CSV)."""

import csv
import json
import io
from typing import List
from pipewatch.reporter import PipelineReport, ReportEntry


def export_json(report: PipelineReport, indent: int = 2) -> str:
    """Serialize a PipelineReport to a JSON string."""
    data = {
        "summary": report.summary(),
        "entries": [e.to_dict() for e in report.entries],
    }
    return json.dumps(data, indent=indent, default=str)


def export_csv(report: PipelineReport) -> str:
    """Serialize a PipelineReport to a CSV string."""
    fieldnames = ["pipeline", "metric", "value", "status", "timestamp"]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for entry in report.entries:
        row = entry.to_dict()
        # Flatten nested metric dict fields
        metric = row.pop("metric", {})
        row["metric"] = metric.get("name", "")
        row["value"] = metric.get("value", "")
        row["timestamp"] = metric.get("timestamp", "")
        writer.writerow(row)
    return output.getvalue()


def export_report(report: PipelineReport, fmt: str) -> str:
    """Export report in the given format ('json' or 'csv')."""
    fmt = fmt.lower()
    if fmt == "json":
        return export_json(report)
    elif fmt == "csv":
        return export_csv(report)
    else:
        raise ValueError(f"Unsupported export format: {fmt!r}. Choose 'json' or 'csv'.")
