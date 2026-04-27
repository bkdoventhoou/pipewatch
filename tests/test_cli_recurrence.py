"""Tests for pipewatch.cli_recurrence."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.cli_recurrence import main, parse_args


def make_metric(
    pipeline: str = "pipe",
    name: str = "rows",
    status: MetricStatus = MetricStatus.WARNING,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        metric_name=name,
        value=10.0,
        status=status,
        timestamp=datetime.now(timezone.utc).timestamp(),
    )


def _mock_collector(history: dict):
    collector = MagicMock()
    collector.get_history.return_value = history
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.threshold == 0.3
    assert args.min_count == 3
    assert args.format == "text"
    assert args.recurring_only is False


def test_parse_args_custom():
    args = parse_args([
        "--config", "custom.yaml",
        "--threshold", "0.5",
        "--min-count", "5",
        "--format", "json",
        "--recurring-only",
    ])
    assert args.config == "custom.yaml"
    assert args.threshold == 0.5
    assert args.min_count == 5
    assert args.format == "json"
    assert args.recurring_only is True


def test_main_text_output(capsys):
    history = {
        "pipe:rows": [make_metric(status=MetricStatus.WARNING)] * 5,
    }
    collector = _mock_collector(history)
    with patch("pipewatch.cli_recurrence.load_config", return_value={}), \
         patch("pipewatch.cli_recurrence.build_collector_from_config", return_value=collector):
        main(["--min-count", "3"])
    out = capsys.readouterr().out
    assert "pipe" in out
    assert "rows" in out


def test_main_json_output(capsys):
    history = {
        "pipe:rows": [make_metric(status=MetricStatus.WARNING)] * 5,
    }
    collector = _mock_collector(history)
    with patch("pipewatch.cli_recurrence.load_config", return_value={}), \
         patch("pipewatch.cli_recurrence.build_collector_from_config", return_value=collector):
        main(["--format", "json", "--min-count", "3"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe"


def test_main_recurring_only_filters(capsys):
    history = {
        "pipe:rows": [make_metric(status=MetricStatus.WARNING)] * 5,
        "pipe:lag": [make_metric(name="lag", status=MetricStatus.OK)] * 5,
    }
    collector = _mock_collector(history)
    with patch("pipewatch.cli_recurrence.load_config", return_value={}), \
         patch("pipewatch.cli_recurrence.build_collector_from_config", return_value=collector):
        main(["--min-count", "3", "--recurring-only", "--format", "json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert all(r["is_recurring"] for r in data)


def test_main_empty_history_text(capsys):
    collector = _mock_collector({})
    with patch("pipewatch.cli_recurrence.load_config", return_value={}), \
         patch("pipewatch.cli_recurrence.build_collector_from_config", return_value=collector):
        main([])
    out = capsys.readouterr().out
    assert "No recurrence data" in out
