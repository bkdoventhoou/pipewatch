"""Tests for pipewatch.cli_budget."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_budget import parse_args, main
from pipewatch.metrics import PipelineMetric, MetricStatus


def make_metric(pipeline="pipe_a", name="rows", status=MetricStatus.WARNING, value=5.0):
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status)


def _mock_collector(metrics_by_key: dict):
    collector = MagicMock()
    collector.get_history.return_value = metrics_by_key
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.max_alerts is None
    assert args.window is None
    assert args.fmt == "text"


def test_parse_args_custom():
    args = parse_args(["--max-alerts", "5", "--window", "1800", "--format", "json"])
    assert args.max_alerts == 5
    assert args.window == 1800
    assert args.fmt == "json"


def test_main_text_output(capsys):
    metrics = {"pipe_a:rows": [make_metric(status=MetricStatus.WARNING)]}
    collector = _mock_collector(metrics)
    with patch("pipewatch.cli_budget.build_collector_from_config", return_value=collector), \
         patch("pipewatch.cli_budget.load_config", return_value={}):
        main(["--max-alerts", "10", "--window", "3600"])
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "Used" in out or "used" in out.lower() or "1" in out


def test_main_json_output(capsys):
    metrics = {"pipe_a:rows": [make_metric(status=MetricStatus.CRITICAL)]}
    collector = _mock_collector(metrics)
    with patch("pipewatch.cli_budget.build_collector_from_config", return_value=collector), \
         patch("pipewatch.cli_budget.load_config", return_value={}):
        main(["--format", "json", "--max-alerts", "10"])
    out = capsys.readouterr().out
    import json
    data = json.loads(out)
    assert "pipe_a" in data


def test_main_no_usage_prints_message(capsys):
    collector = _mock_collector({})
    with patch("pipewatch.cli_budget.build_collector_from_config", return_value=collector), \
         patch("pipewatch.cli_budget.load_config", return_value={}):
        main([])
    out = capsys.readouterr().out
    assert "No alert budget usage" in out


def test_main_config_budget_values_used(capsys):
    metrics = {"pipe_a:rows": [make_metric(status=MetricStatus.WARNING)]}
    collector = _mock_collector(metrics)
    cfg = {"budget": {"max_alerts": 3, "window_seconds": 600}}
    with patch("pipewatch.cli_budget.build_collector_from_config", return_value=collector), \
         patch("pipewatch.cli_budget.load_config", return_value=cfg):
        main([])
    out = capsys.readouterr().out
    assert "3" in out  # max_alerts reflected
