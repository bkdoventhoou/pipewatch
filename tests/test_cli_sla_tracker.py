"""Tests for the cli_sla_tracker CLI entry point."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_sla_tracker import main, parse_args
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(pipeline="pipe_a", name="row_count", value=100.0, status=MetricStatus.OK):
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp="2024-01-01T00:00:00",
    )


def _mock_collector(metrics):
    collector = MagicMock()
    def _get_history(pipeline, metric_name):
        return [m for m in metrics if m.pipeline == pipeline and m.name == metric_name]
    collector.get_history.side_effect = _get_history
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.format == "text"
    assert args.pipeline is None


def test_parse_args_custom():
    args = parse_args(["--format", "json", "--pipeline", "my_pipe", "--config", "alt.yaml"])
    assert args.format == "json"
    assert args.pipeline == "my_pipe"
    assert args.config == "alt.yaml"


def test_main_text_no_breaches(capsys):
    config = {
        "thresholds": [],
        "sla": [
            {"pipeline": "pipe_a", "metric_name": "row_count", "target_pct": 80.0, "window_seconds": 3600}
        ],
    }
    metrics = [make_metric(value=100.0)]
    collector = _mock_collector(metrics)

    with patch("pipewatch.cli_sla_tracker.load_config", return_value=config), \
         patch("pipewatch.cli_sla_tracker.build_collector_from_config", return_value=collector):
        main(["--format", "text"])

    out = capsys.readouterr().out
    assert "No SLA breaches" in out


def test_main_json_output_structure(capsys):
    config = {
        "thresholds": [],
        "sla": [
            {"pipeline": "pipe_a", "metric_name": "row_count", "target_pct": 99.9, "window_seconds": 3600}
        ],
    }
    metrics = [make_metric(value=50.0)]
    collector = _mock_collector(metrics)

    with patch("pipewatch.cli_sla_tracker.load_config", return_value=config), \
         patch("pipewatch.cli_sla_tracker.build_collector_from_config", return_value=collector), \
         patch("pipewatch.cli_sla_tracker.evaluate_sla") as mock_eval:
        from pipewatch.sla import SLAResult
        mock_eval.return_value = SLAResult(
            pipeline="pipe_a", metric_name="row_count",
            met=False, target_pct=99.9, actual_pct=50.0
        )
        main(["--format", "json"])

    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"
    assert "delta_pct" in data[0]
