"""Tests for pipewatch.cli_audit."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_audit import main, parse_args
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(pipeline: str, name: str, value: float, status: MetricStatus) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=datetime.now(timezone.utc),
    )


def _mock_collector(metrics_by_key: dict):
    collector = MagicMock()
    collector.get_history.return_value = metrics_by_key
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.pipeline is None
    assert args.metric is None
    assert args.fmt == "text"


def test_parse_args_custom():
    args = parse_args(["--config", "c.yaml", "--pipeline", "p", "--metric", "m", "--format", "json"])
    assert args.config == "c.yaml"
    assert args.pipeline == "p"
    assert args.metric == "m"
    assert args.fmt == "json"


def test_main_text_output_shows_transition(capsys):
    history = {
        "pipe:rows": [
            make_metric("pipe", "rows", 10.0, MetricStatus.OK),
            make_metric("pipe", "rows", 3.0, MetricStatus.WARNING),
        ]
    }
    with patch("pipewatch.cli_audit.load_config", return_value={}), \
         patch("pipewatch.cli_audit.build_collector_from_config",
               return_value=_mock_collector(history)):
        main(["--format", "text"])

    out = capsys.readouterr().out
    assert "ok" in out
    assert "warning" in out
    assert "pipe/rows" in out


def test_main_json_output(capsys):
    history = {
        "pipe:rows": [
            make_metric("pipe", "rows", 10.0, MetricStatus.OK),
            make_metric("pipe", "rows", 1.0, MetricStatus.CRITICAL),
        ]
    }
    with patch("pipewatch.cli_audit.load_config", return_value={}), \
         patch("pipewatch.cli_audit.build_collector_from_config",
               return_value=_mock_collector(history)):
        main(["--format", "json"])

    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe"


def test_main_no_events_message(capsys):
    with patch("pipewatch.cli_audit.load_config", return_value={}), \
         patch("pipewatch.cli_audit.build_collector_from_config",
               return_value=_mock_collector({})):
        main([])

    out = capsys.readouterr().out
    assert "No audit events" in out
