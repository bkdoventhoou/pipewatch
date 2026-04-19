"""Tests for pipewatch.cli_aggregate."""

from __future__ import annotations

import json
from unittest.mock import patch, MagicMock
import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.cli_aggregate import parse_args, main


def make_metric(pipeline="pipe1", name="rows", value=10.0, status=MetricStatus.OK):
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status)


def _mock_collector(metrics):
    """Build a mock collector whose _history mirrors the given metrics list."""
    collector = MagicMock()
    history = {}
    for m in metrics:
        history.setdefault(m.pipeline, []).append(m)
    collector._history = history
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.pipeline is None
    assert args.metric is None
    assert args.fmt == "text"


def test_parse_args_custom():
    args = parse_args(["--config", "custom.yaml", "--pipeline", "etl", "--format", "json"])
    assert args.config == "custom.yaml"
    assert args.pipeline == "etl"
    assert args.fmt == "json"


def test_main_text_output(capsys):
    metrics = [make_metric(value=50.0), make_metric(value=100.0)]
    collector = _mock_collector(metrics)
    with patch("pipewatch.cli_aggregate.load_config", return_value={}), \
         patch("pipewatch.cli_aggregate.build_collector_from_config", return_value=collector):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "pipe1" in out
    assert "rows" in out
    assert "count=2" in out


def test_main_json_output(capsys):
    metrics = [make_metric(value=75.0)]
    collector = _mock_collector(metrics)
    with patch("pipewatch.cli_aggregate.load_config", return_value={}), \
         patch("pipewatch.cli_aggregate.build_collector_from_config", return_value=collector):
        main(["--format", "json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe1"


def test_main_pipeline_filter(capsys):
    metrics = [
        make_metric(pipeline="pipe1", value=1.0),
        make_metric(pipeline="pipe2", value=2.0),
    ]
    collector = _mock_collector(metrics)
    with patch("pipewatch.cli_aggregate.load_config", return_value={}), \
         patch("pipewatch.cli_aggregate.build_collector_from_config", return_value=collector):
        main(["--pipeline", "pipe1", "--format", "text"])
    out = capsys.readouterr().out
    assert "pipe1" in out
    assert "pipe2" not in out


def test_main_metric_filter(capsys):
    """Only the requested metric name should appear in the output."""
    metrics = [
        make_metric(name="rows", value=10.0),
        make_metric(name="errors", value=3.0),
    ]
    collector = _mock_collector(metrics)
    with patch("pipewatch.cli_aggregate.load_config", return_value={}), \
         patch("pipewatch.cli_aggregate.build_collector_from_config", return_value=collector):
        main(["--metric", "rows", "--format", "text"])
    out = capsys.readouterr().out
    assert "rows" in out
    assert "errors" not in out


def test_main_no_metrics(capsys):
    collector = _mock_collector([])
    with patch("pipewatch.cli_aggregate.load_config", return_value={}), \
         patch("pipewatch.cli_aggregate.build_collector_from_config", return_value=collector):
        main([])
    out = capsys.readouterr().out
    assert "No metrics" in out
