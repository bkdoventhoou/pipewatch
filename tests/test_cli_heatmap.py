"""Tests for pipewatch.cli_heatmap."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_heatmap import parse_args, main
from pipewatch.metrics import PipelineMetric, MetricStatus

BASE_TS = datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc).timestamp()


def make_metric(pipeline, name, value, status):
    return PipelineMetric(
        pipeline=pipeline, name=name, value=value, status=status, timestamp=BASE_TS
    )


def _mock_collector(metrics_by_pipeline: dict):
    collector = MagicMock()
    collector.get_history.return_value = metrics_by_pipeline
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.pipeline is None
    assert args.format == "text"


def test_parse_args_custom():
    args = parse_args(["--pipeline", "etl", "--format", "json", "--config", "custom.yaml"])
    assert args.pipeline == "etl"
    assert args.format == "json"
    assert args.config == "custom.yaml"


def test_main_text_output(capsys):
    history = {
        "etl": [
            make_metric("etl", "rows", 100.0, MetricStatus.OK),
            make_metric("etl", "rows", 50.0, MetricStatus.WARNING),
        ]
    }
    collector = _mock_collector(history)
    with patch("pipewatch.cli_heatmap.load_config", return_value={}), \
         patch("pipewatch.cli_heatmap.build_collector_from_config", return_value=collector):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "etl" in out
    assert "WARNING" in out


def test_main_json_output(capsys):
    history = {
        "etl": [make_metric("etl", "rows", 100.0, MetricStatus.CRITICAL)]
    }
    collector = _mock_collector(history)
    with patch("pipewatch.cli_heatmap.load_config", return_value={}), \
         patch("pipewatch.cli_heatmap.build_collector_from_config", return_value=collector):
        main(["--format", "json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "etl"
    assert data[0]["critical"] == 1


def test_main_empty_history(capsys):
    collector = _mock_collector({})
    with patch("pipewatch.cli_heatmap.load_config", return_value={}), \
         patch("pipewatch.cli_heatmap.build_collector_from_config", return_value=collector):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "No heatmap data" in out
