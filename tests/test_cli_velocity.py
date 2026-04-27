"""Tests for pipewatch.cli_velocity."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.cli_velocity import main, parse_args

_BASE = datetime(2024, 1, 1, 12, 0, 0)


def make_metric(value, offset=0, pipeline="pipe_a", name="rows"):
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=_BASE + timedelta(seconds=offset),
    )


def _mock_collector(history: dict):
    collector = MagicMock()
    collector.get_history.return_value = history
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.format == "text"
    assert args.pipeline is None


def test_parse_args_custom():
    args = parse_args(["--config", "custom.yaml", "--format", "json", "--pipeline", "etl"])
    assert args.config == "custom.yaml"
    assert args.format == "json"
    assert args.pipeline == "etl"


def test_main_text_output(capsys):
    history = {
        "pipe_a:rows": [make_metric(0.0, 0), make_metric(120.0, 60)],
    }
    with patch("pipewatch.cli_velocity.load_config", return_value={}):
        with patch(
            "pipewatch.cli_velocity.build_collector_from_config",
            return_value=_mock_collector(history),
        ):
            main(["--format", "text"])

    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "rising" in out


def test_main_json_output(capsys):
    history = {
        "pipe_a:rows": [make_metric(0.0, 0), make_metric(60.0, 60)],
    }
    with patch("pipewatch.cli_velocity.load_config", return_value={}):
        with patch(
            "pipewatch.cli_velocity.build_collector_from_config",
            return_value=_mock_collector(history),
        ):
            main(["--format", "json"])

    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_a"
    assert "velocity" in data[0]


def test_main_pipeline_filter(capsys):
    history = {
        "pipe_a:rows": [make_metric(0.0, 0), make_metric(10.0, 10)],
        "pipe_b:rows": [
            make_metric(0.0, 0, pipeline="pipe_b"),
            make_metric(5.0, 10, pipeline="pipe_b"),
        ],
    }
    with patch("pipewatch.cli_velocity.load_config", return_value={}):
        with patch(
            "pipewatch.cli_velocity.build_collector_from_config",
            return_value=_mock_collector(history),
        ):
            main(["--pipeline", "pipe_a", "--format", "json"])

    data = json.loads(capsys.readouterr().out)
    assert all(r["pipeline"] == "pipe_a" for r in data)


def test_main_no_data_text(capsys):
    with patch("pipewatch.cli_velocity.load_config", return_value={}):
        with patch(
            "pipewatch.cli_velocity.build_collector_from_config",
            return_value=_mock_collector({}),
        ):
            main([])

    out = capsys.readouterr().out
    assert "No velocity data" in out
