"""Tests for pipewatch.cli_trend module."""
import json
import pytest
from unittest.mock import patch, MagicMock
from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.cli_trend import parse_args, main
from datetime import datetime


def make_metric(pipeline, name, value):
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime.utcnow(),
    )


def _mock_collector(metrics):
    collector = MagicMock()
    collector.get_history.return_value = metrics
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.format == "text"
    assert args.threshold == 0.01
    assert args.pipeline is None


def test_parse_args_custom():
    args = parse_args(["--format", "json", "--threshold", "0.05", "--pipeline", "etl"])
    assert args.format == "json"
    assert args.threshold == 0.05
    assert args.pipeline == "etl"


def test_main_text_output(capsys):
    metrics = [make_metric("pipe1", "latency", float(i)) for i in range(5)]
    with patch("pipewatch.cli_trend.load_config", return_value={}), \
         patch("pipewatch.cli_trend.build_collector_from_config", return_value=_mock_collector(metrics)):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "pipe1" in out
    assert "latency" in out


def test_main_json_output(capsys):
    metrics = [make_metric("pipe1", "latency", float(i)) for i in range(5)]
    with patch("pipewatch.cli_trend.load_config", return_value={}), \
         patch("pipewatch.cli_trend.build_collector_from_config", return_value=_mock_collector(metrics)):
        main(["--format", "json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe1"


def test_main_pipeline_filter(capsys):
    metrics = (
        [make_metric("pipe1", "latency", float(i)) for i in range(5)] +
        [make_metric("pipe2", "rows", float(i)) for i in range(5)]
    )
    with patch("pipewatch.cli_trend.load_config", return_value={}), \
         patch("pipewatch.cli_trend.build_collector_from_config", return_value=_mock_collector(metrics)):
        main(["--pipeline", "pipe1"])
    out = capsys.readouterr().out
    assert "pipe1" in out
    assert "pipe2" not in out


def test_main_no_data_exits(capsys):
    with patch("pipewatch.cli_trend.load_config", return_value={}), \
         patch("pipewatch.cli_trend.build_collector_from_config", return_value=_mock_collector([])):
        with pytest.raises(SystemExit) as exc:
            main([])
    assert exc.value.code == 0
    assert "No trend" in capsys.readouterr().out
