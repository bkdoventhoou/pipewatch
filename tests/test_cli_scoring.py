"""Tests for pipewatch.cli_scoring."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.cli_scoring import parse_args, main


def make_metric(pipeline: str, name: str, status: str, value: float = 1.0) -> PipelineMetric:
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status)


def _mock_collector(metrics_by_key: dict):
    collector = MagicMock()
    collector.get_history.return_value = metrics_by_key
    return collector


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def _run_main(argv, collector):
    with patch("pipewatch.cli_scoring.load_config", return_value={}):
        with patch("pipewatch.cli_scoring.build_collector_from_config", return_value=collector):
            main(argv)


def test_main_text_output(capsys):
    metrics = [
        make_metric("pipe_a", "rows", MetricStatus.OK),
        make_metric("pipe_a", "latency", MetricStatus.OK),
    ]
    collector = _mock_collector({"pipe_a:rows": metrics})
    _run_main(["--format", "text"], collector)
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "score=" in out


def test_main_json_output(capsys):
    metrics = [
        make_metric("pipe_b", "errors", MetricStatus.CRITICAL),
    ]
    collector = _mock_collector({"pipe_b:errors": metrics})
    _run_main(["--format", "json"], collector)
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_b"
    assert data[0]["score"] == pytest.approx(0.0)


def test_main_exits_on_no_data():
    collector = _mock_collector({})
    with pytest.raises(SystemExit) as exc:
        _run_main([], collector)
    assert exc.value.code == 1


def test_main_pipeline_filter(capsys):
    metrics_a = [make_metric("pipe_a", "rows", MetricStatus.OK)]
    metrics_b = [make_metric("pipe_b", "rows", MetricStatus.WARNING)]
    collector = _mock_collector({
        "pipe_a:rows": metrics_a,
        "pipe_b:rows": metrics_b,
    })
    _run_main(["--pipeline", "pipe_a", "--format", "text"], collector)
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "pipe_b" not in out
