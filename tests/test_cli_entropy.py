"""Tests for pipewatch.cli_entropy."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_entropy import parse_args, main
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(pipeline: str, name: str, value: float, ts: float = 0.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        metric_name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime.fromtimestamp(ts, tz=timezone.utc),
    )


def _mock_collector(history: dict) -> MagicMock:
    collector = MagicMock()
    collector.get_history.return_value = history
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.threshold == 0.75
    assert args.bins == 10
    assert args.fmt == "text"


def test_parse_args_custom():
    args = parse_args(["--threshold", "0.5", "--bins", "5", "--format", "json"])
    assert args.threshold == 0.5
    assert args.bins == 5
    assert args.fmt == "json"


def test_main_text_output(capsys):
    values = [float(i) for i in range(50)]
    history = {
        "p|m": [make_metric("p", "m", v, float(i)) for i, v in enumerate(values)]
    }
    collector = _mock_collector(history)
    with patch("pipewatch.cli_entropy.build_collector_from_config", return_value=collector):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "p/m" in out
    assert "entropy=" in out


def test_main_json_output(capsys):
    values = [float(i) for i in range(50)]
    history = {
        "p|m": [make_metric("p", "m", v, float(i)) for i, v in enumerate(values)]
    }
    collector = _mock_collector(history)
    with patch("pipewatch.cli_entropy.build_collector_from_config", return_value=collector):
        main(["--format", "json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "p"
    assert "normalised" in data[0]


def test_main_empty_history_text(capsys):
    collector = _mock_collector({})
    with patch("pipewatch.cli_entropy.build_collector_from_config", return_value=collector):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "No entropy results" in out


def test_main_high_entropy_flag_shown(capsys):
    # uniform distribution → high entropy
    values = [float(i) for i in range(100)]
    history = {
        "p|m": [make_metric("p", "m", v, float(i)) for i, v in enumerate(values)]
    }
    collector = _mock_collector(history)
    with patch("pipewatch.cli_entropy.build_collector_from_config", return_value=collector):
        main(["--threshold", "0.5", "--format", "text"])
    out = capsys.readouterr().out
    assert "[HIGH]" in out
