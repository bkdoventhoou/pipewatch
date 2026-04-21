"""Tests for pipewatch.cli_sampling."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.cli_sampling import main, parse_args


def make_metric(pipeline: str, name: str, value: float, ts: float = 0.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=ts,
    )


def _mock_collector(history: dict):
    collector = MagicMock()
    collector.get_history.return_value = history
    return collector


def test_parse_args_defaults():
    args = parse_args(["--max-count", "5"])
    assert args.config == "pipewatch.yaml"
    assert args.max_count == 5
    assert args.every_nth is None
    assert args.format == "text"


def test_parse_args_custom():
    args = parse_args(["--every-nth", "2", "--format", "json", "--config", "x.yaml"])
    assert args.every_nth == 2
    assert args.format == "json"
    assert args.config == "x.yaml"


def test_main_exits_without_strategy(capsys):
    with pytest.raises(SystemExit):
        main([])


def test_main_text_output(capsys):
    history = {
        ("pipe", "rows"): [make_metric("pipe", "rows", float(i), ts=float(i)) for i in range(6)],
    }
    collector = _mock_collector(history)
    with (
        patch("pipewatch.cli_sampling.load_config", return_value={}),
        patch("pipewatch.cli_sampling.build_collector_from_config", return_value=collector),
    ):
        main(["--max-count", "3"])

    captured = capsys.readouterr()
    assert "pipe" in captured.out
    assert "rows" in captured.out
    assert "3 samples" in captured.out


def test_main_json_output(capsys):
    history = {
        ("pipe", "lag"): [make_metric("pipe", "lag", float(i), ts=float(i)) for i in range(4)],
    }
    collector = _mock_collector(history)
    with (
        patch("pipewatch.cli_sampling.load_config", return_value={}),
        patch("pipewatch.cli_sampling.build_collector_from_config", return_value=collector),
    ):
        main(["--max-count", "2", "--format", "json"])

    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert isinstance(data, list)
    assert data[0]["sample_count"] == 2
    assert data[0]["pipeline"] == "pipe"


def test_main_empty_history(capsys):
    collector = _mock_collector({})
    with (
        patch("pipewatch.cli_sampling.load_config", return_value={}),
        patch("pipewatch.cli_sampling.build_collector_from_config", return_value=collector),
    ):
        main(["--every-nth", "2"])

    captured = capsys.readouterr()
    assert "No series available" in captured.out
