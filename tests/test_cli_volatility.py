"""Tests for pipewatch.cli_volatility."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.volatility import analyze_volatility
from pipewatch.cli_volatility import parse_args, main


def make_metric(pipeline: str, name: str, value: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime.now(tz=timezone.utc),
    )


def _mock_collector(values):
    from pipewatch.collector import MetricCollector
    collector = MetricCollector()
    for v in values:
        collector.record(make_metric("pipe", "metric", v))
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.threshold_cv == pytest.approx(0.5)
    assert args.min_samples == 4
    assert args.format == "text"
    assert args.volatile_only is False


def test_parse_args_custom():
    args = parse_args([
        "--config", "custom.yaml",
        "--threshold-cv", "0.3",
        "--min-samples", "6",
        "--format", "json",
        "--volatile-only",
    ])
    assert args.config == "custom.yaml"
    assert args.threshold_cv == pytest.approx(0.3)
    assert args.min_samples == 6
    assert args.format == "json"
    assert args.volatile_only is True


def test_main_text_output(capsys):
    collector = _mock_collector([10.0, 10.0, 10.0, 10.0, 10.0])
    with patch("pipewatch.cli_volatility.load_config", return_value={}), \
         patch("pipewatch.cli_volatility.build_collector_from_config", return_value=collector):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "pipe/metric" in out


def test_main_json_output(capsys):
    collector = _mock_collector([10.0, 10.0, 10.0, 10.0, 10.0])
    with patch("pipewatch.cli_volatility.load_config", return_value={}), \
         patch("pipewatch.cli_volatility.build_collector_from_config", return_value=collector):
        main(["--format", "json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, dict)
    key = next(iter(data))
    assert "coefficient_of_variation" in data[key]


def test_main_volatile_only_filters(capsys):
    # stable series → not volatile → should be hidden
    collector = _mock_collector([10.0, 10.0, 10.0, 10.0, 10.0])
    with patch("pipewatch.cli_volatility.load_config", return_value={}), \
         patch("pipewatch.cli_volatility.build_collector_from_config", return_value=collector):
        main(["--volatile-only", "--format", "text"])
    out = capsys.readouterr().out
    # stable series is not volatile; output should say no data
    assert "pipe/metric" not in out or "No volatility" in out


def test_main_no_data_message(capsys):
    from pipewatch.collector import MetricCollector
    empty_collector = MetricCollector()
    with patch("pipewatch.cli_volatility.load_config", return_value={}), \
         patch("pipewatch.cli_volatility.build_collector_from_config", return_value=empty_collector):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "No volatility" in out
