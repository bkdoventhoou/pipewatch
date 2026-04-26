"""Tests for pipewatch.cli_outlier module."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.cli_outlier import main, parse_args


def make_metric(
    value: float,
    pipeline: str = "pipe",
    name: str = "rows",
    offset_s: int = 0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=offset_s),
    )


def _mock_collector(history: dict):
    collector = MagicMock()
    collector.get_history.return_value = history
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.multiplier == 1.5
    assert args.only_outliers is False
    assert args.format == "text"


def test_parse_args_custom():
    args = parse_args(["--multiplier", "2.0", "--only-outliers", "--format", "json"])
    assert args.multiplier == 2.0
    assert args.only_outliers is True
    assert args.format == "json"


def test_main_text_output(capsys):
    series = [make_metric(v, offset_s=i * 60) for i, v in enumerate([10, 11, 10.5, 11.5, 12.0])]
    history = {("pipe", "rows"): series}
    with patch("pipewatch.cli_outlier.build_collector_from_config",
               return_value=_mock_collector(history)):
        main(["--config", "pipewatch.yaml"])
    out = capsys.readouterr().out
    assert "pipe/rows" in out


def test_main_json_output(capsys):
    series = [make_metric(v, offset_s=i * 60) for i, v in enumerate([10, 11, 10.5, 11.5, 12.0])]
    history = {("pipe", "rows"): series}
    with patch("pipewatch.cli_outlier.build_collector_from_config",
               return_value=_mock_collector(history)):
        main(["--format", "json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert "is_outlier" in data[0]


def test_main_only_outliers_flag(capsys):
    normal = [make_metric(v, offset_s=i * 60) for i, v in enumerate([10, 11, 10.5, 11.5, 12.0])]
    spike = [make_metric(v, pipeline="p2", offset_s=i * 60) for i, v in enumerate([10, 11, 10.5, 11.5, 999.0])]
    history = {("pipe", "rows"): normal, ("p2", "rows"): spike}
    with patch("pipewatch.cli_outlier.build_collector_from_config",
               return_value=_mock_collector(history)):
        main(["--only-outliers"])
    out = capsys.readouterr().out
    assert "p2/rows" in out
    assert "pipe/rows" not in out


def test_main_no_data_message(capsys):
    history = {}
    with patch("pipewatch.cli_outlier.build_collector_from_config",
               return_value=_mock_collector(history)):
        main([])
    out = capsys.readouterr().out
    assert "No outlier data" in out
