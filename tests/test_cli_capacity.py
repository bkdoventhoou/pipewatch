"""Tests for pipewatch.cli_capacity module."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_capacity import main, parse_args
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(pipeline: str, name: str, value: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime.utcnow(),
    )


def _mock_collector(metrics_by_key: dict):
    collector = MagicMock()
    collector.get_history.return_value = metrics_by_key
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.threshold == 1.0
    assert args.format == "text"
    assert args.pipeline is None


def test_parse_args_custom():
    args = parse_args(["--config", "custom.yaml", "--threshold", "0.9", "--format", "json"])
    assert args.config == "custom.yaml"
    assert args.threshold == 0.9
    assert args.format == "json"


def test_main_exits_when_no_limits(capsys):
    config = {"capacity": {"limits": {}}}
    with patch("pipewatch.cli_capacity.load_config", return_value=config):
        with pytest.raises(SystemExit) as exc:
            main(["--config", "pipewatch.yaml"])
        assert exc.value.code == 1


def test_main_text_output(capsys):
    m = make_metric("etl", "row_count", 800.0)
    config = {"capacity": {"limits": {"row_count": 1000.0}}}
    collector = _mock_collector({"etl:row_count": [m]})
    with patch("pipewatch.cli_capacity.load_config", return_value=config), \
         patch("pipewatch.cli_capacity.build_collector_from_config", return_value=collector):
        main(["--config", "pipewatch.yaml"])
    out = capsys.readouterr().out
    assert "etl/row_count" in out
    assert "80.0%" in out


def test_main_json_output(capsys):
    m = make_metric("etl", "row_count", 1100.0)
    config = {"capacity": {"limits": {"row_count": 1000.0}}}
    collector = _mock_collector({"etl:row_count": [m]})
    with patch("pipewatch.cli_capacity.load_config", return_value=config), \
         patch("pipewatch.cli_capacity.build_collector_from_config", return_value=collector):
        main(["--config", "pipewatch.yaml", "--format", "json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["breached"] == 1
    assert data["entries"][0]["breached"] is True


def test_main_pipeline_filter(capsys):
    m1 = make_metric("etl", "row_count", 500.0)
    m2 = make_metric("other", "row_count", 900.0)
    config = {"capacity": {"limits": {"row_count": 1000.0}}}
    collector = _mock_collector({"etl:row_count": [m1], "other:row_count": [m2]})
    with patch("pipewatch.cli_capacity.load_config", return_value=config), \
         patch("pipewatch.cli_capacity.build_collector_from_config", return_value=collector):
        main(["--config", "pipewatch.yaml", "--pipeline", "etl"])
    out = capsys.readouterr().out
    assert "etl/row_count" in out
    assert "other" not in out
