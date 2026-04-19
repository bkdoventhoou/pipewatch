"""Tests for pipewatch.cli_baseline."""
import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.baseline import BaselineEntry


def make_metric(pipeline, name, value):
    return PipelineMetric(
        pipeline=pipeline, name=name, value=value,
        status=MetricStatus.OK, timestamp=datetime(2024, 1, 1),
    )


def _mock_collector(metrics):
    col = MagicMock()
    col.get_latest.return_value = metrics
    return col


def test_parse_args_defaults():
    from pipewatch.cli_baseline import parse_args
    args = parse_args(["capture"])
    assert args.config == "pipewatch.yaml"
    assert args.output == "baseline.json"


def test_parse_args_compare_custom():
    from pipewatch.cli_baseline import parse_args
    args = parse_args(["compare", "--baseline", "my.json", "--format", "json"])
    assert args.baseline == "my.json"
    assert args.format == "json"


def test_main_capture(tmp_path, capsys):
    from pipewatch.cli_baseline import main
    metrics = [make_metric("etl", "rows", 100.0)]
    out_path = str(tmp_path / "baseline.json")
    with patch("pipewatch.cli_baseline.load_config", return_value={}), \
         patch("pipewatch.cli_baseline.build_collector_from_config", return_value=_mock_collector(metrics)):
        main(["capture", "--output", out_path])
    captured = capsys.readouterr()
    assert "Baseline saved" in captured.out
    data = json.loads(open(out_path).read())
    assert data[0]["metric_name"] == "rows"


def test_main_compare_text(tmp_path, capsys):
    from pipewatch.cli_baseline import main
    from pipewatch.baseline import save_baseline
    bl_path = str(tmp_path / "baseline.json")
    save_baseline([BaselineEntry("etl", "rows", 100.0)], bl_path)
    metrics = [make_metric("etl", "rows", 110.0)]
    with patch("pipewatch.cli_baseline.load_config", return_value={}), \
         patch("pipewatch.cli_baseline.build_collector_from_config", return_value=_mock_collector(metrics)):
        main(["compare", "--baseline", bl_path])
    out = capsys.readouterr().out
    assert "etl/rows" in out
    assert "delta=+10.0" in out


def test_main_compare_json(tmp_path, capsys):
    from pipewatch.cli_baseline import main
    from pipewatch.baseline import save_baseline
    bl_path = str(tmp_path / "baseline.json")
    save_baseline([BaselineEntry("etl", "rows", 50.0)], bl_path)
    metrics = [make_metric("etl", "rows", 75.0)]
    with patch("pipewatch.cli_baseline.load_config", return_value={}), \
         patch("pipewatch.cli_baseline.build_collector_from_config", return_value=_mock_collector(metrics)):
        main(["compare", "--baseline", bl_path, "--format", "json"])
    data = json.loads(capsys.readouterr().out)
    assert data[0]["current_value"] == 75.0
