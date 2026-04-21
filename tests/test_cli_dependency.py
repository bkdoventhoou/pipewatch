"""Tests for pipewatch.cli_dependency."""
import json
from unittest.mock import MagicMock, patch
from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.cli_dependency import parse_args, main


def make_metric(pipeline, name, value, status):
    m = PipelineMetric(pipeline=pipeline, name=name, value=value)
    m.status = status
    return m


def _mock_collector(metrics):
    collector = MagicMock()
    collector.get_history.return_value = metrics
    collector.pipelines = list({m.pipeline for m in metrics})
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.format == "text"


def test_parse_args_custom():
    args = parse_args(["--config", "my.yaml", "--format", "json"])
    assert args.config == "my.yaml"
    assert args.format == "json"


def test_main_text_output(capsys):
    metrics = [
        make_metric("etl_a", "rows", 100, MetricStatus.OK),
        make_metric("etl_b", "rows", 0, MetricStatus.CRITICAL),
    ]
    cfg = {
        "pipelines": {
            "etl_a": {"depends_on": ["etl_b"]},
            "etl_b": {},
        },
        "thresholds": {},
    }
    with patch("pipewatch.cli_dependency.load_config", return_value=cfg), \
         patch("pipewatch.cli_dependency.build_collector_from_config") as mock_bc, \
         patch("pipewatch.cli_dependency.build_report") as mock_br:
        entry_a = MagicMock(pipeline="etl_a", status=MetricStatus.OK)
        entry_b = MagicMock(pipeline="etl_b", status=MetricStatus.CRITICAL)
        mock_br.return_value.entries = [entry_a, entry_b]
        main(["--format", "text"])

    captured = capsys.readouterr()
    assert "etl_a" in captured.out
    assert "etl_b" in captured.out


def test_main_json_output(capsys):
    cfg = {
        "pipelines": {
            "pipe_x": {"depends_on": []},
        },
        "thresholds": {},
    }
    with patch("pipewatch.cli_dependency.load_config", return_value=cfg), \
         patch("pipewatch.cli_dependency.build_collector_from_config"), \
         patch("pipewatch.cli_dependency.build_report") as mock_br:
        entry = MagicMock(pipeline="pipe_x", status=MetricStatus.WARNING)
        mock_br.return_value.entries = [entry]
        main(["--format", "json"])

    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert "pipe_x" in data
    assert "propagated_status" in data["pipe_x"]
