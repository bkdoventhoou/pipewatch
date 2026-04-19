"""Tests for pipewatch.cli_correlation."""
from unittest.mock import patch, MagicMock
from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.correlation import CorrelationResult
from datetime import datetime
import json


def make_metric(pipeline, name, value):
    return PipelineMetric(
        pipeline=pipeline, name=name, value=value,
        status=MetricStatus.OK, timestamp=datetime.utcnow(),
    )


def _mock_collector(metrics_by_name):
    col = MagicMock()
    col.get_metric_names.return_value = list(metrics_by_name.keys())
    col.get_history.side_effect = lambda n: metrics_by_name.get(n, [])
    return col


def test_parse_args_defaults():
    from pipewatch.cli_correlation import parse_args
    args = parse_args(["--pipeline", "etl"])
    assert args.pipeline == "etl"
    assert args.format == "text"
    assert args.metric_a is None


def test_parse_args_custom():
    from pipewatch.cli_correlation import parse_args
    args = parse_args(["--pipeline", "etl", "--metric-a", "rows", "--metric-b", "errors", "--format", "json"])
    assert args.metric_a == "rows"
    assert args.metric_b == "errors"


def test_main_text_output(capsys):
    from pipewatch.cli_correlation import main
    metrics = {
        "rows": [make_metric("etl", "rows", v) for v in [1, 2, 3]],
        "errors": [make_metric("etl", "errors", v) for v in [2, 4, 6]],
    }
    col = _mock_collector(metrics)
    with patch("pipewatch.cli_correlation.load_config", return_value={}), \
         patch("pipewatch.cli_correlation.build_collector_from_config", return_value=col):
        main(["--pipeline", "etl"])
    out = capsys.readouterr().out
    assert "etl" in out
    assert "rows" in out


def test_main_json_output(capsys):
    from pipewatch.cli_correlation import main
    metrics = {
        "rows": [make_metric("etl", "rows", v) for v in [1, 2, 3]],
        "errors": [make_metric("etl", "errors", v) for v in [2, 4, 6]],
    }
    col = _mock_collector(metrics)
    with patch("pipewatch.cli_correlation.load_config", return_value={}), \
         patch("pipewatch.cli_correlation.build_collector_from_config", return_value=col):
        main(["--pipeline", "etl", "--format", "json"])
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert "coefficient" in data[0]


def test_main_no_data_message(capsys):
    from pipewatch.cli_correlation import main
    col = _mock_collector({})
    with patch("pipewatch.cli_correlation.load_config", return_value={}), \
         patch("pipewatch.cli_correlation.build_collector_from_config", return_value=col):
        main(["--pipeline", "etl"])
    assert "No correlation" in capsys.readouterr().out
