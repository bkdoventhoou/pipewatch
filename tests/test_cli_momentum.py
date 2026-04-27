"""Tests for pipewatch.cli_momentum."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.cli_momentum import main, parse_args


def make_metric(value: float, ts: float, pipeline="pipe", name="rows") -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=ts,
    )


def _mock_collector(metrics_by_key: dict):
    col = MagicMock()
    col.get_history.return_value = metrics_by_key
    return col


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------

def test_parse_args_defaults():
    ns = parse_args([])
    assert ns.config == "pipewatch.yaml"
    assert ns.accel_threshold == pytest.approx(0.01)
    assert ns.format == "text"


def test_parse_args_custom():
    ns = parse_args(["--config", "my.yaml", "--accel-threshold", "0.5", "--format", "json"])
    assert ns.config == "my.yaml"
    assert ns.accel_threshold == pytest.approx(0.5)
    assert ns.format == "json"


# ---------------------------------------------------------------------------
# main – text output
# ---------------------------------------------------------------------------

def test_main_text_output_stable(capsys):
    history = {
        "pipe/rows": [make_metric(10.0, float(i)) for i in range(5)]
    }
    col = _mock_collector(history)
    with patch("pipewatch.cli_momentum.load_config", return_value={}), \
         patch("pipewatch.cli_momentum.build_collector_from_config", return_value=col):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "pipe" in out
    assert "stable" in out


def test_main_text_output_no_results(capsys):
    history = {
        "pipe/rows": [make_metric(1.0, float(i)) for i in range(2)]
    }
    col = _mock_collector(history)
    with patch("pipewatch.cli_momentum.load_config", return_value={}), \
         patch("pipewatch.cli_momentum.build_collector_from_config", return_value=col):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "No momentum" in out


def test_main_json_output(capsys):
    history = {
        "pipe/rows": [make_metric(float(i ** 2), float(i)) for i in range(6)]
    }
    col = _mock_collector(history)
    with patch("pipewatch.cli_momentum.load_config", return_value={}), \
         patch("pipewatch.cli_momentum.build_collector_from_config", return_value=col):
        main(["--format", "json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) == 1
    assert "second_derivative" in data[0]


def test_main_accel_threshold_respected(capsys):
    # quadratic series that accelerates; with huge threshold it should NOT flag
    history = {
        "pipe/rows": [make_metric(float(i ** 2), float(i)) for i in range(6)]
    }
    col = _mock_collector(history)
    with patch("pipewatch.cli_momentum.load_config", return_value={}), \
         patch("pipewatch.cli_momentum.build_collector_from_config", return_value=col):
        main(["--format", "text", "--accel-threshold", "999"])
    out = capsys.readouterr().out
    assert "ACCELERATING" not in out
