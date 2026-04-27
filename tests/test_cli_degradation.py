"""Tests for pipewatch.cli_degradation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.cli_degradation import parse_args, main


def make_metric(status: MetricStatus, pipeline="pipe", name="rows") -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=1.0,
        status=status,
        timestamp=datetime.now(timezone.utc),
    )


def _mock_collector(history: dict):
    mc = MagicMock()
    mc.get_all_history.return_value = history
    return mc


# --- parse_args ---

def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.min_samples == 4
    assert args.slope_threshold == pytest.approx(0.1)
    assert args.fmt == "text"
    assert args.only_degrading is False


def test_parse_args_custom():
    args = parse_args([
        "--min-samples", "6",
        "--slope-threshold", "0.25",
        "--format", "json",
        "--only-degrading",
    ])
    assert args.min_samples == 6
    assert args.slope_threshold == pytest.approx(0.25)
    assert args.fmt == "json"
    assert args.only_degrading is True


# --- main ---

def _run_main(history, extra_args=None, config_thresholds=None):
    cfg = {"thresholds": config_thresholds or {}}
    collector = _mock_collector(history)
    with patch("pipewatch.cli_degradation.load_config", return_value=cfg), \
         patch("pipewatch.cli_degradation.MetricCollector", return_value=collector):
        argv = ["--min-samples", "4"] + (extra_args or [])
        main(argv)


def test_main_text_output_no_results(capsys):
    _run_main({})
    out = capsys.readouterr().out
    assert "No degradation results" in out


def test_main_text_output_stable(capsys):
    series = [make_metric(MetricStatus.OK)] * 5
    _run_main({"pipe/rows": series})
    out = capsys.readouterr().out
    assert "stable" in out
    assert "pipe" in out


def test_main_text_output_degrading(capsys):
    series = [
        make_metric(MetricStatus.OK),
        make_metric(MetricStatus.WARNING),
        make_metric(MetricStatus.CRITICAL),
        make_metric(MetricStatus.CRITICAL),
    ]
    _run_main({"pipe/rows": series})
    out = capsys.readouterr().out
    assert "DEGRADING" in out


def test_main_json_output(capsys):
    series = [make_metric(MetricStatus.OK)] * 5
    _run_main({"pipe/rows": series}, extra_args=["--format", "json"])
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert "degrading" in data[0]


def test_main_only_degrading_filters(capsys):
    ok_series = [make_metric(MetricStatus.OK)] * 5
    bad_series = [
        make_metric(MetricStatus.OK),
        make_metric(MetricStatus.WARNING),
        make_metric(MetricStatus.CRITICAL),
        make_metric(MetricStatus.CRITICAL),
    ]
    _run_main(
        {"pipe/rows": ok_series, "pipe/errors": bad_series},
        extra_args=["--only-degrading"],
    )
    out = capsys.readouterr().out
    assert "DEGRADING" in out
    # stable line must not appear
    assert out.count("stable") == 0
