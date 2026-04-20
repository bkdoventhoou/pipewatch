"""Tests for pipewatch.cli_routing."""

import json
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.cli_routing import parse_args, main
from pipewatch.routing import RoutingRule
from pipewatch.metrics import MetricStatus


def _config_with_rules():
    return {
        "routing": [
            {"handler": "console", "min_status": "WARNING"},
            {"handler": "file", "pipeline": "pipe_a", "min_status": "CRITICAL"},
        ]
    }


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.format == "text"


def test_parse_args_custom():
    args = parse_args(["--config", "custom.yaml", "--format", "json"])
    assert args.config == "custom.yaml"
    assert args.format == "json"


def test_main_text_output(capsys):
    with patch("pipewatch.cli_routing.load_config", return_value=_config_with_rules()):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "console" in out
    assert "file" in out
    assert "pipe_a" in out


def test_main_json_output(capsys):
    with patch("pipewatch.cli_routing.load_config", return_value=_config_with_rules()):
        main(["--format", "json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) == 2
    handler_names = [r["handler_name"] for r in data]
    assert "console" in handler_names
    assert "file" in handler_names


def test_main_no_rules_message(capsys):
    with patch("pipewatch.cli_routing.load_config", return_value={}):
        main([])
    out = capsys.readouterr().out
    assert "No routing rules" in out
