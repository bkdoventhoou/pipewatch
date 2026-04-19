"""Tests for AlertDispatcher and built-in handlers."""

import pytest
from unittest.mock import MagicMock
from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.alerts import Alert, AlertDispatcher
from pipewatch.handlers import console_handler, make_file_handler, build_handlers_from_config


def make_metric(name, value, status, pipeline="etl"):
    return PipelineMetric(
        name=name, value=value, status=status, pipeline=pipeline
    )


# --- AlertDispatcher ---

def test_no_alert_on_ok():
    d = AlertDispatcher()
    m = make_metric("rows", 100, MetricStatus.OK)
    assert d.dispatch(m) is None


def test_alert_on_warning():
    handler = MagicMock()
    d = AlertDispatcher(handlers=[handler])
    m = make_metric("rows", 5, MetricStatus.WARNING)
    alert = d.dispatch(m)
    assert alert is not None
    assert alert.status == MetricStatus.WARNING
    handler.assert_called_once_with(alert)


def test_alert_on_critical():
    handler = MagicMock()
    d = AlertDispatcher(handlers=[handler])
    m = make_metric("rows", 1, MetricStatus.CRITICAL)
    alert = d.dispatch(m)
    assert alert.status == MetricStatus.CRITICAL


def test_duplicate_suppression():
    handler = MagicMock()
    d = AlertDispatcher(handlers=[handler])
    m = make_metric("rows", 5, MetricStatus.WARNING)
    d.dispatch(m)
    d.dispatch(m)  # same status — should be suppressed
    assert handler.call_count == 1


def test_escalation_not_suppressed():
    handler = MagicMock()
    d = AlertDispatcher(handlers=[handler])
    d.dispatch(make_metric("rows", 5, MetricStatus.WARNING))
    d.dispatch(make_metric("rows", 0, MetricStatus.CRITICAL))
    assert handler.call_count == 2


# --- handlers ---

def test_console_handler_runs(capsys):
    alert = Alert("etl", "rows", MetricStatus.WARNING, 5.0, "test")
    console_handler(alert)  # should not raise
    captured = capsys.readouterr()
    assert "rows" in captured.err


def test_file_handler(tmp_path):
    log_file = str(tmp_path / "alerts.log")
    handler = make_file_handler(log_file)
    alert = Alert("etl", "rows", MetricStatus.CRITICAL, 0.0, "critical!")
    handler(alert)
    content = (tmp_path / "alerts.log").read_text()
    assert "rows" in content


def test_build_handlers_console_default():
    handlers = build_handlers_from_config({})
    assert console_handler in handlers


def test_build_handlers_no_console():
    handlers = build_handlers_from_config({"alerts": {"console": False}})
    assert console_handler not in handlers
