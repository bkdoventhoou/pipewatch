"""Tests for pipewatch.notification module."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.alerts import Alert
from pipewatch.notification import NotificationChannel, NotificationManager


def make_alert(pipeline="etl", metric="row_count", status="WARNING") -> Alert:
    m = PipelineMetric(
        pipeline=pipeline,
        name=metric,
        value=42.0,
        status=MetricStatus[status],
    )
    return Alert(metric=m, message=f"{metric} is {status}")


# --- NotificationChannel.accepts ---

def test_channel_accepts_warning_when_min_warning():
    ch = NotificationChannel(name="ch", handler=MagicMock(), min_status="warning")
    assert ch.accepts(make_alert(status="WARNING")) is True


def test_channel_accepts_critical_when_min_warning():
    ch = NotificationChannel(name="ch", handler=MagicMock(), min_status="warning")
    assert ch.accepts(make_alert(status="CRITICAL")) is True


def test_channel_rejects_ok_when_min_warning():
    ch = NotificationChannel(name="ch", handler=MagicMock(), min_status="warning")
    assert ch.accepts(make_alert(status="OK")) is False


def test_channel_rejects_warning_when_min_critical():
    ch = NotificationChannel(name="ch", handler=MagicMock(), min_status="critical")
    assert ch.accepts(make_alert(status="WARNING")) is False


def test_channel_accepts_critical_when_min_critical():
    ch = NotificationChannel(name="ch", handler=MagicMock(), min_status="critical")
    assert ch.accepts(make_alert(status="CRITICAL")) is True


def test_channel_filters_by_pipeline_match():
    ch = NotificationChannel(name="ch", handler=MagicMock(), pipelines=["etl"])
    assert ch.accepts(make_alert(pipeline="etl")) is True


def test_channel_filters_by_pipeline_no_match():
    ch = NotificationChannel(name="ch", handler=MagicMock(), pipelines=["other"])
    assert ch.accepts(make_alert(pipeline="etl")) is False


def test_channel_accepts_all_pipelines_when_none():
    ch = NotificationChannel(name="ch", handler=MagicMock(), pipelines=None)
    assert ch.accepts(make_alert(pipeline="anything")) is True


# --- NotificationChannel.send ---

def test_send_calls_handler_when_accepted():
    handler = MagicMock()
    ch = NotificationChannel(name="ch", handler=handler, min_status="warning")
    alert = make_alert(status="WARNING")
    ch.send(alert)
    handler.assert_called_once_with(alert)


def test_send_does_not_call_handler_when_rejected():
    handler = MagicMock()
    ch = NotificationChannel(name="ch", handler=handler, min_status="critical")
    alert = make_alert(status="WARNING")
    ch.send(alert)
    handler.assert_not_called()


# --- NotificationManager ---

def test_manager_notify_returns_channel_names():
    h1, h2 = MagicMock(), MagicMock()
    mgr = NotificationManager()
    mgr.register(NotificationChannel("slack", h1, min_status="warning"))
    mgr.register(NotificationChannel("pagerduty", h2, min_status="critical"))
    notified = mgr.notify(make_alert(status="WARNING"))
    assert notified == ["slack"]
    h1.assert_called_once()
    h2.assert_not_called()


def test_manager_notify_all_channels_on_critical():
    h1, h2 = MagicMock(), MagicMock()
    mgr = NotificationManager()
    mgr.register(NotificationChannel("slack", h1, min_status="warning"))
    mgr.register(NotificationChannel("pagerduty", h2, min_status="critical"))
    notified = mgr.notify(make_alert(status="CRITICAL"))
    assert set(notified) == {"slack", "pagerduty"}


def test_manager_get_channel_returns_correct():
    mgr = NotificationManager()
    ch = NotificationChannel("email", MagicMock())
    mgr.register(ch)
    assert mgr.get_channel("email") is ch


def test_manager_get_channel_returns_none_for_unknown():
    mgr = NotificationManager()
    assert mgr.get_channel("missing") is None


def test_manager_channel_names_lists_all():
    mgr = NotificationManager()
    mgr.register(NotificationChannel("a", MagicMock()))
    mgr.register(NotificationChannel("b", MagicMock()))
    assert mgr.channel_names() == ["a", "b"]
