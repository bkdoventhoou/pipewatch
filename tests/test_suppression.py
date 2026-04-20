"""Tests for pipewatch.suppression."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.suppression import SuppressionRegistry, SuppressionWindow


def make_metric(pipeline: str = "etl", name: str = "row_count", value: float = 100.0) -> PipelineMetric:
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=MetricStatus.WARNING)


def make_window(pipeline="etl", metric_name=None, duration=60.0, offset=0.0, reason="") -> SuppressionWindow:
    now = time.time() + offset
    return SuppressionWindow(
        pipeline=pipeline,
        metric_name=metric_name,
        start_ts=now - 1,
        end_ts=now + duration,
        reason=reason,
    )


class TestSuppressionWindow:
    def test_is_active_within_window(self):
        w = make_window(duration=60.0)
        assert w.is_active() is True

    def test_is_not_active_after_expiry(self):
        now = time.time()
        w = SuppressionWindow("etl", None, now - 120, now - 60)
        assert w.is_active() is False

    def test_matches_pipeline_any_metric(self):
        w = make_window(pipeline="etl", metric_name=None)
        m = make_metric(pipeline="etl", name="anything")
        assert w.matches(m) is True

    def test_does_not_match_wrong_pipeline(self):
        w = make_window(pipeline="etl")
        m = make_metric(pipeline="other")
        assert w.matches(m) is False

    def test_matches_specific_metric(self):
        w = make_window(pipeline="etl", metric_name="row_count")
        m = make_metric(pipeline="etl", name="row_count")
        assert w.matches(m) is True

    def test_does_not_match_wrong_metric(self):
        w = make_window(pipeline="etl", metric_name="row_count")
        m = make_metric(pipeline="etl", name="latency")
        assert w.matches(m) is False

    def test_to_dict_keys(self):
        w = make_window(reason="maintenance")
        d = w.to_dict()
        assert {"pipeline", "metric_name", "start_ts", "end_ts", "reason", "active"} == set(d.keys())
        assert d["reason"] == "maintenance"


class TestSuppressionRegistry:
    def test_is_suppressed_active_window(self):
        reg = SuppressionRegistry()
        reg.add(make_window(pipeline="etl"))
        m = make_metric(pipeline="etl")
        assert reg.is_suppressed(m) is True

    def test_is_not_suppressed_no_window(self):
        reg = SuppressionRegistry()
        m = make_metric(pipeline="etl")
        assert reg.is_suppressed(m) is False

    def test_is_not_suppressed_expired_window(self):
        reg = SuppressionRegistry()
        now = time.time()
        expired = SuppressionWindow("etl", None, now - 120, now - 60)
        reg.add(expired)
        m = make_metric(pipeline="etl")
        assert reg.is_suppressed(m) is False

    def test_prune_expired_removes_old(self):
        reg = SuppressionRegistry()
        now = time.time()
        reg.add(SuppressionWindow("etl", None, now - 120, now - 60))
        reg.add(make_window(pipeline="other", duration=300))
        removed = reg.prune_expired()
        assert removed == 1
        assert len(reg.all_windows()) == 1

    def test_active_windows_filters_expired(self):
        reg = SuppressionRegistry()
        now = time.time()
        reg.add(SuppressionWindow("old", None, now - 120, now - 60))
        reg.add(make_window(pipeline="live", duration=300))
        active = reg.active_windows()
        assert len(active) == 1
        assert active[0].pipeline == "live"
