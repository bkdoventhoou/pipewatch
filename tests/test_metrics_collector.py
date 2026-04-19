"""Tests for metric models and collector logic."""
import pytest
from datetime import datetime
from pipewatch.metrics import PipelineMetric, ThresholdConfig, MetricStatus
from pipewatch.collector import MetricCollector


def make_metric(name="etl_job", metric="row_count", value=100.0):
    return PipelineMetric(pipeline_name=name, metric_name=metric, value=value)


class TestThresholdConfig:
    def test_ok_status(self):
        t = ThresholdConfig("row_count", warning=500, critical=1000)
        assert t.evaluate(100) == MetricStatus.OK

    def test_warning_status(self):
        t = ThresholdConfig("row_count", warning=500, critical=1000)
        assert t.evaluate(600) == MetricStatus.WARNING

    def test_critical_status(self):
        t = ThresholdConfig("row_count", warning=500, critical=1000)
        assert t.evaluate(1500) == MetricStatus.CRITICAL

    def test_lt_comparison(self):
        t = ThresholdConfig("throughput", warning=50, critical=10, comparison="lt")
        assert t.evaluate(5) == MetricStatus.CRITICAL
        assert t.evaluate(30) == MetricStatus.WARNING
        assert t.evaluate(100) == MetricStatus.OK


class TestMetricCollector:
    def test_record_unknown_without_threshold(self):
        collector = MetricCollector()
        m = collector.record(make_metric())
        assert m.status == MetricStatus.UNKNOWN

    def test_record_evaluates_threshold(self):
        t = ThresholdConfig("row_count", warning=50, critical=200)
        collector = MetricCollector(thresholds=[t])
        m = collector.record(make_metric(value=300))
        assert m.status == MetricStatus.CRITICAL

    def test_history_filtered_by_pipeline(self):
        collector = MetricCollector()
        collector.record(make_metric(name="pipe_a"))
        collector.record(make_metric(name="pipe_b"))
        assert len(collector.get_history("pipe_a")) == 1

    def test_latest_returns_most_recent(self):
        collector = MetricCollector()
        collector.record(make_metric(value=10))
        collector.record(make_metric(value=99))
        latest = collector.latest()
        assert len(latest) == 1
        assert latest[0].value == 99

    def test_clear_resets_history(self):
        collector = MetricCollector()
        collector.record(make_metric())
        collector.clear()
        assert collector.get_history() == []

    def test_metric_to_dict(self):
        m = make_metric()
        d = m.to_dict()
        assert d["pipeline_name"] == "etl_job"
        assert "timestamp" in d
        assert d["status"] == MetricStatus.UNKNOWN.value
