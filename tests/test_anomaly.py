import pytest
from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.anomaly import detect_anomaly, detect_all_anomalies, AnomalyResult


def make_metric(pipeline="etl", name="row_count", value=100.0):
    return PipelineMetric(
        pipeline=pipeline, name=name, value=value, status=MetricStatus.OK
    )


def test_detect_anomaly_returns_none_on_too_few():
    metrics = [make_metric(value=v) for v in [100, 200]]
    assert detect_anomaly(metrics) is None


def test_detect_anomaly_returns_none_on_zero_std():
    metrics = [make_metric(value=100) for _ in range(5)]
    assert detect_anomaly(metrics) is None


def test_detect_anomaly_not_anomalous():
    values = [100, 102, 101, 99, 100]
    metrics = [make_metric(value=v) for v in values]
    result = detect_anomaly(metrics, threshold=2.5)
    assert isinstance(result, AnomalyResult)
    assert result.is_anomaly is False


def test_detect_anomaly_detects_spike():
    values = [100, 101, 100, 99, 100, 500]
    metrics = [make_metric(value=v) for v in values]
    result = detect_anomaly(metrics, threshold=2.5)
    assert result is not None
    assert result.is_anomaly is True
    assert result.value == 500


def test_detect_anomaly_z_score_positive():
    values = [10, 10, 10, 10, 100]
    metrics = [make_metric(value=v) for v in values]
    result = detect_anomaly(metrics)
    assert result.z_score > 0


def test_detect_anomaly_to_dict_keys():
    values = [100, 101, 99, 102, 500]
    metrics = [make_metric(value=v) for v in values]
    result = detect_anomaly(metrics)
    d = result.to_dict()
    for key in ["pipeline", "metric_name", "value", "mean", "std", "z_score", "is_anomaly"]:
        assert key in d


def test_detect_all_anomalies_groups_by_key():
    history = {
        ("etl", "rows"): [make_metric("etl", "rows", v) for v in [100, 101, 99, 500]],
        ("etl", "errors"): [make_metric("etl", "errors", v) for v in [1, 1, 1, 1, 1]],
    }
    results = detect_all_anomalies(history, threshold=2.0)
    pipelines = [(r.pipeline, r.metric_name) for r in results]
    assert ("etl", "rows") in pipelines


def test_detect_all_anomalies_skips_zero_std():
    history = {
        ("etl", "stable"): [make_metric(value=50) for _ in range(5)],
    }
    results = detect_all_anomalies(history)
    assert results == []
