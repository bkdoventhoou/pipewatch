"""Tests for pipewatch.digest."""
import pytest
from datetime import datetime

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.digest import build_digest, format_digest_text, DigestReport


def make_metric(pipeline: str, name: str, value: float, status: MetricStatus) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=datetime.utcnow().isoformat(),
    )


def test_build_digest_empty():
    digest = build_digest([])
    assert digest.total_pipelines == 0
    assert digest.healthy == 0
    assert digest.degraded == 0
    assert digest.critical == 0


def test_build_digest_single_healthy_pipeline():
    metrics = [
        make_metric("etl_a", "rows", 100, MetricStatus.OK),
        make_metric("etl_a", "lag", 0.1, MetricStatus.OK),
    ]
    digest = build_digest(metrics)
    assert digest.total_pipelines == 1
    assert digest.healthy == 1
    assert digest.degraded == 0
    assert digest.critical == 0


def test_build_digest_mixed_pipelines():
    metrics = [
        make_metric("etl_a", "rows", 100, MetricStatus.OK),
        make_metric("etl_b", "lag", 5.0, MetricStatus.WARNING),
        make_metric("etl_c", "errors", 99, MetricStatus.CRITICAL),
    ]
    digest = build_digest(metrics)
    assert digest.total_pipelines == 3
    assert digest.healthy == 1
    assert digest.degraded == 1
    assert digest.critical == 1


def test_build_digest_to_dict_keys():
    digest = build_digest([])
    d = digest.to_dict()
    assert "generated_at" in d
    assert "total_pipelines" in d
    assert "summaries" in d


def test_format_digest_text_contains_pipeline_name():
    metrics = [
        make_metric("my_pipeline", "rows", 50, MetricStatus.OK),
    ]
    digest = build_digest(metrics)
    text = format_digest_text(digest)
    assert "my_pipeline" in text
    assert "healthy" in text


def test_format_digest_text_header():
    digest = build_digest([])
    text = format_digest_text(digest)
    assert "Digest Report" in text
    assert "Pipelines:" in text
