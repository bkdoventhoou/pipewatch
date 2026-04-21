"""Tests for pipewatch.scoring."""
from __future__ import annotations

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.scoring import (
    PipelineScore,
    score_all,
    score_pipeline,
)


def make_metric(pipeline: str, name: str, status: str, value: float = 1.0) -> PipelineMetric:
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status)


# ---------------------------------------------------------------------------
# score_pipeline
# ---------------------------------------------------------------------------

class TestScorePipeline:
    def test_returns_none_for_empty_metrics(self):
        assert score_pipeline("pipe_a", []) is None

    def test_returns_none_when_pipeline_not_present(self):
        m = make_metric("pipe_b", "rows", MetricStatus.OK)
        assert score_pipeline("pipe_a", [m]) is None

    def test_all_ok_gives_100(self):
        metrics = [
            make_metric("p", "a", MetricStatus.OK),
            make_metric("p", "b", MetricStatus.OK),
        ]
        result = score_pipeline("p", metrics)
        assert result is not None
        assert result.score == pytest.approx(100.0)

    def test_all_critical_gives_0(self):
        metrics = [
            make_metric("p", "a", MetricStatus.CRITICAL),
            make_metric("p", "b", MetricStatus.CRITICAL),
        ]
        result = score_pipeline("p", metrics)
        assert result.score == pytest.approx(0.0)

    def test_mixed_gives_expected_score(self):
        # 1 OK (weight 1.0) + 1 WARNING (weight 0.5) + 1 CRITICAL (weight 0.0)
        # score = (1.5 / 3) * 100 = 50.0
        metrics = [
            make_metric("p", "a", MetricStatus.OK),
            make_metric("p", "b", MetricStatus.WARNING),
            make_metric("p", "c", MetricStatus.CRITICAL),
        ]
        result = score_pipeline("p", metrics)
        assert result.score == pytest.approx(50.0)

    def test_counts_are_correct(self):
        metrics = [
            make_metric("p", "a", MetricStatus.OK),
            make_metric("p", "b", MetricStatus.WARNING),
            make_metric("p", "c", MetricStatus.CRITICAL),
        ]
        result = score_pipeline("p", metrics)
        assert result.ok_count == 1
        assert result.warning_count == 1
        assert result.critical_count == 1
        assert result.total_metrics == 3


# ---------------------------------------------------------------------------
# grade property
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("score,expected_grade", [
    (100.0, "A"),
    (90.0, "A"),
    (89.9, "B"),
    (75.0, "B"),
    (74.9, "C"),
    (50.0, "C"),
    (49.9, "D"),
    (25.0, "D"),
    (24.9, "F"),
    (0.0, "F"),
])
def test_grade(score, expected_grade):
    ps = PipelineScore(
        pipeline="p", score=score,
        total_metrics=1, ok_count=0, warning_count=0, critical_count=0,
    )
    assert ps.grade == expected_grade


# ---------------------------------------------------------------------------
# score_all
# ---------------------------------------------------------------------------

def test_score_all_groups_by_pipeline():
    metrics = [
        make_metric("pipe_a", "rows", MetricStatus.OK),
        make_metric("pipe_b", "latency", MetricStatus.CRITICAL),
    ]
    results = score_all(metrics)
    pipelines = {r.pipeline for r in results}
    assert pipelines == {"pipe_a", "pipe_b"}


def test_score_all_empty_returns_empty():
    assert score_all([]) == []


def test_to_dict_keys():
    ps = PipelineScore(
        pipeline="p", score=75.0,
        total_metrics=4, ok_count=3, warning_count=1, critical_count=0,
    )
    d = ps.to_dict()
    assert set(d.keys()) == {
        "pipeline", "score", "total_metrics",
        "ok_count", "warning_count", "critical_count",
    }
