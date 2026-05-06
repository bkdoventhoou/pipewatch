"""Tests for pipewatch.symmetry."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List
from unittest.mock import MagicMock

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.symmetry import SymmetryResult, analyze_symmetry, analyze_all_symmetries


def make_metric(value: float, pipeline: str = "pipe", name: str = "rows") -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime.now(timezone.utc),
    )


def make_series(values: List[float]) -> List[PipelineMetric]:
    return [make_metric(v) for v in values]


# ---------------------------------------------------------------------------
# analyze_symmetry
# ---------------------------------------------------------------------------

def test_returns_none_on_too_few_points():
    assert analyze_symmetry(make_series([1.0, 2.0])) is None


def test_returns_none_on_all_identical_values():
    # mean == every value → above == below == 0
    assert analyze_symmetry(make_series([5.0, 5.0, 5.0])) is None


def test_perfectly_symmetric_series():
    # [1, 2, 3, 4, 5] → mean=3, above={4,5}, below={1,2} → ratio=0.5
    result = analyze_symmetry(make_series([1.0, 2.0, 3.0, 4.0, 5.0]))
    assert result is not None
    assert result.above_count == 2
    assert result.below_count == 2
    assert result.ratio == pytest.approx(0.5)
    assert result.is_symmetric is True


def test_right_skewed_series_not_symmetric():
    # Many values below mean, few above
    values = [1.0, 1.0, 1.0, 1.0, 10.0]
    result = analyze_symmetry(values=make_series(values), tolerance=0.15)
    assert result is not None
    assert result.is_symmetric is False
    assert result.above_count < result.below_count


def test_result_fields_populated_correctly():
    metrics = make_series([2.0, 4.0, 6.0])
    result = analyze_symmetry(metrics)
    assert result is not None
    assert result.pipeline == "pipe"
    assert result.metric_name == "rows"
    assert result.sample_count == 3
    assert result.mean == pytest.approx(4.0)


def test_to_dict_contains_expected_keys():
    result = analyze_symmetry(make_series([1.0, 3.0, 5.0]))
    assert result is not None
    d = result.to_dict()
    for key in ("pipeline", "metric_name", "mean", "above_count",
                "below_count", "ratio", "is_symmetric", "sample_count"):
        assert key in d


def test_custom_tolerance_widens_symmetric_band():
    # ratio ~0.33 (1 above, 2 below) — not symmetric at 0.15 but is at 0.20
    values = [1.0, 2.0, 10.0]  # mean ~4.33; above={10}, below={1,2}
    tight = analyze_symmetry(make_series(values), tolerance=0.10)
    loose = analyze_symmetry(make_series(values), tolerance=0.25)
    assert tight is not None and tight.is_symmetric is False
    assert loose is not None and loose.is_symmetric is True


# ---------------------------------------------------------------------------
# analyze_all_symmetries
# ---------------------------------------------------------------------------

def _mock_collector(histories):
    collector = MagicMock()
    collector.get_all_history.return_value = histories
    return collector


def test_analyze_all_skips_too_short_series():
    histories = {
        "pipe:rows": make_series([1.0, 2.0]),  # too short
    }
    results = analyze_all_symmetries(_mock_collector(histories))
    assert results == []


def test_analyze_all_returns_result_per_valid_series():
    histories = {
        "pipe:rows": make_series([1.0, 2.0, 3.0, 4.0, 5.0]),
        "pipe:errors": make_series([10.0, 10.0, 10.0]),  # identical → None
    }
    results = analyze_all_symmetries(_mock_collector(histories))
    assert len(results) == 1
    assert results[0].metric_name == "rows"
