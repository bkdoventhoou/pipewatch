"""Baseline comparison: compare current metrics against stored baselines."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class BaselineEntry:
    pipeline: str
    metric_name: str
    baseline_value: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class BaselineComparison:
    pipeline: str
    metric_name: str
    current_value: float
    baseline_value: float
    delta: float
    pct_change: Optional[float]

    def to_dict(self) -> dict:
        return asdict(self)


def load_baseline(path: str) -> Dict[tuple, BaselineEntry]:
    data = json.loads(Path(path).read_text())
    result = {}
    for item in data:
        entry = BaselineEntry(**item)
        result[(entry.pipeline, entry.metric_name)] = entry
    return result


def save_baseline(entries: List[BaselineEntry], path: str) -> None:
    Path(path).write_text(json.dumps([e.to_dict() for e in entries], indent=2))


def compare_to_baseline(
    metrics: List[PipelineMetric],
    baseline: Dict[tuple, BaselineEntry],
) -> List[BaselineComparison]:
    comparisons = []
    for m in metrics:
        key = (m.pipeline, m.name)
        entry = baseline.get(key)
        if entry is None:
            continue
        delta = m.value - entry.baseline_value
        pct = (delta / entry.baseline_value * 100) if entry.baseline_value != 0 else None
        comparisons.append(
            BaselineComparison(
                pipeline=m.pipeline,
                metric_name=m.name,
                current_value=m.value,
                baseline_value=entry.baseline_value,
                delta=round(delta, 6),
                pct_change=round(pct, 2) if pct is not None else None,
            )
        )
    return comparisons


def build_baseline_from_metrics(metrics: List[PipelineMetric]) -> List[BaselineEntry]:
    seen: Dict[tuple, float] = {}
    for m in metrics:
        seen[(m.pipeline, m.name)] = m.value
    return [
        BaselineEntry(pipeline=k[0], metric_name=k[1], baseline_value=v)
        for k, v in seen.items()
    ]
