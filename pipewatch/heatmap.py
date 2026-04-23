"""Heatmap: aggregate metric status counts into a time-bucketed grid per pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class HeatmapCell:
    bucket: str          # ISO-formatted hour bucket, e.g. "2024-01-15T14"
    pipeline: str
    ok: int = 0
    warning: int = 0
    critical: int = 0

    @property
    def total(self) -> int:
        return self.ok + self.warning + self.critical

    @property
    def dominant_status(self) -> str:
        if self.critical:
            return "critical"
        if self.warning:
            return "warning"
        return "ok"

    def to_dict(self) -> dict:
        return {
            "bucket": self.bucket,
            "pipeline": self.pipeline,
            "ok": self.ok,
            "warning": self.warning,
            "critical": self.critical,
            "total": self.total,
            "dominant_status": self.dominant_status,
        }


def _hour_bucket(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H")


def build_heatmap(
    history: Dict[str, List[PipelineMetric]],
    pipeline: Optional[str] = None,
) -> List[HeatmapCell]:
    """Build a list of HeatmapCells from collector history.

    Args:
        history: mapping of pipeline_name -> list of PipelineMetric.
        pipeline: if given, restrict output to that pipeline.

    Returns:
        Sorted list of HeatmapCell (by bucket then pipeline).
    """
    cells: Dict[tuple, HeatmapCell] = {}

    for pipe_name, metrics in history.items():
        if pipeline and pipe_name != pipeline:
            continue
        for m in metrics:
            bucket = _hour_bucket(m.timestamp)
            key = (bucket, pipe_name)
            if key not in cells:
                cells[key] = HeatmapCell(bucket=bucket, pipeline=pipe_name)
            cell = cells[key]
            if m.status == MetricStatus.OK:
                cell.ok += 1
            elif m.status == MetricStatus.WARNING:
                cell.warning += 1
            elif m.status == MetricStatus.CRITICAL:
                cell.critical += 1

    return sorted(cells.values(), key=lambda c: (c.bucket, c.pipeline))
