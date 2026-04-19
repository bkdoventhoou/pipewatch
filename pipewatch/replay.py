"""Replay historical metric snapshots for debugging and analysis."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
import json
from pathlib import Path
from pipewatch.snapshot import PipelineSnapshot, load_snapshot
from pipewatch.collector import MetricCollector
from pipewatch.metrics import PipelineMetric


@dataclass
class ReplayFrame:
    index: int
    snapshot: PipelineSnapshot

    def to_dict(self) -> dict:
        return {"index": self.index, "snapshot": self.snapshot.to_dict()}


@dataclass
class ReplaySession:
    frames: List[ReplayFrame] = field(default_factory=list)

    def add(self, snapshot: PipelineSnapshot) -> None:
        self.frames.append(ReplayFrame(index=len(self.frames), snapshot=snapshot))

    def __len__(self) -> int:
        return len(self.frames)

    def get(self, index: int) -> Optional[ReplayFrame]:
        if 0 <= index < len(self.frames):
            return self.frames[index]
        return None


def load_replay_session(paths: List[str]) -> ReplaySession:
    session = ReplaySession()
    for p in sorted(paths):
        snap = load_snapshot(p)
        session.add(snap)
    return session


def replay_to_collector(session: ReplaySession, frame_index: int, collector: MetricCollector) -> None:
    """Inject metrics from a replay frame into a collector for re-analysis."""
    frame = session.get(frame_index)
    if frame is None:
        raise IndexError(f"Frame {frame_index} not found in session ({len(session)} frames).")
    for entry in frame.snapshot.entries:
        collector.record(entry.metric)
