from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class AnomalyResult:
    pipeline: str
    metric_name: str
    value: float
    mean: float
    std: float
    z_score: float
    is_anomaly: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "value": self.value,
            "mean": round(self.mean, 4),
            "std": round(self.std, 4),
            "z_score": round(self.z_score, 4),
            "is_anomaly": self.is_anomaly,
        }


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _std(values: List[float], mean: float) -> float:
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return variance ** 0.5


def detect_anomaly(
    metrics: List[PipelineMetric], threshold: float = 2.5
) -> Optional[AnomalyResult]:
    if len(metrics) < 3:
        return None
    values = [m.value for m in metrics]
    mean = _mean(values)
    std = _std(values, mean)
    if std == 0:
        return None
    latest = metrics[-1]
    z = abs(latest.value - mean) / std
    return AnomalyResult(
        pipeline=latest.pipeline,
        metric_name=latest.name,
        value=latest.value,
        mean=mean,
        std=std,
        z_score=z,
        is_anomaly=z >= threshold,
    )


def detect_all_anomalies(
    history: dict, threshold: float = 2.5
) -> List[AnomalyResult]:
    results = []
    for key, metrics in history.items():
        result = detect_anomaly(metrics, threshold)
        if result is not None:
            results.append(result)
    return results
