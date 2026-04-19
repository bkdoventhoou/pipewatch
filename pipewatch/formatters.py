"""Output formatters for pipeline reports."""
import json
from typing import Callable

from pipewatch.reporter import PipelineReport
from pipewatch.metrics import MetricStatus

_STATUS_COLORS = {
    MetricStatus.OK: "\033[92m",
    MetricStatus.WARNING: "\033[93m",
    MetricStatus.CRITICAL: "\033[91m",
}
_RESET = "\033[0m"


def format_text(report: PipelineReport, color: bool = True) -> str:
    lines = [f"Pipeline Health Report — {report.generated_at.isoformat()}", ""]
    summary = report.summary()
    lines.append(
        f"  OK: {summary.get('ok', 0)}  "
        f"WARNING: {summary.get('warning', 0)}  "
        f"CRITICAL: {summary.get('critical', 0)}"
    )
    lines.append("")
    for entry in report.entries:
        prefix = ""
        suffix = ""
        if color:
            prefix = _STATUS_COLORS.get(entry.status, "")
            suffix = _RESET
        status_label = f"{prefix}[{entry.status.value.upper()}]{suffix}"
        lines.append(
            f"  {status_label} {entry.metric_name}: {entry.value:.4f} "
            f"(warn={entry.threshold_warning}, crit={entry.threshold_critical})"
        )
    return "\n".join(lines)


def format_json(report: PipelineReport) -> str:
    return json.dumps(report.to_dict(), indent=2)


FORMATTERS: dict[str, Callable[[PipelineReport], str]] = {
    "text": lambda r: format_text(r, color=True),
    "plain": lambda r: format_text(r, color=False),
    "json": format_json,
}


def get_formatter(fmt: str) -> Callable[[PipelineReport], str]:
    if fmt not in FORMATTERS:
        raise ValueError(f"Unknown format '{fmt}'. Choose from: {list(FORMATTERS)}.")
    return FORMATTERS[fmt]
