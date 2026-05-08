"""CLI entry point for pipeline coverage analysis."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.coverage import analyze_all_coverages


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report metric health coverage per pipeline."
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=1.0,
        help="Minimum OK ratio (0.0–1.0) to consider a pipeline healthy.",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Limit output to a specific pipeline.",
    )
    return parser.parse_args(argv)


def _format_text(results: dict) -> str:
    if not results:
        return "No coverage data available."
    lines = []
    for pipeline, r in results.items():
        status = "HEALTHY" if r.healthy else "DEGRADED"
        lines.append(
            f"{pipeline}: {status}  "
            f"coverage={r.coverage_ratio:.1%}  "
            f"ok={r.ok_count}  warn={r.warning_count}  crit={r.critical_count}  "
            f"total={r.total}"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    collector = build_collector_from_config(args.config)

    all_metrics = [
        m
        for key in collector.get_history()
        for m in collector.get_history()[key]
    ]

    results = analyze_all_coverages(all_metrics, healthy_threshold=args.threshold)

    if args.pipeline:
        results = {k: v for k, v in results.items() if k == args.pipeline}

    if args.format == "json":
        print(json.dumps({k: v.to_dict() for k, v in results.items()}, indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":  # pragma: no cover
    main()
