"""CLI entry point for capacity evaluation."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.capacity import evaluate_capacity
from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate pipeline metric capacity against configured limits."
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--pipeline", default=None, help="Filter to a specific pipeline."
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=1.0,
        help="Utilization fraction that triggers a breach (default: 1.0).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
    limits: dict = config.get("capacity", {}).get("limits", {})

    if not limits:
        print("No capacity limits defined in config.", file=sys.stderr)
        sys.exit(1)

    collector = build_collector_from_config(config)
    all_metrics = []
    for key, history in collector.get_history().items():
        if args.pipeline and not key.startswith(args.pipeline + ":"):
            continue
        all_metrics.extend(history)

    report = evaluate_capacity(all_metrics, limits, breach_threshold=args.threshold)

    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2))
    else:
        if not report.entries:
            print("No capacity data available.")
            return
        for e in report.entries:
            breach_tag = " [BREACHED]" if e.breached else ""
            pct = e.utilization * 100
            print(
                f"{e.pipeline}/{e.metric_name}: "
                f"{e.current:.2f} / {e.limit:.2f} ({pct:.1f}%){breach_tag}"
            )
        breached = len(report.breached_entries())
        print(f"\n{breached}/{len(report.entries)} limits breached.")


if __name__ == "__main__":
    main()
