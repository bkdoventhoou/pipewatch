"""CLI entry point for momentum analysis."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.momentum import analyze_all_momentum


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="pipewatch-momentum",
        description="Detect metric acceleration / deceleration.",
    )
    p.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    p.add_argument(
        "--accel-threshold",
        type=float,
        default=0.01,
        help="Minimum |second_derivative| to flag as accelerating (default: 0.01)",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    return p.parse_args(argv)


def _format_text(results) -> str:
    if not results:
        return "No momentum results (need ≥3 samples per metric)."
    lines = []
    for r in results:
        flag = "⚡ ACCELERATING" if r.accelerating else "  stable"
        lines.append(
            f"{r.pipeline}/{r.metric_name}: "
            f"vel={r.first_derivative:+.4f}  accel={r.second_derivative:+.4f}  "
            f"{flag}  (n={r.samples})"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)
    collector = build_collector_from_config(cfg)
    history = collector.get_history()

    results = analyze_all_momentum(
        history, accel_threshold=args.accel_threshold
    )

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":
    main()
