"""CLI entry-point for cadence analysis."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cadence import analyze_all_cadences
from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch-cadence",
        description="Detect irregular or missed metric emission cadence.",
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="Expected emission interval in seconds (overrides config)",
    )
    parser.add_argument(
        "--irregularity-ratio",
        type=float,
        default=0.25,
        help="Fraction deviation from expected interval to flag as irregular (default 0.25)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    return parser.parse_args(argv)


def _format_text(results) -> str:
    if not results:
        return "No cadence data available."
    lines = []
    for r in results:
        flag = "IRREGULAR" if r.irregular else "ok"
        lines.append(
            f"[{flag}] {r.pipeline}/{r.metric_name}  "
            f"mean={r.mean_interval:.1f}s  max_gap={r.max_gap:.1f}s  "
            f"missed={r.missed_count}  expected={r.expected_interval:.1f}s"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)

    expected_interval = args.interval
    if expected_interval is None:
        expected_interval = cfg.get("cadence", {}).get("expected_interval", 60.0)

    collector = build_collector_from_config(cfg)
    history = collector.get_history()

    results = analyze_all_cadences(
        history,
        expected_interval=expected_interval,
        irregularity_ratio=args.irregularity_ratio,
    )

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":
    main()
