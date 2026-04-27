"""CLI entry-point for stickiness analysis."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.stickiness import analyze_all_stickiness


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch-stickiness",
        description="Detect pipelines stuck in a non-OK status.",
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=3,
        help="Consecutive non-OK readings required to be considered stuck (default: 3).",
    )
    parser.add_argument(
        "--stuck-only",
        action="store_true",
        help="Only show pipelines that have exceeded the streak threshold.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    return parser.parse_args(argv)


def _format_text(results) -> str:
    if not results:
        return "No stickiness detected."
    lines = []
    for r in results:
        stuck_label = " [STUCK]" if r.is_stuck else ""
        lines.append(
            f"{r.pipeline}/{r.metric_name}: status={r.status.value}"
            f" streak={r.streak} duration={r.duration_seconds:.1f}s{stuck_label}"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    collector = build_collector_from_config(args.config)
    history = collector.get_history()

    results = analyze_all_stickiness(history, streak_threshold=args.threshold)

    if args.stuck_only:
        results = [r for r in results if r.is_stuck]

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(_format_text(results))

    if any(r.is_stuck for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
