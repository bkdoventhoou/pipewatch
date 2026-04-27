"""CLI entry point for recurrence detection."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.recurrence import detect_all_recurrences


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch-recurrence",
        description="Detect repeatedly breaching metrics.",
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.3,
        help="Minimum breach rate to flag as recurring (default: 0.3)",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=3,
        help="Minimum observations required (default: 3)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    parser.add_argument(
        "--recurring-only",
        action="store_true",
        help="Only show metrics flagged as recurring",
    )
    return parser.parse_args(argv)


def _format_text(results) -> str:
    if not results:
        return "No recurrence data available."
    lines = []
    for r in results:
        flag = "[RECURRING]" if r.is_recurring else "[stable]  "
        lines.append(
            f"{flag} {r.pipeline}/{r.metric_name} "
            f"breaches={r.breach_count}/{r.total_count} "
            f"rate={r.recurrence_rate:.1%}"
        )
    return "\n".join(lines)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)
    history = collector.get_history()

    results = detect_all_recurrences(
        history,
        threshold=args.threshold,
        min_count=args.min_count,
    )

    if args.recurring_only:
        results = [r for r in results if r.is_recurring]

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(_format_text(results))


if __name__ == "__main__":
    main()
